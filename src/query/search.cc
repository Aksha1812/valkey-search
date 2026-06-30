/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "src/query/search.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "src/attribute_data_type.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/metrics.h"
#include "src/query/planner.h"
#include "src/query/predicate.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

class InlineVectorFilter : public hnswlib::BaseFilterFunctor {
 public:
  InlineVectorFilter(query::Predicate *filter_predicate,
                     indexes::VectorBase *vector_index)
      : filter_predicate_(filter_predicate), vector_index_(vector_index) {}
  ~InlineVectorFilter() override = default;

  bool operator()(hnswlib::labeltype id) override {
    auto key = vector_index_->GetKeyDuringSearch(id);
    if (!key.ok()) {
      return false;
    }
    indexes::InlineVectorEvaluator evaluator;
    return evaluator.Evaluate(*filter_predicate_, *key);
  }

 private:
  query::Predicate *filter_predicate_;
  indexes::VectorBase *vector_index_;
};
absl::StatusOr<std::deque<indexes::Neighbor>> PerformVectorSearch(
    indexes::VectorBase *vector_index,
    const VectorSearchParameters &parameters) {
  std::unique_ptr<InlineVectorFilter> inline_filter;
  if (parameters.filter_parse_results.root_predicate != nullptr) {
    inline_filter = std::make_unique<InlineVectorFilter>(
        parameters.filter_parse_results.root_predicate.get(), vector_index);
    VMSDK_LOG(DEBUG, nullptr) << "Performing vector search with inline filter";
  }
  if (vector_index->GetIndexerType() == indexes::IndexerType::kHNSW) {
    auto vector_hnsw = dynamic_cast<indexes::VectorHNSW<float> *>(vector_index);

    auto latency_sample = SAMPLE_EVERY_N(100);
    auto res = vector_hnsw->Search(parameters.query, parameters.k,
                                   std::move(inline_filter), parameters.ef);
    Metrics::GetStats().hnsw_vector_index_search_latency.SubmitSample(
        std::move(latency_sample));
    return res;
  }
  if (vector_index->GetIndexerType() == indexes::IndexerType::kFlat) {
    auto vector_flat = dynamic_cast<indexes::VectorFlat<float> *>(vector_index);
    auto latency_sample = SAMPLE_EVERY_N(100);
    auto res = vector_flat->Search(parameters.query, parameters.k,
                                   std::move(inline_filter));
    Metrics::GetStats().flat_vector_index_search_latency.SubmitSample(
        std::move(latency_sample));
    return res;
  }
  CHECK(false) << "Unsupported indexer type: "
               << (int)vector_index->GetIndexerType();
}

void AppendQueue(
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &dest,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &src) {
  while (!src.empty()) {
    dest.push(std::move(src.front()));
    src.pop();
  }
}

inline PredicateType EvaluateAsComposedPredicate(
    const Predicate *composed_predicate, bool negate) {
  auto predicate_type = composed_predicate->GetType();

  if (!negate) {
    return predicate_type;
  }
  if (predicate_type == PredicateType::kComposedAnd) {
    return PredicateType::kComposedOr;
  }
  return PredicateType::kComposedAnd;
}

size_t EvaluateFilterAsPrimary(
    const Predicate *predicate,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &entries_fetchers,
    bool negate) {
  if (predicate->GetType() == PredicateType::kComposedAnd ||
      predicate->GetType() == PredicateType::kComposedOr) {
    auto composed_predicate =
        dynamic_cast<const ComposedPredicate *>(predicate);
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>
        lhs_entries_fetchers;
    auto lhs_predicate = composed_predicate->GetLhsPredicate();
    auto lhs =
        EvaluateFilterAsPrimary(lhs_predicate, lhs_entries_fetchers, negate);
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>
        rhs_entries_fetchers;
    auto rhs_predicate = composed_predicate->GetRhsPredicate();
    auto rhs =
        EvaluateFilterAsPrimary(rhs_predicate, rhs_entries_fetchers, negate);
    auto predicate_type =
        EvaluateAsComposedPredicate(composed_predicate, negate);
    if (predicate_type == PredicateType::kComposedAnd) {
      if (lhs < rhs) {
        AppendQueue(entries_fetchers, lhs_entries_fetchers);
        return lhs;
      }
      AppendQueue(entries_fetchers, rhs_entries_fetchers);
      return rhs;
    }
    AppendQueue(entries_fetchers, lhs_entries_fetchers);
    AppendQueue(entries_fetchers, rhs_entries_fetchers);
    return lhs + rhs;
  }
  if (predicate->GetType() == PredicateType::kTag) {
    auto tag_predicate = dynamic_cast<const TagPredicate *>(predicate);
    auto fetcher = tag_predicate->GetIndex()->Search(*tag_predicate, negate);
    size_t size = fetcher->Size();
    entries_fetchers.push(std::move(fetcher));
    return size;
  }
  if (predicate->GetType() == PredicateType::kNumeric) {
    auto numeric_predicate = dynamic_cast<const NumericPredicate *>(predicate);
    auto fetcher =
        numeric_predicate->GetIndex()->Search(*numeric_predicate, negate);
    size_t size = fetcher->Size();
    entries_fetchers.push(std::move(fetcher));
    return size;
  }
  if (predicate->GetType() == PredicateType::kNegate) {
    auto negate_predicate = dynamic_cast<const NegatePredicate *>(predicate);
    return EvaluateFilterAsPrimary(negate_predicate->GetPredicate(),
                                   entries_fetchers, !negate);
  }
  CHECK(false);
}

struct PrefilteredKey {
  std::string key;
  float distance;
};

std::priority_queue<std::pair<float, hnswlib::labeltype>>
CalcBestMatchingPrefiltereddKeys(
    const VectorSearchParameters &parameters,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &entries_fetchers,
    indexes::VectorBase *vector_index) {
  std::priority_queue<std::pair<float, hnswlib::labeltype>> results;
  absl::flat_hash_set<hnswlib::labeltype> top_keys;
  auto predicate = parameters.filter_parse_results.root_predicate.get();
  indexes::InlineVectorEvaluator evaluator;
  while (!entries_fetchers.empty()) {
    auto fetcher = std::move(entries_fetchers.front());
    entries_fetchers.pop();
    auto iterator = fetcher->Begin();
    while (!iterator->Done()) {
      const auto &key = *iterator;
      // TODO: yairg - add bloom filter to ensure distinct keys are processed
      // just once.
      if (evaluator.Evaluate(*predicate, *key)) {
        vector_index->AddPrefilteredKey(parameters.query, parameters.k, *key,
                                        results, top_keys);
      }
      iterator->Next();
    }
  }
  return results;
}

std::string StringFormatVector(std::vector<char> vector) {
  if (vector.size() % sizeof(float) != 0) {
    return {vector.data(), vector.size()};
  }

  std::vector<std::string> float_strings;
  for (size_t i = 0; i < vector.size(); i += sizeof(float)) {
    float value;
    std::memcpy(&value, vector.data() + i, sizeof(float));
    float_strings.push_back(absl::StrCat(value));
  }

  return absl::StrCat("[", absl::StrJoin(float_strings, ","), "]");
}

absl::StatusOr<std::deque<indexes::Neighbor>> MaybeAddIndexedContent(
    absl::StatusOr<std::deque<indexes::Neighbor>> results,
    const VectorSearchParameters &parameters) {
  if (!results.ok()) {
    return results;
  }
  if (parameters.no_content || parameters.return_attributes.empty()) {
    return results;
  }
  struct AttributeInfo {
    const ReturnAttribute *attribute;
    indexes::IndexBase *index;
  };
  std::vector<AttributeInfo> attributes;
  for (auto &attribute : parameters.return_attributes) {
    if (!attribute.attribute_alias.get()) {
      // Any attribute that is not indexed will result in all attributes being
      // fetched from the main thread for consistency.
      return results;
    }
    auto index = parameters.index_schema->GetIndex(
        vmsdk::ToStringView(attribute.attribute_alias.get()));
    if (!index.ok()) {
      return results;
    }
    attributes.push_back(AttributeInfo{&attribute, index.value().get()});
  }
  for (auto &neighbor : *results) {
    if (neighbor.attribute_contents.has_value()) {
      continue;
    }
    neighbor.attribute_contents = RecordsMap();
    bool any_value_missing = false;
    for (auto &attribute_info : attributes) {
      vmsdk::UniqueRedisString attribute_value = nullptr;
      switch (attribute_info.index->GetIndexerType()) {
        case indexes::IndexerType::kTag: {
          auto tag_index = dynamic_cast<indexes::Tag *>(attribute_info.index);
          auto tag_value_ptr = tag_index->GetRawValue(neighbor.external_id);
          if (tag_value_ptr != nullptr) {
            attribute_value = vmsdk::MakeUniqueRedisString(*tag_value_ptr);
          }
          break;
        }
        case indexes::IndexerType::kNumeric: {
          auto numeric_index =
              dynamic_cast<indexes::Numeric *>(attribute_info.index);
          auto numeric = numeric_index->GetValue(neighbor.external_id);
          if (numeric != nullptr) {
            attribute_value =
                vmsdk::MakeUniqueRedisString(absl::StrCat(*numeric));
          }
          break;
        }
        case indexes::IndexerType::kVector:
        case indexes::IndexerType::kHNSW:
        case indexes::IndexerType::kFlat: {
          auto vector_index =
              dynamic_cast<indexes::VectorBase *>(attribute_info.index);
          auto vector = vector_index->GetValue(neighbor.external_id);
          if (vector.ok()) {
            if (parameters.index_schema->GetAttributeDataType().ToProto() ==
                data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON) {
              attribute_value = vmsdk::MakeUniqueRedisString(
                  StringFormatVector(vector.value()));
            } else {
              attribute_value =
                  vmsdk::UniqueRedisString(RedisModule_CreateString(
                      nullptr, vector->data(), vector->size()));
            }
          } else {
            VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
                << "Failed to get vector value during fetching through index "
                   "contents: "
                << vector.status();
          }
          break;
        }
        default:
          CHECK(false) << "Unsupported indexer type: "
                       << (int)attribute_info.index->GetIndexerType();
      }

      if (attribute_value != nullptr) {
        auto identifier = vmsdk::MakeUniqueRedisString(
            vmsdk::ToStringView(attribute_info.attribute->identifier.get()));
        auto identifier_view = vmsdk::ToStringView(identifier.get());
        neighbor.attribute_contents->emplace(
            identifier_view,
            RecordsMapValue(std::move(identifier), std::move(attribute_value)));
      } else {
        // Mark this neighbor as needing content retrieval via the main thread
        // (e.g. the attribute value may exist but not be indexed due to type
        // mismatch).
        any_value_missing = true;
        break;
      }
    }
    if (any_value_missing) {
      neighbor.attribute_contents = std::nullopt;
    }
  }
  return results;
}

absl::StatusOr<std::deque<indexes::Neighbor>> Search(
    const VectorSearchParameters &parameters, bool is_local_search) {
  VMSDK_ASSIGN_OR_RETURN(auto index, parameters.index_schema->GetIndex(
                                         parameters.attribute_alias));
  if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
      index->GetIndexerType() != indexes::IndexerType::kFlat) {
    return absl::InvalidArgumentError(
        absl::StrCat(parameters.attribute_alias, " is not a Vector index "));
  }
  auto vector_index = dynamic_cast<indexes::VectorBase *>(index.get());
  auto &time_sliced_mutex = parameters.index_schema->GetTimeSlicedMutex();
  vmsdk::ReaderMutexLock lock(&time_sliced_mutex);
  if (!parameters.filter_parse_results.root_predicate) {
    return MaybeAddIndexedContent(PerformVectorSearch(vector_index, parameters),
                                  parameters);
  }
  std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> entries_fetchers;
  size_t qualified_entries = EvaluateFilterAsPrimary(
      parameters.filter_parse_results.root_predicate.get(), entries_fetchers,
      false);

  // Query planner makes the decision for pre-filtering vs inline-filtering.
  if (UsePreFiltering(qualified_entries, vector_index)) {
    VMSDK_LOG(DEBUG, nullptr)
        << "Using pre-filter query execution, qualified entries="
        << qualified_entries;
    // Do an exact nearest neighbour search on the reduced search space.
    auto results = CalcBestMatchingPrefiltereddKeys(
        parameters, entries_fetchers, vector_index);

    return vector_index->CreateReply(results);
  }
  if (is_local_search) {
    ++Metrics::GetStats().query_inline_filtering_requests_cnt;
  }
  lock.SetMayProlong();
  return MaybeAddIndexedContent(PerformVectorSearch(vector_index, parameters),
                                parameters);
}

absl::Status SearchAsync(std::unique_ptr<VectorSearchParameters> parameters,
                         vmsdk::ThreadPool *thread_pool,
                         SearchResponseCallback callback,
                         bool is_local_search) {
  thread_pool->Schedule(
      [parameters = std::move(parameters), callback = std::move(callback),
       is_local_search]() mutable {
        auto res = Search(*parameters, is_local_search);
        callback(res, std::move(parameters));
      },
      vmsdk::ThreadPool::Priority::kHigh);
  return absl::OkStatus();
}

<<<<<<< HEAD
=======
bool QueryHasTextPredicate(const SearchParameters &parameters) {
  return parameters.filter_parse_results.query_operations &
         QueryOperations::kContainsText;
}

// Increment query operation metrics based on query operations flags.
// File-internal helper function.
void IncrementQueryOperationMetrics(QueryOperations query_operations) {
  // High-level query type metrics
  if (query_operations & QueryOperations::kContainsText) {
    ++Metrics::GetStats().query_text_requests_cnt;
  }
  if (query_operations & QueryOperations::kContainsNumeric) {
    query_numeric_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTag) {
    query_tag_count.Increment();
  }
  // Text operation type metrics
  if (query_operations & QueryOperations::kContainsTextTerm) {
    query_text_term_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextPrefix) {
    query_text_prefix_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextSuffix) {
    query_text_suffix_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextFuzzy) {
    query_text_fuzzy_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsProximity) {
    query_text_proximity_count.Increment();
  }
}

absl::StatusOr<absl::string_view> SubstituteParam(
    query::SearchParameters &parameters, absl::string_view source) {
  if (source.empty() || source[0] != '$') {
    return source;
  } else {
    source.remove_prefix(1);
    auto itr = parameters.parse_vars.params.find(source);
    if (itr == parameters.parse_vars.params.end()) {
      return absl::NotFoundError(
          absl::StrCat("Parameter ", source, " not found."));
    } else {
      itr->second.first++;
      return itr->second.second;
    }
  }
}

absl::Status ParseKnnInner(query::SearchParameters &parameters,
                           std::string_view filter) {
  absl::InlinedVector<absl::string_view, 8> params =
      absl::StrSplit(filter, ' ', absl::SkipEmpty());
  if (params.empty()) {
    return absl::InvalidArgumentError("Missing parameters");
  }
  // TODO - need some investment to consolidate this with the common parsing
  // functionality
  if (!absl::EqualsIgnoreCase(params[0], "KNN")) {
    return absl::InvalidArgumentError(
        absl::StrCat("`", params[0], "`. Expecting `KNN`"));
  }
  if (params.size() == 1) {
    return absl::InvalidArgumentError("KNN argument is missing");
  }
  parameters.parse_vars.k_string = params[1];
  if (params.size() == 2) {
    return absl::InvalidArgumentError("Vector field argument is missing");
  }
  if (params[2].data()[0] != '@' || params[2].size() == 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected argument `", params[2],
                     "`. Expecting a vector field name, starting with '@'"));
  }
  parameters.attribute_alias =
      absl::string_view(params[2].data() + 1, params[2].size() - 1);
  if (params.size() == 3) {
    return absl::InvalidArgumentError("Blob attribute argument is missing");
  }
  parameters.parse_vars.query_vector_string = params[3];

  size_t i = 4;
  while (i < params.size()) {
    if (absl::EqualsIgnoreCase(params[i], "EF_RUNTIME")) {
      i++;
      if (i == params.size()) {
        return absl::InvalidArgumentError("EF_RUNTIME argument is missing");
      }
      parameters.parse_vars.ef_string = params[i++];
    } else if (absl::EqualsIgnoreCase(params[i], kAsParam)) {
      i++;
      if (i == params.size()) {
        return absl::InvalidArgumentError("AS argument is missing");
      }
      parameters.parse_vars.score_as_string = params[i++];
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Unexpected argument `", params[i], "`"));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<size_t> FindOpenSquareBracket(absl::string_view input) {
  for (size_t position = 0; position < input.size(); ++position) {
    if (input[position] == '[') {
      return position;
    }
    if (!std::isspace(input[position])) {
      return absl::InvalidArgumentError(
          absl::StrCat("Expecting '[' got '", input.substr(position, 1), "'"));
    }
  }
  return absl::InvalidArgumentError("Missing opening bracket");
}

absl::StatusOr<size_t> FindCloseSquareBracket(absl::string_view input) {
  for (auto position = input.size(); position > 0; --position) {
    if (input[position - 1] == ']') {
      return position - 1;
    }
    if (!std::isspace(input[position - 1])) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Expecting ']' got '", input.substr(position - 1, 1), "'"));
    }
  }
  if (input[0] == ']') {
    return 0;
  }
  return absl::InvalidArgumentError("Missing closing bracket");
}

absl::StatusOr<FilterParseResults> ParsePreFilter(
    const IndexSchema &index_schema, absl::string_view pre_filter,
    const query::SearchParameters &search_params) {
  TextParsingOptions options{.verbatim = search_params.verbatim,
                             .inorder = search_params.inorder,
                             .slop = search_params.slop};
  FilterParser parser(index_schema, pre_filter, options);
  return parser.Parse();
}

absl::Status ParseKNN(query::SearchParameters &parameters,
                      absl::string_view filter_str) {
  if (filter_str.empty()) {
    return absl::InvalidArgumentError("Vector query clause is missing");
  }
  VMSDK_ASSIGN_OR_RETURN(auto close_position,
                         FindCloseSquareBracket(filter_str));
  size_t position = 0;
  VMSDK_ASSIGN_OR_RETURN(
      auto open_position,
      FindOpenSquareBracket(absl::string_view(filter_str.data() + position,
                                              close_position - position)));
  position += open_position;
  return ParseKnnInner(parameters,
                       absl::string_view(filter_str.data() + position + 1,
                                         close_position - position - 1));
}

//
// We don't have values for the $ substitution yet. so we break the parsing into
// two pieces
//
absl::Status query::SearchParameters::PreParseQueryString() {
  // Validate the query string's length.
  if (parse_vars.query_string.length() > options::GetQueryStringBytes()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Query string is too long, max length is ",
                     options::GetQueryStringBytes(), " bytes."));
  }
  auto filter_expression = absl::string_view(parse_vars.query_string);
  VMSDK_LOG(DEBUG, nullptr)
      << "Query: '" << vmsdk::config::RedactIfNeeded(parse_vars.query_string)
      << "'";
  auto pos = filter_expression.find(kVectorFilterDelimiter);
  absl::string_view pre_filter;
  absl::string_view vector_filter;
  // If the delimiter is not found (ie - non vector query), treat the whole
  // string as pre-filter.
  if (pos == absl::string_view::npos) {
    pre_filter = absl::StripAsciiWhitespace(filter_expression);
  } else {
    pre_filter = absl::StripAsciiWhitespace(filter_expression.substr(0, pos));
    vector_filter = absl::StripAsciiWhitespace(
        filter_expression.substr(pos + kVectorFilterDelimiter.size()));
  }
  // If INORDER OR SLOP, but the index schema does not support offsets, we
  // reject the query.
  if ((inorder || slop.has_value()) && !index_schema->HasTextOffsets()) {
    return absl::InvalidArgumentError("Index does not support offsets");
  }
  VMSDK_ASSIGN_OR_RETURN(
      filter_parse_results, ParsePreFilter(*index_schema, pre_filter, *this),
      _.SetPrepend() << "Invalid filter expression: `" << pre_filter << "`. ");
  if (!filter_parse_results.root_predicate && vector_filter.empty() &&
      !filter_parse_results.is_match_all) {
    // Return an error if no valid pre-filter and no vector filter is provided.
    return absl::InvalidArgumentError("Invalid query string syntax");
  }
  // Optionally parse the vector filter if it exists.
  if (!vector_filter.empty()) {
    if (filter_parse_results.root_predicate) {
      ++Metrics::GetStats().query_hybrid_requests_cnt;
    } else {
      // Pure vector query
      ++Metrics::GetStats().query_vector_requests_cnt;
    }
    VMSDK_RETURN_IF_ERROR(ParseKNN(*this, vector_filter)).SetPrepend()
        << "Error parsing vector similarity parameters: `" << vector_filter
        << "`. ";
    // Validate the index exists and is a vector index.
    VMSDK_ASSIGN_OR_RETURN(auto index, index_schema->GetIndex(attribute_alias));
    if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
        index->GetIndexerType() != indexes::IndexerType::kFlat) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Index field `", attribute_alias, "` is not a Vector index "));
    }
    if (parse_vars.score_as_string.empty()) {
      VMSDK_ASSIGN_OR_RETURN(
          score_as, index_schema->DefaultReplyScoreAs(attribute_alias));
    } else {
      score_as = vmsdk::MakeUniqueValkeyString(parse_vars.score_as_string);
    }
  }

  // Pure non-vector query (no vector filter)
  if (vector_filter.empty() && filter_parse_results.root_predicate) {
    ++Metrics::GetStats().query_nonvector_requests_cnt;
  }
  // Increment operation-type metrics
  IncrementQueryOperationMetrics(filter_parse_results.query_operations);
  return absl::OkStatus();
}

absl::Status PostParseVectorParameters(query::SearchParameters &parameters) {
  VMSDK_ASSIGN_OR_RETURN(
      auto k_string,
      SubstituteParam(parameters, parameters.parse_vars.k_string));
  VMSDK_ASSIGN_OR_RETURN(parameters.k, vmsdk::To<unsigned>(k_string));

  VMSDK_ASSIGN_OR_RETURN(
      parameters.query,
      SubstituteParam(parameters, parameters.parse_vars.query_vector_string));

  VMSDK_ASSIGN_OR_RETURN(auto index, parameters.index_schema->GetIndex(
                                         parameters.attribute_alias));
  auto *vector_index = dynamic_cast<indexes::VectorBase *>(index.get());
  CHECK(vector_index != nullptr);
  if (parameters.query.size() !=
      static_cast<size_t>(vector_index->GetVectorDataSize())) {
    return absl::InvalidArgumentError(
        absl::StrCat("query vector blob size (", parameters.query.size(),
                     ") does not match index's expected size (",
                     vector_index->GetVectorDataSize(), ")."));
  }

  if (!parameters.parse_vars.ef_string.empty()) {
    VMSDK_ASSIGN_OR_RETURN(
        auto ef_string,
        SubstituteParam(parameters, parameters.parse_vars.ef_string));
    VMSDK_ASSIGN_OR_RETURN(parameters.ef, vmsdk::To<unsigned>(ef_string));
  }

  if (!parameters.parse_vars.score_as_string.empty()) {
    VMSDK_ASSIGN_OR_RETURN(
        parameters.parse_vars.score_as_string,
        SubstituteParam(parameters, parameters.parse_vars.score_as_string));
  }
  return absl::OkStatus();
}

absl::Status query::SearchParameters::PostParseQueryString() {
  if (IsVectorQuery()) {
    VMSDK_RETURN_IF_ERROR(PostParseVectorParameters(*this)).SetPrepend()
        << "Error parsing vector similarity parameters: ";
  }

  return absl::OkStatus();
}

ContentProcessing SearchParameters::GetContentProcessing() const {
  if (no_content) {
    return kNoContent;
  }
  // Currently, ContentAvailable isn't detected. Future use case.
  if (query::QueryHasTextPredicate(*this)) {
    return kContentionCheckRequired;
  }
  return kContentRequired;
}

>>>>>>> ae132be (Validate query vector size against index dimensions at parse time (#1192))
}  // namespace valkey_search::query

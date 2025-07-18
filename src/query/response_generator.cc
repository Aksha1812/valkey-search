/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/response_generator.h"

#include <algorithm>
#include <deque>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/indexes/tag.h"
#include "src/indexes/vector_base.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

class PredicateEvaluator : public query::Evaluator {
 public:
  explicit PredicateEvaluator(const RecordsMap &records) : records_(records) {}
  bool EvaluateTags(const query::TagPredicate &predicate) override {
    auto identifier = predicate.GetRetainedIdentifier();
    auto it = records_.find(vmsdk::ToStringView(identifier.get()));
    if (it == records_.end()) {
      return false;
    }
    auto index = predicate.GetIndex();
    auto tags = indexes::Tag::ParseSearchTags(
        vmsdk::ToStringView(it->second.value.get()), index->GetSeparator());
    if (!tags.ok()) {
      return false;
    }
    return predicate.Evaluate(&tags.value(), index->IsCaseSensitive());
  }

  bool EvaluateNumeric(const query::NumericPredicate &predicate) override {
    auto identifier = predicate.GetRetainedIdentifier();
    auto it = records_.find(vmsdk::ToStringView(identifier.get()));
    if (it == records_.end()) {
      return false;
    }
    auto out_numeric =
        vmsdk::To<double>(vmsdk::ToStringView(it->second.value.get()));
    if (!out_numeric.ok()) {
      return false;
    }
    return predicate.Evaluate(&out_numeric.value());
  }

 private:
  const RecordsMap &records_;
};

bool VerifyFilter(const query::Predicate *predicate,
                  const RecordsMap &records) {
  if (predicate == nullptr) {
    return true;
  }
  PredicateEvaluator evaluator(records);
  return predicate->Evaluate(evaluator);
}

absl::StatusOr<RecordsMap> GetContentNoReturnJson(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    const query::VectorSearchParameters &parameters, absl::string_view key,
    const std::string &vector_identifier) {
  absl::flat_hash_set<absl::string_view> identifiers;
  identifiers.insert(kJsonRootElementQuery);
  for (const auto &filter_identifier :
       parameters.filter_parse_results.filter_identifiers) {
    identifiers.insert(filter_identifier);
  }
  VMSDK_ASSIGN_OR_RETURN(
      auto content, attribute_data_type.FetchAllRecords(ctx, vector_identifier,
                                                        key, identifiers));
  if (parameters.filter_parse_results.filter_identifiers.empty()) {
    return content;
  }
  if (!VerifyFilter(parameters.filter_parse_results.root_predicate.get(),
                    content)) {
    return absl::NotFoundError("Verify filter failed");
  }
  RecordsMap return_content;
  static const vmsdk::UniqueValkeyString kJsonRootElementQueryPtr =
      vmsdk::MakeUniqueValkeyString(kJsonRootElementQuery);
  return_content.emplace(
      kJsonRootElementQuery,
      RecordsMapValue(
          kJsonRootElementQueryPtr.get(),
          std::move(content.find(kJsonRootElementQuery)->second.value)));
  return return_content;
}

absl::StatusOr<RecordsMap> GetContent(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    const query::VectorSearchParameters &parameters, absl::string_view key,
    const std::string &vector_identifier) {
  if (attribute_data_type.ToProto() ==
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON &&
      parameters.return_attributes.empty()) {
    return GetContentNoReturnJson(ctx, attribute_data_type, parameters, key,
                                  vector_identifier);
  }
  absl::flat_hash_set<absl::string_view> identifiers;
  for (const auto &return_attribute : parameters.return_attributes) {
    identifiers.insert(vmsdk::ToStringView(return_attribute.identifier.get()));
  }
  if (!parameters.return_attributes.empty()) {
    for (const auto &filter_identifier :
         parameters.filter_parse_results.filter_identifiers) {
      identifiers.insert(filter_identifier);
    }
  }
  VMSDK_ASSIGN_OR_RETURN(
      auto content, attribute_data_type.FetchAllRecords(ctx, vector_identifier,
                                                        key, identifiers));
  if (parameters.filter_parse_results.filter_identifiers.empty()) {
    return content;
  }
  if (!VerifyFilter(parameters.filter_parse_results.root_predicate.get(),
                    content)) {
    return absl::NotFoundError("Verify filter failed");
  }
  if (parameters.return_attributes.empty()) {
    return content;
  }
  RecordsMap return_content;
  for (auto &return_attribute : parameters.return_attributes) {
    auto itr =
        content.find(vmsdk::ToStringView(return_attribute.identifier.get()));
    if (itr == content.end()) {
      continue;
    }
    return_content.emplace(
        vmsdk::ToStringView(return_attribute.identifier.get()),
        RecordsMapValue(
            return_attribute.identifier.get(),
            vmsdk::RetainUniqueValkeyString(itr->second.value.get())));
  }
  return return_content;
}
// Adds all local content for neighbors to the list of neighbors.
//
// Any neighbors already contained in the attribute content map will be skipped.
// Any data not found locally will be skipped.
void ProcessNeighborsForReply(ValkeyModuleCtx *ctx,
                              const AttributeDataType &attribute_data_type,
                              std::deque<indexes::Neighbor> &neighbors,
                              const query::VectorSearchParameters &parameters,
                              const std::string &identifier) {
  for (auto &neighbor : neighbors) {
    // neighbors which were added from remote nodes already have attribute
    // content
    if (neighbor.attribute_contents.has_value()) {
      continue;
    }
    auto content = GetContent(ctx, attribute_data_type, parameters,
                              *neighbor.external_id, identifier);
    if (!content.ok()) {
      continue;
    }
    neighbor.attribute_contents = std::move(content.value());
  }
  // Remove all entries that don't have content now.
  // TODO: incorporate a retry in case of removal.
  neighbors.erase(
      std::remove_if(neighbors.begin(), neighbors.end(),
                     [](const indexes::Neighbor &neighbor) {
                       return !neighbor.attribute_contents.has_value();
                     }),
      neighbors.end());
}

}  // namespace valkey_search::query

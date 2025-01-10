// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "src/commands/ft_search_parser.h"

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "src/commands/filter_parser.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {
namespace {

constexpr absl::string_view kParamsParam{"PARAMS"};
constexpr absl::string_view kDialectParam{"DIALECT"};
constexpr absl::string_view kLimitParam{"LIMIT"};
constexpr absl::string_view kNoContentParam{"NOCONTENT"};
constexpr absl::string_view kReturnParam{"RETURN"};
constexpr absl::string_view kTimeoutParam{"TIMEOUT"};
constexpr absl::string_view kAsParam{"AS"};
constexpr absl::string_view kLocalOnly{"LOCALONLY"};
constexpr absl::string_view kVectorFilterDelimiter = "=>";

struct ParamVariables {
  absl::string_view attribute_alias;
  absl::string_view query;
  absl::string_view k;
  absl::string_view ef;
  std::string score_as;
};

absl::StatusOr<ParamVariables> ParseVectorFilter(std::string_view filter) {
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
  ParamVariables result;
  result.k = params[1];
  if (params.size() == 2) {
    return absl::InvalidArgumentError("Vector field argument is missing");
  }
  if (params[2].data()[0] != '@' || params[2].size() == 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected argument `", params[2],
                     "`. Expecting a vector field name, starting with '@'"));
  }
  result.attribute_alias =
      absl::string_view(params[2].data() + 1, params[2].size() - 1);
  if (params.size() == 3) {
    return absl::InvalidArgumentError("Blob attribute argument is missing");
  }
  result.query = params[3];
  size_t i = 4;
  if (i == params.size()) {
    return result;
  }
  if (absl::EqualsIgnoreCase(params[i], "EF_RUNTIME")) {
    if (i + 1 == params.size()) {
      return absl::InvalidArgumentError("EF_RUNTIME argument is missing");
    }
    result.ef = params[i + 1];
    i += 2;
    if (i == params.size()) {
      return result;
    }
  }
  if (absl::EqualsIgnoreCase(params[i], kAsParam)) {
    if (i + 1 == params.size()) {
      return absl::InvalidArgumentError("AS argument is missing");
    }
    result.score_as = params[i + 1];
    i += 2;
  }
  if (i < params.size()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected argument `", params[i], "`"));
  }
  return result;
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
    const IndexSchema &index_schema, absl::string_view pre_filter) {
  FilterParser parser(index_schema, pre_filter);
  return parser.Parse();
}

absl::StatusOr<ParamVariables> ParseParamVariables(
    absl::string_view filter_str) {
  VMSDK_ASSIGN_OR_RETURN(auto close_position,
                         FindCloseSquareBracket(filter_str));
  size_t position = 0;
  VMSDK_ASSIGN_OR_RETURN(
      auto open_position,
      FindOpenSquareBracket(absl::string_view(filter_str.data() + position,
                                              close_position - position)));
  position += open_position;
  return ParseVectorFilter(absl::string_view(filter_str.data() + position + 1,
                                             close_position - position - 1));
}

absl::Status Verify(query::VectorSearchParameters &parameters) {
  if (!parameters.ef.has_value() && !parameters.k.has_value()) {
    return absl::InvalidArgumentError("Missing `PARAMS` parameter");
  }
  if (parameters.ef.has_value() && parameters.ef <= 0) {
    return absl::InvalidArgumentError("`EF` value must be positive");
  }
  if (!parameters.k) {
    return absl::InvalidArgumentError("missing KNN parameter");
  }
  if (parameters.k.value() <= 0) {
    return absl::InvalidArgumentError("k must be positive");
  }
  if (parameters.query.empty()) {
    return absl::InvalidArgumentError("missing vector parameter");
  }
  return absl::OkStatus();
}

std::unique_ptr<vmsdk::ParamParser<query::VectorSearchParameters>>
ConstuctLimitParser() {
  return std::make_unique<vmsdk::ParamParser<query::VectorSearchParameters>>(
      [](query::VectorSearchParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        VMSDK_RETURN_IF_ERROR(
            vmsdk::ParseParamValue(itr, parameters.limit.first_index));
        VMSDK_RETURN_IF_ERROR(
            vmsdk::ParseParamValue(itr, parameters.limit.number));
        return absl::OkStatus();
      });
}

absl::Status SetQuery(absl::string_view param_variable,
                      query::VectorSearchParameters &parameters) {
  parameters.query = param_variable;
  return absl::OkStatus();
}

std::unique_ptr<::vmsdk::ParamParser<query::VectorSearchParameters>>
CreateQueryParser() {
  return GENERATE_VALUE_PARSER(query::VectorSearchParameters, query);
}

absl::Status SetEf(absl::string_view param_variable,
                   query::VectorSearchParameters &parameters) {
  VMSDK_ASSIGN_OR_RETURN(parameters.ef, vmsdk::To<uint64_t>(param_variable));
  return absl::OkStatus();
}

std::unique_ptr<::vmsdk::ParamParser<query::VectorSearchParameters>>
CreateEfParser() {
  return GENERATE_VALUE_PARSER(query::VectorSearchParameters, ef);
}

absl::Status SetK(absl::string_view param_variable,
                  query::VectorSearchParameters &parameters) {
  VMSDK_ASSIGN_OR_RETURN(parameters.k, vmsdk::To<uint64_t>(param_variable));
  return absl::OkStatus();
}

std::unique_ptr<::vmsdk::ParamParser<query::VectorSearchParameters>>
CreateKParser() {
  return GENERATE_VALUE_PARSER(query::VectorSearchParameters, k);
}

using FieldSetter = absl::Status (*)(absl::string_view,
                                     query::VectorSearchParameters &);
using ValueParserGenerator =
    std::unique_ptr<::vmsdk::ParamParser<query::VectorSearchParameters>> (*)();

absl::Status HandlePrepareParseParam(
    vmsdk::KeyValueParser<query::VectorSearchParameters> &parser,
    absl::string_view param_variable, query::VectorSearchParameters &parameters,
    FieldSetter field_setter, ValueParserGenerator value_parser_generator) {
  if (param_variable.empty()) {
    return absl::OkStatus();
  }
  if (param_variable.data()[0] == '$') {
    parser.AddParamParser(
        absl::string_view(param_variable.data() + 1, param_variable.size() - 1),
        value_parser_generator());
    return absl::OkStatus();
  }
  VMSDK_RETURN_IF_ERROR(field_setter(param_variable, parameters)).SetPrepend()
      << "Error parsing vector similarity "
         "parameters: ";
  return absl::OkStatus();
}

std::unique_ptr<vmsdk::ParamParser<query::VectorSearchParameters>>
ConstuctParamsParser(const ParamVariables &param_variables) {
  return std::make_unique<vmsdk::ParamParser<query::VectorSearchParameters>>(
      [&param_variables](query::VectorSearchParameters &parameters,
                         vmsdk::ArgsIterator &itr) -> absl::Status {
        vmsdk::KeyValueParser<query::VectorSearchParameters> params_parser;

        VMSDK_RETURN_IF_ERROR(
            HandlePrepareParseParam(params_parser, param_variables.ef,
                                    parameters, &SetEf, CreateEfParser));
        VMSDK_RETURN_IF_ERROR(
            HandlePrepareParseParam(params_parser, param_variables.k,
                                    parameters, &SetK, CreateKParser));
        VMSDK_RETURN_IF_ERROR(
            HandlePrepareParseParam(params_parser, param_variables.query,
                                    parameters, &SetQuery, CreateQueryParser));
        int params{-1};
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, params));
        VMSDK_ASSIGN_OR_RETURN(auto params_itr, itr.SubIterator(params));

        VMSDK_RETURN_IF_ERROR(params_parser.Parse(parameters, params_itr));
        if (params_itr.DistanceEnd() != 0) {
          VMSDK_ASSIGN_OR_RETURN(auto current, params_itr.Get());
          return absl::InvalidArgumentError(absl::StrCat(
              "Unexpected parameter `", vmsdk::ToStringView(current), "`"));
        }
        itr.Next(params);
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<query::VectorSearchParameters>>
ConstuctReturnParser() {
  return std::make_unique<vmsdk::ParamParser<query::VectorSearchParameters>>(
      [](query::VectorSearchParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        uint32_t cnt{0};
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, cnt));
        if (cnt == 0) {
          parameters.no_content = true;
          return absl::OkStatus();
        }
        for (uint32_t i = 0; i < cnt; ++i) {
          vmsdk::UniqueRedisString identifier;
          VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, identifier));
          auto as_property = vmsdk::RetainUniqueRedisString(identifier.get());
          VMSDK_ASSIGN_OR_RETURN(
              auto res, vmsdk::ParseParam(kAsParam, false, itr, as_property));
          if (res) {
            i += 2;
            if (i > cnt) {
              return absl::InvalidArgumentError("Unexpected parameter `AS` ");
            }
          }
          parameters.return_attributes.emplace_back(query::ReturnAttribute{
              std::move(identifier), std::move(as_property)});
        }
        return absl::OkStatus();
      });
}

vmsdk::KeyValueParser<query::VectorSearchParameters> CreateKeyValueParser(
    const ParamVariables &param_variables) {
  vmsdk::KeyValueParser<query::VectorSearchParameters> parser;
  parser.AddParamParser(
      kDialectParam,
      GENERATE_VALUE_PARSER(query::VectorSearchParameters, dialect));
  parser.AddParamParser(
      kLocalOnly,
      GENERATE_FLAG_PARSER(query::VectorSearchParameters, local_only));
  parser.AddParamParser(
      kTimeoutParam,
      GENERATE_VALUE_PARSER(query::VectorSearchParameters, timeout_ms));
  parser.AddParamParser(kLimitParam, ConstuctLimitParser());
  parser.AddParamParser(
      kNoContentParam,
      GENERATE_FLAG_PARSER(query::VectorSearchParameters, no_content));
  parser.AddParamParser(kReturnParam, ConstuctReturnParser());
  parser.AddParamParser(kParamsParam, ConstuctParamsParser(param_variables));
  return parser;
}
}  // namespace
absl::StatusOr<std::unique_ptr<query::VectorSearchParameters>>
ParseVectorSearchParameters(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc, const SchemaManager &schema_manager) {
  vmsdk::ArgsIterator itr{argv, argc};
  auto parameters = std::make_unique<query::VectorSearchParameters>();
  VMSDK_RETURN_IF_ERROR(
      vmsdk::ParseParamValue(itr, parameters->index_schema_name));
  VMSDK_ASSIGN_OR_RETURN(auto itr_arg, itr.Get());
  itr.Next();
  auto filter_expression = vmsdk::ToStringView(itr_arg);
  auto pos = filter_expression.find(kVectorFilterDelimiter);
  if (pos == absl::string_view::npos) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Invalid filter format. Missing `", kVectorFilterDelimiter, "`"));
  }

  VMSDK_ASSIGN_OR_RETURN(
      parameters->index_schema,
      SchemaManager::Instance().GetIndexSchema(RedisModule_GetSelectedDb(ctx),
                                               parameters->index_schema_name));
  auto pre_filter =
      absl::StripAsciiWhitespace(filter_expression.substr(0, pos));
  VMSDK_ASSIGN_OR_RETURN(
      parameters->filter_parse_results,
      ParsePreFilter(*parameters->index_schema, pre_filter),
      _.SetPrepend() << "Invalid filter expression: `" << pre_filter << "`. ");
  if (parameters->filter_parse_results.root_predicate) {
    ++Metrics::GetStats().query_hybrid_requests_cnt;
  }
  auto vector_filter = absl::StripAsciiWhitespace(
      filter_expression.substr(pos + kVectorFilterDelimiter.size()));
  static ParamVariables param_variables;
  VMSDK_ASSIGN_OR_RETURN(param_variables, ParseParamVariables(vector_filter),
                         _.SetPrepend()
                             << "Error parsing vector similarity parameters: `"
                             << vector_filter << "`. ");
  parameters->attribute_alias = param_variables.attribute_alias;

  // Validate the index exists and is a vector index.
  VMSDK_ASSIGN_OR_RETURN(auto index, parameters->index_schema->GetIndex(
                                         parameters->attribute_alias));
  if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
      index->GetIndexerType() != indexes::IndexerType::kFlat) {
    return absl::InvalidArgumentError(absl::StrCat("Index field `",
                                                   parameters->attribute_alias,
                                                   "` is not a Vector index "));
  }

  if (param_variables.score_as.empty()) {
    VMSDK_ASSIGN_OR_RETURN(parameters->score_as,
                           parameters->index_schema->DefaultReplyScoreAs(
                               parameters->attribute_alias));
  } else {
    parameters->score_as =
        vmsdk::MakeUniqueRedisString(param_variables.score_as);
  }

  static vmsdk::KeyValueParser<query::VectorSearchParameters> parser =
      CreateKeyValueParser(param_variables);
  VMSDK_RETURN_IF_ERROR(parser.Parse(*parameters, itr));
  VMSDK_RETURN_IF_ERROR(Verify(*parameters));
  if (parameters->timeout_ms > kMaxTimeoutMs) {
    return absl::InvalidArgumentError(
        absl::StrCat(kTimeoutParam,
                     " must be a positive integer greater than 0 and "
                     "cannot exceed ",
                     kMaxTimeoutMs, "."));
  }
  if (parameters->dialect < 2 || parameters->dialect > 4) {
    return absl::InvalidArgumentError(
        "DIALECT requires a non negative integer >=2 and <= 4");
  }

  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }
  return parameters;
}
}  // namespace valkey_search

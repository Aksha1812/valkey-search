/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_RDF_SPARQL_PARSER_H_
#define VALKEYSEARCH_SRC_RDF_SPARQL_PARSER_H_

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/rdf/sparql/lexer.h"

namespace valkey_search::rdf::sparql {

// A term in a triple pattern: either a concrete term (IRI/literal) or a variable
struct PatternTerm {
  bool is_variable;
  std::string value;  // variable name (without ?) or full term string
};

struct TriplePattern {
  PatternTerm subject;
  PatternTerm predicate;
  PatternTerm object;
};

struct FilterExpr {
  std::string raw;  // raw filter expression string (parsed later during execution)
};

struct ParsedQuery {
  std::vector<std::string> select_vars;  // empty = SELECT *
  std::vector<TriplePattern> patterns;
  std::vector<FilterExpr> filters;
  bool distinct = false;
  int64_t limit = -1;
  int64_t offset = 0;
};

// Parse a SPARQL SELECT query into a ParsedQuery
absl::StatusOr<ParsedQuery> Parse(absl::string_view sparql);

}  // namespace valkey_search::rdf::sparql

#endif  // VALKEYSEARCH_SRC_RDF_SPARQL_PARSER_H_

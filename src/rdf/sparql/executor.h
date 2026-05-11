/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_RDF_SPARQL_EXECUTOR_H_
#define VALKEYSEARCH_SRC_RDF_SPARQL_EXECUTOR_H_

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "src/rdf/dictionary.h"
#include "src/rdf/graph.h"
#include "src/rdf/sparql/parser.h"

namespace valkey_search::rdf::sparql {

// A solution binding: variable name → term ID
using Solution = absl::flat_hash_map<std::string, TermId>;

struct QueryResult {
  std::vector<std::string> variables;  // column names
  std::vector<Solution> solutions;     // rows
};

// Execute a parsed SPARQL query against a graph
absl::StatusOr<QueryResult> Execute(const ParsedQuery& query, RDFGraph* graph);

}  // namespace valkey_search::rdf::sparql

#endif  // VALKEYSEARCH_SRC_RDF_SPARQL_EXECUTOR_H_

/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/sparql/executor.h"

#include <algorithm>
#include <optional>

namespace valkey_search::rdf::sparql {

namespace {

// Resolve a pattern term: if constant, encode it; if variable, return nullopt
std::optional<TermId> ResolveConstant(const PatternTerm& term,
                                      RDFGraph* graph) {
  if (term.is_variable) return std::nullopt;
  auto id = graph->EncodeTerm(term.value);
  if (!id.ok()) return std::nullopt;
  return *id;
}

// Execute a single triple pattern scan, producing solutions
std::vector<Solution> ScanPattern(const TriplePattern& pattern,
                                  RDFGraph* graph) {
  auto s = ResolveConstant(pattern.subject, graph);
  auto p = ResolveConstant(pattern.predicate, graph);
  auto o = ResolveConstant(pattern.object, graph);

  std::vector<Solution> results;
  graph->Scan(s, p, o, [&](const Triple& triple) -> bool {
    Solution sol;
    if (pattern.subject.is_variable)
      sol[pattern.subject.value] = triple.s;
    if (pattern.predicate.is_variable)
      sol[pattern.predicate.value] = triple.p;
    if (pattern.object.is_variable)
      sol[pattern.object.value] = triple.o;
    results.push_back(std::move(sol));
    return true;
  });
  return results;
}

// Find shared variables between two solution sets
std::vector<std::string> SharedVars(const std::vector<Solution>& left,
                                    const std::vector<Solution>& right) {
  if (left.empty() || right.empty()) return {};
  std::vector<std::string> shared;
  for (const auto& [var, _] : left[0]) {
    if (right[0].contains(var)) shared.push_back(var);
  }
  return shared;
}

// Hash join two solution sets on shared variables
std::vector<Solution> HashJoin(const std::vector<Solution>& left,
                               const std::vector<Solution>& right) {
  auto join_vars = SharedVars(left, right);

  // If no shared variables, produce cartesian product
  if (join_vars.empty()) {
    std::vector<Solution> result;
    for (const auto& l : left) {
      for (const auto& r : right) {
        Solution merged = l;
        merged.insert(r.begin(), r.end());
        result.push_back(std::move(merged));
      }
    }
    return result;
  }

  // Build hash table on the smaller side
  const auto& build_side = (left.size() <= right.size()) ? left : right;
  const auto& probe_side = (left.size() <= right.size()) ? right : left;

  // Key: concatenation of join variable values
  absl::flat_hash_map<std::vector<TermId>, std::vector<const Solution*>> hash_table;
  for (const auto& sol : build_side) {
    std::vector<TermId> key;
    for (const auto& var : join_vars) {
      auto it = sol.find(var);
      key.push_back(it != sol.end() ? it->second : 0);
    }
    hash_table[key].push_back(&sol);
  }

  // Probe
  std::vector<Solution> result;
  for (const auto& sol : probe_side) {
    std::vector<TermId> key;
    for (const auto& var : join_vars) {
      auto it = sol.find(var);
      key.push_back(it != sol.end() ? it->second : 0);
    }
    auto it = hash_table.find(key);
    if (it != hash_table.end()) {
      for (const Solution* match : it->second) {
        Solution merged = *match;
        merged.insert(sol.begin(), sol.end());
        result.push_back(std::move(merged));
      }
    }
  }
  return result;
}

// Simple selectivity estimate: count of bound positions
int Selectivity(const TriplePattern& p) {
  int score = 0;
  if (!p.subject.is_variable) ++score;
  if (!p.predicate.is_variable) ++score;
  if (!p.object.is_variable) ++score;
  return score;
}

}  // namespace

absl::StatusOr<QueryResult> Execute(const ParsedQuery& query, RDFGraph* graph) {
  if (query.patterns.empty()) {
    return QueryResult{query.select_vars, {}};
  }

  // Sort patterns by selectivity (most selective first)
  auto patterns = query.patterns;
  std::sort(patterns.begin(), patterns.end(),
            [](const TriplePattern& a, const TriplePattern& b) {
              return Selectivity(a) > Selectivity(b);
            });

  // Execute first pattern
  auto solutions = ScanPattern(patterns[0], graph);

  // Join remaining patterns
  for (size_t i = 1; i < patterns.size(); ++i) {
    auto right = ScanPattern(patterns[i], graph);
    solutions = HashJoin(solutions, right);
    if (solutions.empty()) break;
  }

  // Apply LIMIT/OFFSET
  if (query.offset > 0 && static_cast<size_t>(query.offset) < solutions.size()) {
    solutions.erase(solutions.begin(), solutions.begin() + query.offset);
  } else if (query.offset > 0) {
    solutions.clear();
  }
  if (query.limit >= 0 && static_cast<size_t>(query.limit) < solutions.size()) {
    solutions.resize(query.limit);
  }

  // Project to selected variables
  std::vector<std::string> result_vars = query.select_vars;
  if (result_vars.empty() && !solutions.empty()) {
    // SELECT * — collect all variables
    for (const auto& [var, _] : solutions[0]) {
      result_vars.push_back(var);
    }
    std::sort(result_vars.begin(), result_vars.end());
  }

  // Apply DISTINCT
  if (query.distinct) {
    std::vector<Solution> unique;
    absl::flat_hash_map<std::vector<TermId>, bool> seen;
    for (auto& sol : solutions) {
      std::vector<TermId> key;
      for (const auto& var : result_vars) {
        auto it = sol.find(var);
        key.push_back(it != sol.end() ? it->second : 0);
      }
      if (!seen.contains(key)) {
        seen[key] = true;
        unique.push_back(std::move(sol));
      }
    }
    solutions = std::move(unique);
  }

  return QueryResult{result_vars, std::move(solutions)};
}

}  // namespace valkey_search::rdf::sparql

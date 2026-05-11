/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_RDF_GRAPH_H_
#define VALKEYSEARCH_SRC_RDF_GRAPH_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/rdf/dictionary.h"
#include "src/rdf/triple_store.h"

namespace valkey_search::rdf {

class RDFGraph {
 public:
  explicit RDFGraph(std::string name) : name_(std::move(name)) {}

  // Add a triple (parses terms, encodes, inserts)
  absl::StatusOr<bool> AddTriple(absl::string_view subject,
                                 absl::string_view predicate,
                                 absl::string_view object);

  // Delete a triple
  absl::StatusOr<bool> DeleteTriple(absl::string_view subject,
                                    absl::string_view predicate,
                                    absl::string_view object);

  // Pattern scan (nullopt = unbound variable)
  void Scan(std::optional<TermId> s, std::optional<TermId> p,
            std::optional<TermId> o, ScanCallback callback) const;

  // Encode a term string to its ID (for query constants)
  absl::StatusOr<TermId> EncodeTerm(absl::string_view term_str);

  // Decode an ID to string (for results)
  absl::StatusOr<std::string> DecodeTerm(TermId id) const;

  const std::string& Name() const { return name_; }
  size_t TripleCount() const { return store_.Size(); }
  size_t TermCount() const { return dict_.TermCount(); }
  size_t MemoryUsage() const;

 private:
  std::string name_;
  Dictionary dict_;
  TripleStore store_;
};

// Global graph manager
class GraphManager {
 public:
  static GraphManager& Instance();

  absl::Status CreateGraph(absl::string_view name);
  absl::Status DropGraph(absl::string_view name);
  RDFGraph* GetGraph(absl::string_view name);
  std::vector<std::string> ListGraphs() const;

 private:
  GraphManager() = default;
  absl::flat_hash_map<std::string, std::unique_ptr<RDFGraph>> graphs_;
};

}  // namespace valkey_search::rdf

#endif  // VALKEYSEARCH_SRC_RDF_GRAPH_H_

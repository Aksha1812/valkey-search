/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/graph.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "src/rdf/rdf_term.h"

namespace valkey_search::rdf {

absl::StatusOr<bool> RDFGraph::AddTriple(absl::string_view subject,
                                         absl::string_view predicate,
                                         absl::string_view object) {
  auto s_term = ParseRDFTerm(subject);
  if (!s_term.ok()) return s_term.status();
  auto p_term = ParseRDFTerm(predicate);
  if (!p_term.ok()) return p_term.status();
  auto o_term = ParseRDFTerm(object);
  if (!o_term.ok()) return o_term.status();

  TermId s_id = dict_.Encode(*s_term);
  TermId p_id = dict_.Encode(*p_term);
  TermId o_id = dict_.Encode(*o_term);

  return store_.Insert(s_id, p_id, o_id);
}

absl::StatusOr<bool> RDFGraph::DeleteTriple(absl::string_view subject,
                                            absl::string_view predicate,
                                            absl::string_view object) {
  auto s_term = ParseRDFTerm(subject);
  if (!s_term.ok()) return s_term.status();
  auto p_term = ParseRDFTerm(predicate);
  if (!p_term.ok()) return p_term.status();
  auto o_term = ParseRDFTerm(object);
  if (!o_term.ok()) return o_term.status();

  TermId s_id = dict_.Encode(*s_term);
  TermId p_id = dict_.Encode(*p_term);
  TermId o_id = dict_.Encode(*o_term);

  return store_.Delete(s_id, p_id, o_id);
}

void RDFGraph::Scan(std::optional<TermId> s, std::optional<TermId> p,
                    std::optional<TermId> o, ScanCallback callback) const {
  store_.Scan(s, p, o, std::move(callback));
}

absl::StatusOr<TermId> RDFGraph::EncodeTerm(absl::string_view term_str) {
  auto term = ParseRDFTerm(term_str);
  if (!term.ok()) return term.status();
  return dict_.Encode(*term);
}

absl::StatusOr<std::string> RDFGraph::DecodeTerm(TermId id) const {
  return dict_.Decode(id);
}

size_t RDFGraph::MemoryUsage() const {
  return dict_.MemoryUsage() + store_.MemoryUsage();
}

// GraphManager singleton
GraphManager& GraphManager::Instance() {
  static GraphManager instance;
  return instance;
}

absl::Status GraphManager::CreateGraph(absl::string_view name) {
  std::string key(name);
  if (graphs_.contains(key)) {
    return absl::AlreadyExistsError(
        absl::StrCat("Graph '", name, "' already exists"));
  }
  graphs_[key] = std::make_unique<RDFGraph>(key);
  return absl::OkStatus();
}

absl::Status GraphManager::DropGraph(absl::string_view name) {
  std::string key(name);
  auto it = graphs_.find(key);
  if (it == graphs_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Graph '", name, "' not found"));
  }
  graphs_.erase(it);
  return absl::OkStatus();
}

RDFGraph* GraphManager::GetGraph(absl::string_view name) {
  auto it = graphs_.find(name);
  if (it == graphs_.end()) return nullptr;
  return it->second.get();
}

std::vector<std::string> GraphManager::ListGraphs() const {
  std::vector<std::string> result;
  result.reserve(graphs_.size());
  for (const auto& [name, _] : graphs_) {
    result.push_back(name);
  }
  return result;
}

}  // namespace valkey_search::rdf

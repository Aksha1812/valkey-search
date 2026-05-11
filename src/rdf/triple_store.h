/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_RDF_TRIPLE_STORE_H_
#define VALKEYSEARCH_SRC_RDF_TRIPLE_STORE_H_

#include <algorithm>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

#include "src/rdf/dictionary.h"

namespace valkey_search::rdf {

struct Triple {
  TermId s, p, o;

  bool operator==(const Triple& other) const {
    return s == other.s && p == other.p && o == other.o;
  }
};

// Comparators for different sort orders
struct SPOCompare {
  bool operator()(const Triple& a, const Triple& b) const {
    if (a.s != b.s) return a.s < b.s;
    if (a.p != b.p) return a.p < b.p;
    return a.o < b.o;
  }
};

struct POSCompare {
  bool operator()(const Triple& a, const Triple& b) const {
    if (a.p != b.p) return a.p < b.p;
    if (a.o != b.o) return a.o < b.o;
    return a.s < b.s;
  }
};

struct OSPCompare {
  bool operator()(const Triple& a, const Triple& b) const {
    if (a.o != b.o) return a.o < b.o;
    if (a.s != b.s) return a.s < b.s;
    return a.p < b.p;
  }
};

// Callback for scan results
using ScanCallback = std::function<bool(const Triple&)>;

class TripleStore {
 public:
  static constexpr size_t kDefaultBufferThreshold = 65536;

  explicit TripleStore(size_t buffer_threshold = kDefaultBufferThreshold);

  // Insert a triple. Returns true if new, false if duplicate.
  bool Insert(TermId s, TermId p, TermId o);

  // Delete a triple. Returns true if found and removed.
  bool Delete(TermId s, TermId p, TermId o);

  // Point lookup: does this exact triple exist?
  bool Contains(TermId s, TermId p, TermId o) const;

  // Scan by pattern. Unbound positions use std::nullopt.
  // Callback returns false to stop iteration.
  void Scan(std::optional<TermId> s, std::optional<TermId> p,
            std::optional<TermId> o, ScanCallback callback) const;

  // Force compaction of write buffer into main array
  void Compact();

  size_t Size() const { return primary_.size() + write_buffer_.size(); }
  size_t MemoryUsage() const;

 private:
  // Scan the sorted primary array using the best index
  void ScanPrimary(std::optional<TermId> s, std::optional<TermId> p,
                   std::optional<TermId> o, ScanCallback& callback) const;

  // Scan the write buffer (linear)
  void ScanBuffer(std::optional<TermId> s, std::optional<TermId> p,
                  std::optional<TermId> o, ScanCallback& callback) const;

  // Main sorted arrays (The Ring approach)
  std::vector<Triple> primary_;           // sorted by SPO
  std::vector<uint32_t> pos_perm_;        // indices into primary_, sorted by POS
  std::vector<uint32_t> osp_perm_;        // indices into primary_, sorted by OSP

  // Write buffer
  std::vector<Triple> write_buffer_;
  size_t buffer_threshold_;

  // Rebuild permutation arrays after compaction
  void RebuildPermutations();
};

}  // namespace valkey_search::rdf

#endif  // VALKEYSEARCH_SRC_RDF_TRIPLE_STORE_H_

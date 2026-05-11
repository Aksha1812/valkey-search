/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/triple_store.h"

#include <algorithm>
#include <numeric>

namespace valkey_search::rdf {

TripleStore::TripleStore(size_t buffer_threshold)
    : buffer_threshold_(buffer_threshold) {}

bool TripleStore::Insert(TermId s, TermId p, TermId o) {
  Triple t{s, p, o};

  // Check for duplicate in primary
  if (std::binary_search(primary_.begin(), primary_.end(), t, SPOCompare{})) {
    return false;
  }
  // Check for duplicate in write buffer
  for (const auto& existing : write_buffer_) {
    if (existing == t) return false;
  }

  write_buffer_.push_back(t);

  if (write_buffer_.size() >= buffer_threshold_) {
    Compact();
  }
  return true;
}

bool TripleStore::Delete(TermId s, TermId p, TermId o) {
  Triple t{s, p, o};

  // Check write buffer first
  auto buf_it = std::find(write_buffer_.begin(), write_buffer_.end(), t);
  if (buf_it != write_buffer_.end()) {
    write_buffer_.erase(buf_it);
    return true;
  }

  // Check primary
  auto it = std::lower_bound(primary_.begin(), primary_.end(), t, SPOCompare{});
  if (it != primary_.end() && *it == t) {
    size_t idx = it - primary_.begin();
    primary_.erase(it);
    RebuildPermutations();
    return true;
  }
  return false;
}

bool TripleStore::Contains(TermId s, TermId p, TermId o) const {
  Triple t{s, p, o};
  if (std::binary_search(primary_.begin(), primary_.end(), t, SPOCompare{})) {
    return true;
  }
  for (const auto& existing : write_buffer_) {
    if (existing == t) return true;
  }
  return false;
}

static bool Matches(const Triple& t, std::optional<TermId> s,
                    std::optional<TermId> p, std::optional<TermId> o) {
  if (s && t.s != *s) return false;
  if (p && t.p != *p) return false;
  if (o && t.o != *o) return false;
  return true;
}

void TripleStore::Scan(std::optional<TermId> s, std::optional<TermId> p,
                       std::optional<TermId> o, ScanCallback callback) const {
  ScanPrimary(s, p, o, callback);
  ScanBuffer(s, p, o, callback);
}

void TripleStore::ScanPrimary(std::optional<TermId> s,
                              std::optional<TermId> p,
                              std::optional<TermId> o,
                              ScanCallback& callback) const {
  if (primary_.empty()) return;

  // Choose best index based on bound positions
  if (s.has_value()) {
    // SPO index: binary search on S, then scan
    Triple lower{*s, p.value_or(0), o.value_or(0)};
    Triple upper{*s, p.value_or(UINT64_MAX), o.value_or(UINT64_MAX)};
    auto begin = std::lower_bound(primary_.begin(), primary_.end(), lower,
                                  SPOCompare{});
    auto end = std::upper_bound(primary_.begin(), primary_.end(), upper,
                                SPOCompare{});
    for (auto it = begin; it != end; ++it) {
      if (Matches(*it, s, p, o)) {
        if (!callback(*it)) return;
      }
    }
  } else if (p.has_value()) {
    // POS index: use permutation array
    for (uint32_t idx : pos_perm_) {
      const auto& t = primary_[idx];
      if (t.p < *p) continue;
      if (t.p > *p) break;
      if (Matches(t, s, p, o)) {
        if (!callback(t)) return;
      }
    }
  } else if (o.has_value()) {
    // OSP index: use permutation array
    for (uint32_t idx : osp_perm_) {
      const auto& t = primary_[idx];
      if (t.o < *o) continue;
      if (t.o > *o) break;
      if (Matches(t, s, p, o)) {
        if (!callback(t)) return;
      }
    }
  } else {
    // Full scan
    for (const auto& t : primary_) {
      if (!callback(t)) return;
    }
  }
}

void TripleStore::ScanBuffer(std::optional<TermId> s,
                             std::optional<TermId> p,
                             std::optional<TermId> o,
                             ScanCallback& callback) const {
  for (const auto& t : write_buffer_) {
    if (Matches(t, s, p, o)) {
      if (!callback(t)) return;
    }
  }
}

void TripleStore::Compact() {
  if (write_buffer_.empty()) return;

  // Sort buffer
  std::sort(write_buffer_.begin(), write_buffer_.end(), SPOCompare{});

  // Merge into primary
  std::vector<Triple> merged;
  merged.reserve(primary_.size() + write_buffer_.size());
  std::merge(primary_.begin(), primary_.end(), write_buffer_.begin(),
             write_buffer_.end(), std::back_inserter(merged), SPOCompare{});

  // Deduplicate
  merged.erase(std::unique(merged.begin(), merged.end()), merged.end());

  primary_ = std::move(merged);
  write_buffer_.clear();
  RebuildPermutations();
}

void TripleStore::RebuildPermutations() {
  size_t n = primary_.size();

  // Build POS permutation
  pos_perm_.resize(n);
  std::iota(pos_perm_.begin(), pos_perm_.end(), 0);
  std::sort(pos_perm_.begin(), pos_perm_.end(), [this](uint32_t a, uint32_t b) {
    return POSCompare{}(primary_[a], primary_[b]);
  });

  // Build OSP permutation
  osp_perm_.resize(n);
  std::iota(osp_perm_.begin(), osp_perm_.end(), 0);
  std::sort(osp_perm_.begin(), osp_perm_.end(), [this](uint32_t a, uint32_t b) {
    return OSPCompare{}(primary_[a], primary_[b]);
  });
}

size_t TripleStore::MemoryUsage() const {
  return primary_.capacity() * sizeof(Triple) +
         pos_perm_.capacity() * sizeof(uint32_t) +
         osp_perm_.capacity() * sizeof(uint32_t) +
         write_buffer_.capacity() * sizeof(Triple);
}

}  // namespace valkey_search::rdf

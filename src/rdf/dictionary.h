/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_RDF_DICTIONARY_H_
#define VALKEYSEARCH_SRC_RDF_DICTIONARY_H_

#include <cstdint>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "src/rdf/rdf_term.h"

namespace valkey_search::rdf {

// 8-byte term ID layout:
//   Bit 63 = 0: pointer into term_data_ (offset)
//   Bit 63 = 1: inline value
//     Bits 57-62: type tag (6 bits)
//     Bits 0-56: value (56 bits)
using TermId = uint64_t;

inline constexpr uint64_t kInlineBit = 1ULL << 63;
inline constexpr uint64_t kTypeMask = 0x3FULL << 57;
inline constexpr uint64_t kValueMask = (1ULL << 57) - 1;
inline constexpr int kTypeShift = 57;

// Type tags for inline encoding
inline constexpr uint64_t kTagInteger = 0x01ULL;
inline constexpr uint64_t kTagBoolean = 0x03ULL;
inline constexpr uint64_t kTagBlankNode = 0x10ULL;

// Check if an ID is inline-encoded
inline bool IsInline(TermId id) { return (id & kInlineBit) != 0; }

// Extract type tag from inline ID
inline uint64_t InlineTypeTag(TermId id) {
  return (id >> kTypeShift) & 0x3F;
}

// Extract value bits from inline ID
inline int64_t InlineIntValue(TermId id) {
  // Sign-extend from 57 bits
  int64_t raw = static_cast<int64_t>(id & kValueMask);
  if (raw & (1LL << 56)) raw |= ~kValueMask;  // sign extend
  return raw;
}

// Build an inline integer ID
inline TermId MakeInlineInt(int64_t val) {
  uint64_t bits = static_cast<uint64_t>(val) & kValueMask;
  return kInlineBit | (kTagInteger << kTypeShift) | bits;
}

// Build an inline boolean ID
inline TermId MakeInlineBool(bool val) {
  return kInlineBit | (kTagBoolean << kTypeShift) | (val ? 1ULL : 0ULL);
}

// Build an inline blank node ID
inline TermId MakeInlineBlank(uint64_t auto_id) {
  return kInlineBit | (kTagBlankNode << kTypeShift) | (auto_id & kValueMask);
}

class Dictionary {
 public:
  Dictionary() = default;

  // Encode a term to its ID (insert if new)
  TermId Encode(const RDFTerm& term);

  // Decode an ID back to a term string (for query results)
  absl::StatusOr<std::string> Decode(TermId id) const;

  size_t TermCount() const { return term_count_; }
  size_t MemoryUsage() const;

 private:
  // Try inline encoding; returns 0 if not possible
  TermId TryInline(const RDFTerm& term);

  // Serialize a term for storage in term_data_
  std::string Serialize(const RDFTerm& term) const;

  // Term table: sequential storage of serialized terms
  std::vector<char> term_data_;

  // Hash of serialized term → offset in term_data_ (the TermId with bit63=0)
  absl::flat_hash_map<std::string, TermId> term_to_id_;

  // Auto-increment for blank nodes
  uint64_t next_blank_id_ = 0;

  size_t term_count_ = 0;
};

}  // namespace valkey_search::rdf

#endif  // VALKEYSEARCH_SRC_RDF_DICTIONARY_H_

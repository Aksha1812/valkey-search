/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_HIGHLIGHT_H_
#define VALKEYSEARCH_SRC_QUERY_HIGHLIGHT_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/indexes/text/lexer.h"
#include "src/query/predicate.h"

namespace valkey_search::query {

// A text predicate reduced to what is needed to decide whether a word in a
// document would have matched it. Patterns are lowercased; kTerm is also
// stemmed, so it is compared against the stem of a document word.
struct TextMatchSpec {
  enum class Kind { kTerm, kPrefix, kSuffix, kInfix };
  Kind kind;
  std::string pattern;
};

// Byte range of one word within the field value it came from.
struct WordSpan {
  size_t start;
  size_t len;
};

// Walks the query predicate tree and reduces every text predicate to a spec.
// Fuzzy predicates are skipped: reproducing their matching needs the stem tree
// walk that evaluation uses, so their matches are simply not marked.
std::vector<TextMatchSpec> CollectTextMatchSpecs(
    const Predicate* root, const indexes::text::Lexer& lexer);

// Spans of the words in `text` that any spec matches, in order,
// non-overlapping.
std::vector<WordSpan> FindMatchingSpans(absl::string_view text,
                                        const std::vector<TextMatchSpec>& specs,
                                        const indexes::text::Lexer& lexer);

// `text` with each span wrapped in the tags.
std::string Highlight(absl::string_view text,
                      const std::vector<WordSpan>& spans,
                      absl::string_view open_tag, absl::string_view close_tag);

// Up to `frags` windows of `len` words around the spans, joined by `separator`
// and followed by one trailing separator, as Redis does. Empty tags disable
// wrapping, so this composes with HIGHLIGHT.
std::string Summarize(absl::string_view text,
                      const std::vector<WordSpan>& spans,
                      const indexes::text::Lexer& lexer, uint32_t frags,
                      uint32_t len, absl::string_view separator,
                      absl::string_view open_tag, absl::string_view close_tag);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_HIGHLIGHT_H_

/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/highlight.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/indexes/text/language.h"
#include "src/query/predicate.h"
#include "src/utils/scanner.h"

namespace valkey_search::query {

namespace {

// Splits on the language's punctuation set, iterating code points so a
// multi-byte separator such as the Arabic comma U+060C breaks a word.
std::vector<WordSpan> AllWordSpans(absl::string_view text,
                                   const indexes::text::Language& language) {
  const auto& punctuation = language.GetPunctuationSet();
  std::vector<WordSpan> spans;
  utils::Scanner scanner(text);
  size_t offset = 0;
  size_t word_start = 0;
  bool in_word = false;
  while (true) {
    auto code_point = scanner.NextUtf8();
    if (code_point == utils::Scanner::kEOF) {
      break;
    }
    size_t consumed = scanner.LastUtf8ByteLen();
    // A malformed sequence is not a separator, so it stays inside a word.
    bool separator = code_point != utils::Scanner::kInvalidCp &&
                     punctuation.Contains(code_point);
    if (separator) {
      if (in_word) {
        spans.push_back(WordSpan{word_start, offset - word_start});
        in_word = false;
      }
    } else if (!in_word) {
      word_start = offset;
      in_word = true;
    }
    offset += consumed;
  }
  if (in_word) {
    spans.push_back(WordSpan{word_start, offset - word_start});
  }
  return spans;
}

std::string Normalized(absl::string_view word,
                       const indexes::text::Language& language) {
  std::string out(word);
  language.NormalizeInPlace(out);
  return out;
}

// Mirrors the query path, which stems without a minimum word length.
std::string Stemmed(absl::string_view normalized_word,
                    const indexes::text::Language& language) {
  auto* stemmer = language.GetStemmer();
  if (stemmer == nullptr) {
    return std::string(normalized_word);
  }
  return stemmer->GetStemRoot(normalized_word);
}

bool SpecMatches(const TextMatchSpec& spec, absl::string_view normalized,
                 absl::string_view stemmed) {
  switch (spec.kind) {
    case TextMatchSpec::Kind::kTerm:
      return stemmed == spec.pattern;
    case TextMatchSpec::Kind::kPrefix:
      return absl::StartsWith(normalized, spec.pattern);
    case TextMatchSpec::Kind::kSuffix:
      return absl::EndsWith(normalized, spec.pattern);
    case TextMatchSpec::Kind::kInfix:
      return absl::StrContains(normalized, spec.pattern);
  }
  return false;
}

void CollectInto(const Predicate* predicate,
                 const indexes::text::Language& language,
                 std::vector<TextMatchSpec>& specs) {
  if (predicate == nullptr) {
    return;
  }
  switch (predicate->GetType()) {
    case PredicateType::kComposedAnd:
    case PredicateType::kComposedOr: {
      auto composed = dynamic_cast<const ComposedPredicate*>(predicate);
      if (composed != nullptr) {
        for (const auto& child : composed->GetChildren()) {
          CollectInto(child.get(), language, specs);
        }
      }
      break;
    }
    case PredicateType::kNegate: {
      // A negated term is absent from any document that matched, so it can
      // never be highlighted.
      break;
    }
    case PredicateType::kText: {
      if (auto term = dynamic_cast<const TermPredicate*>(predicate)) {
        specs.push_back(TextMatchSpec{
            TextMatchSpec::Kind::kTerm,
            Stemmed(Normalized(term->GetTextString(), language), language)});
      } else if (auto prefix =
                     dynamic_cast<const PrefixPredicate*>(predicate)) {
        specs.push_back(
            TextMatchSpec{TextMatchSpec::Kind::kPrefix,
                          Normalized(prefix->GetTextString(), language)});
      } else if (auto suffix =
                     dynamic_cast<const SuffixPredicate*>(predicate)) {
        specs.push_back(
            TextMatchSpec{TextMatchSpec::Kind::kSuffix,
                          Normalized(suffix->GetTextString(), language)});
      } else if (auto infix = dynamic_cast<const InfixPredicate*>(predicate)) {
        specs.push_back(
            TextMatchSpec{TextMatchSpec::Kind::kInfix,
                          Normalized(infix->GetTextString(), language)});
      }
      break;
    }
    case PredicateType::kTag:
    case PredicateType::kNumeric:
    case PredicateType::kNone:
      break;
  }
}

}  // namespace

std::vector<TextMatchSpec> CollectTextMatchSpecs(
    const Predicate* root, const indexes::text::Language& language) {
  std::vector<TextMatchSpec> specs;
  CollectInto(root, language, specs);
  return specs;
}

std::vector<WordSpan> FindMatchingSpans(
    absl::string_view text, const std::vector<TextMatchSpec>& specs,
    const indexes::text::Language& language) {
  std::vector<WordSpan> matches;
  if (specs.empty()) {
    return matches;
  }
  for (const auto& span : AllWordSpans(text, language)) {
    auto word = text.substr(span.start, span.len);
    auto normalized = Normalized(word, language);
    auto stemmed = Stemmed(normalized, language);
    for (const auto& spec : specs) {
      if (SpecMatches(spec, normalized, stemmed)) {
        matches.push_back(span);
        break;
      }
    }
  }
  return matches;
}

std::string Highlight(absl::string_view text,
                      const std::vector<WordSpan>& spans,
                      absl::string_view open_tag, absl::string_view close_tag) {
  std::string out;
  out.reserve(text.size() +
              spans.size() * (open_tag.size() + close_tag.size()));
  size_t cursor = 0;
  for (const auto& span : spans) {
    out.append(text.substr(cursor, span.start - cursor));
    absl::StrAppend(&out, open_tag, text.substr(span.start, span.len),
                    close_tag);
    cursor = span.start + span.len;
  }
  out.append(text.substr(cursor));
  return out;
}

std::string Summarize(absl::string_view text,
                      const std::vector<WordSpan>& spans,
                      const indexes::text::Language& language, uint32_t frags,
                      uint32_t len, absl::string_view separator,
                      absl::string_view open_tag, absl::string_view close_tag) {
  auto words = AllWordSpans(text, language);
  if (words.empty() || spans.empty() || frags == 0 || len == 0) {
    return std::string(text);
  }

  // Word index of each match, so a window can be measured in words.
  std::vector<size_t> match_indexes;
  size_t next = 0;
  for (size_t w = 0; w < words.size() && next < spans.size(); ++w) {
    if (words[w].start == spans[next].start) {
      match_indexes.push_back(w);
      ++next;
    }
  }

  // Windows of `len` words around each match, merged where they overlap.
  std::vector<std::pair<size_t, size_t>> windows;
  for (size_t idx : match_indexes) {
    size_t half = len / 2;
    size_t begin = idx > half ? idx - half : 0;
    size_t end = std::min(begin + len, words.size());
    if (!windows.empty() && begin <= windows.back().second) {
      windows.back().second = std::max(windows.back().second, end);
    } else {
      windows.emplace_back(begin, end);
    }
    if (windows.size() >= frags) {
      break;
    }
  }

  std::string out;
  for (const auto& [begin, end] : windows) {
    size_t from = words[begin].start;
    size_t to = words[end - 1].start + words[end - 1].len;
    auto fragment = text.substr(from, to - from);
    if (open_tag.empty() && close_tag.empty()) {
      out.append(fragment);
    } else {
      std::vector<WordSpan> shifted;
      for (const auto& span : spans) {
        if (span.start >= from && span.start + span.len <= to) {
          shifted.push_back(WordSpan{span.start - from, span.len});
        }
      }
      out.append(Highlight(fragment, shifted, open_tag, close_tag));
    }
    out.append(separator);
  }
  return out;
}

}  // namespace valkey_search::query

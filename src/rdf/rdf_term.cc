/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/rdf_term.h"

#include <cerrno>
#include <cstdlib>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"

namespace valkey_search::rdf {

static constexpr absl::string_view kXSDPrefix =
    "http://www.w3.org/2001/XMLSchema#";

XSDDatatype ResolveDatatype(absl::string_view iri) {
  // Strip angle brackets if present
  if (iri.size() >= 2 && iri.front() == '<' && iri.back() == '>') {
    iri = iri.substr(1, iri.size() - 2);
  }
  if (!absl::StartsWith(iri, kXSDPrefix)) return XSDDatatype::kUnknown;
  auto local = iri.substr(kXSDPrefix.size());
  if (local == "integer" || local == "int" || local == "long" ||
      local == "short" || local == "byte")
    return XSDDatatype::kInteger;
  if (local == "double") return XSDDatatype::kDouble;
  if (local == "float") return XSDDatatype::kFloat;
  if (local == "decimal") return XSDDatatype::kDecimal;
  if (local == "boolean") return XSDDatatype::kBoolean;
  if (local == "date") return XSDDatatype::kDate;
  if (local == "dateTime") return XSDDatatype::kDateTime;
  if (local == "string") return XSDDatatype::kString;
  return XSDDatatype::kUnknown;
}

// Find the closing quote, handling escaped quotes
static size_t FindClosingQuote(absl::string_view s, size_t start) {
  for (size_t i = start; i < s.size(); ++i) {
    if (s[i] == '"' && (i == start || s[i - 1] != '\\')) return i;
  }
  return absl::string_view::npos;
}

absl::StatusOr<RDFTerm> ParseRDFTerm(absl::string_view input) {
  if (input.empty()) {
    return absl::InvalidArgumentError("Empty RDF term");
  }

  // IRI: <...>
  if (input.front() == '<' && input.back() == '>') {
    if (input.size() < 3) {
      return absl::InvalidArgumentError("Empty IRI");
    }
    return RDFTerm{.type = RDFTermType::kIRI,
                   .value = std::string(input.substr(1, input.size() - 2))};
  }

  // Blank node: _:xxx
  if (absl::StartsWith(input, "_:")) {
    auto id = input.substr(2);
    if (id.empty()) {
      return absl::InvalidArgumentError("Empty blank node ID");
    }
    return RDFTerm{.type = RDFTermType::kBlankNode,
                   .value = std::string(id)};
  }

  // Literal: "..."[@lang | ^^<datatype>]
  if (input.front() != '"') {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid RDF term, expected <IRI>, \"literal\", or "
                     "_:blank, got: ",
                     input));
  }

  size_t close_quote = FindClosingQuote(input, 1);
  if (close_quote == absl::string_view::npos) {
    return absl::InvalidArgumentError("Unterminated literal string");
  }

  auto lexical = std::string(input.substr(1, close_quote - 1));
  auto rest = input.substr(close_quote + 1);

  // Language tag: @xx
  if (absl::StartsWith(rest, "@")) {
    auto lang = rest.substr(1);
    if (lang.empty()) {
      return absl::InvalidArgumentError("Empty language tag");
    }
    return RDFTerm{.type = RDFTermType::kLangLiteral,
                   .value = std::move(lexical),
                   .language = std::string(lang)};
  }

  // Typed literal: ^^<datatype>
  if (absl::StartsWith(rest, "^^")) {
    auto dt_str = rest.substr(2);
    XSDDatatype dt = ResolveDatatype(dt_str);

    RDFTerm term{.type = RDFTermType::kTypedLiteral,
                 .value = std::move(lexical),
                 .datatype = dt};

    // Pre-parse numeric values for inline encoding
    switch (dt) {
      case XSDDatatype::kInteger: {
        int64_t v;
        if (absl::SimpleAtoi(term.value, &v)) term.as_integer = v;
        break;
      }
      case XSDDatatype::kDouble:
      case XSDDatatype::kFloat:
      case XSDDatatype::kDecimal: {
        double v;
        if (absl::SimpleAtod(term.value, &v)) term.as_double = v;
        break;
      }
      case XSDDatatype::kBoolean:
        term.as_boolean = (term.value == "true" || term.value == "1");
        break;
      default:
        break;
    }
    return term;
  }

  // Plain literal (no lang, no datatype)
  if (rest.empty()) {
    return RDFTerm{.type = RDFTermType::kPlainLiteral,
                   .value = std::move(lexical)};
  }

  return absl::InvalidArgumentError(
      absl::StrCat("Invalid suffix after literal: ", rest));
}

}  // namespace valkey_search::rdf

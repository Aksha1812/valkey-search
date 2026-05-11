/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_RDF_RDF_TERM_H_
#define VALKEYSEARCH_SRC_RDF_RDF_TERM_H_

#include <cstdint>
#include <optional>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace valkey_search::rdf {

enum class RDFTermType : uint8_t {
  kIRI = 0,
  kPlainLiteral = 1,
  kLangLiteral = 2,
  kTypedLiteral = 3,
  kBlankNode = 4,
};

enum class XSDDatatype : uint8_t {
  kString = 0,
  kInteger = 1,
  kDouble = 2,
  kFloat = 3,
  kDecimal = 4,
  kBoolean = 5,
  kDate = 6,
  kDateTime = 7,
  kUnknown = 255,
};

struct RDFTerm {
  RDFTermType type;
  std::string value;       // IRI content, lexical form, or blank node ID
  std::string language;    // non-empty only for kLangLiteral
  XSDDatatype datatype = XSDDatatype::kString;

  // Pre-parsed values for inline encoding
  std::optional<int64_t> as_integer;
  std::optional<double> as_double;
  std::optional<bool> as_boolean;
};

// Parse an RDF term from command input.
// Accepts: <IRI>, "literal", "literal"@lang, "literal"^^<datatype>, _:blankid
absl::StatusOr<RDFTerm> ParseRDFTerm(absl::string_view input);

// Resolve common XSD datatype IRIs to enum
XSDDatatype ResolveDatatype(absl::string_view iri);

}  // namespace valkey_search::rdf

#endif  // VALKEYSEARCH_SRC_RDF_RDF_TERM_H_

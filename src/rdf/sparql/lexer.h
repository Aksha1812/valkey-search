/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_RDF_SPARQL_LEXER_H_
#define VALKEYSEARCH_SRC_RDF_SPARQL_LEXER_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"

namespace valkey_search::rdf::sparql {

enum class TokenType {
  kSelect,
  kWhere,
  kFilter,
  kOptional,
  kPrefix,
  kDistinct,
  kLimit,
  kOffset,
  kOrderBy,
  kAsc,
  kDesc,
  kAs,
  kBind,
  kUnion,
  kIRI,          // <...>
  kVariable,     // ?name
  kLiteral,      // "..."
  kTypedLiteral, // "..."^^<type>
  kLangLiteral,  // "..."@lang
  kDot,          // .
  kLBrace,       // {
  kRBrace,       // }
  kLParen,       // (
  kRParen,       // )
  kStar,         // *
  kComma,        // ,
  kSemicolon,    // ;
  kA,            // 'a' (shorthand for rdf:type)
  kLT,           // <  (comparison)
  kGT,           // >  (comparison)
  kLE,           // <=
  kGE,           // >=
  kEQ,           // =
  kNE,           // !=
  kAnd,          // &&
  kOr,           // ||
  kNot,          // !
  kInteger,      // 42
  kDouble,       // 3.14
  kTrue,         // true
  kFalse,        // false
  kIdentifier,   // unquoted word (function names, etc.)
  kEOF,
};

struct Token {
  TokenType type;
  std::string value;
};

// Tokenize a SPARQL query string
std::vector<Token> Tokenize(absl::string_view input);

}  // namespace valkey_search::rdf::sparql

#endif  // VALKEYSEARCH_SRC_RDF_SPARQL_LEXER_H_

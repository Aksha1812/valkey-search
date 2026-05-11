/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/sparql/lexer.h"

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"

namespace valkey_search::rdf::sparql {

static bool IsWordChar(char c) {
  return absl::ascii_isalnum(c) || c == '_' || c == '-';
}

std::vector<Token> Tokenize(absl::string_view input) {
  std::vector<Token> tokens;
  size_t i = 0;

  while (i < input.size()) {
    // Skip whitespace
    if (absl::ascii_isspace(input[i])) { ++i; continue; }

    // IRI: <...>
    if (input[i] == '<' && i + 1 < input.size() && input[i + 1] != '=') {
      // Check if this looks like an IRI (contains :// or starts with known prefix)
      size_t end = input.find('>', i);
      if (end != absl::string_view::npos) {
        tokens.push_back({TokenType::kIRI, std::string(input.substr(i, end - i + 1))});
        i = end + 1;
        continue;
      }
    }

    // Variable: ?name
    if (input[i] == '?') {
      size_t start = i++;
      while (i < input.size() && IsWordChar(input[i])) ++i;
      tokens.push_back({TokenType::kVariable, std::string(input.substr(start, i - start))});
      continue;
    }

    // String literal: "..."
    if (input[i] == '"') {
      size_t start = i++;
      while (i < input.size() && !(input[i] == '"' && input[i - 1] != '\\')) ++i;
      if (i < input.size()) ++i; // consume closing quote
      // Check for ^^<type> or @lang
      if (i < input.size() && input[i] == '^' && i + 1 < input.size() && input[i + 1] == '^') {
        i += 2;
        size_t type_start = i;
        if (i < input.size() && input[i] == '<') {
          size_t end = input.find('>', i);
          if (end != absl::string_view::npos) { i = end + 1; }
        }
        tokens.push_back({TokenType::kTypedLiteral, std::string(input.substr(start, i - start))});
      } else if (i < input.size() && input[i] == '@') {
        size_t lang_start = i++;
        while (i < input.size() && absl::ascii_isalpha(input[i])) ++i;
        tokens.push_back({TokenType::kLangLiteral, std::string(input.substr(start, i - start))});
      } else {
        tokens.push_back({TokenType::kLiteral, std::string(input.substr(start, i - start))});
      }
      continue;
    }

    // Two-char operators
    if (i + 1 < input.size()) {
      auto two = input.substr(i, 2);
      if (two == "<=") { tokens.push_back({TokenType::kLE, "<="}); i += 2; continue; }
      if (two == ">=") { tokens.push_back({TokenType::kGE, ">="}); i += 2; continue; }
      if (two == "!=") { tokens.push_back({TokenType::kNE, "!="}); i += 2; continue; }
      if (two == "&&") { tokens.push_back({TokenType::kAnd, "&&"}); i += 2; continue; }
      if (two == "||") { tokens.push_back({TokenType::kOr, "||"}); i += 2; continue; }
    }

    // Single-char tokens
    switch (input[i]) {
      case '.': tokens.push_back({TokenType::kDot, "."}); ++i; continue;
      case '{': tokens.push_back({TokenType::kLBrace, "{"}); ++i; continue;
      case '}': tokens.push_back({TokenType::kRBrace, "}"}); ++i; continue;
      case '(': tokens.push_back({TokenType::kLParen, "("}); ++i; continue;
      case ')': tokens.push_back({TokenType::kRParen, ")"}); ++i; continue;
      case '*': tokens.push_back({TokenType::kStar, "*"}); ++i; continue;
      case ',': tokens.push_back({TokenType::kComma, ","}); ++i; continue;
      case ';': tokens.push_back({TokenType::kSemicolon, ";"}); ++i; continue;
      case '=': tokens.push_back({TokenType::kEQ, "="}); ++i; continue;
      case '<': tokens.push_back({TokenType::kLT, "<"}); ++i; continue;
      case '>': tokens.push_back({TokenType::kGT, ">"}); ++i; continue;
      case '!': tokens.push_back({TokenType::kNot, "!"}); ++i; continue;
      default: break;
    }

    // Numbers
    if (absl::ascii_isdigit(input[i]) || (input[i] == '-' && i + 1 < input.size() && absl::ascii_isdigit(input[i + 1]))) {
      size_t start = i++;
      bool is_double = false;
      while (i < input.size() && (absl::ascii_isdigit(input[i]) || input[i] == '.')) {
        if (input[i] == '.') is_double = true;
        ++i;
      }
      tokens.push_back({is_double ? TokenType::kDouble : TokenType::kInteger,
                         std::string(input.substr(start, i - start))});
      continue;
    }

    // Keywords and identifiers
    if (absl::ascii_isalpha(input[i]) || input[i] == '_') {
      size_t start = i;
      while (i < input.size() && IsWordChar(input[i])) ++i;
      std::string word(input.substr(start, i - start));
      // Check keywords (case-insensitive)
      std::string lower = absl::AsciiStrToLower(word);
      if (lower == "select") { tokens.push_back({TokenType::kSelect, word}); }
      else if (lower == "where") { tokens.push_back({TokenType::kWhere, word}); }
      else if (lower == "filter") { tokens.push_back({TokenType::kFilter, word}); }
      else if (lower == "optional") { tokens.push_back({TokenType::kOptional, word}); }
      else if (lower == "prefix") { tokens.push_back({TokenType::kPrefix, word}); }
      else if (lower == "distinct") { tokens.push_back({TokenType::kDistinct, word}); }
      else if (lower == "limit") { tokens.push_back({TokenType::kLimit, word}); }
      else if (lower == "offset") { tokens.push_back({TokenType::kOffset, word}); }
      else if (lower == "order") { tokens.push_back({TokenType::kOrderBy, word}); }
      else if (lower == "asc") { tokens.push_back({TokenType::kAsc, word}); }
      else if (lower == "desc") { tokens.push_back({TokenType::kDesc, word}); }
      else if (lower == "bind") { tokens.push_back({TokenType::kBind, word}); }
      else if (lower == "union") { tokens.push_back({TokenType::kUnion, word}); }
      else if (lower == "true") { tokens.push_back({TokenType::kTrue, "true"}); }
      else if (lower == "false") { tokens.push_back({TokenType::kFalse, "false"}); }
      else if (lower == "a") { tokens.push_back({TokenType::kA, "a"}); }
      else { tokens.push_back({TokenType::kIdentifier, word}); }
      continue;
    }

    // Skip unknown characters
    ++i;
  }

  tokens.push_back({TokenType::kEOF, ""});
  return tokens;
}

}  // namespace valkey_search::rdf::sparql

/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/sparql/parser.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"

namespace valkey_search::rdf::sparql {

namespace {

class Parser {
 public:
  explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

  absl::StatusOr<ParsedQuery> Parse() {
    ParsedQuery q;
    // Parse SELECT
    if (!Expect(TokenType::kSelect)) {
      return absl::InvalidArgumentError("Expected SELECT");
    }
    if (Peek().type == TokenType::kDistinct) { Advance(); q.distinct = true; }

    // Parse projection: * or variable list
    if (Peek().type == TokenType::kStar) {
      Advance();
    } else {
      while (Peek().type == TokenType::kVariable) {
        q.select_vars.push_back(Peek().value.substr(1));  // strip ?
        Advance();
      }
      if (q.select_vars.empty()) {
        return absl::InvalidArgumentError("Expected * or variables after SELECT");
      }
    }

    // Parse WHERE { ... }
    if (!Expect(TokenType::kWhere)) {
      return absl::InvalidArgumentError("Expected WHERE");
    }
    if (!Expect(TokenType::kLBrace)) {
      return absl::InvalidArgumentError("Expected {");
    }

    // Parse triple patterns and filters until }
    while (Peek().type != TokenType::kRBrace && Peek().type != TokenType::kEOF) {
      if (Peek().type == TokenType::kFilter) {
        auto filter = ParseFilter();
        if (!filter.ok()) return filter.status();
        q.filters.push_back(*filter);
      } else {
        auto pattern = ParseTriplePattern();
        if (!pattern.ok()) return pattern.status();
        q.patterns.push_back(*pattern);
        // Optional dot separator
        if (Peek().type == TokenType::kDot) Advance();
      }
    }

    if (!Expect(TokenType::kRBrace)) {
      return absl::InvalidArgumentError("Expected }");
    }

    // Parse optional LIMIT/OFFSET
    while (Peek().type != TokenType::kEOF) {
      if (Peek().type == TokenType::kLimit) {
        Advance();
        if (Peek().type != TokenType::kInteger) {
          return absl::InvalidArgumentError("Expected integer after LIMIT");
        }
        q.limit = std::stoll(Peek().value);
        Advance();
      } else if (Peek().type == TokenType::kOffset) {
        Advance();
        if (Peek().type != TokenType::kInteger) {
          return absl::InvalidArgumentError("Expected integer after OFFSET");
        }
        q.offset = std::stoll(Peek().value);
        Advance();
      } else {
        Advance();  // skip unknown trailing tokens
      }
    }

    return q;
  }

 private:
  const Token& Peek() const { return tokens_[pos_]; }
  const Token& Advance() { return tokens_[pos_++]; }
  bool Expect(TokenType type) {
    if (Peek().type == type) { Advance(); return true; }
    return false;
  }

  absl::StatusOr<PatternTerm> ParsePatternTerm() {
    const auto& tok = Peek();
    switch (tok.type) {
      case TokenType::kVariable:
        Advance();
        return PatternTerm{true, tok.value.substr(1)};
      case TokenType::kIRI:
        Advance();
        return PatternTerm{false, tok.value};
      case TokenType::kTypedLiteral:
      case TokenType::kLangLiteral:
      case TokenType::kLiteral:
        Advance();
        return PatternTerm{false, tok.value};
      case TokenType::kA:
        Advance();
        return PatternTerm{false, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"};
      case TokenType::kInteger:
        Advance();
        return PatternTerm{false, absl::StrCat("\"", tok.value, "\"^^<http://www.w3.org/2001/XMLSchema#integer>")};
      case TokenType::kTrue:
      case TokenType::kFalse:
        Advance();
        return PatternTerm{false, absl::StrCat("\"", tok.value, "\"^^<http://www.w3.org/2001/XMLSchema#boolean>")};
      default:
        return absl::InvalidArgumentError(
            absl::StrCat("Unexpected token in triple pattern: ", tok.value));
    }
  }

  absl::StatusOr<TriplePattern> ParseTriplePattern() {
    auto s = ParsePatternTerm();
    if (!s.ok()) return s.status();
    auto p = ParsePatternTerm();
    if (!p.ok()) return p.status();
    auto o = ParsePatternTerm();
    if (!o.ok()) return o.status();
    return TriplePattern{*s, *p, *o};
  }

  absl::StatusOr<FilterExpr> ParseFilter() {
    Advance();  // consume FILTER
    if (!Expect(TokenType::kLParen)) {
      return absl::InvalidArgumentError("Expected ( after FILTER");
    }
    // Collect everything until matching )
    int depth = 1;
    std::string raw;
    while (depth > 0 && Peek().type != TokenType::kEOF) {
      if (Peek().type == TokenType::kLParen) ++depth;
      if (Peek().type == TokenType::kRParen) { --depth; if (depth == 0) break; }
      raw += Peek().value + " ";
      Advance();
    }
    Expect(TokenType::kRParen);
    return FilterExpr{raw};
  }

  std::vector<Token> tokens_;
  size_t pos_ = 0;
};

}  // namespace

absl::StatusOr<ParsedQuery> Parse(absl::string_view sparql) {
  auto tokens = Tokenize(sparql);
  Parser parser(std::move(tokens));
  return parser.Parse();
}

}  // namespace valkey_search::rdf::sparql

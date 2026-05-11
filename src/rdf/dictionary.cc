/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/dictionary.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"

namespace valkey_search::rdf {

// Max value for signed 57-bit integer: 2^56 - 1
static constexpr int64_t kMaxInlineInt = (1LL << 56) - 1;
static constexpr int64_t kMinInlineInt = -(1LL << 56);

TermId Dictionary::TryInline(const RDFTerm& term) {
  switch (term.type) {
    case RDFTermType::kTypedLiteral:
      if (term.datatype == XSDDatatype::kInteger && term.as_integer) {
        int64_t v = *term.as_integer;
        if (v >= kMinInlineInt && v <= kMaxInlineInt) {
          return MakeInlineInt(v);
        }
      }
      if (term.datatype == XSDDatatype::kBoolean && term.as_boolean) {
        return MakeInlineBool(*term.as_boolean);
      }
      break;
    case RDFTermType::kBlankNode:
      return MakeInlineBlank(next_blank_id_++);
    default:
      break;
  }
  return 0;  // cannot inline
}

std::string Dictionary::Serialize(const RDFTerm& term) const {
  // Format: type_byte + value [+ language/datatype]
  std::string s;
  s.push_back(static_cast<char>(term.type));
  switch (term.type) {
    case RDFTermType::kIRI:
    case RDFTermType::kPlainLiteral:
    case RDFTermType::kBlankNode:
      s.append(term.value);
      break;
    case RDFTermType::kLangLiteral:
      s.append(term.value);
      s.push_back('\0');
      s.append(term.language);
      break;
    case RDFTermType::kTypedLiteral:
      s.append(term.value);
      s.push_back('\0');
      s.push_back(static_cast<char>(term.datatype));
      break;
  }
  return s;
}

TermId Dictionary::Encode(const RDFTerm& term) {
  // Try inline first
  TermId inline_id = TryInline(term);
  if (inline_id != 0) {
    ++term_count_;
    return inline_id;
  }

  // Check if already in dictionary
  std::string key = Serialize(term);
  auto it = term_to_id_.find(key);
  if (it != term_to_id_.end()) return it->second;

  // Insert into term table
  TermId id = static_cast<TermId>(term_data_.size());
  // Store: [len:4 bytes][serialized data]
  uint32_t len = static_cast<uint32_t>(key.size());
  term_data_.insert(term_data_.end(), reinterpret_cast<char*>(&len),
                    reinterpret_cast<char*>(&len) + 4);
  term_data_.insert(term_data_.end(), key.begin(), key.end());

  term_to_id_[key] = id;
  ++term_count_;
  return id;
}

absl::StatusOr<std::string> Dictionary::Decode(TermId id) const {
  if (IsInline(id)) {
    uint64_t tag = InlineTypeTag(id);
    if (tag == kTagInteger) {
      return absl::StrCat("\"", InlineIntValue(id),
                          "\"^^<http://www.w3.org/2001/XMLSchema#integer>");
    }
    if (tag == kTagBoolean) {
      bool v = (id & kValueMask) != 0;
      return absl::StrCat("\"", v ? "true" : "false",
                          "\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
    }
    if (tag == kTagBlankNode) {
      return absl::StrCat("_:b", id & kValueMask);
    }
    return absl::InternalError("Unknown inline type tag");
  }

  // Pointer into term_data_
  size_t offset = static_cast<size_t>(id);
  if (offset + 4 > term_data_.size()) {
    return absl::InternalError("Invalid term table offset");
  }
  uint32_t len;
  memcpy(&len, term_data_.data() + offset, 4);
  if (offset + 4 + len > term_data_.size()) {
    return absl::InternalError("Term data overflow");
  }

  absl::string_view data(term_data_.data() + offset + 4, len);
  auto type = static_cast<RDFTermType>(data[0]);
  auto content = data.substr(1);

  switch (type) {
    case RDFTermType::kIRI:
      return absl::StrCat("<", content, ">");
    case RDFTermType::kPlainLiteral:
      return absl::StrCat("\"", content, "\"");
    case RDFTermType::kLangLiteral: {
      auto nul = content.find('\0');
      return absl::StrCat("\"", content.substr(0, nul), "\"@",
                          content.substr(nul + 1));
    }
    case RDFTermType::kTypedLiteral: {
      auto nul = content.find('\0');
      auto val = content.substr(0, nul);
      auto dt = static_cast<XSDDatatype>(content[nul + 1]);
      // Reconstruct datatype IRI
      absl::string_view dt_local;
      switch (dt) {
        case XSDDatatype::kInteger: dt_local = "integer"; break;
        case XSDDatatype::kDouble: dt_local = "double"; break;
        case XSDDatatype::kFloat: dt_local = "float"; break;
        case XSDDatatype::kDecimal: dt_local = "decimal"; break;
        case XSDDatatype::kBoolean: dt_local = "boolean"; break;
        case XSDDatatype::kDate: dt_local = "date"; break;
        case XSDDatatype::kDateTime: dt_local = "dateTime"; break;
        default: dt_local = "string"; break;
      }
      return absl::StrCat("\"", val, "\"^^<http://www.w3.org/2001/XMLSchema#",
                          dt_local, ">");
    }
    case RDFTermType::kBlankNode:
      return absl::StrCat("_:", content);
  }
  return absl::InternalError("Unknown term type");
}

size_t Dictionary::MemoryUsage() const {
  return term_data_.capacity() +
         term_to_id_.bucket_count() * sizeof(decltype(term_to_id_)::value_type);
}

}  // namespace valkey_search::rdf

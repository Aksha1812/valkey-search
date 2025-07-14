#include "src/indexes/text/posting.h"

#include <algorithm>
#include <map>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <type_traits>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"

namespace valkey_search::text {

// Factory method to create optimal field mask based on field count
std::unique_ptr<FieldMask> FieldMask::Create(size_t num_fields) {
  if (num_fields == 0) {
    throw std::invalid_argument("num_fields must be greater than 0");
  }
  
  // Select most memory-efficient implementation
  if (num_fields <= 1) {
    return std::make_unique<SingleFieldMask>();  // bool (1 byte)
  } else if (num_fields <= 8) {
    return std::make_unique<ByteFieldMask>(num_fields);  // uint8_t (1 byte)
  } else if (num_fields <= 64) {
    return std::make_unique<Uint64FieldMask>(num_fields);  // uint64_t (8 bytes)
  } else {
    throw std::invalid_argument("Too many text fields (max 64 supported)");
  }
}

// Template implementation - uses compile-time branching for optimal performance
template<typename MaskType, size_t MAX_FIELDS>
FieldMaskImpl<MaskType, MAX_FIELDS>::FieldMaskImpl(size_t num_fields) 
    : mask_(MaskType{}), num_fields_(num_fields) {
  if (num_fields > MAX_FIELDS) {
    throw std::invalid_argument("Field count exceeds maximum for this mask type");
  }
}

template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::SetField(size_t field_index) {
  if (field_index >= num_fields_) {
    throw std::out_of_range("Field index out of range");
  }
  
  // Compile-time branching: bool vs bitwise operations
  if constexpr (std::is_same_v<MaskType, bool>) {
    mask_ = true;  // Single field case
  } else {
    mask_ |= (MaskType(1) << field_index);  // Set bit at field_index
  }
}

template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::ClearField(size_t field_index) {
  if (field_index >= num_fields_) {
    throw std::out_of_range("Field index out of range");
  }
  
  if constexpr (std::is_same_v<MaskType, bool>) {
    mask_ = false;  // Single field case
  } else {
    mask_ &= ~(MaskType(1) << field_index);  // Clear bit at field_index
  }
}

template<typename MaskType, size_t MAX_FIELDS>
bool FieldMaskImpl<MaskType, MAX_FIELDS>::HasField(size_t field_index) const {
  if (field_index >= num_fields_) {
    return false;
  }
  
  if constexpr (std::is_same_v<MaskType, bool>) {
    return mask_;  // Single field case
  } else {
    return (mask_ & (MaskType(1) << field_index)) != 0;  // Test bit at field_index
  }
}

template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::SetAllFields() {
  if constexpr (std::is_same_v<MaskType, bool>) {
    mask_ = true;
  } else {
    if (num_fields_ == MAX_FIELDS && MAX_FIELDS == 64) {
      mask_ = ~MaskType(0);  // Special case: avoid undefined behavior for 64-bit shift
    } else {
      mask_ = (MaskType(1) << num_fields_) - 1;  // Set num_fields_ bits
    }
  }
}

template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::ClearAllFields() {
  mask_ = MaskType{};  // Zero-initialize
}

template<typename MaskType, size_t MAX_FIELDS>
uint64_t FieldMaskImpl<MaskType, MAX_FIELDS>::AsUint64() const {
  if constexpr (std::is_same_v<MaskType, bool>) {
    return mask_ ? 1ULL : 0ULL;  // Convert bool to 0/1
  } else {
    return static_cast<uint64_t>(mask_);  // Cast integer types to uint64_t
  }
}

template<typename MaskType, size_t MAX_FIELDS>
std::unique_ptr<FieldMask> FieldMaskImpl<MaskType, MAX_FIELDS>::Clone() const {
  auto clone = std::make_unique<FieldMaskImpl<MaskType, MAX_FIELDS>>(num_fields_);
  clone->mask_ = mask_;
  return clone;
}

// Explicit template instantiations
template class FieldMaskImpl<bool, 1>;
template class FieldMaskImpl<uint8_t, 8>;
template class FieldMaskImpl<uint64_t, 64>;



// TODO : Posting Implementation 
// Posting entry structure - combines position with field mask
struct PostingEntry {
  Position position;
  uint64_t field_mask;  // Uses FieldMask::AsUint64() for storage
  
  PostingEntry(Position pos, uint64_t mask) 
      : position(pos), field_mask(mask) {}
      
  bool operator<(const PostingEntry& other) const {
    return position < other.position;
  }
};

class Postings::Impl {
public:
  bool save_positions_;
  size_t num_text_fields_;
  std::map<Key, std::vector<PostingEntry>> key_to_positions_;
  
  Impl(bool save_positions, size_t num_text_fields)
      : save_positions_(save_positions), num_text_fields_(num_text_fields) {}
};

Postings::Postings(bool save_positions, size_t num_text_fields) 
    : impl_(std::make_unique<Impl>(save_positions, num_text_fields)) {
}

Postings::~Postings() = default;

Postings::Postings(const Postings& other) 
    : impl_(std::make_unique<Impl>(*other.impl_)) {}

Postings& Postings::operator=(const Postings& other) {
  if (this != &other) {
    impl_ = std::make_unique<Impl>(*other.impl_);
  }
  return *this;
}

bool Postings::IsEmpty() const { return true; }
void Postings::SetKey(const Key&, std::span<Position>) {}
void Postings::RemoveKey(const Key&) {}
size_t Postings::GetKeyCount() const { return 0; }
size_t Postings::GetPostingCount() const { return 0; }
Postings* Postings::Defrag() { return this; }
}

#ifndef VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_
#define VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_

#include <memory>
#include <optional>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/text/radix_tree.h"
#include "src/indexes/text/posting.h"
#include "src/utils/string_interning.h"
#include "src/index_schema.pb.h"

namespace valkey_search {
namespace indexes {

using Key = vmsdk::InternedStringPtr;
using Position = uint32_t;

// Forward declaration for Postings
namespace text {
struct Postings;
}

class TextIndex {
 public:
  // Constructor - initializes complete text processing pipeline
  explicit TextIndex(const data_model::TextIndex& text_index_proto);
  ~TextIndex() = default;

  // Core radix tree management methods
  void InitializeRadixTree();
  
  // Word processing methods for radix tree operations
  absl::Status AddWordToTree(absl::string_view word, const InternedStringPtr& key, Position position);
  absl::Status RemoveWordFromTree(absl::string_view word, const InternedStringPtr& key);
  
  // Access methods for radix tree
  text::RadixTree<std::unique_ptr<text::Postings>, false>* GetPrefixTree() const { 
    return prefix_tree_.get(); 
  }

 private:
  // Internal radix tree operations
  void UpdatePositionMap(const InternedStringPtr& key, 
                        const std::vector<Position>& positions);

  //
  // The main query data structure maps Words into Postings objects. This
  // is always done with a prefix tree. Optionally, a suffix tree can also be maintained.
  // But in any case for the same word the two trees must point to the same Postings object,
  // which is owned by this pair of trees. Plus, updates to these two trees need
  // to be atomic when viewed externally. The locking provided by the RadixTree object
  // is NOT quite sufficient to guarantee that the two trees are always in lock step.
  // thus this object becomes responsible for cross-tree locking issues.
  // Multiple locking strategies are possible. TBD (a shared-ed word lock table should work well)
  //
  std::unique_ptr<text::RadixTree<std::unique_ptr<text::Postings>, false>> prefix_tree_;
  std::optional<text::RadixTree<std::unique_ptr<text::Postings>, true>> suffix_tree_;

  // Key to position mapping - placeholder for now
  absl::flat_hash_map<Key, std::vector<Position>> key_position_map_;

  // Set of untracked keys for negation support
  absl::flat_hash_set<Key> untracked_keys_;
  
  // Mutex for thread safety
  mutable absl::Mutex mutex_;
};

}  // namespace indexes
}  // namespace valkey_search

#endif

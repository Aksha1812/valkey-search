/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "src/indexes/text_index.h"

#include <memory>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "src/indexes/text/radix_tree.h"
#include "src/indexes/text/posting.h"

namespace valkey_search::indexes {

TextIndex::TextIndex(const data_model::TextIndex& text_index_proto) {
  LOG(INFO) << "Constructing TextIndex - radix tree only";
  
  // Initialize the radix tree - this is the main responsibility of TextIndex
  InitializeRadixTree();
  
  LOG(INFO) << "TextIndex construction completed";
}

void TextIndex::InitializeRadixTree() {
  LOG(INFO) << "Initializing radix tree for TextIndex";
  
  // Create the prefix tree - this is the main data structure for word storage
  // The template parameters are:
  // - std::unique_ptr<text::Postings>: The value type stored in the tree
  // - false: Indicates this is a prefix tree (not suffix)
  prefix_tree_ = std::make_unique<text::RadixTree<std::unique_ptr<text::Postings>, false>>();
  
  // TODO: Initialize suffix tree if needed based on configuration
  // suffix_tree_ = text::RadixTree<std::unique_ptr<text::Postings>, true>();
  
  LOG(INFO) << "Radix tree initialization completed";
}

absl::Status TextIndex::AddWordToTree(absl::string_view word, 
                                      const InternedStringPtr& key, 
                                      Position position) {
  absl::MutexLock lock(&mutex_);
  
  LOG(INFO) << "Adding word to radix tree: " << word << " for key: " << key->Str();
  
  // Use the radix tree's Mutate function to add/modify postings
  prefix_tree_->Mutate(
      word,
      [&](std::optional<std::unique_ptr<text::Postings>> existing_postings) 
      -> std::optional<std::unique_ptr<text::Postings>> {
        if (!existing_postings.has_value()) {
          // Create new postings object
          auto new_postings = std::make_unique<text::Postings>();
          // TODO: Add key and position to postings
          // new_postings->AddPosition(key, position);
          LOG(INFO) << "Created new postings for word: " << word;
          return std::move(new_postings);
        } else {
          // Add to existing postings
          // TODO: Add key and position to existing postings
          // existing_postings.value()->AddPosition(key, position);
          LOG(INFO) << "Added to existing postings for word: " << word;
          return std::move(existing_postings.value());
        }
      });
  
  // Update position mapping
  key_position_map_[key].push_back(position);
  
  return absl::OkStatus();
}

absl::Status TextIndex::RemoveWordFromTree(absl::string_view word, 
                                           const InternedStringPtr& key) {
  absl::MutexLock lock(&mutex_);
  
  LOG(INFO) << "Removing word from radix tree: " << word << " for key: " << key->Str();
  
  // Use the radix tree's Mutate function to remove/modify postings
  prefix_tree_->Mutate(
      word,
      [&](std::optional<std::unique_ptr<text::Postings>> existing_postings) 
      -> std::optional<std::unique_ptr<text::Postings>> {
        if (existing_postings.has_value()) {
          // TODO: Remove key from existing postings
          // if (existing_postings.value()->RemoveKey(key)) {
          //   // If postings becomes empty, remove the word completely
          //   return std::nullopt;
          // }
          LOG(INFO) << "Removed key from postings for word: " << word;
          return std::move(existing_postings.value());
        }
        return std::nullopt;
      });
  
  // Remove from position mapping
  key_position_map_.erase(key);
  
  return absl::OkStatus();
}

void TextIndex::UpdatePositionMap(const InternedStringPtr& key, 
                                  const std::vector<Position>& positions) {
  // Update the key->position mapping
  key_position_map_[key] = positions;
  
  LOG(INFO) << "Updated position map for key: " << key->Str() 
            << " with " << positions.size() << " positions";
}

}  // namespace valkey_search::indexes

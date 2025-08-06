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

#include "src/indexes/text.h"

#include <memory>
#include <sstream>
#include <string>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/text/lexer.h"
#include "src/indexes/text/radix_tree.h"
#include "src/query/predicate.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

Text::Text(const data_model::TextIndex& text_index_proto, IndexSchema* schema)
    : IndexBase(IndexerType::kText),
      text_field_number_(0),
      index_schema_(schema) {
  LOG(INFO) << "Initializing Text index";
  
  // Use shared TextIndex from schema instead of creating our own
  // Individual TextIndex instances are no longer needed
  
  LOG(INFO) << "Text index initialized successfully";
}

absl::StatusOr<bool> Text::AddRecord(const InternedStringPtr& key,
                                     absl::string_view data) {
  LOG(INFO) << "Adding record to text index for key: " << key->Str();
  
  absl::MutexLock lock(&index_mutex_);
  
  // Check if key already exists
  if (tracked_keys_.contains(key)) {
    LOG(WARNING) << "Key already exists in text index: " << key->Str();
    return absl::AlreadyExistsError(
        absl::StrCat("Key `", key->Str(), "` already exists"));
  }
  
  // Remove from untracked keys if present
  untracked_keys_.erase(key);
  
  // ProcessTextData expects lexed tokens from IndexSchema level
  // Here we handle stemming and IndexBase-level processing
  
  // TODO: Apply stemming to the text data (IndexBase-level processing)
  // This would involve stemming each word before adding to radix tree
  std::string processed_data = std::string(data); // Placeholder for stemming
  
  // For now, create simple word tokens (this should come from lexer at schema level)
  std::vector<std::string> words;
  // Simple word splitting as placeholder - in real implementation,
  // this would receive pre-lexed tokens from schema level
  std::istringstream iss(processed_data);
  std::string word;
  Position position = 0;
  while (iss >> word) {
    words.push_back(word);
    
    // Add each word to the radix tree via shared TextIndex from schema
    if (index_schema_->GetTextIndexSchema()) {
      auto status = index_schema_->GetTextIndexSchema()->GetTextIndex()->AddWordToTree(word, key, position);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to add word to radix tree: " << status;
        untracked_keys_.insert(key);
        return false;
      }
    }
    position++;
  }
  
  // Track the key and store raw value
  auto raw_value = InternString(data);
  tracked_keys_[key] = raw_value;
  
  LOG(INFO) << "Successfully added record for key: " << key->Str() 
            << " with " << words.size() << " words";
  
  return true;
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  LOG(INFO) << "Modifying record in text index for key: " << key->Str();
  
  absl::MutexLock lock(&index_mutex_);
  
  // Check if key exists
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) {
    LOG(WARNING) << "Key not found for modification: " << key->Str();
    return absl::NotFoundError(
        absl::StrCat("Key `", key->Str(), "` not found"));
  }
  
  // Remove existing entries first
  // TODO: Implement proper removal from radix tree
  // For now, we'll just remove the key from tracking and re-add
  InternedStringPtr old_value = it->second;
  tracked_keys_.erase(it);
  // Remove from shared TextIndex if available
  // TODO: Implement proper removal from shared TextIndex
  
  // Add the new record
  auto result = AddRecord(key, data);
  
  if (result.ok() && result.value()) {
    LOG(INFO) << "Successfully modified record for key: " << key->Str();
  } else {
    LOG(ERROR) << "Failed to modify record for key: " << key->Str();
    // Restore old value on failure
    tracked_keys_[key] = old_value;
  }
  
  return result;
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr& key,
                                        DeletionType deletion_type) {
  LOG(INFO) << "Removing record from text index for key: " << key->Str();
  
  absl::MutexLock lock(&index_mutex_);
  
  if (deletion_type == DeletionType::kRecord) {
    // If key is DELETED, remove it from untracked_keys_
    untracked_keys_.erase(key);
  } else {
    // If key doesn't have TEXT but exists, insert it to untracked_keys_
    untracked_keys_.insert(key);
  }
  
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) {
    LOG(WARNING) << "Key not found for removal: " << key->Str();
    return false;
  }
  
  // TODO: Remove from radix tree using Mutate function
  // This would involve processing the text again to identify words to remove
  // For now, we'll just remove from our tracking structures
  
  tracked_keys_.erase(it);
  // Remove from shared TextIndex if available  
  // TODO: Implement proper removal from shared TextIndex
  
  LOG(INFO) << "Successfully removed record for key: " << key->Str();
  
  return true;
}

int Text::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "TEXT");
  ValkeyModule_ReplyWithSimpleString(ctx, "size");
  absl::MutexLock lock(&index_mutex_);
  ValkeyModule_ReplyWithCString(ctx,
                                std::to_string(tracked_keys_.size()).c_str());
  return 4;
}

bool Text::IsTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.contains(key);
}

uint64_t Text::GetRecordCount() const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.size();
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto text_index = std::make_unique<data_model::TextIndex>();
  index_proto->set_allocated_text_index(text_index.release());
  return index_proto;
}

InternedStringPtr Text::GetRawValue(const InternedStringPtr& key) const {
  // Note that the Text index is not mutated while the time sliced mutex is
  // in a read mode and therefore it is safe to skip lock acquiring.
  if (auto it = tracked_keys_.find(key); it != tracked_keys_.end()) {
    return it->second;
  }
  return nullptr;
}

std::unique_ptr<Text::EntriesFetcher> Text::Search(
    const query::TextPredicate& predicate, bool negate) const {
  // TODO: Implement text search using radix tree
  // This should use the radix tree's WordIterator to find matching words
  // and return the associated keys
  
  LOG(INFO) << "Text search not fully implemented yet";
  
  // Return empty fetcher for now
  // In a complete implementation, this would:
  // 1. Use the predicate to determine search terms
  // 2. Query the radix tree for matching words
  // 3. Collect all keys associated with those words
  // 4. Return an EntriesFetcher with the results
  
  return nullptr;
}

// Placeholder implementations for inner classes
bool Text::EntriesFetcherIterator::Done() const {
  // TODO: Implement iterator logic
  return true;
}

void Text::EntriesFetcherIterator::Next() {
  // TODO: Implement iterator logic
}

const InternedStringPtr& Text::EntriesFetcherIterator::operator*() const {
  // TODO: Implement iterator logic
  static InternedStringPtr dummy;
  return dummy;
}

size_t Text::EntriesFetcher::Size() const {
  // TODO: Implement fetcher size
  return 0;
}

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  // TODO: Implement fetcher begin
  return nullptr;
}

}  // namespace valkey_search::indexes

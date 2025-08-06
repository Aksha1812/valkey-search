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

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_H_

#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/indexes/text_index.h"
#include "src/index_schema.pb.h"
#include "src/utils/string_interning.h"
#include "src/query/predicate.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
class IndexSchema;
}

namespace valkey_search::indexes {

// Position type for word positions in text
using Position = uint32_t;

// Forward declarations
namespace query {
class TextPredicate;
}

class Text : public IndexBase {
 public:
  explicit Text(const data_model::TextIndex& text_index_proto, IndexSchema* schema);
  absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                 absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> RemoveRecord(
      const InternedStringPtr& key,
      DeletionType deletion_type = DeletionType::kNone) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                    absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  int RespondWithInfo(ValkeyModuleCtx* ctx) const override;
  bool IsTracked(const InternedStringPtr& key) const override;
  absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const override {
    return absl::OkStatus();
  }

  inline void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const override {
    absl::MutexLock lock(&index_mutex_);
    for (const auto& [key, _] : tracked_keys_) {
      fn(key);
    }
  }
  uint64_t GetRecordCount() const override;
  std::unique_ptr<data_model::Index> ToProto() const override;

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    size_t Size() const override;
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
  };

  virtual std::unique_ptr<EntriesFetcher> Search(
      const query::TextPredicate& predicate,
      bool negate) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  // Reference to parent IndexSchema for shared TextIndex access
  IndexSchema* index_schema_;
  
  // Each text field is assigned a unique number within the containing index
  size_t text_field_number_;
  
  // Track keys and their raw values for text processing
  InternedStringMap<InternedStringPtr> tracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  
  // Untracked keys for negation support  
  InternedStringSet untracked_keys_ ABSL_GUARDED_BY(index_mutex_);

  mutable absl::Mutex index_mutex_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_

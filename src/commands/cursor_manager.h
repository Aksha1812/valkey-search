/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_CURSOR_MANAGER_H
#define VALKEYSEARCH_SRC_COMMANDS_CURSOR_MANAGER_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/commands/ft_aggregate_parser.h"
#include "src/index_schema.h"

namespace valkey_search {
namespace aggregate {

// Metadata needed to serialize remaining cursor rows into RESP replies.
// Copied from AggregateParameters at cursor creation time so the cursor
// outlives the originating command's lifetime.
struct CursorReplyMeta {
  std::vector<AggregateParameters::AttributeRecordInfo> record_info_by_index;
  std::shared_ptr<IndexSchema> index_schema;
  int dialect{2};
};

struct CursorState {
  RecordSet records;
  CursorReplyMeta meta;
  size_t count{100};
  absl::Duration max_idle;
  absl::Time expires_at;
  std::string index_name;
  long long total{0};  // original total result count, reported on every READ

  CursorState(RecordSet records_in, CursorReplyMeta meta_in, size_t count_in,
              absl::Duration max_idle_in, absl::string_view index_name_in,
              long long total_in)
      : records(std::move(records_in)),
        meta(std::move(meta_in)),
        count(count_in),
        max_idle(max_idle_in),
        expires_at(absl::Now() + max_idle_in),
        index_name(index_name_in),
        total(total_in) {}
};

class CursorManager {
 public:
  static CursorManager &Instance();
  static void InitInstance(std::unique_ptr<CursorManager> instance);

  uint64_t Create(RecordSet records, CursorReplyMeta meta, size_t count,
                  absl::Duration max_idle, absl::string_view index_name,
                  long long total);

  struct ReadResult {
    std::vector<RecordPtr> batch;
    const CursorReplyMeta *meta{nullptr};
    long long total{0};
    bool exhausted{false};
  };
  std::optional<ReadResult> Read(uint64_t cursor_id, size_t count);

  bool Delete(uint64_t cursor_id);

  size_t Reset(absl::string_view index_name);

  void PurgeExpired();

  size_t Size() const { return cursors_.size(); }

 private:
  absl::flat_hash_map<uint64_t, CursorState> cursors_;
  uint64_t next_id_{1};

  static std::unique_ptr<CursorManager> instance_;
};

}  // namespace aggregate
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COMMANDS_CURSOR_MANAGER_H

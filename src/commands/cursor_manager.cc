/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/cursor_manager.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "absl/log/check.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace valkey_search {
namespace aggregate {

std::unique_ptr<CursorManager> CursorManager::instance_;

CursorManager& CursorManager::Instance() {
  CHECK(instance_ != nullptr);
  return *instance_;
}

void CursorManager::InitInstance(std::unique_ptr<CursorManager> instance) {
  instance_ = std::move(instance);
}

uint64_t CursorManager::Create(RecordSet records, CursorReplyMeta meta,
                               size_t count, absl::Duration max_idle,
                               absl::string_view index_name, long long total) {
  uint64_t id = next_id_++;
  if (next_id_ == 0) {
    next_id_ = 1;  // skip 0
  }
  cursors_.emplace(std::piecewise_construct, std::forward_as_tuple(id),
                   std::forward_as_tuple(std::move(records), std::move(meta),
                                         count, max_idle, index_name, total));
  return id;
}

std::optional<CursorManager::ReadResult> CursorManager::Read(uint64_t cursor_id,
                                                             size_t count) {
  auto it = cursors_.find(cursor_id);
  if (it == cursors_.end()) {
    return std::nullopt;
  }
  CursorState& state = it->second;
  if (absl::Now() >= state.expires_at) {
    cursors_.erase(it);
    return std::nullopt;
  }

  if (count == 0) {
    count = state.count;
  }

  ReadResult result;
  result.total = state.total;

  size_t taken = 0;
  while (taken < count && !state.records.empty()) {
    result.batch.push_back(state.records.pop_front());
    ++taken;
  }

  if (state.records.empty()) {
    result.exhausted = true;
    result.meta = nullptr;
    cursors_.erase(it);
  } else {
    state.expires_at = absl::Now() + state.max_idle;
    result.meta = &it->second.meta;
  }

  return result;
}

bool CursorManager::Delete(uint64_t cursor_id) {
  return cursors_.erase(cursor_id) > 0;
}

size_t CursorManager::Reset(absl::string_view index_name) {
  std::vector<uint64_t> to_erase;
  for (const auto &[id, state] : cursors_) {
    if (state.index_name == index_name) {
      to_erase.push_back(id);
    }
  }
  for (uint64_t id : to_erase) {
    cursors_.erase(id);
  }
  return to_erase.size();
}

void CursorManager::PurgeExpired() {
  absl::Time now = absl::Now();
  std::vector<uint64_t> to_erase;
  for (const auto &[id, state] : cursors_) {
    if (now >= state.expires_at) {
      to_erase.push_back(id);
    }
  }
  for (uint64_t id : to_erase) {
    cursors_.erase(id);
  }
}

}  // namespace aggregate
}  // namespace valkey_search

/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/defrag_coordinator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/defrag.h"
#include "src/keyspace_event_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::defrag {

namespace {

// Cursor field widths; see the diagram in defrag_coordinator.h.
constexpr int kCounterBits = 36;
constexpr int kPhaseBits = 4;
constexpr uint64_t kCounterMask = (1ULL << kCounterBits) - 1;
constexpr uint64_t kPhaseMask = (1ULL << kPhaseBits) - 1;

// Collects keys produced by ValkeyModule_Scan for one bounded chunk.
struct ScanCollector {
  std::vector<std::string> keys;
  uint32_t limit = 0;
};

void ScanCallback(ValkeyModuleCtx *ctx, ValkeyModuleString *keyname,
                  ValkeyModuleKey *key, void *privdata) {
  auto *collector = static_cast<ScanCollector *>(privdata);
  if (collector->keys.size() >= collector->limit) {
    return;
  }
  collector->keys.emplace_back(vmsdk::ToStringView(keyname));
}

}  // namespace

uint64_t CursorState::Encode() const {
  if (phase == Phase::kDone) {
    // Done must encode to exactly 0: that is what core reads as "finished".
    return 0;
  }
  return (static_cast<uint64_t>(generation) << (kCounterBits + kPhaseBits)) |
         ((static_cast<uint64_t>(phase) & kPhaseMask) << kCounterBits) |
         (counter & kCounterMask);
}

CursorState CursorState::Decode(uint64_t cursor) {
  CursorState state;
  if (cursor == 0) {
    return state;  // kDone
  }
  state.counter = cursor & kCounterMask;
  state.phase = static_cast<Phase>((cursor >> kCounterBits) & kPhaseMask);
  state.generation =
      static_cast<uint32_t>(cursor >> (kCounterBits + kPhaseBits));
  return state;
}

std::vector<std::string> DefragCoordinator::ScanNextKeys(ValkeyModuleCtx *ctx,
                                                         uint32_t limit) {
  std::vector<std::string> keys;
  if (ctx == nullptr || limit == 0) {
    return keys;
  }
  if (scan_cursor_ == nullptr) {
    scan_cursor_ = vmsdk::MakeUniqueValkeyScanCursor();
  }
  ScanCollector collector;
  collector.limit = limit;
  // ValkeyModule_Scan returns 0 once the iteration is exhausted. Each call
  // advances the cursor by roughly one hash-table bucket, so we loop until we
  // have a chunk or the keyspace is done.
  while (collector.keys.size() < limit) {
    if (!ValkeyModule_Scan(ctx, scan_cursor_.get(), ScanCallback, &collector)) {
      break;  // exhausted
    }
  }
  return collector.keys;
}

bool DefragCoordinator::ReingestKey(ValkeyModuleCtx *ctx,
                                    absl::string_view key) {
  if (ctx == nullptr) {
    return false;
  }
  auto key_str = vmsdk::MakeUniqueValkeyString(key);

  // Reingestion is expressed as "this key changed". That routes it down the
  // ordinary mutation path, which is already incremental, already runs on the
  // background pool, and already knows how to rebuild every index type. No
  // defrag-specific rebuild logic is needed.
  //
  // Why this defragments rather than merely churns: the mutation path builds
  // the replacement index entry from the source key and only releases the
  // previous entry once the new one is in place. The old allocation is
  // therefore still live when the new one is requested, so jemalloc's thread
  // cache cannot hand the same block back and the rebuilt entry lands in a
  // different, denser slab. Inverting that order (free first, allocate second)
  // would return the block we just released and the pass would accomplish
  // nothing. See the tcache section in defrag_coordinator.h and
  // valkey-io/valkey#1210.
  KeyspaceEventManager::Instance().NotifySubscribers(
      ctx, VALKEYMODULE_NOTIFY_HASH, "hset", key_str.get());
  return true;
}

uint32_t DefragCoordinator::MutationQueueDepth() const {
  auto *pool = ValkeySearch::Instance().GetWriterThreadPool();
  if (pool == nullptr) {
    return 0;
  }
  return static_cast<uint32_t>(pool->QueueSize());
}

void DefragCoordinator::ResetScan() { scan_cursor_.reset(); }

uint64_t DefragCoordinator::Step(ValkeyModuleCtx *ctx, uint64_t cursor_in,
                                 ShouldStopFn should_stop,
                                 void *should_stop_arg) {
  CursorState state = CursorState::Decode(cursor_in);
  Budget budget;
  {
    absl::MutexLock lock(&mu_);
    budget = budget_;

    if (state.phase == Phase::kDone) {
      // Fresh pass. Stamp the current generation so that if this pass is later
      // invalidated, the cursor we hand out stops being honoured.
      state.generation = generation_;
      state.phase = Phase::kScanning;
      state.counter = 0;
      stats_.passes_started++;
      ResetScan();
    } else if (state.generation != generation_) {
      // A cursor from a pass that no longer exists: module reload, FLUSHALL, or
      // an explicit Invalidate(). Its counter refers to a scan cursor we no
      // longer have, so restart rather than trust it.
      stats_.stale_cursors_rejected++;
      state.generation = generation_;
      state.phase = Phase::kScanning;
      state.counter = 0;
      ResetScan();
      stats_.passes_started++;
    }
  }

  if (state.phase == Phase::kScanning) {
    // Backpressure gate. Checked once per invocation rather than per key: the
    // point is to stay out of the way when the pool is busy with user writes,
    // and re-reading the depth per key would just add contention.
    if (MutationQueueDepth() >= budget.pool_backpressure_depth) {
      absl::MutexLock lock(&mu_);
      stats_.backpressure_deferrals++;
      return state.Encode();  // non-zero: come back to us
    }

    uint32_t scanned = 0;
    while (scanned < budget.keys_per_invocation) {
      // Poll the deadline between chunks, not only at the top. Being able to
      // stop here and resume is the entire reason the cursor exists.
      if (should_stop != nullptr && should_stop(should_stop_arg)) {
        absl::MutexLock lock(&mu_);
        stats_.deadline_yields++;
        return state.Encode();
      }

      const uint32_t chunk =
          std::min<uint32_t>(32, budget.keys_per_invocation - scanned);
      std::vector<std::string> keys = ScanNextKeys(ctx, chunk);
      if (keys.empty()) {
        // Keyspace exhausted: the queueing half of the pass is done, but the
        // work we queued may still be running.
        state.phase = Phase::kDraining;
        break;
      }

      for (const auto &key : keys) {
        scanned++;
        state.counter++;
        const bool queued = ReingestKey(ctx, key);
        absl::MutexLock lock(&mu_);
        stats_.keys_scanned++;
        if (queued) {
          stats_.keys_reingested++;
        }
      }
    }

    if (state.phase == Phase::kScanning) {
      // Budget spent but keyspace not exhausted; resume next invocation.
      return state.Encode();
    }
  }

  if (state.phase == Phase::kDraining) {
    // The mutation pool's queue depth is the drain signal. While it is
    // non-empty there is reingestion still to run, and core cannot see that
    // work, so we report "not finished" by keeping the cursor non-zero. This is
    // precisely the case the cursor-as-done-signal contract exists to express.
    if (MutationQueueDepth() > 0) {
      absl::MutexLock lock(&mu_);
      stats_.drain_waits++;
      return state.Encode();
    }
    absl::MutexLock lock(&mu_);
    stats_.passes_completed++;
    ResetScan();
  }

  // Done. Encoding kDone yields 0, which is the only thing that stops core
  // rescheduling the global defrag stage.
  return CursorState{}.Encode();
}

CoordinatorStats DefragCoordinator::GetStats() const {
  absl::MutexLock lock(&mu_);
  return stats_;
}

void DefragCoordinator::ResetStats() {
  absl::MutexLock lock(&mu_);
  stats_ = CoordinatorStats{};
}

void DefragCoordinator::Invalidate() {
  absl::MutexLock lock(&mu_);
  // Bumping the generation is what invalidates outstanding cursors: the next
  // Step() sees a mismatch and restarts instead of resuming against scan state
  // that has been thrown away.
  generation_++;
  ResetScan();
}

void DefragCoordinator::SetBudgetForTesting(Budget budget) {
  absl::MutexLock lock(&mu_);
  budget_ = budget;
}

uint32_t DefragCoordinator::GenerationForTesting() const {
  absl::MutexLock lock(&mu_);
  return generation_;
}

DefragCoordinator &DefragCoordinator::Instance() {
  static DefragCoordinator instance;
  return instance;
}

namespace {

// Bridge matching defrag::WorkFn. The module's long-lived detached context is
// used for the scan and for queueing reingestion; the defrag ctx that core
// passes is only valid for the Defrag* accessors, which is why the deadline
// predicate carries it separately.
uint64_t ReingestionWork(uint64_t cursor_in, bool (*should_stop)(void *),
                         void *arg) {
  return DefragCoordinator::Instance().Step(
      ValkeySearch::Instance().GetBackgroundCtx(), cursor_in, should_stop, arg);
}

}  // namespace

void InstallReingestionWorkFn() { SetWorkFn(&ReingestionWork); }

}  // namespace valkey_search::defrag

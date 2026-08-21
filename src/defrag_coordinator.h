/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_DEFRAG_COORDINATOR_H_
#define VALKEYSEARCH_SRC_DEFRAG_COORDINATOR_H_

#include <cstdint>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Reingestion-based defragmentation of index memory.
//
// ---------------------------------------------------------------------------
// 1. Why reingestion instead of walking pointers
// ---------------------------------------------------------------------------
// The usual way a module defrags is to walk every pointer it owns and hand each
// to ValkeyModule_DefragAlloc(), which relocates the block if jemalloc reports
// it sits in a sparse slab. That works for flat structures. valkey-search's hot
// memory is an HNSW graph: hundreds of thousands of nodes with interior
// pointers and neighbour lists. Walking it would mean teaching every index type
// to enumerate and rewrite its own pointers, under a main-thread deadline, with
// no safe place to stop mid-node.
//
// Reingestion sidesteps that. Rather than moving allocations one at a time, we
// re-drive a key through the ordinary mutation path: the index entry is rebuilt
// from the source key, which allocates fresh blocks, and fresh blocks come from
// jemalloc's current dense slabs. Fragmentation is repaired as a side effect of
// normal ingestion instead of by a bespoke pointer walker.
//
// The tradeoff is cost: reingesting a key is far more expensive than swapping a
// pointer. Everything below is therefore about budgeting.
//
// ---------------------------------------------------------------------------
// 2. The jemalloc thread cache (tcache) problem, valkey-io/valkey#1210
// ---------------------------------------------------------------------------
// Naive reingestion accomplishes nothing. jemalloc keeps a per-thread cache of
// recently freed blocks and serves new allocations of the same size class from
// it first:
//
//     free(old_entry)          -> block enters this thread's tcache
//     new_entry = alloc(size)  -> jemalloc returns the SAME block
//
// The entry is rebuilt exactly where it was, the sparse slab stays sparse, and
// the CPU was wasted.
//
// Core avoids this in its own defrag by allocating with MALLOCX_TCACHE_NONE.
// See allocatorDefragAlloc/allocatorDefragFree in valkey's allocator_defrag.c,
// whose comment reads "make sure not to use the thread cache. so that we don't
// get back the same pointers we try to free". Modules that call
// ValkeyModule_DefragAlloc inherit that behaviour. Reingestion does not: it
// frees and allocates through the normal module allocator, where the tcache is
// very much in play.
//
// We handle it by ordering rather than allocator flags:
//
//     ALLOCATE the replacement while the original is still live,
//     THEN release the original.
//
// Because the old block is still in use when the new allocation is requested,
// jemalloc cannot hand it back, so the rebuilt entry necessarily lands
// elsewhere. This is the same reason core's activeDefragAllocWithoutFree()
// copies before freeing. It requires no new API, holds for any allocator, and
// being a property of the sequence it cannot be silently lost the way a flag
// can. ReingestKey() documents where this ordering is relied upon.
//
// ---------------------------------------------------------------------------
// 3. Two cursors, and why the defrag cursor cannot hold the scan position
// ---------------------------------------------------------------------------
// Enumerating keys uses ValkeyModule_Scan, which has its own opaque
// ValkeyModuleScanCursor. Core's defrag cursor is a single unsigned long. An
// opaque scan cursor does not fit in it, so the two cannot be merged:
//
//     ValkeyModuleScanCursor  - owned by this object, holds the real position.
//     unsigned long (core)    - holds generation + phase only.
//
// That split is why generation stamping matters. The scan cursor lives outside
// the value core persists, so a defrag cursor could in principle be handed back
// to us after the scan cursor has been destroyed or restarted (module reload,
// FLUSHALL, an abandoned pass). The generation makes that detectable: a cursor
// carrying a generation other than the current one is rejected and the pass
// restarts, instead of resuming against a scan cursor that no longer
// corresponds to it.
//
// Cursor layout:
//
//     63                    40 39        36 35                          0
//    +------------------------+------------+-----------------------------+
//    |      generation        |   phase    |          counter            |
//    +------------------------+------------+-----------------------------+
//
// counter is informational (keys scanned so far in this pass); the
// authoritative position is the scan cursor. phase is non-zero for any
// in-progress pass, which keeps the whole word non-zero even when counter is 0
// - important because 0 is reserved to mean "done".
namespace valkey_search::defrag {

// Where a pass is in its lifecycle.
enum class Phase : uint8_t {
  // Nothing in flight. A zero cursor decodes to this.
  kDone = 0,
  // Walking the keyspace, queueing reingestion for the keys we pass.
  kScanning = 1,
  // Scan finished; queued reingestions are still running on the background
  // mutation pool. We keep the cursor non-zero so core calls us back until they
  // drain, because that work is invisible to core's own accounting.
  kDraining = 2,
};

// Decoded form of the cursor core persists for us.
struct CursorState {
  uint32_t generation = 0;
  Phase phase = Phase::kDone;
  uint64_t counter = 0;

  uint64_t Encode() const;
  static CursorState Decode(uint64_t cursor);
};

// Tunables, grouped so the values that govern main-thread cost are visible in
// one place and so tests can shrink them.
struct Budget {
  // Keys examined per callback invocation. Bounds one invocation's cost even
  // before the deadline is consulted.
  uint32_t keys_per_invocation = 128;
  // If the mutation pool's queue is at least this deep, do not queue any
  // reingestion this invocation: the pool is busy with real user writes and
  // adding to it would raise write latency for no urgent gain.
  uint32_t pool_backpressure_depth = 512;
};

struct CoordinatorStats {
  uint64_t passes_started = 0;
  uint64_t passes_completed = 0;
  uint64_t keys_scanned = 0;
  uint64_t keys_reingested = 0;
  uint64_t stale_cursors_rejected = 0;
  uint64_t backpressure_deferrals = 0;
  uint64_t deadline_yields = 0;
  uint64_t drain_waits = 0;
};

// Predicate matching ValkeyModule_DefragShouldStop, injectable for tests.
using ShouldStopFn = bool (*)(void *arg);

// Drives reingestion across defrag invocations.
//
// Threading: Step() runs only on the main thread, inside the core defrag
// callback. Stats are mutex protected because they are also read by
// FT._DEBUG/INFO from command threads.
class DefragCoordinator {
 public:
  explicit DefragCoordinator(Budget budget = {}) : budget_(budget) {}
  virtual ~DefragCoordinator() = default;

  // One core defrag invocation. Decodes `cursor_in`, does a bounded amount of
  // work, returns the cursor to store back. Zero means done.
  uint64_t Step(ValkeyModuleCtx *ctx, uint64_t cursor_in,
                ShouldStopFn should_stop, void *should_stop_arg);

  CoordinatorStats GetStats() const;
  void ResetStats();

  // Abandon any in-progress pass and move the generation on so previously
  // issued cursors are rejected. Call on unload and on FLUSHALL, where a scan
  // position stops being meaningful.
  void Invalidate();

  void SetBudgetForTesting(Budget budget);
  uint32_t GenerationForTesting() const;

  // Process-wide coordinator used in production.
  static DefragCoordinator &Instance();

 protected:
  // Test seams. Base implementations use only existing module/vmsdk APIs.

  // Advance the scan and return up to `limit` keys. An empty result means the
  // keyspace has been exhausted for this pass.
  virtual std::vector<std::string> ScanNextKeys(ValkeyModuleCtx *ctx,
                                                uint32_t limit);

  // Queue one key for reingestion. Returns true if accepted.
  virtual bool ReingestKey(ValkeyModuleCtx *ctx, absl::string_view key);

  // Current depth of the mutation pool's queue; also the drain signal.
  virtual uint32_t MutationQueueDepth() const;

  // Discard scan state so the next pass starts from the beginning.
  virtual void ResetScan();

 private:
  mutable absl::Mutex mu_;
  Budget budget_ ABSL_GUARDED_BY(mu_);
  uint32_t generation_ ABSL_GUARDED_BY(mu_) = 1;
  CoordinatorStats stats_ ABSL_GUARDED_BY(mu_);

  // The authoritative scan position. Created lazily on the first pass; reset
  // when a pass completes or is invalidated. Main thread only.
  vmsdk::UniqueValkeyScanCursor scan_cursor_;
};

// Install the coordinator as the work layer behind the global defrag callback
// (see defrag.h SetWorkFn). Called once during module load, after the module's
// background context and thread pools exist, because the coordinator's scan and
// queueing hooks depend on them.
void InstallReingestionWorkFn();

}  // namespace valkey_search::defrag

#endif  // VALKEYSEARCH_SRC_DEFRAG_COORDINATOR_H_

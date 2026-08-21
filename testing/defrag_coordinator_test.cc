/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Tests for the reingestion coordinator (src/defrag_coordinator.cc).
//
// The coordinator is the part of defrag that has real logic: a state machine
// spread across many short main-thread invocations, driven by a cursor it does
// not fully own. These tests use a subclass that replaces the three production
// hooks (key enumeration, reingestion, mutation-queue depth) with in-memory
// fakes, so the state machine can be driven deterministically without a server.
//
// Cursor encoding:
//   EncodesDoneAsZero - done must be exactly 0, nothing else may be.
//   RoundTripsGenerationPhaseCounter - the packed fields survive a round trip.
//   InProgressCursorIsNeverZero - a pass at counter 0 still encodes non-zero.
//
// Pass lifecycle:
//   ScansAcrossInvocations - budget is respected and the pass resumes.
//   YieldsOnDeadline - a deadline mid-scan stops work and keeps the cursor
//   live. EntersDrainWhenKeyspaceExhausted - scan end moves to draining, not
//   done. StaysNonZeroWhileDraining - queued background work keeps the cursor
//   live. CompletesWhenDrained - an empty queue finally reports done (cursor
//   0).
//
// Safety:
//   RejectsStaleCursorAfterInvalidate - a cursor from a dead pass restarts.
//   DefersUnderBackpressure - a busy mutation pool suppresses queueing.
//   ReingestsEveryScannedKey - every key the scan yields is queued.

#include "src/defrag_coordinator.h"

#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::defrag {

namespace {

// Deadline predicate helpers. The coordinator takes a plain function pointer so
// it stays free of std::function and of the module API.
bool NeverStop(void *) { return false; }
bool AlwaysStop(void *) { return true; }

// Coordinator with the three production hooks replaced by fakes.
class FakeCoordinator : public DefragCoordinator {
 public:
  using DefragCoordinator::DefragCoordinator;

  // Keys the fake keyspace will hand out, consumed front to back.
  std::vector<std::string> pending_keys;
  // Keys that were actually queued for reingestion, in order.
  std::vector<std::string> reingested;
  // What MutationQueueDepth() reports.
  uint32_t queue_depth = 0;
  int scan_resets = 0;

 protected:
  std::vector<std::string> ScanNextKeys(ValkeyModuleCtx *,
                                        uint32_t limit) override {
    std::vector<std::string> out;
    while (!pending_keys.empty() && out.size() < limit) {
      out.push_back(pending_keys.front());
      pending_keys.erase(pending_keys.begin());
    }
    return out;
  }

  bool ReingestKey(ValkeyModuleCtx *, absl::string_view key) override {
    reingested.emplace_back(key);
    return true;
  }

  uint32_t MutationQueueDepth() const override { return queue_depth; }

  void ResetScan() override { scan_resets++; }
};

class DefragCoordinatorTest : public vmsdk::ValkeyTest {
 protected:
  // A non-null stand-in; the fakes never dereference it.
  ValkeyModuleCtx *Ctx() {
    return reinterpret_cast<ValkeyModuleCtx *>(&ctx_storage_);
  }

  // Small budgets keep the tests fast and make the boundaries obvious.
  Budget SmallBudget() {
    Budget b;
    b.keys_per_invocation = 4;
    b.pool_backpressure_depth = 100;
    return b;
  }

 private:
  int ctx_storage_ = 0;
};

// --- Cursor encoding ------------------------------------------------------

// Zero is reserved. Core reads a stored cursor of 0 as "this module is
// finished", so kDone must encode to exactly 0 and nothing else may collide
// with it.
TEST_F(DefragCoordinatorTest, EncodesDoneAsZero) {
  CursorState done;
  done.phase = Phase::kDone;
  EXPECT_EQ(done.Encode(), 0u);

  const CursorState decoded = CursorState::Decode(0);
  EXPECT_EQ(decoded.phase, Phase::kDone);
}

TEST_F(DefragCoordinatorTest, RoundTripsGenerationPhaseCounter) {
  CursorState in;
  in.generation = 7;
  in.phase = Phase::kScanning;
  in.counter = 123456;

  const CursorState out = CursorState::Decode(in.Encode());
  EXPECT_EQ(out.generation, 7u);
  EXPECT_EQ(out.phase, Phase::kScanning);
  EXPECT_EQ(out.counter, 123456u);
}

// A pass that has just started sits at counter 0. If the encoding did not set
// the phase bits, the whole word would be 0 and core would think we had
// finished before doing anything. This is the subtle case the layout guards.
TEST_F(DefragCoordinatorTest, InProgressCursorIsNeverZero) {
  CursorState fresh;
  fresh.generation = 1;
  fresh.phase = Phase::kScanning;
  fresh.counter = 0;
  EXPECT_NE(fresh.Encode(), 0u);
}

// --- Pass lifecycle -------------------------------------------------------

// The per-invocation key budget bounds main-thread cost, so one invocation must
// stop at the budget and the next must pick up where it stopped.
TEST_F(DefragCoordinatorTest, ScansAcrossInvocations) {
  FakeCoordinator c(SmallBudget());
  c.pending_keys = {"k1", "k2", "k3", "k4", "k5", "k6"};

  const uint64_t after_first = c.Step(Ctx(), 0, &NeverStop, nullptr);
  EXPECT_NE(after_first, 0u) << "pass is not finished, cursor must stay live";
  EXPECT_EQ(c.reingested.size(), 4u) << "budget is 4 keys per invocation";

  const uint64_t after_second = c.Step(Ctx(), after_first, &NeverStop, nullptr);
  EXPECT_EQ(c.reingested.size(), 6u) << "resumed and consumed the remainder";
  // Keyspace exhausted and nothing queued in the fake pool, so this completes.
  EXPECT_EQ(after_second, 0u);
  EXPECT_EQ(c.GetStats().passes_completed, 1u);
}

// The callback shares the main thread with user traffic. When the deadline
// fires, work must stop immediately and the cursor must stay non-zero so the
// remainder is picked up later rather than dropped.
TEST_F(DefragCoordinatorTest, YieldsOnDeadline) {
  FakeCoordinator c(SmallBudget());
  c.pending_keys = {"k1", "k2"};

  const uint64_t cursor = c.Step(Ctx(), 0, &AlwaysStop, nullptr);

  EXPECT_NE(cursor, 0u) << "must be resumable, not reported as done";
  EXPECT_TRUE(c.reingested.empty()) << "deadline fired before any work";
  EXPECT_EQ(c.GetStats().deadline_yields, 1u);
}

// Running out of keys does not mean the pass is over: reingestion we queued is
// still executing on the background pool. The pass must move to draining.
TEST_F(DefragCoordinatorTest, EntersDrainWhenKeyspaceExhausted) {
  FakeCoordinator c(SmallBudget());
  c.pending_keys = {"only"};
  c.queue_depth = 5;  // background work still outstanding

  const uint64_t cursor = c.Step(Ctx(), 0, &NeverStop, nullptr);

  ASSERT_NE(cursor, 0u);
  EXPECT_EQ(CursorState::Decode(cursor).phase, Phase::kDraining);
}

// While background reingestion is outstanding we must keep reporting "not
// done". Core cannot see that work, so the cursor is the only channel that
// expresses it. Reporting done here would end the cycle with work still in
// flight.
TEST_F(DefragCoordinatorTest, StaysNonZeroWhileDraining) {
  FakeCoordinator c(SmallBudget());
  c.pending_keys = {};  // scan already exhausted
  c.queue_depth = 3;    // still draining

  uint64_t cursor = c.Step(Ctx(), 0, &NeverStop, nullptr);
  for (int i = 0; i < 3; ++i) {
    cursor = c.Step(Ctx(), cursor, &NeverStop, nullptr);
    EXPECT_NE(cursor, 0u) << "queue is still non-empty on iteration " << i;
  }
  EXPECT_GT(c.GetStats().drain_waits, 0u);
}

// Once the queue empties the pass finally completes, and completion must encode
// to 0 so core stops rescheduling the stage. This is the termination guarantee.
TEST_F(DefragCoordinatorTest, CompletesWhenDrained) {
  FakeCoordinator c(SmallBudget());
  c.pending_keys = {};
  c.queue_depth = 1;

  uint64_t cursor = c.Step(Ctx(), 0, &NeverStop, nullptr);
  ASSERT_NE(cursor, 0u);

  c.queue_depth = 0;  // background work finished
  cursor = c.Step(Ctx(), cursor, &NeverStop, nullptr);

  EXPECT_EQ(cursor, 0u);
  EXPECT_EQ(c.GetStats().passes_completed, 1u);
}

// --- Safety ---------------------------------------------------------------

// The authoritative scan position lives in a ValkeyModuleScanCursor outside the
// value core persists. If that state is thrown away (module reload, FLUSHALL) a
// previously issued cursor becomes meaningless, and resuming against it would
// silently scan the wrong thing. The generation stamp makes it detectable.
TEST_F(DefragCoordinatorTest, RejectsStaleCursorAfterInvalidate) {
  FakeCoordinator c(SmallBudget());
  c.pending_keys = {"k1", "k2", "k3", "k4", "k5"};

  const uint64_t stale = c.Step(Ctx(), 0, &NeverStop, nullptr);
  ASSERT_NE(stale, 0u);
  const uint32_t generation_before = c.GenerationForTesting();

  c.Invalidate();
  EXPECT_NE(c.GenerationForTesting(), generation_before);

  // Feeding the pre-invalidate cursor back must not be honoured.
  c.Step(Ctx(), stale, &NeverStop, nullptr);
  EXPECT_EQ(c.GetStats().stale_cursors_rejected, 1u);
}

// Reingestion is best-effort background work; ordinary writes are not. When the
// mutation pool is already deep, queueing more would raise user-visible write
// latency, so this invocation must do nothing but stay resumable.
TEST_F(DefragCoordinatorTest, DefersUnderBackpressure) {
  Budget b = SmallBudget();
  b.pool_backpressure_depth = 10;
  FakeCoordinator c(b);
  c.pending_keys = {"k1", "k2"};
  c.queue_depth = 50;  // well over the threshold

  const uint64_t cursor = c.Step(Ctx(), 0, &NeverStop, nullptr);

  EXPECT_NE(cursor, 0u) << "deferred, not finished";
  EXPECT_TRUE(c.reingested.empty()) << "nothing queued while pool is busy";
  EXPECT_EQ(c.GetStats().backpressure_deferrals, 1u);
}

// Every key the scan yields must be handed to reingestion; a key silently
// skipped is memory that never gets defragmented.
TEST_F(DefragCoordinatorTest, ReingestsEveryScannedKey) {
  FakeCoordinator c(SmallBudget());
  c.pending_keys = {"a", "b", "c"};

  c.Step(Ctx(), 0, &NeverStop, nullptr);

  EXPECT_EQ(c.reingested, std::vector<std::string>({"a", "b", "c"}));
  EXPECT_EQ(c.GetStats().keys_scanned, 3u);
  EXPECT_EQ(c.GetStats().keys_reingested, 3u);
}

}  // namespace

}  // namespace valkey_search::defrag

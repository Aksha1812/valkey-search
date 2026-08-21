/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Tests for the module-global defrag callback (src/defrag.cc).
//
// These cover every element of the contract core imposes on a global defrag
// callback, one test per element, so a failure points at a specific broken
// behavior rather than "defrag is broken".
//
// Registration:
//   RegistersWithCore - we hand our callback to core.
//   SkipsRegistrationOnOlderCore - absent API is tolerated, not fatal.
//
// Per-invocation protocol:
//   ReadsPersistentCursor - we read the cursor core gave us.
//   PollsDeadline - we ask core whether time is up.
//   CompletesPassWhenTimeRemains - finishing stores cursor 0 ("done").
//   PassesWorkLayerCursorThroughToCore - the work layer's cursor is forwarded.
//   HandsStoredCursorToWorkLayer - the stored cursor reaches the work layer.
//   WorkLayerCanPollTheDeadline - the deadline predicate is wired to core.
//   ToleratesMissingCursor - a core that gives no cursor still works.
//
// Bookkeeping:
//   CountsEveryInvocation - invocation counter tracks calls.
//   ResetStatsClearsCounters - FT._DEBUG DEFRAG_STATS RESET works.
//
// The ValkeyModuleDefragCtx type is opaque (core owns its layout), so tests
// pass an arbitrary non-null pointer as the ctx: everything the callback does
// with it goes through the mocked ValkeyModule_Defrag* accessors.

#include "src/defrag.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::defrag {

namespace {

using testing::_;
using testing::Return;
using testing::SetArgPointee;

class DefragTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ResetStats();
    // No work layer by default: these tests are about the protocol, and an
    // uninstalled work layer means "complete immediately".
    SetWorkFn(nullptr);
  }

  void TearDown() override {
    SetWorkFn(nullptr);  // don't leak an installed fake into other tests
    vmsdk::ValkeyTest::TearDown();
  }

  // Stand-in for the opaque context core passes in. Never dereferenced by the
  // callback; it only travels back into the mocked accessors.
  ValkeyModuleDefragCtx *FakeDefragCtx() {
    return reinterpret_cast<ValkeyModuleDefragCtx *>(&ctx_storage_);
  }

  // Convenience: make DefragShouldStop answer `stop` for this invocation.
  void ExpectDeadline(bool stop) {
    EXPECT_CALL(*kMockValkeyModule, DefragShouldStop(FakeDefragCtx()))
        .WillRepeatedly(Return(stop ? 1 : 0));
  }

  // Convenience: make DefragCursorGet report `cursor` and succeed.
  void ExpectCursorGet(unsigned long cursor) {  // NOLINT(runtime/int)
    EXPECT_CALL(*kMockValkeyModule, DefragCursorGet(FakeDefragCtx(), _))
        .WillRepeatedly(
            testing::DoAll(SetArgPointee<1>(cursor), Return(VALKEYMODULE_OK)));
  }

 private:
  int ctx_storage_ = 0;
};

// --- Registration ----------------------------------------------------------

// The whole feature hinges on core being handed our callback, so assert the
// exact function pointer is what gets registered.
TEST_F(DefragTest, RegistersWithCore) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule,
              RegisterDefragFunc(&fake_ctx, &OnGlobalDefragCallback))
      .WillOnce(Return(VALKEYMODULE_OK));

  EXPECT_TRUE(RegisterGlobalDefragCallback(&fake_ctx));
}

// Module API pointers are resolved from the running server, so a core without
// module global defrag leaves RegisterDefragFunc null. That must degrade to "we
// don't participate in defrag", never to a load failure or a null call.
TEST_F(DefragTest, SkipsRegistrationOnOlderCore) {
  auto *saved = ValkeyModule_RegisterDefragFunc;
  ValkeyModule_RegisterDefragFunc = nullptr;

  ValkeyModuleCtx fake_ctx;
  EXPECT_FALSE(RegisterGlobalDefragCallback(&fake_ctx));

  ValkeyModule_RegisterDefragFunc = saved;
}

// --- Per-invocation protocol ----------------------------------------------

// Core gives each module a cursor that survives between invocations. Reading it
// is how a pass knows where it left off. Before the core-side change this
// returned an error (global callbacks were invoked with cursor == NULL), so
// this assertion is what distinguishes a working core from a stale one.
TEST_F(DefragTest, ReadsPersistentCursor) {
  ExpectCursorGet(0);
  ExpectDeadline(false);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), _))
      .WillRepeatedly(Return(VALKEYMODULE_OK));

  OnGlobalDefragCallback(FakeDefragCtx());

  EXPECT_EQ(GetStats().cursor_reads, 1u);
}

// The callback runs on the main thread under a deadline. It must poll that
// deadline rather than assume it has unlimited time.
TEST_F(DefragTest, PollsDeadline) {
  ExpectCursorGet(0);
  ExpectDeadline(false);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), _))
      .WillRepeatedly(Return(VALKEYMODULE_OK));

  OnGlobalDefragCallback(FakeDefragCtx());

  EXPECT_EQ(GetStats().deadline_checks, 1u);
  EXPECT_EQ(GetStats().incomplete_returns, 0u);
}

// Finishing a pass MUST store a cursor of 0. That zero is the only thing that
// tells core this module is done; without it core reschedules the global defrag
// stage forever. Asserting the exact value 0 is the point of this test.
TEST_F(DefragTest, CompletesPassWhenTimeRemains) {
  ExpectCursorGet(0);
  ExpectDeadline(/*stop=*/false);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), 0u))
      .WillOnce(Return(VALKEYMODULE_OK));

  OnGlobalDefragCallback(FakeDefragCtx());

  EXPECT_EQ(GetStats().completed_passes, 1u);
}

// This file owns the protocol, not the work. Whatever the installed work layer
// returns must be handed to core verbatim: a non-zero result means "call me
// again" and must not be recorded as a completed pass.
TEST_F(DefragTest, PassesWorkLayerCursorThroughToCore) {
  ExpectCursorGet(0);
  ExpectDeadline(false);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), 12345u))
      .WillOnce(Return(VALKEYMODULE_OK));
  SetWorkFn([](uint64_t, bool (*)(void *), void *) -> uint64_t {
    return 12345;  // work layer says: more to do
  });

  OnGlobalDefragCallback(FakeDefragCtx());

  EXPECT_EQ(GetStats().completed_passes, 0u);
}

// The cursor core stored must reach the work layer unchanged; that value is the
// only way a resumed pass knows where it was.
TEST_F(DefragTest, HandsStoredCursorToWorkLayer) {
  ExpectCursorGet(41);
  ExpectDeadline(false);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), _))
      .WillRepeatedly(Return(VALKEYMODULE_OK));
  static uint64_t seen = 0;
  seen = 0;
  SetWorkFn([](uint64_t cursor_in, bool (*)(void *), void *) -> uint64_t {
    seen = cursor_in;
    return 0;
  });

  OnGlobalDefragCallback(FakeDefragCtx());

  EXPECT_EQ(seen, 41u) << "work layer must receive the cursor core stored";
  EXPECT_EQ(GetStats().cursor_reads, 1u);
}

// The work layer is handed a deadline predicate so it can poll core's deadline
// between chunks. Verify it is wired to the real DefragShouldStop.
TEST_F(DefragTest, WorkLayerCanPollTheDeadline) {
  ExpectCursorGet(0);
  ExpectDeadline(/*stop=*/true);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), _))
      .WillRepeatedly(Return(VALKEYMODULE_OK));
  static bool observed = false;
  observed = false;
  SetWorkFn([](uint64_t, bool (*should_stop)(void *), void *arg) -> uint64_t {
    observed = should_stop(arg);
    return 0;
  });

  OnGlobalDefragCallback(FakeDefragCtx());

  EXPECT_TRUE(observed) << "predicate must report the deadline core set";
}

// If core provides no cursor (DefragCursorGet fails), the callback cannot
// resume, but it must still run and complete rather than misbehave.
TEST_F(DefragTest, ToleratesMissingCursor) {
  EXPECT_CALL(*kMockValkeyModule, DefragCursorGet(FakeDefragCtx(), _))
      .WillRepeatedly(Return(VALKEYMODULE_ERR));
  ExpectDeadline(/*stop=*/false);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), 0u))
      .WillRepeatedly(Return(VALKEYMODULE_OK));

  OnGlobalDefragCallback(FakeDefragCtx());

  EXPECT_EQ(GetStats().cursor_reads, 0u);
  EXPECT_EQ(GetStats().callback_invocations, 1u);
  EXPECT_EQ(GetStats().completed_passes, 1u);
}

// --- Bookkeeping ----------------------------------------------------------

// The invocation counter is what an end-to-end test watches to confirm core is
// really driving the callback, so it must count every call.
TEST_F(DefragTest, CountsEveryInvocation) {
  ExpectCursorGet(0);
  ExpectDeadline(false);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), _))
      .WillRepeatedly(Return(VALKEYMODULE_OK));

  for (int i = 0; i < 3; ++i) {
    OnGlobalDefragCallback(FakeDefragCtx());
  }

  EXPECT_EQ(GetStats().callback_invocations, 3u);
}

// FT._DEBUG DEFRAG_STATS RESET relies on this, so that a test can measure a
// specific window instead of a total since module load.
TEST_F(DefragTest, ResetStatsClearsCounters) {
  ExpectCursorGet(0);
  ExpectDeadline(false);
  EXPECT_CALL(*kMockValkeyModule, DefragCursorSet(FakeDefragCtx(), _))
      .WillRepeatedly(Return(VALKEYMODULE_OK));
  OnGlobalDefragCallback(FakeDefragCtx());
  ASSERT_GT(GetStats().callback_invocations, 0u);

  ResetStats();

  const Stats stats = GetStats();
  EXPECT_EQ(stats.callback_invocations, 0u);
  EXPECT_EQ(stats.cursor_reads, 0u);
  EXPECT_EQ(stats.deadline_checks, 0u);
  EXPECT_EQ(stats.incomplete_returns, 0u);
  EXPECT_EQ(stats.completed_passes, 0u);
}

}  // namespace

}  // namespace valkey_search::defrag

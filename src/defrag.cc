/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/defrag.h"

#include <atomic>
#include <cstdint>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::defrag {

namespace {

// Counters backing Stats. Atomic because the callback runs on the main thread
// while FT._DEBUG DEFRAG_STATS reads them from a command thread.
std::atomic<uint64_t> callback_invocations{0};
std::atomic<uint64_t> cursor_reads{0};
std::atomic<uint64_t> deadline_checks{0};
std::atomic<uint64_t> incomplete_returns{0};
std::atomic<uint64_t> completed_passes{0};

}  // namespace

Stats GetStats() {
  Stats s;
  s.callback_invocations = callback_invocations.load(std::memory_order_relaxed);
  s.cursor_reads = cursor_reads.load(std::memory_order_relaxed);
  s.deadline_checks = deadline_checks.load(std::memory_order_relaxed);
  s.incomplete_returns = incomplete_returns.load(std::memory_order_relaxed);
  s.completed_passes = completed_passes.load(std::memory_order_relaxed);
  return s;
}

void ResetStats() {
  callback_invocations.store(0, std::memory_order_relaxed);
  cursor_reads.store(0, std::memory_order_relaxed);
  deadline_checks.store(0, std::memory_order_relaxed);
  incomplete_returns.store(0, std::memory_order_relaxed);
  completed_passes.store(0, std::memory_order_relaxed);
}

namespace {

// The work layer, installed at registration time. Kept as a seam rather than a
// direct call so that this file stays purely about the core protocol: it can be
// tested (and can run) with no reingestion machinery behind it, and the
// coordinator can be tested with no module API in front of it.
//
// Null means "no work configured", in which case every pass immediately reports
// done. That is the correct degenerate behaviour, not a failure.
WorkFn work_fn = nullptr;

// Adapter letting the work layer poll the core deadline without depending on
// the module API itself.
bool ShouldStopAdapter(void *arg) {
  auto *ctx = static_cast<ValkeyModuleDefragCtx *>(arg);
  return ValkeyModule_DefragShouldStop(ctx) != 0;
}

}  // namespace

void SetWorkFn(WorkFn fn) { work_fn = fn; }

int OnGlobalDefragCallback(ValkeyModuleDefragCtx *ctx) {
  callback_invocations.fetch_add(1, std::memory_order_relaxed);

  // Step 1: recover where the previous invocation stopped. On the first call of
  // a pass the stored cursor is 0, meaning "start a new pass". DefragCursorGet
  // fails only if core gave us no cursor at all; treat that as "start over".
  unsigned long resume_from = 0;  // NOLINT(runtime/int) - core API type
  if (ValkeyModule_DefragCursorGet(ctx, &resume_from) == VALKEYMODULE_OK) {
    cursor_reads.fetch_add(1, std::memory_order_relaxed);
  } else {
    resume_from = 0;
  }

  // Step 2: do a bounded amount of work. The work layer polls the deadline
  // between chunks through the adapter, so this returns promptly. With no work
  // layer installed the pass completes immediately.
  deadline_checks.fetch_add(1, std::memory_order_relaxed);
  const unsigned long next =  // NOLINT(runtime/int) - core API type
      work_fn == nullptr ? 0UL
                         : static_cast<unsigned long>(
                               work_fn(resume_from, &ShouldStopAdapter, ctx));

  // Step 3: hand the cursor back. A non-zero value means "call me again": the
  // scan has more keyspace, or queued reingestion is still draining on the
  // background pool. Zero means done, and is the only thing that stops core
  // rescheduling the global defrag stage.
  ValkeyModule_DefragCursorSet(ctx, next);
  if (next == 0) {
    completed_passes.fetch_add(1, std::memory_order_relaxed);
  } else {
    incomplete_returns.fetch_add(1, std::memory_order_relaxed);
  }
  return 0;
}

bool RegisterGlobalDefragCallback(ValkeyModuleCtx *ctx) {
  // Module API pointers are bound from the running server at load time. A core
  // without module global defrag leaves this null, in which case we simply do
  // not register and the module behaves exactly as before.
  if (ValkeyModule_RegisterDefragFunc == nullptr) {
    return false;
  }
  return ValkeyModule_RegisterDefragFunc(ctx, OnGlobalDefragCallback) ==
         VALKEYMODULE_OK;
}

}  // namespace valkey_search::defrag

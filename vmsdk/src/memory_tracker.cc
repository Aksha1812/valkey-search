/*
 * Copyright (c) 2025, valkey-search contributors
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

#include "memory_tracker.h"

#include <cstdio>
#include "vmsdk/src/memory_allocation.h"

MemoryScope::MemoryScope(MemoryPool& pool)
    : target_pool_(pool), baseline_memory_(vmsdk::GetMemoryDelta()) {
  // Debug statement for memory scope creation
  fprintf(stderr, "[MEMORY_DEBUG] MemoryScope created: baseline_memory=%ld, pool_usage=%ld\n", 
          baseline_memory_, target_pool_.GetUsage());
}

IsolatedMemoryScope::IsolatedMemoryScope(MemoryPool& pool)
    : MemoryScope(pool) {
  // Debug statement for isolated memory scope creation
  fprintf(stderr, "[MEMORY_DEBUG] IsolatedMemoryScope created: baseline_memory=%ld\n", 
          baseline_memory_);
}

IsolatedMemoryScope::~IsolatedMemoryScope() {
  int64_t current_delta = vmsdk::GetMemoryDelta();
  int64_t net_change = current_delta - baseline_memory_;
  
  // Debug statement for isolated memory scope destruction
  fprintf(stderr, "[MEMORY_DEBUG] IsolatedMemoryScope destroyed: baseline=%ld, current_delta=%ld, net_change=%ld, pool_before=%ld\n", 
          baseline_memory_, current_delta, net_change, target_pool_.GetUsage());
  
  target_pool_.Add(net_change);

  vmsdk::SetMemoryDelta(baseline_memory_);
  
  // Debug statement after pool update
  fprintf(stderr, "[MEMORY_DEBUG] IsolatedMemoryScope: pool_after=%ld, reset_delta_to=%ld\n", 
          target_pool_.GetUsage(), baseline_memory_);
}

NestedMemoryScope::NestedMemoryScope(MemoryPool& pool) : MemoryScope(pool) {
  // Debug statement for nested memory scope creation
  fprintf(stderr, "[MEMORY_DEBUG] NestedMemoryScope created: baseline_memory=%ld\n", 
          baseline_memory_);
}

NestedMemoryScope::~NestedMemoryScope() {
  int64_t current_delta = vmsdk::GetMemoryDelta();
  int64_t net_change = current_delta - baseline_memory_;
  
  // Debug statement for nested memory scope destruction
  fprintf(stderr, "[MEMORY_DEBUG] NestedMemoryScope destroyed: baseline=%ld, current_delta=%ld, net_change=%ld, pool_before=%ld\n", 
          baseline_memory_, current_delta, net_change, target_pool_.GetUsage());
  
  target_pool_.Add(net_change);
  
  // Debug statement after pool update (delta not reset in nested scope)
  fprintf(stderr, "[MEMORY_DEBUG] NestedMemoryScope: pool_after=%ld, delta_unchanged=%ld\n", 
          target_pool_.GetUsage(), current_delta);
}

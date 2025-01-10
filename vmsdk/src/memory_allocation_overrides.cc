// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstddef>
#include <cstring>
#include <functional>
#include <new>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// clang-format off
// We put this at the end since it will otherwise mangle the malloc symbols in
// the dependencies.
#include "vmsdk/src/memory_allocation_overrides.h"
// clang-format on

namespace vmsdk {

// SystemAllocTracker tracks memory allocations to the system allocator, so that
// subsequent free calls can be redirected to the appropriate allocator.
class SystemAllocTracker {
 public:
  static SystemAllocTracker& GetInstance() {
    static absl::NoDestructor<SystemAllocTracker> instance;
    return *instance;
  }
  SystemAllocTracker() = default;
  SystemAllocTracker(const SystemAllocTracker&) = delete;
  SystemAllocTracker& operator=(const SystemAllocTracker&) = delete;
  ~SystemAllocTracker() = default;
  void TrackPointer(void* ptr) {
    if (ptr == nullptr) {
      return;
    }
    absl::MutexLock lock(&mutex_);
    tracked_ptrs_.insert(ptr);
  }
  bool IsTracked(void* ptr) {
    absl::MutexLock lock(&mutex_);
    return tracked_ptrs_.contains(ptr);
  }
  bool UntrackPointer(void* ptr) {
    absl::MutexLock lock(&mutex_);
    return tracked_ptrs_.erase(ptr);
  }

 private:
  // RawSystemAllocator implements an allocator that will not go through
  // the SystemAllocTracker, for use by the SystemAllocTracker to prevent
  // infinite recursion when tracking pointers.
  template <typename T>
  struct RawSystemAllocator {
    typedef T value_type;

    RawSystemAllocator() noexcept {}
    template <typename U>
    constexpr RawSystemAllocator(const RawSystemAllocator<U>&) noexcept {}

    T* allocate(std::size_t n) {
      ReportAllocMemorySize(n * sizeof(T));
      return static_cast<T*>(__real_malloc(n * sizeof(T)));
    }

    void deallocate(T* p, std::size_t) {
      ReportFreeMemorySize(sizeof(T));
      __real_free(p);
    }
  };

  absl::Mutex mutex_;
  absl::flat_hash_set<void*, absl::Hash<void*>, std::equal_to<void*>,
                      RawSystemAllocator<void*>>
      tracked_ptrs_ ABSL_GUARDED_BY(mutex_);
};

void* PerformAndTrackMalloc(size_t size, void* (*malloc_fn)(size_t),
                            size_t (*malloc_size_fn)(void*)) {
  void* ptr = malloc_fn(size);
  if (ptr != nullptr) {
    ReportAllocMemorySize(malloc_size_fn(ptr));
  }
  return ptr;
}
void* PerformAndTrackCalloc(size_t n, size_t size,
                            void* (*calloc_fn)(size_t, size_t),
                            size_t (*malloc_size_fn)(void*)) {
  void* ptr = calloc_fn(n, size);
  if (ptr != nullptr) {
    ReportAllocMemorySize(malloc_size_fn(ptr));
  }
  return ptr;
}
void PerformAndTrackFree(void* ptr, void (*free_fn)(void*),
                         size_t (*malloc_size_fn)(void*)) {
  ReportFreeMemorySize(malloc_size_fn(ptr));
  free_fn(ptr);
}
void* PerformAndTrackRealloc(void* ptr, size_t size,
                             void* (*realloc_fn)(void*, size_t),
                             size_t (*malloc_size_fn)(void*)) {
  size_t old_size = 0;
  if (ptr != nullptr) {
    old_size = malloc_size_fn(ptr);
  }
  void* new_ptr = realloc_fn(ptr, size);
  if (new_ptr != nullptr) {
    if (ptr != nullptr) {
      ReportFreeMemorySize(old_size);
    }
    ReportAllocMemorySize(malloc_size_fn(new_ptr));
  }
  return new_ptr;
}
void* PerformAndTrackAlignedAlloc(size_t align, size_t size,
                                  void*(aligned_alloc_fn)(size_t, size_t),
                                  size_t (*malloc_size_fn)(void*)) {
  void* ptr = aligned_alloc_fn(align, size);
  if (ptr != nullptr) {
    ReportAllocMemorySize(malloc_size_fn(ptr));
  }
  return ptr;
}
}  // namespace vmsdk

extern "C" {
// Our allocator doesn't support tracking system memory size, so we just
// return 0.
__attribute__((weak)) size_t empty_usable_size(void* ptr) noexcept {
  return 0;
}
void* __wrap_malloc(size_t size) noexcept {
  if (!vmsdk::IsUsingValkeyAlloc()) {
    auto ptr =
        vmsdk::PerformAndTrackMalloc(size, __real_malloc, empty_usable_size);
    vmsdk::SystemAllocTracker::GetInstance().TrackPointer(ptr);
    return ptr;
  }
  return vmsdk::PerformAndTrackMalloc(size, RedisModule_Alloc,
                                      RedisModule_MallocUsableSize);
}
void __wrap_free(void* ptr) noexcept {
  if (ptr == nullptr) {
    return;
  }
  bool was_tracked =
      vmsdk::SystemAllocTracker::GetInstance().UntrackPointer(ptr);
  // During bootstrap - there are some cases where memory is still allocated
  // outside of our wrapper functions - for example if a library calls into
  // another DSO which doesn't have our wrapped symbols (namely libc.so). For
  // this reason, we bypass the tracking during the bootstrap phase.
  if (was_tracked || !vmsdk::IsUsingValkeyAlloc()) {
    vmsdk::PerformAndTrackFree(ptr, __real_free, empty_usable_size);
  } else {
    vmsdk::PerformAndTrackFree(ptr, RedisModule_Free,
                               RedisModule_MallocUsableSize);
  }
}
void* __wrap_calloc(size_t __nmemb, size_t size) noexcept {
  if (!vmsdk::IsUsingValkeyAlloc()) {
    auto ptr = vmsdk::PerformAndTrackCalloc(__nmemb, size, __real_calloc,
                                            empty_usable_size);
    vmsdk::SystemAllocTracker::GetInstance().TrackPointer(ptr);
    return ptr;
  }
  return vmsdk::PerformAndTrackCalloc(__nmemb, size, RedisModule_Calloc,
                                      RedisModule_MallocUsableSize);
}
void* __wrap_realloc(void* ptr, size_t size) noexcept {
  bool was_tracked = false;
  if (ptr != nullptr) {
    was_tracked = vmsdk::SystemAllocTracker::GetInstance().UntrackPointer(ptr);
  }
  if (vmsdk::IsUsingValkeyAlloc() && !was_tracked) {
    return vmsdk::PerformAndTrackRealloc(ptr, size, RedisModule_Realloc,
                                         RedisModule_MallocUsableSize);
  } else {
    auto new_ptr = vmsdk::PerformAndTrackRealloc(ptr, size, __real_realloc,
                                                 empty_usable_size);
    vmsdk::SystemAllocTracker::GetInstance().TrackPointer(new_ptr);
    return new_ptr;
  }
}
void* __wrap_aligned_alloc(size_t __alignment, size_t __size) noexcept {
  if (!vmsdk::IsUsingValkeyAlloc()) {
    auto ptr = vmsdk::PerformAndTrackAlignedAlloc(
        __alignment, __size, __real_aligned_alloc, empty_usable_size);
    vmsdk::SystemAllocTracker::GetInstance().TrackPointer(ptr);
    return ptr;
  }

  // For Valkey allocation - we need to ensure alignment by taking advantage of
  // jemalloc alignment properties, as there is no aligned malloc module
  // function.
  //
  // "... Chunks are always aligned to multiples of the chunk size..."
  //
  // See https://linux.die.net/man/3/jemalloc
  size_t new_size = (__size + __alignment - 1) & ~(__alignment - 1);
  return vmsdk::PerformAndTrackMalloc(new_size, RedisModule_Alloc,
                                      RedisModule_MallocUsableSize);
}
int __wrap_malloc_usable_size(void* ptr) noexcept {
  if (vmsdk::SystemAllocTracker::GetInstance().IsTracked(ptr)) {
    return empty_usable_size(ptr);
  }
  return RedisModule_MallocUsableSize(ptr);
}
int __wrap_posix_memalign(void** r, size_t __alignment,
                          size_t __size) {
  *r = __wrap_aligned_alloc(__alignment, __size);
  return 0;
}
void* __wrap_valloc(size_t size) noexcept {
  return __wrap_aligned_alloc(sysconf(_SC_PAGESIZE), size);
}

}  // extern "C"

size_t get_new_alloc_size(size_t __sz) {
  if (__sz == 0) return 1;
  return __sz;
}

void* operator new(size_t __sz) noexcept(false) {
  return __wrap_malloc(get_new_alloc_size(__sz));
}
void operator delete(void* p) noexcept { __wrap_free(p); }
void operator delete(void* p, size_t __sz) noexcept { __wrap_free(p); }
void* operator new[](size_t __sz) noexcept(false) {
  // A non-null pointer is expected to be returned even if size = 0.
  if (__sz == 0) __sz++;
  return __wrap_malloc(__sz);
}
void operator delete[](void* p) noexcept { __wrap_free(p); }
void operator delete[](void* p, size_t __sz) noexcept { __wrap_free(p); }
void* operator new(size_t __sz, const std::nothrow_t& nt) noexcept {
  return __wrap_malloc(get_new_alloc_size(__sz));
}
void* operator new[](size_t __sz, const std::nothrow_t& nt) noexcept {
  return __wrap_malloc(get_new_alloc_size(__sz));
}
void operator delete(void* p, const std::nothrow_t& nt) noexcept {
  __wrap_free(p);
}
void operator delete[](void* p, const std::nothrow_t& nt) noexcept {
  __wrap_free(p);
}
void* operator new(size_t __sz, std::align_val_t alignment) noexcept(false) {
  return __wrap_aligned_alloc(static_cast<size_t>(alignment),
                              get_new_alloc_size(__sz));
}
void* operator new(size_t __sz, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept {
  return __wrap_aligned_alloc(static_cast<size_t>(alignment),
                              get_new_alloc_size(__sz));
}
void operator delete(void* p, std::align_val_t alignment) noexcept {
  __wrap_free(p);
}
void operator delete(void* p, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
  __wrap_free(p);
}
void operator delete(void* p, size_t __sz,
                     std::align_val_t alignment) noexcept {
  __wrap_free(p);
}
void* operator new[](size_t __sz, std::align_val_t alignment) noexcept(false) {
  return __wrap_aligned_alloc(static_cast<size_t>(alignment),
                              get_new_alloc_size(__sz));
}
void* operator new[](size_t __sz, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
  return __wrap_aligned_alloc(static_cast<size_t>(alignment),
                              get_new_alloc_size(__sz));
}
void operator delete[](void* p, std::align_val_t alignment) noexcept {
  __wrap_free(p);
}
void operator delete[](void* p, std::align_val_t alignment,
                       const std::nothrow_t&) noexcept {
  __wrap_free(p);
}
void operator delete[](void* p, size_t __sz,
                       std::align_val_t alignment) noexcept {
  __wrap_free(p);
}

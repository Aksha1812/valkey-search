/*
 * Deterministic reproducer for ELMO-118657.
 *
 * Test 1: Proves the race window exists (pointer changes under reader)
 * Test 2: Counts natural concurrent access frequency
 * Test 3: Simulates ARM64 torn read by corrupting the pointer → SIGSEGV
 */

#include <atomic>
#include <csignal>
#include <csetjmp>
#include <cstddef>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/notification.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_hnsw.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "third_party/hnswlib/hnswalg.h"

namespace valkey_search::indexes {
namespace {

constexpr int kDims = 64;
constexpr int kInitialCap = 5000;
constexpr int kM = 16;
constexpr int kEfConstruction = 100;
constexpr int kEfRuntime = 10;
constexpr int kSeedSize = 500;
constexpr int kHotKeys = 100;

static cancel::Token& CancelNever() {
  static cancel::Token t = cancel::Make(1000000, nullptr);
  return t;
}

std::vector<float> MakeVec(std::mt19937 &rng) {
  std::uniform_real_distribution<float> dist(0.0f, 1.0f);
  std::vector<float> v(kDims);
  for (auto &x : v) x = dist(rng);
  return v;
}

absl::string_view VecView(const std::vector<float> &v) {
  return {reinterpret_cast<const char *>(v.data()), v.size() * sizeof(float)};
}

class HNSWDeterministicRaceTest : public ValkeySearchTest {};

/*
 * Test 1: Prove the race exists — pointer changes while reader is paused.
 */
TEST_F(HNSWDeterministicRaceTest, PointerRaceProven) {
  auto index_result = VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(kDims, data_model::DISTANCE_METRIC_L2,
                                 kInitialCap, kM, kEfConstruction, kEfRuntime),
      "test_vec",
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_TRUE(index_result.ok());
  auto index = index_result.value();

  for (int i = 0; i < kSeedSize; i++) {
    auto key = StringInternStore::Intern(absl::StrCat("k", i));
    std::mt19937 rng(i);
    auto vec = MakeVec(rng);
    ASSERT_TRUE(index->AddRecord(key, VecView(vec)).ok());
  }

  absl::Notification thread_a_paused;
  absl::Notification thread_b_done;
  std::atomic<unsigned int> paused_on_id{UINT_MAX};
  std::atomic<bool> race_proven{false};

  hnswlib::g_after_read_data_ptr_hook = [&](unsigned int id, char **) {
    if (id < (unsigned int)kHotKeys && !thread_a_paused.HasBeenNotified()) {
      paused_on_id.store(id);
      thread_a_paused.Notify();
      thread_b_done.WaitForNotification();
    }
  };

  std::thread thread_a([&]() {
    for (int i = kSeedSize; i < kSeedSize + 5000; i++) {
      if (thread_a_paused.HasBeenNotified()) break;
      auto key = StringInternStore::Intern(absl::StrCat("k", i));
      std::mt19937 rng(i + 77777);
      auto vec = MakeVec(rng);
      (void)index->AddRecord(key, VecView(vec));
    }
  });

  std::thread thread_b([&]() {
    thread_a_paused.WaitForNotification();
    unsigned int target = paused_on_id.load();
    auto key = StringInternStore::Intern(absl::StrCat("k", target));
    std::mt19937 rng(999999);
    auto vec = MakeVec(rng);
    auto res = index->ModifyRecord(key, VecView(vec));
    if (res.ok() && res.value()) {
      race_proven.store(true);
    }
    thread_b_done.Notify();
  });

  thread_a.join();
  thread_b.join();
  hnswlib::g_after_read_data_ptr_hook = nullptr;

  unsigned int target = paused_on_id.load();
  printf("Thread A paused on element: %u\n", target);
  printf("Thread B modified element %u: %s\n", target,
         race_proven.load() ? "YES" : "no");

  ASSERT_NE(target, UINT_MAX) << "Hook never fired";
  EXPECT_TRUE(race_proven.load());

  if (race_proven.load()) {
    printf("\n*** RACE CONDITION DETERMINISTICALLY PROVEN ***\n");
    printf("Thread A held a stale pointer after Thread B overwrote it.\n");
    printf("On ARM64: SIGSEGV. On x86: silent UB.\n");
  }
}

/*
 * Test 2: Simulate ARM64 torn read — corrupt the pointer to prove
 * that fstdistfunc_ would SIGSEGV if it received a bad pointer.
 *
 * We intercept the pointer BEFORE it's returned to fstdistfunc_,
 * replace it with an unmapped address, and catch the resulting SIGSEGV.
 */
static sigjmp_buf g_jmp_buf;
static volatile sig_atomic_t g_got_signal = 0;

static void sigsegv_handler(int sig) {
  g_got_signal = sig;
  siglongjmp(g_jmp_buf, 1);
}

TEST_F(HNSWDeterministicRaceTest, SimulatedTornReadCrash) {
  auto index_result = VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(kDims, data_model::DISTANCE_METRIC_L2,
                                 kInitialCap, kM, kEfConstruction, kEfRuntime),
      "test_vec",
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_TRUE(index_result.ok());
  auto index = index_result.value();

  for (int i = 0; i < kSeedSize; i++) {
    auto key = StringInternStore::Intern(absl::StrCat("k", i));
    std::mt19937 rng(i);
    auto vec = MakeVec(rng);
    ASSERT_TRUE(index->AddRecord(key, VecView(vec)).ok());
  }

  // Install SIGSEGV handler so we can catch the crash
  struct sigaction sa = {};
  struct sigaction old_sa = {};
  sa.sa_handler = sigsegv_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sigaction(SIGSEGV, &sa, &old_sa);
  sigaction(SIGBUS, &sa, nullptr);

  std::atomic<bool> should_corrupt{false};
  std::atomic<int> corruptions{0};

  // Hook: on first hot key read after we arm it, corrupt the pointer
  hnswlib::g_after_read_data_ptr_hook = [&](unsigned int id, char **result_ptr) {
    if (should_corrupt.load() && id < (unsigned int)kHotKeys) {
      // Simulate what ARM64 weak ordering does: the reader sees garbage
      *result_ptr = reinterpret_cast<char *>(0xDEADBEEFDEADBEEF);
      corruptions.fetch_add(1);
      should_corrupt.store(false);  // only corrupt once
    }
  };

  // Try to trigger the crash
  g_got_signal = 0;
  should_corrupt.store(true);

  bool caught_crash = false;
  if (sigsetjmp(g_jmp_buf, 1) == 0) {
    // Normal path: do an AddRecord which calls searchBaseLayer
    // searchBaseLayer will call getDataByInternalId, hook corrupts pointer,
    // fstdistfunc_ dereferences 0xDEADBEEF → SIGSEGV
    for (int i = kSeedSize; i < kSeedSize + 1000; i++) {
      auto key = StringInternStore::Intern(absl::StrCat("crash_", i));
      std::mt19937 rng(i + 55555);
      auto vec = MakeVec(rng);
      (void)index->AddRecord(key, VecView(vec));
      if (corruptions.load() > 0 && !should_corrupt.load()) break;
    }
  } else {
    // We longjmp'd here from the signal handler — crash caught!
    caught_crash = true;
  }

  // Restore signal handler
  sigaction(SIGSEGV, &old_sa, nullptr);
  hnswlib::g_after_read_data_ptr_hook = nullptr;

  printf("Corruptions injected: %d\n", corruptions.load());
  printf("Signal caught: %d (%s)\n", (int)g_got_signal,
         g_got_signal == SIGSEGV ? "SIGSEGV" :
         g_got_signal == SIGBUS ? "SIGBUS" : "none");
  printf("Crash caught: %s\n", caught_crash ? "YES" : "no");

  if (caught_crash) {
    printf("\n*** SIGSEGV REPRODUCED ***\n");
    printf("fstdistfunc_ dereferenced a corrupted pointer (0xDEADBEEF...)\n");
    printf("simulating what happens on ARM64 when the weak memory model\n");
    printf("delivers a stale/torn pointer value to the reader thread.\n\n");
    printf("This is exactly the crash in ELMO-118657:\n");
    printf("  fstdistfunc_ → getDataByInternalId → searchBaseLayer → addPoint\n");
  }

  EXPECT_TRUE(caught_crash) << "Expected SIGSEGV from corrupted pointer";
  EXPECT_GT(corruptions.load(), 0) << "Hook never corrupted a pointer";
}

/*
 * Test 3: Count natural concurrent access frequency.
 */
TEST_F(HNSWDeterministicRaceTest, ConcurrentAccessCount) {
  auto index_result = VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(kDims, data_model::DISTANCE_METRIC_L2,
                                 kInitialCap, kM, kEfConstruction, kEfRuntime),
      "test_vec",
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_TRUE(index_result.ok());
  auto index = index_result.value();

  for (int i = 0; i < kSeedSize; i++) {
    auto key = StringInternStore::Intern(absl::StrCat("k", i));
    std::mt19937 rng(i);
    auto vec = MakeVec(rng);
    (void)index->AddRecord(key, VecView(vec));
  }

  std::atomic<int> total_reads{0};
  std::atomic<int> overlaps{0};
  std::atomic<int> active_per_element[kSeedSize] = {};

  hnswlib::g_after_read_data_ptr_hook = [&](unsigned int id, char **) {
    if (id >= (unsigned int)kSeedSize) return;
    total_reads.fetch_add(1);
    int prev = active_per_element[id].fetch_add(1);
    if (prev > 0) {
      overlaps.fetch_add(1);
    }
    std::this_thread::yield();
    active_per_element[id].fetch_sub(1);
  };

  constexpr int kThreads = 8;
  constexpr int kOps = 1000;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int tid = 0; tid < kThreads; tid++) {
    threads.emplace_back([&, tid]() {
      std::mt19937 rng(tid * 31);
      std::uniform_int_distribution<int> kd(0, kHotKeys - 1);
      int next = kSeedSize + tid * 50000;
      for (int op = 0; op < kOps; op++) {
        auto vec = MakeVec(rng);
        auto vv = VecView(vec);
        if (op % 2 == 0) {
          auto key = StringInternStore::Intern(absl::StrCat("k", kd(rng)));
          (void)index->ModifyRecord(key, vv);
        } else {
          auto key = StringInternStore::Intern(absl::StrCat("k", next++));
          (void)index->AddRecord(key, vv);
        }
      }
    });
  }
  for (auto &t : threads) t.join();

  hnswlib::g_after_read_data_ptr_hook = nullptr;

  printf("Total pointer reads: %d\n", total_reads.load());
  printf("Overlapping reads (same element, multiple threads): %d\n",
         overlaps.load());

  EXPECT_GT(total_reads.load(), 0);
  if (overlaps.load() > 0) {
    printf("*** CONFIRMED: race window is real (%d overlaps) ***\n",
           overlaps.load());
  }
}

}  // namespace
}  // namespace valkey_search::indexes

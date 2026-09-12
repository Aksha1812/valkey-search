/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <cstdint>
#include <memory>
#include <optional>

#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "src/commands/cursor_manager.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/commands/ft_aggregate_parser.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace aggregate {
namespace {

// Returns a RecordSet with `n` empty records, each with `fields` slots.
RecordSet MakeRecordSet(const AggregateParameters *params, size_t n,
                        size_t fields = 0) {
  RecordSet rs(params);
  for (size_t i = 0; i < n; ++i) {
    rs.push_back(std::make_unique<Record>(fields));
  }
  return rs;
}

CursorReplyMeta EmptyMeta() {
  return CursorReplyMeta{
      .record_info_by_index = {},
      .index_schema = nullptr,
      .dialect = 2,
  };
}

class CursorManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    CursorManager::InitInstance(std::make_unique<CursorManager>());
  }
  void TearDown() override { CursorManager::InitInstance(nullptr); }
};

TEST_F(CursorManagerTest, CreateAndReadExhausted) {
  AggregateParameters params(0);
  auto rs = MakeRecordSet(&params, 3);
  uint64_t id = CursorManager::Instance().Create(std::move(rs), EmptyMeta(), 10,
                                                 absl::Seconds(300), "idx", 3);
  EXPECT_GT(id, 0u);

  // Read all 3 in one call.
  auto result = CursorManager::Instance().Read(id, 10);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->batch.size(), 3u);
  EXPECT_TRUE(result->exhausted);
  EXPECT_EQ(result->total, 3);

  // Cursor is gone after exhaustion.
  EXPECT_FALSE(CursorManager::Instance().Read(id, 10).has_value());
}

TEST_F(CursorManagerTest, PaginatesInBatches) {
  AggregateParameters params(0);
  auto rs = MakeRecordSet(&params, 10);
  uint64_t id = CursorManager::Instance().Create(std::move(rs), EmptyMeta(), 3,
                                                 absl::Seconds(300), "idx", 10);

  // First read: 3 rows, not exhausted.
  auto r1 = CursorManager::Instance().Read(id, 3);
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(r1->batch.size(), 3u);
  EXPECT_FALSE(r1->exhausted);

  // Second read: another 3.
  auto r2 = CursorManager::Instance().Read(id, 3);
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(r2->batch.size(), 3u);
  EXPECT_FALSE(r2->exhausted);

  // Third read: 3 more.
  auto r3 = CursorManager::Instance().Read(id, 3);
  ASSERT_TRUE(r3.has_value());
  EXPECT_EQ(r3->batch.size(), 3u);
  EXPECT_FALSE(r3->exhausted);

  // Fourth read: 1 remaining, exhausted.
  auto r4 = CursorManager::Instance().Read(id, 3);
  ASSERT_TRUE(r4.has_value());
  EXPECT_EQ(r4->batch.size(), 1u);
  EXPECT_TRUE(r4->exhausted);
}

TEST_F(CursorManagerTest, DefaultCountUsedWhenReadCountIsZero) {
  AggregateParameters params(0);
  auto rs = MakeRecordSet(&params, 5);
  // cursor_count_ = 2
  uint64_t id = CursorManager::Instance().Create(std::move(rs), EmptyMeta(), 2,
                                                 absl::Seconds(300), "idx", 5);

  // Pass count=0 → falls back to cursor's stored count (2).
  auto r = CursorManager::Instance().Read(id, 0);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->batch.size(), 2u);
}

TEST_F(CursorManagerTest, DeleteBeforeExhaustion) {
  AggregateParameters params(0);
  auto rs = MakeRecordSet(&params, 5);
  uint64_t id = CursorManager::Instance().Create(std::move(rs), EmptyMeta(), 2,
                                                 absl::Seconds(300), "idx", 5);

  EXPECT_TRUE(CursorManager::Instance().Delete(id));
  EXPECT_FALSE(CursorManager::Instance().Read(id, 2).has_value());

  // Delete of unknown id returns false.
  EXPECT_FALSE(CursorManager::Instance().Delete(id));
}

TEST_F(CursorManagerTest, Reset) {
  AggregateParameters params(0);

  uint64_t id1 = CursorManager::Instance().Create(
      MakeRecordSet(&params, 3), EmptyMeta(), 1, absl::Seconds(300), "idx1", 3);
  uint64_t id2 = CursorManager::Instance().Create(
      MakeRecordSet(&params, 3), EmptyMeta(), 1, absl::Seconds(300), "idx1", 3);
  uint64_t id3 = CursorManager::Instance().Create(
      MakeRecordSet(&params, 3), EmptyMeta(), 1, absl::Seconds(300), "idx2", 3);

  EXPECT_EQ(CursorManager::Instance().Size(), 3u);
  EXPECT_EQ(CursorManager::Instance().Reset("idx1"), 2u);
  EXPECT_EQ(CursorManager::Instance().Size(), 1u);

  EXPECT_FALSE(CursorManager::Instance().Read(id1, 1).has_value());
  EXPECT_FALSE(CursorManager::Instance().Read(id2, 1).has_value());
  EXPECT_TRUE(CursorManager::Instance().Read(id3, 1).has_value());
}

TEST_F(CursorManagerTest, PurgeExpired) {
  AggregateParameters params(0);

  // Cursor with a TTL already in the past.
  uint64_t id =
      CursorManager::Instance().Create(MakeRecordSet(&params, 5), EmptyMeta(),
                                       2, absl::Milliseconds(-1), "idx", 5);

  CursorManager::Instance().PurgeExpired();
  EXPECT_FALSE(CursorManager::Instance().Read(id, 2).has_value());
}

TEST_F(CursorManagerTest, ExpiredCursorReturnedAsNotFound) {
  AggregateParameters params(0);

  uint64_t id =
      CursorManager::Instance().Create(MakeRecordSet(&params, 5), EmptyMeta(),
                                       2, absl::Milliseconds(-1), "idx", 5);

  // Lazy expiry on Read.
  EXPECT_FALSE(CursorManager::Instance().Read(id, 2).has_value());
}

TEST_F(CursorManagerTest, CursorIdNeverZero) {
  AggregateParameters params(0);
  // Exhaust the id counter by wrapping: set next_id_ to UINT64_MAX-1 via
  // successive creates. (Not feasible; just verify the guard via inspection.)
  // We can verify that all generated IDs are non-zero for a small sample.
  for (int i = 0; i < 100; ++i) {
    uint64_t id =
        CursorManager::Instance().Create(MakeRecordSet(&params, 1), EmptyMeta(),
                                         1, absl::Seconds(300), "idx", 1);
    EXPECT_NE(id, 0u);
    CursorManager::Instance().Delete(id);
  }
}

TEST_F(CursorManagerTest, TotalReportedOnEveryRead) {
  AggregateParameters params(0);
  auto rs = MakeRecordSet(&params, 4);
  uint64_t id = CursorManager::Instance().Create(std::move(rs), EmptyMeta(), 2,
                                                 absl::Seconds(300), "idx", 4);

  auto r1 = CursorManager::Instance().Read(id, 2);
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(r1->total, 4);

  auto r2 = CursorManager::Instance().Read(id, 2);
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(r2->total, 4);
  EXPECT_TRUE(r2->exhausted);
}

}  // namespace
}  // namespace aggregate
}  // namespace valkey_search

int main(int argc, char **argv) {
  vmsdk::TrackCurrentAsMainThread();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

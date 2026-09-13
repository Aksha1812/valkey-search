/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "src/indexes/index_base.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace {

constexpr int kTestDbNum = 1;

int ExecuteCmd(ValkeyModuleCtx* ctx, const std::vector<std::string>& argv,
               int expected_return = VALKEYMODULE_OK,
               const std::string& expected_reply = "+OK\r\n") {
  std::vector<ValkeyModuleString*> cmd_argv;
  std::transform(argv.begin(), argv.end(), std::back_inserter(cmd_argv),
                 [&](std::string val) {
                   return TestValkeyModule_CreateStringPrintf(ctx, "%s",
                                                              val.data());
                 });
  int result;
  if (argv[0] == "FT.CREATE") {
    result = vmsdk::CreateCommand<FTCreateCmd>(ctx, cmd_argv.data(),
                                               cmd_argv.size());
  } else {
    result =
        vmsdk::CreateCommand<FTAlterCmd>(ctx, cmd_argv.data(), cmd_argv.size());
  }
  EXPECT_EQ(result, expected_return);
  if (!expected_reply.empty()) {
    EXPECT_EQ(ctx->reply_capture.GetReply(), expected_reply);
  }
  ctx->reply_capture.ClearReply();
  for (auto* s : cmd_argv) TestValkeyModule_FreeString(ctx, s);
  return result;
}

class FTAlterTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kTestDbNum));
    ExecuteCmd(&fake_ctx_, {"FT.CREATE", "idx", "SCHEMA", "f1", "NUMERIC"});
  }

  void TearDown() override {
    SchemaManager::Instance()
        .RemoveIndexSchema(kTestDbNum, "idx")
        .IgnoreError();
    ValkeySearchTest::TearDown();
  }
};

TEST_F(FTAlterTest, AddNumericField) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SCHEMA", "ADD", "f2", "NUMERIC"});

  auto schema = SchemaManager::Instance().GetIndexSchema(kTestDbNum, "idx");
  VMSDK_EXPECT_OK(schema);
  VMSDK_EXPECT_OK(schema.value()->GetIndex("f1"));
  VMSDK_EXPECT_OK(schema.value()->GetIndex("f2"));
  EXPECT_EQ(schema.value()->GetIndex("f2").value()->GetIndexerType(),
            indexes::IndexerType::kNumeric);
}

TEST_F(FTAlterTest, AddTagField) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SCHEMA", "ADD", "t1", "TAG"});

  auto schema = SchemaManager::Instance().GetIndexSchema(kTestDbNum, "idx");
  VMSDK_EXPECT_OK(schema);
  VMSDK_EXPECT_OK(schema.value()->GetIndex("t1"));
  EXPECT_EQ(schema.value()->GetIndex("t1").value()->GetIndexerType(),
            indexes::IndexerType::kTag);
}

TEST_F(FTAlterTest, AddTextField) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SCHEMA", "ADD", "body", "TEXT"});

  auto schema = SchemaManager::Instance().GetIndexSchema(kTestDbNum, "idx");
  VMSDK_EXPECT_OK(schema);
  VMSDK_EXPECT_OK(schema.value()->GetIndex("body"));
  EXPECT_EQ(schema.value()->GetIndex("body").value()->GetIndexerType(),
            indexes::IndexerType::kText);
}

TEST_F(FTAlterTest, AddVectorField) {
  ExecuteCmd(&fake_ctx_,
             {"FT.ALTER", "idx", "SCHEMA", "ADD", "vec", "VECTOR", "HNSW", "6",
              "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2"});

  auto schema = SchemaManager::Instance().GetIndexSchema(kTestDbNum, "idx");
  VMSDK_EXPECT_OK(schema);
  VMSDK_EXPECT_OK(schema.value()->GetIndex("vec"));
  EXPECT_EQ(schema.value()->GetIndex("vec").value()->GetIndexerType(),
            indexes::IndexerType::kHNSW);
}

TEST_F(FTAlterTest, AddMultipleFieldsInOneAlter) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SCHEMA", "ADD", "f2", "NUMERIC",
                          "t1", "TAG"});

  auto schema = SchemaManager::Instance().GetIndexSchema(kTestDbNum, "idx");
  VMSDK_EXPECT_OK(schema);
  VMSDK_EXPECT_OK(schema.value()->GetIndex("f2"));
  VMSDK_EXPECT_OK(schema.value()->GetIndex("t1"));
}

TEST_F(FTAlterTest, SkipInitialScan) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SKIPINITIALSCAN", "SCHEMA", "ADD",
                          "f2", "NUMERIC"});

  auto schema = SchemaManager::Instance().GetIndexSchema(kTestDbNum, "idx");
  VMSDK_EXPECT_OK(schema);
  VMSDK_EXPECT_OK(schema.value()->GetIndex("f2"));
}

TEST_F(FTAlterTest, DuplicateFieldRejected) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SCHEMA", "ADD", "f1", "NUMERIC"},
             VALKEYMODULE_OK,
             "-Attribute `f1` already exists in the index\r\n");
}

TEST_F(FTAlterTest, IndexNotFound) {
  ExecuteCmd(&fake_ctx_,
             {"FT.ALTER", "no_such_idx", "SCHEMA", "ADD", "f2", "NUMERIC"},
             VALKEYMODULE_OK,
             "-Index with name 'no_such_idx' not found in database 1\r\n");
}

TEST_F(FTAlterTest, MissingSchemaSyntax) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "ADD", "f2", "NUMERIC"},
             VALKEYMODULE_OK, "-Expected SCHEMA, got `ADD`\r\n");
}

TEST_F(FTAlterTest, MissingAddKeyword) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SCHEMA", "f2", "NUMERIC"},
             VALKEYMODULE_OK, "-Expected ADD after SCHEMA, got `f2`\r\n");
}

TEST_F(FTAlterTest, TooFewArgs) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SCHEMA", "ADD"}, VALKEYMODULE_OK,
             "-SCHEMA ADD requires at least one field definition\r\n");
}

TEST_F(FTAlterTest, ExistingFieldsPreserved) {
  ExecuteCmd(&fake_ctx_, {"FT.ALTER", "idx", "SCHEMA", "ADD", "f2", "NUMERIC"});

  auto schema = SchemaManager::Instance().GetIndexSchema(kTestDbNum, "idx");
  VMSDK_EXPECT_OK(schema);
  // f1 from CREATE must still be present
  VMSDK_EXPECT_OK(schema.value()->GetIndex("f1"));
}

}  // namespace
}  // namespace valkey_search

/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_cursor.h"

#include <cstdint>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "src/commands/cursor_manager.h"
#include "src/commands/ft_aggregate.h"
#include "src/indexes/index_base.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

constexpr absl::string_view kReadSubcmd{"READ"};
constexpr absl::string_view kDelSubcmd{"DEL"};
constexpr absl::string_view kResetSubcmd{"RESET"};
constexpr absl::string_view kCountParam{"COUNT"};

// Reply a batch from a cursor READ as [total, row0, row1, ...].
void ReplyCursorBatch(ValkeyModuleCtx *ctx,
                      const aggregate::CursorReplyMeta &meta,
                      const std::vector<aggregate::RecordPtr> &batch,
                      long long total) {
  ValkeyModule_ReplyWithArray(ctx, 1 + batch.size());
  ValkeyModule_ReplyWithLongLong(ctx, total);
  for (const auto &rec : batch) {
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    size_t count = 0;
    for (size_t i = 0;
         i < rec->fields_.size() && i < meta.record_info_by_index.size(); ++i) {
      if (aggregate::ReplyWithValue(
              ctx, meta.index_schema->GetAttributeDataType().ToProto(),
              meta.record_info_by_index[i].output_name_,
              meta.record_info_by_index[i].data_type_, rec->fields_[i],
              meta.dialect)) {
        count += 2;
      }
    }
    for (const auto &[name, value] : rec->extra_fields_) {
      if (aggregate::ReplyWithValue(
              ctx, meta.index_schema->GetAttributeDataType().ToProto(), name,
              indexes::IndexerType::kNone, value, meta.dialect)) {
        count += 2;
      }
    }
    ValkeyModule_ReplySetArrayLength(ctx, count);
  }
}

absl::Status HandleRead(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  // argv: FT.CURSOR READ <index> <cursor_id> [COUNT <n>]
  if (argc < 4) {
    return absl::InvalidArgumentError(
        "FT.CURSOR READ requires index and cursor_id");
  }
  absl::string_view index_name = vmsdk::ToStringView(argv[2]);
  (void)
      index_name;  // used for routing/validation; cursor_id is globally unique

  uint64_t cursor_id = 0;
  VMSDK_ASSIGN_OR_RETURN(cursor_id, vmsdk::To<uint64_t>(argv[3]));

  size_t count = 0;
  if (argc >= 6 &&
      absl::EqualsIgnoreCase(vmsdk::ToStringView(argv[4]), kCountParam)) {
    VMSDK_ASSIGN_OR_RETURN(count, vmsdk::To<size_t>(argv[5]));
  }

  auto result = aggregate::CursorManager::Instance().Read(cursor_id, count);
  if (!result.has_value()) {
    return absl::NotFoundError("Cursor not found");
  }

  const uint64_t next_id = result->exhausted ? 0 : cursor_id;

  ValkeyModule_ReplyWithArray(ctx, 2);
  if (result->meta != nullptr) {
    ReplyCursorBatch(ctx, *result->meta, result->batch, result->total);
  } else {
    // Exhausted: emit empty inner array.
    ValkeyModule_ReplyWithArray(ctx, 1);
    ValkeyModule_ReplyWithLongLong(ctx, result->total);
  }
  ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(next_id));

  return absl::OkStatus();
}

absl::Status HandleDel(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  if (argc < 4) {
    return absl::InvalidArgumentError(
        "FT.CURSOR DEL requires index and cursor_id");
  }
  uint64_t cursor_id = 0;
  VMSDK_ASSIGN_OR_RETURN(cursor_id, vmsdk::To<uint64_t>(argv[3]));
  aggregate::CursorManager::Instance().Delete(cursor_id);
  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

absl::Status HandleReset(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  if (argc < 3) {
    return absl::InvalidArgumentError("FT.CURSOR RESET requires index");
  }
  absl::string_view index_name = vmsdk::ToStringView(argv[2]);
  size_t dropped = aggregate::CursorManager::Instance().Reset(index_name);
  ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(dropped));
  return absl::OkStatus();
}

}  // namespace

absl::Status FTCursorCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  // argv[0] = "FT.CURSOR", argv[1] = subcommand
  if (argc < 2) {
    return absl::InvalidArgumentError(
        "FT.CURSOR requires a subcommand (READ|DEL|RESET)");
  }
  absl::string_view subcmd = vmsdk::ToStringView(argv[1]);

  if (absl::EqualsIgnoreCase(subcmd, kReadSubcmd)) {
    return HandleRead(ctx, argv, argc);
  }
  if (absl::EqualsIgnoreCase(subcmd, kDelSubcmd)) {
    return HandleDel(ctx, argv, argc);
  }
  if (absl::EqualsIgnoreCase(subcmd, kResetSubcmd)) {
    return HandleReset(ctx, argv, argc);
  }

  return absl::InvalidArgumentError(
      "FT.CURSOR subcommand must be READ, DEL, or RESET");
}

}  // namespace valkey_search

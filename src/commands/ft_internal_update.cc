/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.pb.h"
#include "src/metrics.h"
#include "src/schema_manager.h"
#include "src/valkey_search_options.h"

namespace valkey_search {

constexpr int kFTInternalUpdateArgCount = 4;

absl::Status FTInternalUpdateCmd(ValkeyModuleCtx *ctx,
                                 ValkeyModuleString **argv, int argc) {
  CHECK_EQ(argc, kFTInternalUpdateArgCount)
      << "FT.INTERNAL_UPDATE called with wrong argument count: " << argc;

  auto id_view = vmsdk::ToStringView(argv[1]);
  std::string id(id_view);

  auto metadata_view = vmsdk::ToStringView(argv[2]);
  coordinator::GlobalMetadataEntry metadata_entry;
  if (!metadata_entry.ParseFromArray(metadata_view.data(),
                                     metadata_view.size())) {
    Metrics::GetStats().ft_internal_update_parse_failures_cnt++;
    VMSDK_LOG(WARNING, ctx)
        << "CRITICAL: GlobalMetadataEntry parse failed in FT.INTERNAL_UPDATE. "
        << "Index ID: " << vmsdk::config::RedactIfNeeded(id);

    if (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING) {
      if (options::GetSkipCorruptedInternalUpdateEntries().GetValue()) {
        VMSDK_LOG(WARNING, ctx)
            << "SKIPPING corrupted AOF entry due to configuration";
        Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        return absl::OkStatus();
      }
      return absl::DataLossError(
          "Corrupt FT.INTERNAL_UPDATE entry during AOF loading. "
          "Set search.skip-corrupted-internal-update-entries=yes to skip.");
    }
    return absl::InvalidArgumentError("Failed to parse GlobalMetadataEntry");
  }

  auto header_view = vmsdk::ToStringView(argv[3]);
  coordinator::GlobalMetadataVersionHeader version_header;
  if (!version_header.ParseFromArray(header_view.data(), header_view.size())) {
    Metrics::GetStats().ft_internal_update_parse_failures_cnt++;
    VMSDK_LOG(WARNING, ctx)
        << "CRITICAL: GlobalMetadataVersionHeader parse failed in "
           "FT.INTERNAL_UPDATE. "
        << "Index ID: " << vmsdk::config::RedactIfNeeded(id);

    if (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING) {
      if (options::GetSkipCorruptedInternalUpdateEntries().GetValue()) {
        VMSDK_LOG(WARNING, ctx)
            << "SKIPPING corrupted AOF entry due to configuration";
        Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        return absl::OkStatus();
      }
      return absl::DataLossError(
          "Corrupt FT.INTERNAL_UPDATE entry during AOF loading. "
          "Set search.skip-corrupted-internal-update-entries=yes to skip.");
    }
    return absl::InvalidArgumentError(
        "Failed to parse GlobalMetadataVersionHeader");
  }

  int flags = ValkeyModule_GetContextFlags(ctx);
  if ((flags & VALKEYMODULE_CTX_FLAGS_SLAVE) ||
      (flags & VALKEYMODULE_CTX_FLAGS_LOADING)) {
    auto status = coordinator::MetadataManager::Instance().CreateEntryOnReplica(
        ctx, kSchemaManagerMetadataTypeName, id, &metadata_entry,
        &version_header);
    if (!status.ok()) {
      Metrics::GetStats().ft_internal_update_process_failures_cnt++;
      VMSDK_LOG(WARNING, ctx)
          << "CRITICAL: CreateEntryOnReplica failed in FT.INTERNAL_UPDATE. "
          << "Index ID: " << vmsdk::config::RedactIfNeeded(id);

      if (flags & VALKEYMODULE_CTX_FLAGS_LOADING) {
        if (options::GetSkipCorruptedInternalUpdateEntries().GetValue()) {
          VMSDK_LOG(WARNING, ctx)
              << "SKIPPING failed AOF entry due to configuration";
          Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
          ValkeyModule_ReplyWithSimpleString(ctx, "OK");
          return absl::OkStatus();
        }
        return status;
      }
      return status;
    }
  }

  ValkeyModule_ReplicateVerbatim(ctx);

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

}  // namespace valkey_search

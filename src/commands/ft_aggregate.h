/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H
#define VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "valkey_module.h"

namespace valkey_search {
namespace aggregate {

// Returns true if a field/value pair was written to the reply.
bool ReplyWithValue(ValkeyModuleCtx *ctx,
                    data_model::AttributeDataType data_type,
                    std::string_view name, indexes::IndexerType indexer_type,
                    const expr::Value &value, int dialect);

absl::Status FTAggregateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc);

}  // namespace aggregate
};  // namespace valkey_search
#endif

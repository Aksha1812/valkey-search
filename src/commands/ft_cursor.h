/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_CURSOR_H
#define VALKEYSEARCH_SRC_COMMANDS_FT_CURSOR_H

#include "absl/status/status.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTCursorCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc);

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_CURSOR_H

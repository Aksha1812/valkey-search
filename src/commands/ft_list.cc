/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <regex>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTListCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  if (argc > 2) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kListCommand));
  }
  absl::flat_hash_set<std::string> names =
      SchemaManager::Instance().GetIndexSchemasInDB(
          ValkeyModule_GetSelectedDb(ctx));

  std::optional<std::regex> pattern;
  if (argc == 2) {
    auto pattern_str = vmsdk::ToStringView(argv[1]);
    try {
      pattern.emplace(std::string(pattern_str),
                      std::regex::ECMAScript | std::regex::icase);
    } catch (const std::regex_error &e) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid regex pattern: ", e.what()));
    }
  }

  std::vector<std::string> filtered_names;
  for (const auto &name : names) {
    if (!pattern || std::regex_search(name, *pattern)) {
      filtered_names.push_back(name);
    }
  }

  ValkeyModule_ReplyWithArray(ctx, filtered_names.size());
  for (const auto &name : filtered_names) {
    ValkeyModule_ReplyWithSimpleString(ctx, name.c_str());
  }
  return absl::OkStatus();
}
}  // namespace valkey_search

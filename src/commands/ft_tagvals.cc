/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/indexes/index_base.h"
#include "src/indexes/tag.h"
#include "src/schema_manager.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTTagValsCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                          int argc) {
  if (argc != 3) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kTagValsCommand));
  }
  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(ValkeyModule_GetSelectedDb(ctx),
                                               vmsdk::ToStringView(argv[1])));
  VMSDK_RETURN_IF_ERROR(AclPrefixCheck(ctx, acl::KeyAccess::kRead,
                                       index_schema->GetKeyPrefixes()));

  VMSDK_ASSIGN_OR_RETURN(auto index,
                         index_schema->GetIndex(vmsdk::ToStringView(argv[2])));
  if (index->GetIndexerType() != indexes::IndexerType::kTag) {
    return absl::InvalidArgumentError("Not a tag field");
  }

  auto tag_index = dynamic_cast<const indexes::Tag *>(index.get());
  std::vector<std::string> values = tag_index->GetTagValues();
  // RESP3 renders this as a set, RESP2 as an array.
  ValkeyModule_ReplyWithSet(ctx, values.size());
  for (const auto &value : values) {
    ValkeyModule_ReplyWithStringBuffer(ctx, value.data(), value.size());
  }
  return absl::OkStatus();
}
}  // namespace valkey_search

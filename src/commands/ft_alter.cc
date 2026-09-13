/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/commands/ft_create_parser.h"
#include "src/query/cluster_info_fanout_operation.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class AlterConsistencyCheckFanoutOperation
    : public query::cluster_info_fanout::ClusterInfoFanoutOperation {
 public:
  AlterConsistencyCheckFanoutOperation(
      uint32_t db_num, const std::string &index_name, unsigned timeout_ms,
      coordinator::IndexFingerprintVersion new_entry_fingerprint_version)
      : ClusterInfoFanoutOperation(db_num, index_name, timeout_ms, false,
                                   false),
        new_entry_fingerprint_version_(new_entry_fingerprint_version) {}

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const vmsdk::cluster_map::NodeInfo &) override {
    coordinator::InfoIndexPartitionRequest req;
    req.set_db_num(db_num_);
    req.set_index_name(index_name_);
    auto *expected_ifv = req.mutable_index_fingerprint_version();
    expected_ifv->set_fingerprint(new_entry_fingerprint_version_.fingerprint());
    expected_ifv->set_version(new_entry_fingerprint_version_.version());
    return req;
  }

  int GenerateReply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                    int argc) override {
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

 private:
  coordinator::IndexFingerprintVersion new_entry_fingerprint_version_;
};

absl::Status FTAlterCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  // argv[0] = "FT.ALTER", argv[1] = index_name, argv[2..] = options
  if (argc < 3) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kAlterCommand));
  }

  auto index_name = vmsdk::ToStringView(argv[1]);
  const int db_num = ValkeyModule_GetSelectedDb(ctx);

  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(db_num, index_name));

  VMSDK_RETURN_IF_ERROR(AclPrefixCheck(ctx, acl::KeyAccess::kWrite,
                                       index_schema->GetKeyPrefixes()));

  auto existing_proto = index_schema->ToProto();
  existing_proto->set_db_num(db_num);

  VMSDK_ASSIGN_OR_RETURN(auto updated_proto,
                         ParseFTAlterArgs(*existing_proto, argv + 2, argc - 2));

  VMSDK_ASSIGN_OR_RETURN(
      auto new_entry_fingerprint_version,
      SchemaManager::Instance().AlterIndexSchema(ctx, updated_proto));

  const bool is_loading =
      ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING;
  const bool inside_multi_exec = vmsdk::MultiOrLua(ctx);
  if (ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator() && !is_loading &&
      !inside_multi_exec) {
    unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
    auto op = new AlterConsistencyCheckFanoutOperation(
        db_num, std::string(index_name), timeout_ms,
        new_entry_fingerprint_version);
    op->StartOperation(ctx);
  } else {
    if (is_loading || inside_multi_exec) {
      VMSDK_LOG(NOTICE, nullptr)
          << "The server is loading AOF or inside multi/exec or lua script, "
             "skip fanout operation";
    }
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

  if (!options::GetUseCoordinator().GetValue()) {
    ValkeyModule_ReplicateVerbatim(ctx);
  }
  return absl::OkStatus();
}

}  // namespace valkey_search

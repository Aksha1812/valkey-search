/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_RDF_COMMANDS_RDF_COMMANDS_H_
#define VALKEYSEARCH_SRC_RDF_COMMANDS_RDF_COMMANDS_H_

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::rdf {

// Command names
constexpr absl::string_view kRDFGraphCreateCommand{"RDF.GRAPH.CREATE"};
constexpr absl::string_view kRDFGraphDropCommand{"RDF.GRAPH.DROP"};
constexpr absl::string_view kRDFGraphListCommand{"RDF.GRAPH.LIST"};
constexpr absl::string_view kRDFGraphInfoCommand{"RDF.GRAPH.INFO"};
constexpr absl::string_view kRDFTripleAddCommand{"RDF.TRIPLE.ADD"};
constexpr absl::string_view kRDFTripleDelCommand{"RDF.TRIPLE.DEL"};
constexpr absl::string_view kRDFQueryCommand{"RDF.QUERY"};

// ACL category
constexpr absl::string_view kRDFCategory{"@rdf"};

// Permissions
const absl::flat_hash_set<absl::string_view> kRDFWritePermissions{
    kRDFCategory, absl::string_view("@write"), absl::string_view("@fast")};
const absl::flat_hash_set<absl::string_view> kRDFReadPermissions{
    kRDFCategory, absl::string_view("@read"), absl::string_view("@fast")};

// Command implementations
absl::Status RDFGraphCreateCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                               int argc);
absl::Status RDFGraphDropCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc);
absl::Status RDFGraphListCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc);
absl::Status RDFGraphInfoCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc);
absl::Status RDFTripleAddCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc);
absl::Status RDFTripleDelCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc);
absl::Status RDFQueryCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                         int argc);

}  // namespace valkey_search::rdf

#endif  // VALKEYSEARCH_SRC_RDF_COMMANDS_RDF_COMMANDS_H_

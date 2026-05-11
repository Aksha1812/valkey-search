/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/commands/rdf_commands.h"

#include "src/rdf/graph.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::rdf {

absl::Status RDFGraphCreateCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                               int argc) {
  if (argc != 2) {
    return absl::InvalidArgumentError("usage: RDF.GRAPH.CREATE <name>");
  }
  auto name = vmsdk::ToStringView(argv[1]);
  auto status = GraphManager::Instance().CreateGraph(name);
  if (!status.ok()) return status;
  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

absl::Status RDFGraphDropCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc) {
  if (argc != 2) {
    return absl::InvalidArgumentError("usage: RDF.GRAPH.DROP <name>");
  }
  auto name = vmsdk::ToStringView(argv[1]);
  auto status = GraphManager::Instance().DropGraph(name);
  if (!status.ok()) return status;
  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

absl::Status RDFGraphListCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc) {
  if (argc != 1) {
    return absl::InvalidArgumentError("usage: RDF.GRAPH.LIST");
  }
  auto graphs = GraphManager::Instance().ListGraphs();
  ValkeyModule_ReplyWithArray(ctx, graphs.size());
  for (const auto& name : graphs) {
    ValkeyModule_ReplyWithStringBuffer(ctx, name.data(), name.size());
  }
  return absl::OkStatus();
}

absl::Status RDFGraphInfoCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc) {
  if (argc != 2) {
    return absl::InvalidArgumentError("usage: RDF.GRAPH.INFO <name>");
  }
  auto name = vmsdk::ToStringView(argv[1]);
  auto* graph = GraphManager::Instance().GetGraph(name);
  if (!graph) {
    return absl::NotFoundError("graph not found");
  }
  ValkeyModule_ReplyWithArray(ctx, 8);
  ValkeyModule_ReplyWithStringBuffer(ctx, "triples", 7);
  ValkeyModule_ReplyWithLongLong(ctx, graph->TripleCount());
  ValkeyModule_ReplyWithStringBuffer(ctx, "terms", 5);
  ValkeyModule_ReplyWithLongLong(ctx, graph->TermCount());
  ValkeyModule_ReplyWithStringBuffer(ctx, "memory_bytes", 12);
  ValkeyModule_ReplyWithLongLong(ctx, graph->MemoryUsage());
  ValkeyModule_ReplyWithStringBuffer(ctx, "name", 4);
  auto& gname = graph->Name();
  ValkeyModule_ReplyWithStringBuffer(ctx, gname.data(), gname.size());
  return absl::OkStatus();
}

absl::Status RDFTripleAddCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc) {
  if (argc != 5) {
    return absl::InvalidArgumentError(
        "usage: RDF.TRIPLE.ADD <graph> <subject> <predicate> <object>");
  }
  auto graph_name = vmsdk::ToStringView(argv[1]);
  auto subject = vmsdk::ToStringView(argv[2]);
  auto predicate = vmsdk::ToStringView(argv[3]);
  auto object = vmsdk::ToStringView(argv[4]);

  auto* graph = GraphManager::Instance().GetGraph(graph_name);
  if (!graph) {
    return absl::NotFoundError("graph not found");
  }

  auto result = graph->AddTriple(subject, predicate, object);
  if (!result.ok()) return result.status();
  ValkeyModule_ReplyWithLongLong(ctx, *result ? 1 : 0);
  return absl::OkStatus();
}

absl::Status RDFTripleDelCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                             int argc) {
  if (argc != 5) {
    return absl::InvalidArgumentError(
        "usage: RDF.TRIPLE.DEL <graph> <subject> <predicate> <object>");
  }
  auto graph_name = vmsdk::ToStringView(argv[1]);
  auto subject = vmsdk::ToStringView(argv[2]);
  auto predicate = vmsdk::ToStringView(argv[3]);
  auto object = vmsdk::ToStringView(argv[4]);

  auto* graph = GraphManager::Instance().GetGraph(graph_name);
  if (!graph) {
    return absl::NotFoundError("graph not found");
  }

  auto result = graph->DeleteTriple(subject, predicate, object);
  if (!result.ok()) return result.status();
  ValkeyModule_ReplyWithLongLong(ctx, *result ? 1 : 0);
  return absl::OkStatus();
}

}  // namespace valkey_search::rdf

/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/rdf/graph.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::rdf {

// RDF.GRAPH.CREATE <graph_name>
int RDFGraphCreateCommand(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                          int argc) {
  if (argc != 2) {
    return ValkeyModule_WrongArity(ctx);
  }
  size_t len;
  const char* name = ValkeyModule_StringPtrLen(argv[1], &len);
  auto status = GraphManager::Instance().CreateGraph(
      absl::string_view(name, len));
  if (!status.ok()) {
    ValkeyModule_ReplyWithError(ctx, status.message().data());
    return VALKEYMODULE_OK;
  }
  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return VALKEYMODULE_OK;
}

// RDF.GRAPH.DROP <graph_name>
int RDFGraphDropCommand(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                        int argc) {
  if (argc != 2) {
    return ValkeyModule_WrongArity(ctx);
  }
  size_t len;
  const char* name = ValkeyModule_StringPtrLen(argv[1], &len);
  auto status = GraphManager::Instance().DropGraph(
      absl::string_view(name, len));
  if (!status.ok()) {
    ValkeyModule_ReplyWithError(ctx, status.message().data());
    return VALKEYMODULE_OK;
  }
  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return VALKEYMODULE_OK;
}

// RDF.GRAPH.LIST
int RDFGraphListCommand(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                        int argc) {
  if (argc != 1) {
    return ValkeyModule_WrongArity(ctx);
  }
  auto graphs = GraphManager::Instance().ListGraphs();
  ValkeyModule_ReplyWithArray(ctx, graphs.size());
  for (const auto& name : graphs) {
    ValkeyModule_ReplyWithStringBuffer(ctx, name.data(), name.size());
  }
  return VALKEYMODULE_OK;
}

// RDF.GRAPH.INFO <graph_name>
int RDFGraphInfoCommand(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                        int argc) {
  if (argc != 2) {
    return ValkeyModule_WrongArity(ctx);
  }
  size_t len;
  const char* name = ValkeyModule_StringPtrLen(argv[1], &len);
  auto* graph = GraphManager::Instance().GetGraph(absl::string_view(name, len));
  if (!graph) {
    ValkeyModule_ReplyWithError(ctx, "ERR graph not found");
    return VALKEYMODULE_OK;
  }
  ValkeyModule_ReplyWithArray(ctx, 8);
  ValkeyModule_ReplyWithStringBuffer(ctx, "triples", 7);
  ValkeyModule_ReplyWithLongLong(ctx, graph->TripleCount());
  ValkeyModule_ReplyWithStringBuffer(ctx, "terms", 5);
  ValkeyModule_ReplyWithLongLong(ctx, graph->TermCount());
  ValkeyModule_ReplyWithStringBuffer(ctx, "memory_bytes", 12);
  ValkeyModule_ReplyWithLongLong(ctx, graph->MemoryUsage());
  ValkeyModule_ReplyWithStringBuffer(ctx, "name", 4);
  ValkeyModule_ReplyWithStringBuffer(ctx, graph->Name().data(),
                                     graph->Name().size());
  return VALKEYMODULE_OK;
}

// RDF.TRIPLE.ADD <graph_name> <subject> <predicate> <object>
int RDFTripleAddCommand(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                        int argc) {
  if (argc != 5) {
    return ValkeyModule_WrongArity(ctx);
  }
  size_t g_len, s_len, p_len, o_len;
  const char* g = ValkeyModule_StringPtrLen(argv[1], &g_len);
  const char* s = ValkeyModule_StringPtrLen(argv[2], &s_len);
  const char* p = ValkeyModule_StringPtrLen(argv[3], &p_len);
  const char* o = ValkeyModule_StringPtrLen(argv[4], &o_len);

  auto* graph = GraphManager::Instance().GetGraph(
      absl::string_view(g, g_len));
  if (!graph) {
    ValkeyModule_ReplyWithError(ctx, "ERR graph not found");
    return VALKEYMODULE_OK;
  }

  auto result = graph->AddTriple(absl::string_view(s, s_len),
                                 absl::string_view(p, p_len),
                                 absl::string_view(o, o_len));
  if (!result.ok()) {
    ValkeyModule_ReplyWithError(ctx, result.status().message().data());
    return VALKEYMODULE_OK;
  }
  ValkeyModule_ReplyWithLongLong(ctx, *result ? 1 : 0);
  return VALKEYMODULE_OK;
}

// RDF.TRIPLE.DEL <graph_name> <subject> <predicate> <object>
int RDFTripleDelCommand(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                        int argc) {
  if (argc != 5) {
    return ValkeyModule_WrongArity(ctx);
  }
  size_t g_len, s_len, p_len, o_len;
  const char* g = ValkeyModule_StringPtrLen(argv[1], &g_len);
  const char* s = ValkeyModule_StringPtrLen(argv[2], &s_len);
  const char* p = ValkeyModule_StringPtrLen(argv[3], &p_len);
  const char* o = ValkeyModule_StringPtrLen(argv[4], &o_len);

  auto* graph = GraphManager::Instance().GetGraph(
      absl::string_view(g, g_len));
  if (!graph) {
    ValkeyModule_ReplyWithError(ctx, "ERR graph not found");
    return VALKEYMODULE_OK;
  }

  auto result = graph->DeleteTriple(absl::string_view(s, s_len),
                                    absl::string_view(p, p_len),
                                    absl::string_view(o, o_len));
  if (!result.ok()) {
    ValkeyModule_ReplyWithError(ctx, result.status().message().data());
    return VALKEYMODULE_OK;
  }
  ValkeyModule_ReplyWithLongLong(ctx, *result ? 1 : 0);
  return VALKEYMODULE_OK;
}

// Register all RDF commands
int RegisterRDFCommands(ValkeyModuleCtx* ctx) {
  if (ValkeyModule_CreateCommand(ctx, "RDF.GRAPH.CREATE", RDFGraphCreateCommand,
                                 "write", 0, 0, 0) == VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;
  if (ValkeyModule_CreateCommand(ctx, "RDF.GRAPH.DROP", RDFGraphDropCommand,
                                 "write", 0, 0, 0) == VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;
  if (ValkeyModule_CreateCommand(ctx, "RDF.GRAPH.LIST", RDFGraphListCommand,
                                 "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;
  if (ValkeyModule_CreateCommand(ctx, "RDF.GRAPH.INFO", RDFGraphInfoCommand,
                                 "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;
  if (ValkeyModule_CreateCommand(ctx, "RDF.TRIPLE.ADD", RDFTripleAddCommand,
                                 "write", 0, 0, 0) == VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;
  if (ValkeyModule_CreateCommand(ctx, "RDF.TRIPLE.DEL", RDFTripleDelCommand,
                                 "write", 0, 0, 0) == VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;
  return VALKEYMODULE_OK;
}

}  // namespace valkey_search::rdf

add_library(rdf
  rdf_term.cc
  dictionary.cc
  triple_store.cc
  graph.cc
  commands/rdf_commands.cc
)

target_include_directories(rdf PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(rdf PRIVATE absl::flat_hash_map absl::status absl::strings absl::hash vmsdk)

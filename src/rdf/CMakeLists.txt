set(SRCS_RDF
    ${CMAKE_CURRENT_LIST_DIR}/rdf_term.cc
    ${CMAKE_CURRENT_LIST_DIR}/rdf_term.h
    ${CMAKE_CURRENT_LIST_DIR}/dictionary.cc
    ${CMAKE_CURRENT_LIST_DIR}/dictionary.h
    ${CMAKE_CURRENT_LIST_DIR}/triple_store.cc
    ${CMAKE_CURRENT_LIST_DIR}/triple_store.h
    ${CMAKE_CURRENT_LIST_DIR}/graph.cc
    ${CMAKE_CURRENT_LIST_DIR}/graph.h
    ${CMAKE_CURRENT_LIST_DIR}/commands/rdf_commands.cc
    ${CMAKE_CURRENT_LIST_DIR}/commands/rdf_commands.h)

valkey_search_add_static_library(rdf "${SRCS_RDF}")
target_include_directories(rdf PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(rdf PUBLIC type_conversions)
target_link_libraries(rdf PUBLIC valkey_module)

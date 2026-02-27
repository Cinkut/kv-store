# ── Per-module libraries ──────────────────────────────────────────────────────
add_subdirectory(storage)
add_subdirectory(network)
add_subdirectory(raft)
add_subdirectory(persistence)
add_subdirectory(server)
add_subdirectory(cli)

# ── kvstore_lib: aggregate interface library ──────────────────────────────────
add_library(kvstore_lib INTERFACE)
target_link_libraries(kvstore_lib
    INTERFACE
        kv_storage
        kv_network
        kv_raft
        kv_persistence
        kv_compile_options
)
target_include_directories(kvstore_lib INTERFACE ${CMAKE_SOURCE_DIR}/src)

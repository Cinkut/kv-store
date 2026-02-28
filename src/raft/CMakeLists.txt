# ── kv_raft static library ───────────────────────────────────────────────────
add_library(kv_raft STATIC
    raft_node.cpp
    raft_log.cpp
    state_machine.cpp
)

# ── Protobuf code generation ──────────────────────────────────────────────────
set(_proto_file "${CMAKE_SOURCE_DIR}/proto/raft.proto")
protobuf_generate(
    TARGET      kv_raft
    PROTOS      ${_proto_file}
    IMPORT_DIRS "${CMAKE_SOURCE_DIR}/proto"
    PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}"
)

target_include_directories(kv_raft
    PUBLIC
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_CURRENT_BINARY_DIR}   # generated *.pb.h files live here
)
target_link_libraries(kv_raft
    PUBLIC
        kv_common
        kv_storage
        protobuf::libprotobuf
        Boost::asio
    PRIVATE
        kv_compile_options
)

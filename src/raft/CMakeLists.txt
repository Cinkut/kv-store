add_library(kv_raft STATIC
    raft_node.cpp
    raft_log.cpp
    state_machine.cpp
)
target_include_directories(kv_raft PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kv_raft
    PUBLIC
        kv_common
        kv_storage
    PRIVATE
        kv_compile_options
)

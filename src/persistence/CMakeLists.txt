add_library(kv_persistence STATIC
    wal.cpp
    snapshot.cpp
    snapshot_io_impl.cpp
    wal_persist_callback.cpp
)
target_include_directories(kv_persistence PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kv_persistence
    PUBLIC
        kv_common
        kv_storage
        kv_raft
    PRIVATE
        kv_compile_options
)

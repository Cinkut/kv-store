add_library(kv_persistence STATIC
    wal.cpp
    snapshot.cpp
)
target_include_directories(kv_persistence PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kv_persistence
    PUBLIC
        spdlog::spdlog
        kv_storage
    PRIVATE
        kv_compile_options
)

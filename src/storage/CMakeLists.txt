add_library(kv_storage STATIC
    storage.cpp
    rocksdb_storage.cpp
)
target_include_directories(kv_storage PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kv_storage
    PUBLIC  kv_common
    PRIVATE kv_compile_options rocksdb
)

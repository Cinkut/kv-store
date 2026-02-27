add_library(kv_common STATIC
    logger.cpp
)
target_include_directories(kv_common PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kv_common
    PUBLIC  spdlog::spdlog
    PRIVATE kv_compile_options
)

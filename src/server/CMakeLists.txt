add_executable(kv-server main.cpp)
target_link_libraries(kv-server
    PRIVATE
        kvstore_lib
        Boost::program_options
        kv_compile_options
)

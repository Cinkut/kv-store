add_executable(kv-bench benchmark.cpp)
target_link_libraries(kv-bench
    PRIVATE
        kv_common
        kv_storage
        kv_network
        Boost::asio
        Boost::system
        kv_compile_options
)

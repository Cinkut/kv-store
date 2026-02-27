add_executable(kv-cli main.cpp)
target_link_libraries(kv-cli
    PRIVATE
        kv_common
        Boost::asio
        Boost::system
        Boost::program_options
        kv_compile_options
)

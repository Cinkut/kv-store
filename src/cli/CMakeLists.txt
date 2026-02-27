add_executable(kv-cli main.cpp)
target_link_libraries(kv-cli
    PRIVATE
        Boost::asio
        Boost::system
        spdlog::spdlog
        kv_compile_options
)

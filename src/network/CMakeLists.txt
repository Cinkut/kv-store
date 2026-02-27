add_library(kv_network STATIC
    server.cpp
    session.cpp
    protocol.cpp
)
target_include_directories(kv_network PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kv_network
    PUBLIC
        Boost::asio
        Boost::system
        kv_common
        kv_storage
    PRIVATE
        kv_compile_options
)

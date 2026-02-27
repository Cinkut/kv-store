#pragma once

#include "storage/storage.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <cstdint>
#include <string>

namespace kv::network {

// Owns the io_context and TCP acceptor.
//
// Usage:
//   Server srv{"0.0.0.0", 6379, storage};
//   srv.run();   // blocks until SIGINT/SIGTERM
class Server {
public:
    Server(std::string host, std::uint16_t port, Storage& storage);

    // Starts the thread pool, begins accepting connections, and installs signal
    // handlers for graceful shutdown (SIGINT / SIGTERM).
    // Blocks until the server stops.
    void run();

    // Stops the io_context, causing run() to return.  Safe to call from a
    // signal handler.
    void stop();

private:
    // Accept loop coroutine – runs indefinitely on the io_context.
    boost::asio::awaitable<void> accept_loop();

    std::string host_;
    std::uint16_t port_;
    Storage& storage_;

    boost::asio::io_context ioc_;
    boost::asio::ip::tcp::acceptor acceptor_;
};

} // namespace kv::network

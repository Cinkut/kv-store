#pragma once

#include "network/protocol.hpp"
#include "storage/storage.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <memory>

namespace kv::network {

// Handles one TCP connection for its lifetime.
//
// Each Session is co_spawned from Server::accept_loop() and runs until the
// client disconnects or an error occurs.  All I/O is done via co_await on the
// strand owned by the parent Server.
class Session {
public:
    Session(boost::asio::ip::tcp::socket socket, Storage& storage);

    // Main coroutine.  Reads newline-delimited commands, dispatches to Storage,
    // sends back serialized responses.  Returns when the connection closes.
    boost::asio::awaitable<void> run();

private:
    // Execute a parsed Command against storage and return the appropriate Response.
    [[nodiscard]] Response dispatch(const Command& cmd);

    boost::asio::ip::tcp::socket socket_;
    Storage& storage_;
};

} // namespace kv::network

#pragma once

#include "network/protocol.hpp"
#include "storage/storage.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <memory>
#include <optional>
#include <string>

namespace kv::network {

// Abstract interface providing cluster state to Session.
// When running a Raft cluster, the implementation queries RaftNode state.
// When running standalone (no Raft), no context is provided and all commands
// execute locally.
class ClusterContext {
public:
    virtual ~ClusterContext() = default;

    // Returns true if this node is the Raft leader (or if running standalone).
    [[nodiscard]] virtual bool is_leader() const noexcept = 0;

    // Returns the "host:port" client address of the current leader,
    // or nullopt if the leader is unknown.
    [[nodiscard]] virtual std::optional<std::string> leader_address() const = 0;
};

// Handles one TCP connection for its lifetime.
//
// Each Session is co_spawned from Server::accept_loop() and runs until the
// client disconnects or an error occurs.  All I/O is done via co_await on the
// strand owned by the parent Server.
class Session {
public:
    // Standalone mode (no Raft) – all commands execute locally.
    Session(boost::asio::ip::tcp::socket socket, Storage& storage);

    // Cluster mode – write commands may be redirected to the leader.
    Session(boost::asio::ip::tcp::socket socket, Storage& storage,
            ClusterContext& cluster_ctx);

    // Main coroutine.  Reads newline-delimited commands, dispatches to Storage,
    // sends back serialized responses.  Returns when the connection closes.
    boost::asio::awaitable<void> run();

private:
    // Execute a parsed Command against storage and return the appropriate Response.
    [[nodiscard]] Response dispatch(const Command& cmd);

    boost::asio::ip::tcp::socket socket_;
    Storage& storage_;
    ClusterContext* cluster_ctx_ = nullptr; // null in standalone mode
};

} // namespace kv::network

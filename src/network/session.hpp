#pragma once

#include "network/protocol.hpp"
#include "storage/storage_engine.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

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

    // Returns true if this node holds a valid read lease — meaning it can
    // serve GET/KEYS locally with linearizability guarantees.
    // Always returns true in standalone mode (no cluster).
    [[nodiscard]] virtual bool has_read_lease() const noexcept = 0;

    // Returns the "host:port" client address of the current leader,
    // or nullopt if the leader is unknown.
    [[nodiscard]] virtual std::optional<std::string> leader_address() const = 0;

    // Submit a write command to the Raft cluster and wait for it to be committed.
    // Returns true if the command was committed successfully.
    // Returns false on timeout, leadership loss, or if this node is not the leader.
    //
    // `type` is 1 for SET, 2 for DEL (matching protobuf CommandType enum).
    [[nodiscard]] virtual boost::asio::awaitable<bool>
    submit_write(const std::string& key, const std::string& value, int type) = 0;

    // Submit a cluster configuration change (add/remove server).
    // `new_nodes` is the desired final cluster membership list.
    // Returns true if the config change was successfully initiated.
    // Returns false if this node is not the leader or another config change
    // is already in progress.
    [[nodiscard]] virtual bool
    submit_config_change(std::vector<AddServerCmd> add_nodes,
                         std::vector<uint32_t> remove_node_ids) = 0;

    // Get current cluster membership info for building the new config.
    // Returns a list of {node_id, host, raft_port, client_port} tuples
    // for all nodes (including self) in the current configuration.
    struct NodeEntry {
        uint32_t id;
        std::string host;
        uint16_t raft_port;
        uint16_t client_port;
    };
    [[nodiscard]] virtual std::vector<NodeEntry> current_nodes() const = 0;
};

// Handles one TCP connection for its lifetime.
//
// Each Session is co_spawned from Server::accept_loop() and runs until the
// client disconnects or an error occurs.  All I/O is done via co_await on the
// strand owned by the parent Server.
//
// Protocol auto-detection: the first byte received determines the protocol.
// Bytes 0x00–0x1F → binary, {$,*,+,-,:} → RESP, rest of 0x20–0x7F → text.
class Session {
public:
    // Standalone mode (no Raft) – all commands execute locally.
    Session(boost::asio::ip::tcp::socket socket, StorageEngine& storage);

    // Cluster mode – write commands may be redirected to the leader.
    Session(boost::asio::ip::tcp::socket socket, StorageEngine& storage,
            ClusterContext& cluster_ctx);

    // Main coroutine.  Auto-detects protocol, then loops reading commands,
    // dispatching to Storage, and sending responses.  Returns when the
    // connection closes.
    boost::asio::awaitable<void> run();

private:
    // Execute a parsed Command against storage and return the appropriate Response.
    [[nodiscard]] boost::asio::awaitable<Response> dispatch(const Command& cmd);

    // Process a single parsed request and send the response.
    // Shared between text and binary loops.
    [[nodiscard]] boost::asio::awaitable<Response>
    process_request(std::variant<Command, ErrorResp>& parse_result);

    // Text protocol loop: reads newline-delimited commands.
    // `seed` contains any bytes already read that must be prepended to the buffer.
    boost::asio::awaitable<void> run_text(const std::string& remote, std::string seed);

    // Binary protocol loop: reads 5-byte headers + payloads.
    // `first_header` contains the already-read first 5-byte header.
    boost::asio::awaitable<void> run_binary(const std::string& remote,
                                            std::vector<uint8_t> first_header);

    // RESP protocol loop: reads RESP arrays, dispatches commands.
    // `first_byte` is the already-consumed auto-detect byte.
    boost::asio::awaitable<void> run_resp(const std::string& remote,
                                          uint8_t first_byte);

    boost::asio::ip::tcp::socket socket_;
    StorageEngine& storage_;
    ClusterContext* cluster_ctx_ = nullptr; // null in standalone mode
};

} // namespace kv::network

#pragma once

#include "raft/raft_node.hpp"
#include "network/peer_client.hpp"
#include "network/peer_manager.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>

#include <spdlog/spdlog.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace kv::raft {

// ── RaftTransport ────────────────────────────────────────────────────────────
//
// Production Transport implementation that sends outbound RaftMessages via
// PeerClient connections managed by PeerManager.
//
// Serializes the protobuf RaftMessage and sends it as a length-prefixed frame
// through the PeerClient for the target peer.

class RaftTransport final : public Transport {
public:
    // Constructor.
    //   peer_manager – outbound peer connections (owned externally)
    //   logger       – per-node logger
    RaftTransport(kv::network::PeerManager& peer_manager,
                  std::shared_ptr<spdlog::logger> logger);

    // Send a RaftMessage to a peer.  Returns true on success.
    boost::asio::awaitable<bool>
    send(uint32_t peer_id, RaftMessage msg) override;

private:
    // Find the PeerClient for a given peer ID.  Returns nullptr if not found.
    std::shared_ptr<kv::network::PeerClient> find_client(uint32_t peer_id) const;

    kv::network::PeerManager& peer_manager_;
    std::shared_ptr<spdlog::logger> logger_;
};

// ── RaftRpcListener ──────────────────────────────────────────────────────────
//
// Accepts inbound TCP connections on the Raft RPC port and dispatches
// incoming RaftMessages to the local RaftNode.
//
// Wire format (same as PeerClient): [uint32 BE length][protobuf payload]
//
// For request-type messages (RequestVote, AppendEntries, InstallSnapshot),
// the response is sent back on the same connection.
//
// For response-type messages (vote resp, AE resp, IS resp), no reply is sent.

class RaftRpcListener {
public:
    // Maximum inbound message size (same as PeerClient).
    static constexpr uint32_t kMaxMessageBytes = 64u * 1024u * 1024u; // 64 MiB

    // Constructor.
    //   ioc        – shared io_context
    //   host       – bind address for the Raft RPC port
    //   port       – Raft RPC port number
    //   raft_node  – the local RaftNode to dispatch messages to
    //   logger     – per-node logger
    RaftRpcListener(boost::asio::io_context& ioc,
                    const std::string& host,
                    uint16_t port,
                    RaftNode& raft_node,
                    std::shared_ptr<spdlog::logger> logger);

    // Start accepting connections.
    void start();

    // Stop accepting connections and close all active sessions.
    void stop();

private:
    // Accept loop: spawns one session coroutine per accepted connection.
    boost::asio::awaitable<void> accept_loop();

    // Per-connection session: reads messages, dispatches, sends replies.
    boost::asio::awaitable<void> handle_connection(
        boost::asio::ip::tcp::socket socket);

    boost::asio::io_context& ioc_;
    boost::asio::ip::tcp::acceptor acceptor_;
    RaftNode& raft_node_;
    std::shared_ptr<spdlog::logger> logger_;
    std::atomic<bool> stopped_{false};
};

} // namespace kv::raft

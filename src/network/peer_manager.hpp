#pragma once

#include "common/node_config.hpp"
#include "network/peer_client.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

namespace kv::network {

// ── PeerManager ───────────────────────────────────────────────────────────────
//
// Manages all outbound PeerClient connections for a single cluster node.
//
// On start(): creates one PeerClient per peer in NodeConfig.peers, starts each
// client (triggering async connection with auto-reconnect), and launches a
// periodic monitor coroutine that logs cluster connectivity status.
//
// On stop(): gracefully shuts down all clients and the monitor.
//
// Thread-safety: all state is mutated only from the internal strand. The public
// accessors (peer_count, connected_count) use atomic state_ in PeerClient and
// are safe to call from any thread.

class PeerManager {
public:
    // Interval between periodic connectivity status log messages.
    static constexpr auto kMonitorInterval = std::chrono::seconds{5};

    // Construct a PeerManager.
    //   ioc    – shared io_context
    //   cfg    – this node's configuration (peers list is read from here)
    //   logger – per-node logger (from make_node_logger)
    PeerManager(boost::asio::io_context& ioc,
                const kv::NodeConfig& cfg,
                std::shared_ptr<spdlog::logger> logger);

    // Non-copyable, non-movable.
    PeerManager(const PeerManager&)            = delete;
    PeerManager& operator=(const PeerManager&) = delete;
    PeerManager(PeerManager&&)                 = delete;
    PeerManager& operator=(PeerManager&&)      = delete;

    ~PeerManager() = default;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    // Start all peer clients and the periodic monitor.
    // Must be called once before any other method.
    void start();

    // Stop all peer clients and the monitor.
    // Safe to call from any thread.
    void stop();

    // ── Accessors ─────────────────────────────────────────────────────────────

    // Total number of configured peers.
    [[nodiscard]] std::size_t peer_count() const noexcept {
        return clients_.size();
    }

    // Number of peers currently in Connected state.
    [[nodiscard]] std::size_t connected_count() const noexcept;

    // Read-only view of the peer clients (for Raft RPC layer).
    [[nodiscard]] const std::vector<std::shared_ptr<PeerClient>>&
    clients() const noexcept { return clients_; }

    // ── Dynamic membership ────────────────────────────────────────────────────

    // Add a new peer at runtime.  Creates a PeerClient and starts it.
    // Returns false if a peer with the given id already exists.
    bool add_peer(uint32_t peer_id, const std::string& host, uint16_t raft_port);

    // Remove a peer at runtime.  Stops the PeerClient and removes it.
    // Returns false if no peer with the given id exists.
    bool remove_peer(uint32_t peer_id);

private:
    // Periodic coroutine: logs connectivity status every kMonitorInterval.
    boost::asio::awaitable<void> monitor_loop();

    // ── Data members ──────────────────────────────────────────────────────────

    boost::asio::io_context&        ioc_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    boost::asio::steady_timer       monitor_timer_;
    std::shared_ptr<spdlog::logger> logger_;
    uint32_t                        node_id_;  // This node's id (for logging)

    std::vector<std::shared_ptr<PeerClient>> clients_;

    std::atomic<bool> stopped_{false};
};

} // namespace kv::network

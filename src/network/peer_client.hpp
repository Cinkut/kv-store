#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/steady_timer.hpp>

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace kv::network {

// ── ConnectionState ───────────────────────────────────────────────────────────

enum class ConnectionState : uint8_t {
    Disconnected = 0,
    Connecting   = 1,
    Connected    = 2,
};

// ── PeerClient ────────────────────────────────────────────────────────────────
//
// Async TCP client for Raft peer-to-peer communication.
//
// Features:
//   - Non-blocking connect via co_await (Boost.Asio coroutines)
//   - Auto-reconnect with exponential backoff: 100ms → 200ms → 400ms → 5s cap
//   - Length-prefixed protobuf framing: [uint32 BE length][payload bytes]
//   - All state accessed from the strand_ – not thread-safe from outside
//
// Typical usage (from a coroutine running on the same strand):
//   co_await client.send(serialized_bytes);
//   auto msg = co_await client.receive();
//
// The client starts connecting immediately when start() is called.
// Call stop() to cancel all pending operations and close the socket.

class PeerClient : public std::enable_shared_from_this<PeerClient> {
public:
    // Backoff constants (spec: 100ms → 5s, ×2 each attempt)
    static constexpr auto kMinBackoff = std::chrono::milliseconds{100};
    static constexpr auto kMaxBackoff = std::chrono::milliseconds{5000};

    // Maximum message size we are willing to receive (protect against runaway senders).
    static constexpr uint32_t kMaxMessageBytes = 64u * 1024u * 1024u; // 64 MiB

    // Callback type invoked whenever the connection state changes.
    using StateCallback = std::function<void(ConnectionState)>;

    // Constructor.
    //   ioc        – shared io_context (owned by the Server / application)
    //   peer_id    – numeric id of the remote node (for logging)
    //   host       – remote host
    //   port       – remote Raft port
    //   logger     – per-node spdlog logger
    //   on_state   – optional callback invoked on every state transition
    PeerClient(boost::asio::io_context& ioc,
               uint32_t peer_id,
               std::string host,
               uint16_t port,
               std::shared_ptr<spdlog::logger> logger,
               StateCallback on_state = {});

    // Non-copyable, non-movable (shared_ptr ownership).
    PeerClient(const PeerClient&)            = delete;
    PeerClient& operator=(const PeerClient&) = delete;
    PeerClient(PeerClient&&)                 = delete;
    PeerClient& operator=(PeerClient&&)      = delete;

    ~PeerClient() = default;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    // Start the connect loop (spawns a coroutine on the strand).
    // Safe to call once after construction.
    void start();

    // Stop the client: cancel timers, close socket, suppress future reconnects.
    // Safe to call from any thread.
    void stop();

    // ── State ────────────────────────────────────────────────────────────────

    [[nodiscard]] ConnectionState state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool is_connected() const noexcept {
        return state() == ConnectionState::Connected;
    }

    [[nodiscard]] uint32_t peer_id() const noexcept { return peer_id_; }

    // ── Send / Receive ────────────────────────────────────────────────────────

    // Send a length-prefixed message.
    //   payload – raw serialized protobuf bytes
    // Returns true on success, false if not connected or write failed.
    // Must be called from a coroutine running on the strand.
    [[nodiscard]] boost::asio::awaitable<bool>
    send(std::span<const std::byte> payload);

    // Receive one length-prefixed message.
    // Returns the payload bytes, or empty vector on error / disconnect.
    // Must be called from a coroutine running on the strand.
    [[nodiscard]] boost::asio::awaitable<std::vector<std::byte>> receive();

    // ── Strand accessor (for dispatching work) ────────────────────────────────
    [[nodiscard]] boost::asio::strand<boost::asio::io_context::executor_type>&
    strand() noexcept { return strand_; }

private:
    // ── Internal coroutines ───────────────────────────────────────────────────

    // Top-level reconnect loop – runs until stop() is called.
    boost::asio::awaitable<void> connect_loop();

    // Attempt a single TCP connect to host_:port_.
    // Returns true on success.
    boost::asio::awaitable<bool> try_connect();

    // Update state_ and invoke on_state_ callback.
    void set_state(ConnectionState s);

    // ── Data members ──────────────────────────────────────────────────────────

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    boost::asio::ip::tcp::socket   socket_;
    boost::asio::steady_timer      reconnect_timer_;

    uint32_t    peer_id_;
    std::string host_;
    uint16_t    port_;

    std::shared_ptr<spdlog::logger> logger_;
    StateCallback                   on_state_;

    std::atomic<ConnectionState>    state_{ConnectionState::Disconnected};
    std::atomic<bool>               stopped_{false};
};

} // namespace kv::network

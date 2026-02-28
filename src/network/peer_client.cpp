#include "network/peer_client.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

#include <array>
#include <bit>
#include <cstdint>
#include <cstring>

namespace kv::network {

namespace {
    constexpr auto use_awaitable = boost::asio::as_tuple(boost::asio::use_awaitable);

    // Encode uint32 as big-endian into a 4-byte array.
    std::array<std::byte, 4> encode_be32(uint32_t v) noexcept {
        std::array<std::byte, 4> buf{};
        buf[0] = static_cast<std::byte>((v >> 24) & 0xFF);
        buf[1] = static_cast<std::byte>((v >> 16) & 0xFF);
        buf[2] = static_cast<std::byte>((v >>  8) & 0xFF);
        buf[3] = static_cast<std::byte>( v        & 0xFF);
        return buf;
    }

    // Decode big-endian uint32 from a 4-byte array.
    uint32_t decode_be32(const std::array<std::byte, 4>& buf) noexcept {
        return (static_cast<uint32_t>(buf[0]) << 24)
             | (static_cast<uint32_t>(buf[1]) << 16)
             | (static_cast<uint32_t>(buf[2]) <<  8)
             |  static_cast<uint32_t>(buf[3]);
    }
} // anonymous namespace

// ── Constructor ───────────────────────────────────────────────────────────────

PeerClient::PeerClient(boost::asio::io_context& ioc,
                       uint32_t    peer_id,
                       std::string host,
                       uint16_t    port,
                       std::shared_ptr<spdlog::logger> logger,
                       StateCallback on_state)
    : strand_(boost::asio::make_strand(ioc)),
      socket_(strand_),
      reconnect_timer_(strand_),
      peer_id_(peer_id),
      host_(std::move(host)),
      port_(port),
      logger_(std::move(logger)),
      on_state_(std::move(on_state)) {}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

void PeerClient::start() {
    boost::asio::co_spawn(
        strand_,
        [self = shared_from_this()]() -> boost::asio::awaitable<void> {
            co_await self->connect_loop();
        },
        boost::asio::detached);
}

void PeerClient::stop() {
    stopped_.store(true, std::memory_order_release);

    // Dispatch to the strand so socket/timer operations are safe.
    boost::asio::dispatch(strand_, [self = shared_from_this()]() {
        boost::system::error_code ec;
        self->socket_.close(ec);
        self->reconnect_timer_.cancel();
    });
}

// ── State helper ──────────────────────────────────────────────────────────────

void PeerClient::set_state(ConnectionState s) {
    const auto prev = state_.exchange(s, std::memory_order_acq_rel);
    if (prev == s) return; // no change

    const char* name = [](ConnectionState st) {
        switch (st) {
            case ConnectionState::Disconnected: return "Disconnected";
            case ConnectionState::Connecting:   return "Connecting";
            case ConnectionState::Connected:    return "Connected";
        }
        return "Unknown";
    }(s);

    logger_->info("PeerClient [peer={}] state → {}", peer_id_, name);

    if (on_state_) {
        on_state_(s);
    }
}

// ── Connect loop ──────────────────────────────────────────────────────────────

boost::asio::awaitable<void> PeerClient::connect_loop() {
    auto backoff = kMinBackoff;

    while (!stopped_.load(std::memory_order_acquire)) {
        set_state(ConnectionState::Connecting);

        const bool ok = co_await try_connect();

        if (stopped_.load(std::memory_order_acquire)) {
            break;
        }

        if (ok) {
            set_state(ConnectionState::Connected);
            // Stay connected until an I/O error kicks us back here.
            // The send()/receive() callers will notice is_connected() == false
            // or get error returns.  We just wait for the socket to go dead,
            // detected by the next send/receive returning an error.
            //
            // In practice the caller (Raft RPC layer) will call send() and
            // detect the failure, then call start() again – or more precisely,
            // connect_loop() handles reconnnection itself after send()/receive()
            // detect the error and close the socket.  We simply block here
            // until the socket closes.
            //
            // We wait on the timer with an "infinite" duration so that
            // when stop() cancels it we exit cleanly.
            boost::system::error_code ec;
            reconnect_timer_.expires_after(std::chrono::hours{24 * 365});
            co_await reconnect_timer_.async_wait(use_awaitable);
            // If we get here the timer was cancelled (stop() called) or
            // we were woken up because is_connected() went false (see send/receive
            // error paths below which cancel the timer).
            if (stopped_.load(std::memory_order_acquire)) break;
            // Socket was dropped: fall through to reconnect.
            backoff = kMinBackoff; // reset backoff on successful connect
        } else {
            // Connect attempt failed – log and back off.
            logger_->warn("PeerClient [peer={}] connect to {}:{} failed, retry in {}ms",
                peer_id_, host_, port_,
                std::chrono::duration_cast<std::chrono::milliseconds>(backoff).count());

            set_state(ConnectionState::Disconnected);

            reconnect_timer_.expires_after(backoff);
            auto [ec] = co_await reconnect_timer_.async_wait(use_awaitable);
            if (stopped_.load(std::memory_order_acquire)) break;

            // Exponential backoff with cap.
            backoff = std::min(backoff * 2, kMaxBackoff);
        }
    }

    set_state(ConnectionState::Disconnected);
    logger_->info("PeerClient [peer={}] connect loop exited", peer_id_);
}

// ── try_connect ───────────────────────────────────────────────────────────────

boost::asio::awaitable<bool> PeerClient::try_connect() {
    using tcp = boost::asio::ip::tcp;

    // Close any leftover socket state.
    boost::system::error_code ec;
    socket_.close(ec);

    // Resolve
    tcp::resolver resolver(strand_);
    auto [rec, endpoints] = co_await resolver.async_resolve(
        host_, std::to_string(port_), use_awaitable);
    if (rec) {
        logger_->debug("PeerClient [peer={}] resolve {}:{} failed: {}",
            peer_id_, host_, port_, rec.message());
        co_return false;
    }

    // Connect
    auto [cec, ep] = co_await boost::asio::async_connect(
        socket_, endpoints, use_awaitable);
    if (cec) {
        logger_->debug("PeerClient [peer={}] TCP connect to {}:{} failed: {}",
            peer_id_, host_, port_, cec.message());
        co_return false;
    }

    // Disable Nagle for lower latency on small Raft messages.
    socket_.set_option(tcp::no_delay(true), ec);

    logger_->info("PeerClient [peer={}] connected to {}:{}", peer_id_, host_, port_);
    co_return true;
}

// ── send ──────────────────────────────────────────────────────────────────────

boost::asio::awaitable<bool>
PeerClient::send(std::span<const std::byte> payload) {
    if (!is_connected()) {
        co_return false;
    }

    const auto length = static_cast<uint32_t>(payload.size());
    const auto header = encode_be32(length);

    // Gather-write: header + payload in two buffers.
    std::array<boost::asio::const_buffer, 2> bufs{
        boost::asio::buffer(header.data(), header.size()),
        boost::asio::buffer(payload.data(), payload.size()),
    };

    auto [ec, bytes] = co_await boost::asio::async_write(socket_, bufs, use_awaitable);
    if (ec) {
        logger_->warn("PeerClient [peer={}] send error: {}", peer_id_, ec.message());
        // Close socket so connect_loop knows to reconnect.
        boost::system::error_code close_ec;
        socket_.close(close_ec);
        set_state(ConnectionState::Disconnected);
        // Wake up connect_loop's "wait forever" timer so it can reconnect.
        reconnect_timer_.cancel();
        co_return false;
    }

    logger_->trace("PeerClient [peer={}] sent {} bytes", peer_id_, bytes);
    co_return true;
}

// ── receive ───────────────────────────────────────────────────────────────────

boost::asio::awaitable<std::vector<std::byte>> PeerClient::receive() {
    if (!is_connected()) {
        co_return std::vector<std::byte>{};
    }

    // Read 4-byte header.
    std::array<std::byte, 4> header{};
    auto [hec, hbytes] = co_await boost::asio::async_read(
        socket_,
        boost::asio::buffer(header.data(), header.size()),
        use_awaitable);
    if (hec) {
        logger_->warn("PeerClient [peer={}] receive header error: {}",
            peer_id_, hec.message());
        boost::system::error_code ec;
        socket_.close(ec);
        set_state(ConnectionState::Disconnected);
        reconnect_timer_.cancel();
        co_return std::vector<std::byte>{};
    }

    const uint32_t length = decode_be32(header);
    if (length == 0) {
        co_return std::vector<std::byte>{};
    }
    if (length > kMaxMessageBytes) {
        logger_->error("PeerClient [peer={}] message too large: {} bytes",
            peer_id_, length);
        boost::system::error_code ec;
        socket_.close(ec);
        set_state(ConnectionState::Disconnected);
        reconnect_timer_.cancel();
        co_return std::vector<std::byte>{};
    }

    // Read payload.
    std::vector<std::byte> payload(length);
    auto [pec, pbytes] = co_await boost::asio::async_read(
        socket_,
        boost::asio::buffer(payload.data(), payload.size()),
        use_awaitable);
    if (pec) {
        logger_->warn("PeerClient [peer={}] receive payload error: {}",
            peer_id_, pec.message());
        boost::system::error_code ec;
        socket_.close(ec);
        set_state(ConnectionState::Disconnected);
        reconnect_timer_.cancel();
        co_return std::vector<std::byte>{};
    }

    logger_->trace("PeerClient [peer={}] received {} bytes", peer_id_, pbytes);
    co_return payload;
}

} // namespace kv::network

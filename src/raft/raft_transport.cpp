#include "raft/raft_transport.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>

#include <array>
#include <cstring>

namespace kv::raft {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

// ── Helper: encode/decode 4-byte big-endian length prefix ────────────────────

namespace {

std::array<std::byte, 4> encode_length(uint32_t len)
{
    return {
        static_cast<std::byte>((len >> 24) & 0xFF),
        static_cast<std::byte>((len >> 16) & 0xFF),
        static_cast<std::byte>((len >>  8) & 0xFF),
        static_cast<std::byte>((len      ) & 0xFF),
    };
}

uint32_t decode_length(const std::array<std::byte, 4>& buf)
{
    return (static_cast<uint32_t>(buf[0]) << 24)
         | (static_cast<uint32_t>(buf[1]) << 16)
         | (static_cast<uint32_t>(buf[2]) <<  8)
         | (static_cast<uint32_t>(buf[3]));
}

} // anonymous namespace

// ── RaftTransport ────────────────────────────────────────────────────────────

RaftTransport::RaftTransport(kv::network::PeerManager& peer_manager,
                             std::shared_ptr<spdlog::logger> logger)
    : peer_manager_{peer_manager}
    , logger_{std::move(logger)}
{}

asio::awaitable<bool>
RaftTransport::send(uint32_t peer_id, RaftMessage msg)
{
    auto client = find_client(peer_id);
    if (!client) {
        logger_->warn("RaftTransport: no PeerClient for peer {}", peer_id);
        co_return false;
    }

    if (!client->is_connected()) {
        logger_->debug("RaftTransport: peer {} not connected, dropping message",
                       peer_id);
        co_return false;
    }

    // Serialize the protobuf message.
    std::string serialized;
    if (!msg.SerializeToString(&serialized)) {
        logger_->error("RaftTransport: failed to serialize RaftMessage for peer {}",
                       peer_id);
        co_return false;
    }

    // Send as length-prefixed frame via PeerClient.
    auto payload = std::span<const std::byte>{
        reinterpret_cast<const std::byte*>(serialized.data()),
        serialized.size()
    };

    bool ok = co_await client->send(payload);
    if (!ok) {
        logger_->debug("RaftTransport: send to peer {} failed", peer_id);
    }

    co_return ok;
}

std::shared_ptr<kv::network::PeerClient>
RaftTransport::find_client(uint32_t peer_id) const
{
    for (const auto& client : peer_manager_.clients()) {
        if (client->peer_id() == peer_id) {
            return client;
        }
    }
    return nullptr;
}

void RaftTransport::start_receive_loops()
{
    for (const auto& client : peer_manager_.clients()) {
        asio::co_spawn(raft_node_->strand(),
                       receive_loop(client),
                       asio::detached);
    }
    logger_->info("RaftTransport: started receive loops for {} peers",
                  peer_manager_.clients().size());
}

void RaftTransport::start_receive_loop_for(uint32_t peer_id)
{
    auto client = find_client(peer_id);
    if (!client) {
        logger_->warn("RaftTransport: cannot start receive loop for peer {} "
                      "(not found in PeerManager)", peer_id);
        return;
    }

    asio::co_spawn(raft_node_->strand(),
                   receive_loop(client),
                   asio::detached);

    logger_->info("RaftTransport: started receive loop for peer {}", peer_id);
}

asio::awaitable<void>
RaftTransport::receive_loop(std::shared_ptr<kv::network::PeerClient> client)
{
    uint32_t peer_id = client->peer_id();

    while (true) {
        // Wait until connected before trying to receive.
        if (!client->is_connected()) {
            // Poll periodically — PeerClient reconnects automatically.
            asio::steady_timer delay{co_await asio::this_coro::executor,
                                     std::chrono::milliseconds{100}};
            auto [ec] = co_await delay.async_wait(
                asio::as_tuple(asio::use_awaitable));
            if (ec) co_return;  // cancelled
            continue;
        }

        auto data = co_await client->receive();
        if (data.empty()) {
            // Connection lost or peer closed.  The PeerClient auto-reconnects,
            // so just loop back and wait for reconnection.
            logger_->debug("RaftTransport: receive from peer {} returned empty "
                           "(disconnected?)", peer_id);
            continue;
        }

        RaftMessage msg;
        if (!msg.ParseFromArray(data.data(), static_cast<int>(data.size()))) {
            logger_->warn("RaftTransport: failed to parse response from peer {}",
                          peer_id);
            continue;
        }

        co_await raft_node_->handle_message(peer_id, msg);
    }
}

// ── RaftRpcListener ──────────────────────────────────────────────────────────

RaftRpcListener::RaftRpcListener(asio::io_context& ioc,
                                 const std::string& host,
                                 uint16_t port,
                                 RaftNode& raft_node,
                                 std::shared_ptr<spdlog::logger> logger)
    : ioc_{ioc}
    , acceptor_{ioc}
    , raft_node_{raft_node}
    , logger_{std::move(logger)}
{
    // Resolve and open the acceptor.
    tcp::endpoint endpoint(asio::ip::make_address(host), port);
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();

    logger_->info("RaftRpcListener bound to {}:{}", host, port);
}

void RaftRpcListener::start()
{
    asio::co_spawn(ioc_, accept_loop(), asio::detached);
}

void RaftRpcListener::stop()
{
    stopped_.store(true, std::memory_order_release);
    boost::system::error_code ec;
    acceptor_.close(ec);
}

asio::awaitable<void> RaftRpcListener::accept_loop()
{
    while (!stopped_.load(std::memory_order_acquire)) {
        auto [ec, socket] = co_await acceptor_.async_accept(
            asio::as_tuple(asio::use_awaitable));

        if (ec) {
            if (!stopped_.load(std::memory_order_acquire)) {
                logger_->warn("RaftRpcListener: accept error: {}", ec.message());
            }
            break;
        }

        logger_->debug("RaftRpcListener: accepted connection from {}",
                       socket.remote_endpoint().address().to_string());

        // Spawn a coroutine on the raft strand to handle this connection.
        asio::co_spawn(raft_node_.strand(),
                       handle_connection(std::move(socket)),
                       asio::detached);
    }
}

asio::awaitable<void>
RaftRpcListener::handle_connection(tcp::socket socket)
{
    while (!stopped_.load(std::memory_order_acquire)) {
        // ── Read length prefix (4 bytes, big-endian) ─────────────────────
        std::array<std::byte, 4> len_buf{};
        {
            auto [ec, n] = co_await asio::async_read(
                socket,
                asio::buffer(len_buf),
                asio::as_tuple(asio::use_awaitable));
            if (ec) {
                if (ec != asio::error::eof &&
                    ec != asio::error::operation_aborted) {
                    logger_->debug("RaftRpcListener: read length error: {}",
                                   ec.message());
                }
                co_return;
            }
        }

        uint32_t msg_len = decode_length(len_buf);
        if (msg_len == 0 || msg_len > kMaxMessageBytes) {
            logger_->warn("RaftRpcListener: invalid message length: {}", msg_len);
            co_return;
        }

        // ── Read payload ─────────────────────────────────────────────────
        std::vector<std::byte> payload(msg_len);
        {
            auto [ec, n] = co_await asio::async_read(
                socket,
                asio::buffer(payload),
                asio::as_tuple(asio::use_awaitable));
            if (ec) {
                logger_->debug("RaftRpcListener: read payload error: {}",
                               ec.message());
                co_return;
            }
        }

        // ── Deserialize ──────────────────────────────────────────────────
        RaftMessage msg;
        if (!msg.ParseFromArray(payload.data(), static_cast<int>(msg_len))) {
            logger_->warn("RaftRpcListener: failed to parse RaftMessage "
                          "({} bytes)", msg_len);
            co_return;
        }

        // ── Determine sender ID from the message ─────────────────────────
        // Request messages carry the sender's node ID; response messages
        // need it extracted from their payload.
        uint32_t from_peer = 0;
        if (msg.has_request_vote_req()) {
            from_peer = msg.request_vote_req().candidate_id();
        } else if (msg.has_append_entries_req()) {
            from_peer = msg.append_entries_req().leader_id();
        } else if (msg.has_install_snapshot_req()) {
            from_peer = msg.install_snapshot_req().leader_id();
        }
        // For response messages, from_peer remains 0. The existing
        // handle_vote_response / handle_append_entries_response handlers
        // don't use from_peer for critical logic beyond logging.
        // In practice, responses arrive via outbound PeerClient connections
        // where the peer ID is already known.

        // ── Dispatch to RaftNode ─────────────────────────────────────────
        auto reply_opt = co_await raft_node_.handle_message(from_peer, msg);

        // ── Send reply if one was generated ──────────────────────────────
        if (reply_opt) {
            std::string serialized;
            if (!reply_opt->SerializeToString(&serialized)) {
                logger_->error("RaftRpcListener: failed to serialize reply");
                co_return;
            }

            auto len_bytes = encode_length(
                static_cast<uint32_t>(serialized.size()));

            // Write length + payload.
            std::array<asio::const_buffer, 2> bufs = {
                asio::buffer(len_bytes),
                asio::buffer(serialized),
            };

            auto [ec, n] = co_await asio::async_write(
                socket, bufs,
                asio::as_tuple(asio::use_awaitable));

            if (ec) {
                logger_->debug("RaftRpcListener: write reply error: {}",
                               ec.message());
                co_return;
            }
        }
    }
}

} // namespace kv::raft

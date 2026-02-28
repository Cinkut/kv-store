#include "raft/raft_transport.hpp"
#include "raft/raft_node.hpp"
#include "common/logger.hpp"

#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <array>
#include <chrono>
#include <cstring>
#include <memory>

#include "raft.pb.h"

namespace {

using namespace kv::raft;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;

// ── Helper: encode/decode length prefix ──────────────────────────────────────

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

// ── Helper: send a RaftMessage on a socket, read response ────────────────────

asio::awaitable<RaftMessage> send_and_recv(tcp::socket& sock, RaftMessage msg)
{
    std::string serialized;
    msg.SerializeToString(&serialized);

    auto len_bytes = encode_length(static_cast<uint32_t>(serialized.size()));
    std::array<asio::const_buffer, 2> bufs = {
        asio::buffer(len_bytes), asio::buffer(serialized),
    };
    co_await asio::async_write(sock, bufs,
        asio::as_tuple(asio::use_awaitable));

    std::array<std::byte, 4> resp_len_buf{};
    co_await asio::async_read(sock, asio::buffer(resp_len_buf),
        asio::as_tuple(asio::use_awaitable));
    uint32_t resp_len = decode_length(resp_len_buf);

    std::vector<std::byte> resp_payload(resp_len);
    co_await asio::async_read(sock, asio::buffer(resp_payload),
        asio::as_tuple(asio::use_awaitable));

    RaftMessage reply;
    reply.ParseFromArray(resp_payload.data(), static_cast<int>(resp_len));
    co_return reply;
}

// ── Mocks for RaftNode construction ──────────────────────────────────────────

class NullTransport : public Transport {
public:
    asio::awaitable<bool> send(uint32_t, RaftMessage) override {
        co_return true;
    }
};

class NullTimer : public Timer {
public:
    asio::awaitable<bool> async_wait() override {
        auto exec = co_await asio::this_coro::executor;
        asio::steady_timer t(exec, std::chrono::hours{24});
        wait_timer_ = &t;
        auto [ec] = co_await t.async_wait(
            asio::as_tuple(asio::use_awaitable));
        wait_timer_ = nullptr;
        co_return false;
    }
    void expires_after(std::chrono::milliseconds) override {
        if (wait_timer_) wait_timer_->cancel();
    }
    void cancel() override {
        if (wait_timer_) wait_timer_->cancel();
    }
private:
    asio::steady_timer* wait_timer_ = nullptr;
};

class NullTimerFactory : public TimerFactory {
public:
    std::unique_ptr<Timer> create_timer() override {
        return std::make_unique<NullTimer>();
    }
};

// ── Helper: find free port ───────────────────────────────────────────────────

uint16_t find_free_port() {
    asio::io_context tmp_ioc;
    tcp::acceptor a(tmp_ioc, tcp::endpoint(tcp::v4(), 0));
    auto port = a.local_endpoint().port();
    a.close();
    return port;
}

// ════════════════════════════════════════════════════════════════════════════
//  RaftRpcListener tests
// ════════════════════════════════════════════════════════════════════════════

class RaftRpcListenerTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        logger_ = kv::make_node_logger(1);
    }

    void TearDown() override {
        spdlog::drop("kv");
    }

    asio::io_context ioc_;
    std::shared_ptr<spdlog::logger> logger_;
};

// Listener accepts a connection and dispatches a RequestVote RPC.
TEST_F(RaftRpcListenerTest, AcceptsAndDispatchesRequestVote) {
    uint16_t port = find_free_port();

    NullTransport transport;
    NullTimerFactory timer_factory;
    RaftNode node(ioc_, 1, {2, 3}, transport, timer_factory, logger_);
    node.start();

    RaftRpcListener listener(ioc_, "127.0.0.1", port, node, logger_);
    listener.start();

    bool test_done = false;
    RaftMessage received_reply;

    asio::co_spawn(ioc_, [&]() -> asio::awaitable<void> {
        tcp::socket sock(ioc_);
        co_await sock.async_connect(
            tcp::endpoint(asio::ip::make_address("127.0.0.1"), port),
            asio::as_tuple(asio::use_awaitable));

        RaftMessage msg;
        auto* req = msg.mutable_request_vote_req();
        req->set_term(1);
        req->set_candidate_id(2);
        req->set_last_log_index(0);
        req->set_last_log_term(0);

        received_reply = co_await send_and_recv(sock, std::move(msg));
        test_done = true;

        sock.close();
        listener.stop();
        node.stop();
    }, asio::detached);

    ioc_.run();

    EXPECT_TRUE(test_done);
    ASSERT_TRUE(received_reply.has_request_vote_resp());
    EXPECT_TRUE(received_reply.request_vote_resp().vote_granted());
    EXPECT_EQ(received_reply.request_vote_resp().term(), 1u);
}

// Listener dispatches AppendEntries heartbeat.
TEST_F(RaftRpcListenerTest, AcceptsAndDispatchesAppendEntries) {
    uint16_t port = find_free_port();

    NullTransport transport;
    NullTimerFactory timer_factory;
    RaftNode node(ioc_, 1, {2, 3}, transport, timer_factory, logger_);
    node.start();

    RaftRpcListener listener(ioc_, "127.0.0.1", port, node, logger_);
    listener.start();

    bool test_done = false;
    RaftMessage received_reply;

    asio::co_spawn(ioc_, [&]() -> asio::awaitable<void> {
        tcp::socket sock(ioc_);
        co_await sock.async_connect(
            tcp::endpoint(asio::ip::make_address("127.0.0.1"), port),
            asio::as_tuple(asio::use_awaitable));

        RaftMessage msg;
        auto* req = msg.mutable_append_entries_req();
        req->set_term(1);
        req->set_leader_id(2);
        req->set_prev_log_index(0);
        req->set_prev_log_term(0);
        req->set_leader_commit(0);

        received_reply = co_await send_and_recv(sock, std::move(msg));
        test_done = true;

        sock.close();
        listener.stop();
        node.stop();
    }, asio::detached);

    ioc_.run();

    EXPECT_TRUE(test_done);
    ASSERT_TRUE(received_reply.has_append_entries_resp());
    EXPECT_TRUE(received_reply.append_entries_resp().success());
    EXPECT_EQ(received_reply.append_entries_resp().term(), 1u);
}

// Multiple messages on one connection (connection reuse).
TEST_F(RaftRpcListenerTest, HandlesMultipleMessagesOnConnection) {
    uint16_t port = find_free_port();

    NullTransport transport;
    NullTimerFactory timer_factory;
    RaftNode node(ioc_, 1, {2, 3}, transport, timer_factory, logger_);
    node.start();

    RaftRpcListener listener(ioc_, "127.0.0.1", port, node, logger_);
    listener.start();

    int replies_received = 0;

    asio::co_spawn(ioc_, [&]() -> asio::awaitable<void> {
        tcp::socket sock(ioc_);
        co_await sock.async_connect(
            tcp::endpoint(asio::ip::make_address("127.0.0.1"), port),
            asio::as_tuple(asio::use_awaitable));

        for (int i = 0; i < 3; ++i) {
            RaftMessage msg;
            auto* req = msg.mutable_request_vote_req();
            req->set_term(static_cast<uint64_t>(i + 1));
            req->set_candidate_id(2);
            req->set_last_log_index(0);
            req->set_last_log_term(0);

            auto reply = co_await send_and_recv(sock, std::move(msg));
            if (reply.has_request_vote_resp()) {
                ++replies_received;
            }
        }

        sock.close();
        listener.stop();
        node.stop();
    }, asio::detached);

    ioc_.run();

    EXPECT_EQ(replies_received, 3);
}

// Listener handles clean client disconnect gracefully.
TEST_F(RaftRpcListenerTest, HandlesClientDisconnect) {
    uint16_t port = find_free_port();

    NullTransport transport;
    NullTimerFactory timer_factory;
    RaftNode node(ioc_, 1, {2, 3}, transport, timer_factory, logger_);
    node.start();

    RaftRpcListener listener(ioc_, "127.0.0.1", port, node, logger_);
    listener.start();

    bool connected = false;

    asio::co_spawn(ioc_, [&]() -> asio::awaitable<void> {
        tcp::socket sock(ioc_);
        auto [ec] = co_await sock.async_connect(
            tcp::endpoint(asio::ip::make_address("127.0.0.1"), port),
            asio::as_tuple(asio::use_awaitable));
        if (ec) co_return;
        connected = true;

        // Close immediately — listener should handle EOF gracefully.
        sock.close();

        asio::steady_timer wait(ioc_, std::chrono::milliseconds{50});
        co_await wait.async_wait(asio::as_tuple(asio::use_awaitable));

        listener.stop();
        node.stop();
    }, asio::detached);

    ioc_.run();
    EXPECT_TRUE(connected);
}

// Stopping the listener closes the acceptor cleanly.
TEST_F(RaftRpcListenerTest, StopClosesAcceptor) {
    uint16_t port = find_free_port();

    NullTransport transport;
    NullTimerFactory timer_factory;
    RaftNode node(ioc_, 1, {2, 3}, transport, timer_factory, logger_);
    node.start();

    RaftRpcListener listener(ioc_, "127.0.0.1", port, node, logger_);
    listener.start();

    asio::co_spawn(ioc_, [&]() -> asio::awaitable<void> {
        listener.stop();
        node.stop();
        co_return;
    }, asio::detached);

    ioc_.run();
    // Should complete without hanging.
}

// ════════════════════════════════════════════════════════════════════════════
//  RaftTransport tests
// ════════════════════════════════════════════════════════════════════════════

class RaftTransportTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        logger_ = kv::make_node_logger(1);
    }

    void TearDown() override {
        spdlog::drop("kv");
    }

    asio::io_context ioc_;
    std::shared_ptr<spdlog::logger> logger_;
};

// Send to an unknown peer returns false.
TEST_F(RaftTransportTest, SendToUnknownPeerReturnsFalse) {
    kv::NodeConfig cfg{
        .id = 1,
        .host = "127.0.0.1",
        .client_port = 9000,
        .raft_port = 9001,
        .data_dir = "/tmp/test",
        .snapshot_interval = 0,
        .log_level = "debug",
        .peers = {},
    };

    kv::network::PeerManager pm(ioc_, cfg, logger_);
    RaftTransport transport(pm, logger_);

    bool result = true;
    asio::co_spawn(ioc_, [&]() -> asio::awaitable<void> {
        RaftMessage msg;
        msg.mutable_request_vote_req()->set_term(1);
        result = co_await transport.send(99, std::move(msg));
    }, asio::detached);

    ioc_.run();
    EXPECT_FALSE(result);
}

} // namespace

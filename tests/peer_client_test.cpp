#include "network/peer_client.hpp"
#include "common/logger.hpp"

#include <gtest/gtest.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;
constexpr auto use_awaitable = asio::as_tuple(asio::use_awaitable);

// ── Helper: encode/decode BE32 (mirrors peer_client.cpp) ─────────────────────

static std::array<std::byte, 4> be32(uint32_t v) {
    return {
        static_cast<std::byte>((v >> 24) & 0xFF),
        static_cast<std::byte>((v >> 16) & 0xFF),
        static_cast<std::byte>((v >>  8) & 0xFF),
        static_cast<std::byte>( v        & 0xFF),
    };
}

static uint32_t from_be32(const std::array<std::byte, 4>& b) {
    return (static_cast<uint32_t>(b[0]) << 24)
         | (static_cast<uint32_t>(b[1]) << 16)
         | (static_cast<uint32_t>(b[2]) <<  8)
         |  static_cast<uint32_t>(b[3]);
}

// ── Helper: run io_context in background thread ───────────────────────────────

struct IocFixture {
    asio::io_context ioc{1};
    asio::executor_work_guard<asio::io_context::executor_type> work{
        asio::make_work_guard(ioc)};
    std::thread thread{[this] { ioc.run(); }};

    ~IocFixture() {
        work.reset();
        ioc.stop();
        if (thread.joinable()) thread.join();
    }
};

// ── Helper: find a free port ──────────────────────────────────────────────────

static uint16_t free_port(asio::io_context& ioc) {
    tcp::acceptor a(ioc);
    a.open(tcp::v4());
    a.set_option(tcp::acceptor::reuse_address(true));
    a.bind({tcp::v4(), 0});
    a.listen(1);
    return a.local_endpoint().port();
}

// ── Helper: run coroutine synchronously (blocks calling thread) ───────────────

template <typename Coro>
static void run_coro(asio::io_context& ioc, Coro&& coro) {
    std::promise<void> p;
    auto fut = p.get_future();
    asio::co_spawn(
        ioc,
        [c = std::forward<Coro>(coro), &p]() mutable -> asio::awaitable<void> {
            co_await c();
            p.set_value();
        },
        asio::detached);
    fut.wait();
}

// ── Helper: simple length-prefixed echo server ────────────────────────────────
// Accepts one connection, echoes back exactly one message, then closes.

class EchoServer {
public:
    explicit EchoServer(asio::io_context& ioc)
        : acceptor_(ioc, {tcp::v4(), 0}) {
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
        acceptor_.listen();
        port_ = acceptor_.local_endpoint().port();
    }

    uint16_t port() const noexcept { return port_; }

    // Accept one connection and echo one length-prefixed message back.
    void accept_and_echo_once() {
        asio::co_spawn(
            acceptor_.get_executor(),
            [this]() -> asio::awaitable<void> {
                auto [ec, sock] = co_await acceptor_.async_accept(use_awaitable);
                if (ec) co_return;

                // Read header
                std::array<std::byte, 4> hdr{};
                auto [hec, hb] = co_await asio::async_read(
                    sock, asio::buffer(hdr.data(), 4), use_awaitable);
                if (hec) co_return;

                uint32_t len = from_be32(hdr);
                std::vector<std::byte> payload(len);
                auto [pec, pb] = co_await asio::async_read(
                    sock, asio::buffer(payload.data(), payload.size()), use_awaitable);
                if (pec) co_return;

                // Echo back
                std::array<asio::const_buffer, 2> bufs{
                    asio::buffer(hdr.data(), 4),
                    asio::buffer(payload.data(), payload.size()),
                };
                co_await asio::async_write(sock, bufs, use_awaitable);
            },
            asio::detached);
    }

    // Accept one connection and immediately close it (simulate refusal).
    void accept_and_close() {
        asio::co_spawn(
            acceptor_.get_executor(),
            [this]() -> asio::awaitable<void> {
                auto [ec, sock] = co_await acceptor_.async_accept(use_awaitable);
                // sock destructs immediately → remote sees connection reset
                (void)ec;
            },
            asio::detached);
    }

private:
    tcp::acceptor acceptor_;
    uint16_t      port_{};
};

// ── Null logger (suppress output in tests) ───────────────────────────────────

static std::shared_ptr<spdlog::logger> null_logger() {
    auto l = spdlog::get("null_test");
    if (!l) {
        l = std::make_shared<spdlog::logger>("null_test");
        l->set_level(spdlog::level::off);
    }
    return l;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

class PeerClientTest : public ::testing::Test {
protected:
    IocFixture env_;
    asio::io_context& ioc_{env_.ioc};

    std::shared_ptr<kv::network::PeerClient> make_client(
        uint16_t port,
        kv::network::PeerClient::StateCallback cb = {}) {
        return std::make_shared<kv::network::PeerClient>(
            ioc_, /*peer_id=*/2, "127.0.0.1", port, null_logger(), std::move(cb));
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// 1. Initial state is Disconnected
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerClientTest, InitialStateIsDisconnected) {
    auto client = make_client(19999); // nothing listening there
    EXPECT_EQ(client->state(), kv::network::ConnectionState::Disconnected);
    EXPECT_FALSE(client->is_connected());
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Connects successfully and transitions to Connected
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerClientTest, ConnectsToListeningServer) {
    EchoServer srv(ioc_);
    // Don't need echo for this test; just accept to allow the connection.
    srv.accept_and_echo_once();

    std::promise<kv::network::ConnectionState> state_promise;
    auto state_future = state_promise.get_future();
    bool got_connected = false;

    auto client = make_client(srv.port(),
        [&](kv::network::ConnectionState s) {
            if (s == kv::network::ConnectionState::Connected && !got_connected) {
                got_connected = true;
                state_promise.set_value(s);
            }
        });

    client->start();

    // Wait up to 2s for Connected state.
    const auto status = state_future.wait_for(2s);
    ASSERT_EQ(status, std::future_status::ready);
    EXPECT_EQ(state_future.get(), kv::network::ConnectionState::Connected);
    EXPECT_TRUE(client->is_connected());

    client->stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. send() returns false when not connected
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerClientTest, SendReturnsFalseWhenDisconnected) {
    auto client = make_client(19998); // nothing listening

    std::promise<bool> result_promise;
    asio::co_spawn(
        client->strand(),
        [&]() -> asio::awaitable<void> {
            std::vector<std::byte> data{std::byte{0x01}, std::byte{0x02}};
            bool ok = co_await client->send(data);
            result_promise.set_value(ok);
        },
        asio::detached);

    EXPECT_FALSE(result_promise.get_future().get());
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. receive() returns empty when not connected
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerClientTest, ReceiveReturnsEmptyWhenDisconnected) {
    auto client = make_client(19997);

    std::promise<std::size_t> result_promise;
    asio::co_spawn(
        client->strand(),
        [&]() -> asio::awaitable<void> {
            auto data = co_await client->receive();
            result_promise.set_value(data.size());
        },
        asio::detached);

    EXPECT_EQ(result_promise.get_future().get(), 0u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Send and receive a length-prefixed message round-trip
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerClientTest, SendReceiveRoundTrip) {
    EchoServer srv(ioc_);
    srv.accept_and_echo_once();

    // Wait for connection first.
    std::promise<void> connected_p;
    bool got = false;
    auto client = make_client(srv.port(),
        [&](kv::network::ConnectionState s) {
            if (s == kv::network::ConnectionState::Connected && !got) {
                got = true;
                connected_p.set_value();
            }
        });
    client->start();
    ASSERT_EQ(connected_p.get_future().wait_for(2s), std::future_status::ready);

    // Send a message and receive the echo.
    const std::vector<std::byte> msg{std::byte{'h'}, std::byte{'i'}};
    std::promise<std::vector<std::byte>> recv_p;

    asio::co_spawn(
        client->strand(),
        [&]() -> asio::awaitable<void> {
            bool sent = co_await client->send(msg);
            if (!sent) {
                recv_p.set_value({});
                co_return;
            }
            auto reply = co_await client->receive();
            recv_p.set_value(std::move(reply));
        },
        asio::detached);

    const auto reply = recv_p.get_future().get();
    ASSERT_EQ(reply.size(), msg.size());
    EXPECT_EQ(reply, msg);

    client->stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. stop() transitions to Disconnected
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerClientTest, StopTransitionsToDisconnected) {
    EchoServer srv(ioc_);
    srv.accept_and_echo_once();

    std::promise<void> connected_p;
    bool got = false;
    auto client = make_client(srv.port(),
        [&](kv::network::ConnectionState s) {
            if (s == kv::network::ConnectionState::Connected && !got) {
                got = true;
                connected_p.set_value();
            }
        });
    client->start();
    ASSERT_EQ(connected_p.get_future().wait_for(2s), std::future_status::ready);

    client->stop();

    // Give the strand a moment to process the close.
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(client->state(), kv::network::ConnectionState::Disconnected);
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. peer_id() accessor
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerClientTest, PeerIdAccessor) {
    auto client = make_client(19996);
    EXPECT_EQ(client->peer_id(), 2u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Framing: send an empty payload (length == 0)
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerClientTest, SendEmptyPayloadRoundTrip) {
    EchoServer srv(ioc_);
    srv.accept_and_echo_once();

    std::promise<void> connected_p;
    bool got = false;
    auto client = make_client(srv.port(),
        [&](kv::network::ConnectionState s) {
            if (s == kv::network::ConnectionState::Connected && !got) {
                got = true;
                connected_p.set_value();
            }
        });
    client->start();
    ASSERT_EQ(connected_p.get_future().wait_for(2s), std::future_status::ready);

    std::promise<std::size_t> recv_p;
    asio::co_spawn(
        client->strand(),
        [&]() -> asio::awaitable<void> {
            // empty span
            bool sent = co_await client->send(std::span<const std::byte>{});
            if (!sent) { recv_p.set_value(99); co_return; }
            auto reply = co_await client->receive();
            recv_p.set_value(reply.size());
        },
        asio::detached);

    EXPECT_EQ(recv_p.get_future().get(), 0u);
    client->stop();
}

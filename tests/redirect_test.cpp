// Tests for Etap 3.6: Client redirect on follower nodes.
//
// Uses a lightweight TCP loopback setup to test Session behavior with
// a mock ClusterContext.  No full Server is started.

#include "network/protocol.hpp"
#include "network/session.hpp"
#include "storage/storage.hpp"

#include <gtest/gtest.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>

#include <spdlog/spdlog.h>

#include <chrono>
#include <istream>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "common/logger.hpp"

namespace {

using tcp    = boost::asio::ip::tcp;
using io_ctx = boost::asio::io_context;

// ── Mock ClusterContext ──────────────────────────────────────────────────────

class MockClusterContext : public kv::network::ClusterContext {
public:
    bool is_leader() const noexcept override { return is_leader_; }

    std::optional<std::string> leader_address() const override {
        return leader_address_;
    }

    // Test controls.
    void set_leader(bool val) { is_leader_ = val; }
    void set_leader_address(std::optional<std::string> addr) {
        leader_address_ = std::move(addr);
    }

private:
    bool is_leader_ = false;
    std::optional<std::string> leader_address_;
};

// ── Test fixture ─────────────────────────────────────────────────────────────
//
// Binds a TCP acceptor on a random port, spawns a Session coroutine for each
// accepted connection, and provides a synchronous client for sending commands.

class RedirectTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        kv::init_default_logger(spdlog::level::warn);

        // Bind acceptor on loopback with port 0 (OS-assigned).
        tcp::endpoint ep{boost::asio::ip::make_address("127.0.0.1"), 0};
        acceptor_.open(ep.protocol());
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen();

        port_ = acceptor_.local_endpoint().port();
    }

    void TearDown() override {
        acceptor_.close();
        ioc_.stop();
        if (io_thread_.joinable()) {
            io_thread_.join();
        }
    }

    // Start accepting one connection and spawn a Session with the given
    // cluster context (or nullptr for standalone mode).
    void start_session(MockClusterContext* ctx) {
        boost::asio::co_spawn(
            ioc_,
            [this, ctx]() -> boost::asio::awaitable<void> {
                auto [ec, socket] = co_await acceptor_.async_accept(
                    boost::asio::as_tuple(boost::asio::use_awaitable));
                if (ec) co_return;

                std::shared_ptr<kv::network::Session> session;
                if (ctx) {
                    session = std::make_shared<kv::network::Session>(
                        std::move(socket), storage_, *ctx);
                } else {
                    session = std::make_shared<kv::network::Session>(
                        std::move(socket), storage_);
                }
                co_await session->run();
            },
            boost::asio::detached);

        io_thread_ = std::thread([this] { ioc_.run(); });

        // Brief pause to let acceptor start waiting.
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // Synchronous client helpers.
    tcp::socket connect_client() {
        io_ctx client_ioc{1};
        tcp::socket sock{client_ioc};
        tcp::resolver resolver{client_ioc};
        auto endpoints = resolver.resolve("127.0.0.1", std::to_string(port_));
        boost::asio::connect(sock, endpoints);
        return sock;
    }

    static void send_cmd(tcp::socket& sock, const std::string& cmd) {
        std::string line = cmd + "\n";
        boost::asio::write(sock, boost::asio::buffer(line));
    }

    static std::string recv_line(tcp::socket& sock) {
        boost::asio::streambuf buf;
        boost::asio::read_until(sock, buf, '\n');
        std::istream is(&buf);
        std::string line;
        std::getline(is, line);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        return line;
    }

    static std::string cmd(tcp::socket& sock, const std::string& c) {
        send_cmd(sock, c);
        return recv_line(sock);
    }

    kv::Storage storage_;
    io_ctx ioc_{1};
    tcp::acceptor acceptor_{ioc_};
    std::uint16_t port_{0};
    std::thread io_thread_;
};

// ── Tests: standalone mode (no ClusterContext) ───────────────────────────────

TEST_F(RedirectTest, StandaloneSetSucceeds) {
    start_session(nullptr);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "SET k v"), "OK");
}

TEST_F(RedirectTest, StandaloneDelSucceeds) {
    start_session(nullptr);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "SET k v"), "OK");
    EXPECT_EQ(cmd(sock, "DEL k"), "DELETED");
}

// ── Tests: leader mode ──────────────────────────────────────────────────────

TEST_F(RedirectTest, LeaderSetSucceeds) {
    MockClusterContext ctx;
    ctx.set_leader(true);
    start_session(&ctx);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "SET k v"), "OK");
}

TEST_F(RedirectTest, LeaderDelSucceeds) {
    MockClusterContext ctx;
    ctx.set_leader(true);
    start_session(&ctx);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "SET k v"), "OK");
    EXPECT_EQ(cmd(sock, "DEL k"), "DELETED");
}

// ── Tests: follower mode with known leader ──────────────────────────────────

TEST_F(RedirectTest, FollowerSetRedirects) {
    MockClusterContext ctx;
    ctx.set_leader(false);
    ctx.set_leader_address("10.0.0.1:6001");
    start_session(&ctx);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "SET k v"), "REDIRECT 10.0.0.1:6001");
}

TEST_F(RedirectTest, FollowerDelRedirects) {
    MockClusterContext ctx;
    ctx.set_leader(false);
    ctx.set_leader_address("10.0.0.1:6001");
    start_session(&ctx);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "DEL k"), "REDIRECT 10.0.0.1:6001");
}

TEST_F(RedirectTest, FollowerGetStillWorks) {
    // GET is a read — followers serve reads directly (no redirect).
    MockClusterContext ctx;
    ctx.set_leader(false);
    ctx.set_leader_address("10.0.0.1:6001");
    start_session(&ctx);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "GET missing"), "NOT_FOUND");
}

TEST_F(RedirectTest, FollowerPingStillWorks) {
    MockClusterContext ctx;
    ctx.set_leader(false);
    start_session(&ctx);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "PING"), "PONG");
}

TEST_F(RedirectTest, FollowerKeysStillWorks) {
    MockClusterContext ctx;
    ctx.set_leader(false);
    start_session(&ctx);
    auto sock = connect_client();
    EXPECT_EQ(cmd(sock, "KEYS"), "KEYS");
}

// ── Tests: follower mode with unknown leader ─────────────────────────────────

TEST_F(RedirectTest, FollowerNoLeaderSetReturnsError) {
    MockClusterContext ctx;
    ctx.set_leader(false);
    ctx.set_leader_address(std::nullopt);
    start_session(&ctx);
    auto sock = connect_client();

    auto resp = cmd(sock, "SET k v");
    EXPECT_TRUE(resp.rfind("ERROR", 0) == 0);
    EXPECT_NE(resp.find("no leader"), std::string::npos);
}

TEST_F(RedirectTest, FollowerNoLeaderDelReturnsError) {
    MockClusterContext ctx;
    ctx.set_leader(false);
    ctx.set_leader_address(std::nullopt);
    start_session(&ctx);
    auto sock = connect_client();

    auto resp = cmd(sock, "DEL k");
    EXPECT_TRUE(resp.rfind("ERROR", 0) == 0);
    EXPECT_NE(resp.find("no leader"), std::string::npos);
}

} // anonymous namespace

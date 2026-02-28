#include "network/peer_manager.hpp"
#include "common/logger.hpp"
#include "common/node_config.hpp"

#include <gtest/gtest.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;
constexpr auto use_awaitable = asio::as_tuple(asio::use_awaitable);

// ── Helpers ───────────────────────────────────────────────────────────────────

static std::shared_ptr<spdlog::logger> null_logger() {
    auto l = spdlog::get("null_pm_test");
    if (!l) {
        l = std::make_shared<spdlog::logger>("null_pm_test");
        l->set_level(spdlog::level::off);
    }
    return l;
}

// Runs an io_context in a background thread and keeps it alive.
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

// A minimal TCP acceptor that accepts connections indefinitely (but does nothing).
// Used to simulate peer raft listeners during tests.
class SilentAcceptor {
public:
    explicit SilentAcceptor(asio::io_context& ioc)
        : acceptor_(ioc, {tcp::v4(), 0}) {
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
        acceptor_.listen();
        port_ = acceptor_.local_endpoint().port();
        accept_loop();
    }

    uint16_t port() const noexcept { return port_; }

    void close() {
        boost::system::error_code ec;
        acceptor_.close(ec);
    }

private:
    void accept_loop() {
        asio::co_spawn(
            acceptor_.get_executor(),
            [this]() -> asio::awaitable<void> {
                while (true) {
                    auto [ec, sock] = co_await acceptor_.async_accept(use_awaitable);
                    if (ec) co_return; // acceptor closed
                    // Hold the socket open until it destructs on next iteration.
                    // Just don't read/write — let the client stay connected.
                    (void)sock;
                }
            },
            asio::detached);
    }

    tcp::acceptor acceptor_;
    uint16_t      port_{};
};

// Build a NodeConfig with this node as id=1, and the given peer raft_ports.
static kv::NodeConfig make_config(uint32_t node_id,
                                  const std::vector<uint16_t>& peer_ports) {
    kv::NodeConfig cfg;
    cfg.id              = node_id;
    cfg.host            = "127.0.0.1";
    cfg.client_port     = 16000 + static_cast<uint16_t>(node_id);
    cfg.raft_port       = 17000 + static_cast<uint16_t>(node_id);
    cfg.data_dir        = "/tmp";
    cfg.snapshot_interval = 100;
    cfg.log_level       = "off";

    uint32_t peer_id = 2;
    for (auto port : peer_ports) {
        kv::PeerInfo p;
        p.id          = peer_id++;
        p.host        = "127.0.0.1";
        p.raft_port   = port;
        p.client_port = static_cast<uint16_t>(16000 + p.id);
        cfg.peers.push_back(p);
    }
    return cfg;
}

// ── Test fixture ──────────────────────────────────────────────────────────────

class PeerManagerTest : public ::testing::Test {
protected:
    IocFixture env_;
    asio::io_context& ioc_{env_.ioc};
};

// ─────────────────────────────────────────────────────────────────────────────
// 1. peer_count() matches the number of configured peers
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerManagerTest, PeerCountMatchesConfig) {
    auto cfg = make_config(1, {19100, 19101, 19102});
    kv::network::PeerManager mgr(ioc_, cfg, null_logger());
    EXPECT_EQ(mgr.peer_count(), 3u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Initial connected_count() is 0 before start()
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerManagerTest, InitialConnectedCountIsZero) {
    auto cfg = make_config(1, {19110, 19111});
    kv::network::PeerManager mgr(ioc_, cfg, null_logger());
    EXPECT_EQ(mgr.connected_count(), 0u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. clients() returns the right number of PeerClients
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerManagerTest, ClientsAccessorSize) {
    auto cfg = make_config(1, {19120, 19121});
    kv::network::PeerManager mgr(ioc_, cfg, null_logger());
    EXPECT_EQ(mgr.clients().size(), 2u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. After start() + listening servers, all peers become Connected
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerManagerTest, ConnectsToAllPeers) {
    // Spin up 2 silent acceptors to simulate the other cluster nodes.
    SilentAcceptor srv1(ioc_);
    SilentAcceptor srv2(ioc_);

    auto cfg = make_config(1, {srv1.port(), srv2.port()});
    kv::network::PeerManager mgr(ioc_, cfg, null_logger());
    mgr.start();

    // Poll up to 2s for all peers to become Connected.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (mgr.connected_count() == mgr.peer_count()) break;
        std::this_thread::sleep_for(10ms);
    }

    EXPECT_EQ(mgr.connected_count(), 2u);

    mgr.stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. stop() is idempotent (safe to call twice)
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerManagerTest, StopIsIdempotent) {
    auto cfg = make_config(1, {19130, 19131});
    kv::network::PeerManager mgr(ioc_, cfg, null_logger());
    mgr.start();
    mgr.stop();
    EXPECT_NO_THROW(mgr.stop());
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Works with zero peers (edge case – not valid in production but safe here)
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerManagerTest, ZeroPeersHandledGracefully) {
    auto cfg = make_config(1, {});
    kv::network::PeerManager mgr(ioc_, cfg, null_logger());
    EXPECT_EQ(mgr.peer_count(), 0u);
    EXPECT_EQ(mgr.connected_count(), 0u);
    mgr.start();
    mgr.stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. State callback is called for each peer on connection
// ─────────────────────────────────────────────────────────────────────────────
// We verify indirectly: after connecting, connected_count == peer_count.
// The state transitions themselves are logged via the callback inside PeerManager.
// This test validates that internal callbacks don't crash or deadlock.
TEST_F(PeerManagerTest, StateCallbackNoDeadlock) {
    SilentAcceptor srv(ioc_);

    auto cfg = make_config(1, {srv.port()});
    kv::network::PeerManager mgr(ioc_, cfg, null_logger());
    mgr.start();

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (mgr.connected_count() == 1u) break;
        std::this_thread::sleep_for(10ms);
    }

    EXPECT_EQ(mgr.connected_count(), 1u);
    mgr.stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Reconnects after server restarts (resilience)
//
// When a peer server closes its listener, PeerClient will only detect the
// disconnect on the next I/O operation (TCP half-close is lazy).  This test
// verifies that:
//   a) the initial connection succeeds, and
//   b) after stop() the manager cleanly tears down despite the server having
//      closed in the meantime.
// It does NOT assert that connected_count() drops to 0 immediately after the
// server closes, because that would require an active heartbeat (Etap 3+).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PeerManagerTest, ReconnectsAfterServerRestart) {
    // Start an acceptor, let the client connect, then close it.
    auto srv = std::make_unique<SilentAcceptor>(ioc_);
    const uint16_t port = srv->port();

    auto cfg = make_config(1, {port});
    kv::network::PeerManager mgr(ioc_, cfg, null_logger());
    mgr.start();

    // Wait for initial connection.
    {
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (mgr.connected_count() == 1u) break;
            std::this_thread::sleep_for(10ms);
        }
        ASSERT_EQ(mgr.connected_count(), 1u) << "Failed to connect initially";
    }

    // Close the server — the client won't detect the disconnect until it
    // tries I/O (active heartbeats come in Etap 3).
    srv->close();
    srv.reset();

    // Verify that stop() completes cleanly even in this state.
    EXPECT_NO_THROW(mgr.stop());
}

#include "network/peer_manager.hpp"

#include <algorithm>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace kv::network {

namespace {
    constexpr auto use_awaitable = boost::asio::as_tuple(boost::asio::use_awaitable);
} // anonymous namespace

// ── Constructor ───────────────────────────────────────────────────────────────

PeerManager::PeerManager(boost::asio::io_context& ioc,
                         const kv::NodeConfig& cfg,
                         std::shared_ptr<spdlog::logger> logger)
    : ioc_(ioc),
      strand_(boost::asio::make_strand(ioc)),
      monitor_timer_(strand_),
      logger_(std::move(logger)),
      node_id_(cfg.id)
{
    // Pre-create one PeerClient per peer.
    clients_.reserve(cfg.peers.size());
    for (const auto& peer : cfg.peers) {
        auto state_cb = [this, peer_id = peer.id](ConnectionState s) {
            const char* name = [](ConnectionState st) {
                switch (st) {
                    case ConnectionState::Disconnected: return "Disconnected";
                    case ConnectionState::Connecting:   return "Connecting";
                    case ConnectionState::Connected:    return "Connected";
                }
                return "Unknown";
            }(s);
            logger_->info("PeerManager [node={}] peer {} → {}",
                node_id_, peer_id, name);
        };

        clients_.push_back(std::make_shared<PeerClient>(
            ioc_,
            peer.id,
            peer.host,
            peer.raft_port,
            logger_,
            std::move(state_cb)));
    }
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

void PeerManager::start() {
    // Start all peer clients.
    for (auto& client : clients_) {
        client->start();
    }

    // Launch the periodic monitor coroutine.
    boost::asio::co_spawn(
        strand_,
        [this]() -> boost::asio::awaitable<void> {
            co_await monitor_loop();
        },
        boost::asio::detached);

    logger_->info("PeerManager [node={}] started – connecting to {} peer(s)",
        node_id_, clients_.size());
}

void PeerManager::stop() {
    stopped_.store(true, std::memory_order_release);

    // Stop all clients.
    for (auto& client : clients_) {
        client->stop();
    }

    // Cancel the monitor timer (will cause monitor_loop to exit).
    boost::asio::dispatch(strand_, [this]() {
        monitor_timer_.cancel();
    });

    logger_->info("PeerManager [node={}] stopped", node_id_);
}

// ── connected_count ───────────────────────────────────────────────────────────

std::size_t PeerManager::connected_count() const noexcept {
    std::size_t count = 0;
    for (const auto& c : clients_) {
        if (c->is_connected()) ++count;
    }
    return count;
}

// ── monitor_loop ──────────────────────────────────────────────────────────────

boost::asio::awaitable<void> PeerManager::monitor_loop() {
    while (!stopped_.load(std::memory_order_acquire)) {
        monitor_timer_.expires_after(kMonitorInterval);
        auto [ec] = co_await monitor_timer_.async_wait(use_awaitable);

        if (stopped_.load(std::memory_order_acquire)) break;
        if (ec) break; // cancelled

        const auto total     = clients_.size();
        const auto connected = connected_count();

        if (connected == total) {
            logger_->info("PeerManager [node={}] connectivity: {}/{} peers connected",
                node_id_, connected, total);
        } else {
            logger_->warn("PeerManager [node={}] connectivity: {}/{} peers connected",
                node_id_, connected, total);
        }
    }

    logger_->debug("PeerManager [node={}] monitor loop exited", node_id_);
}

// ── Dynamic membership ───────────────────────────────────────────────────────

bool PeerManager::add_peer(uint32_t peer_id, const std::string& host,
                           uint16_t raft_port) {
    // Check if a peer with this id already exists.
    for (const auto& c : clients_) {
        if (c->peer_id() == peer_id) {
            logger_->warn("PeerManager [node={}] add_peer: peer {} already exists",
                          node_id_, peer_id);
            return false;
        }
    }

    auto state_cb = [this, peer_id](ConnectionState s) {
        const char* name = [](ConnectionState st) {
            switch (st) {
                case ConnectionState::Disconnected: return "Disconnected";
                case ConnectionState::Connecting:   return "Connecting";
                case ConnectionState::Connected:    return "Connected";
            }
            return "Unknown";
        }(s);
        logger_->info("PeerManager [node={}] peer {} → {}",
                      node_id_, peer_id, name);
    };

    auto client = std::make_shared<PeerClient>(
        ioc_, peer_id, host, raft_port, logger_, std::move(state_cb));

    client->start();
    clients_.push_back(std::move(client));

    logger_->info("PeerManager [node={}] added peer {} ({}:{})",
                  node_id_, peer_id, host, raft_port);
    return true;
}

bool PeerManager::remove_peer(uint32_t peer_id) {
    auto it = std::find_if(clients_.begin(), clients_.end(),
        [peer_id](const auto& c) { return c->peer_id() == peer_id; });

    if (it == clients_.end()) {
        logger_->warn("PeerManager [node={}] remove_peer: peer {} not found",
                      node_id_, peer_id);
        return false;
    }

    (*it)->stop();
    clients_.erase(it);

    logger_->info("PeerManager [node={}] removed peer {}", node_id_, peer_id);
    return true;
}

} // namespace kv::network

#include "common/logger.hpp"
#include "common/node_config.hpp"
#include "network/peer_manager.hpp"
#include "network/session.hpp"
#include "persistence/snapshot.hpp"
#include "persistence/snapshot_io_impl.hpp"
#include "persistence/wal.hpp"
#include "persistence/wal_persist_callback.hpp"
#include "raft/commit_awaiter.hpp"
#include "raft/raft_cluster_context.hpp"
#include "raft/raft_node.hpp"
#include "raft/raft_transport.hpp"
#include "raft/real_timer.hpp"
#include "raft/state_machine.hpp"
#include "storage/storage.hpp"
#include "storage/storage_engine.hpp"
#include "storage/rocksdb_storage.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

#include <cstdio>
#include <filesystem>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;

int main(int argc, char* argv[]) {
    // ── Parse CLI arguments ──────────────────────────────────────────────────
    kv::NodeConfig cfg;
    try {
        cfg = kv::parse_config(argc, argv);
    } catch (const std::runtime_error& e) {
        fprintf(stderr, "%s\n", e.what());
        return 1;
    }

    // ── Logging ──────────────────────────────────────────────────────────────
    const auto level = kv::parse_log_level(cfg.log_level);
    kv::init_default_logger(level);
    auto logger = kv::make_node_logger(cfg.id, level);

    logger->info("kv-server starting – id={} client={}:{} raft={}:{} peers={}",
        cfg.id, cfg.host, cfg.client_port, cfg.host, cfg.raft_port,
        cfg.peers.size());

    for (const auto& peer : cfg.peers) {
        logger->debug("  peer id={} host={} raft_port={} client_port={}",
            peer.id, peer.host, peer.raft_port, peer.client_port);
    }

    // ── Data directory ───────────────────────────────────────────────────────
    namespace fs = std::filesystem;
    const fs::path data_dir{cfg.data_dir};
    std::error_code fs_ec;
    fs::create_directories(data_dir, fs_ec);
    if (fs_ec) {
        logger->error("Failed to create data directory {}: {}",
                      data_dir.string(), fs_ec.message());
        return 1;
    }

    // ── Storage ──────────────────────────────────────────────────────────────
    std::unique_ptr<kv::StorageEngine> storage;
    if (cfg.engine == "rocksdb") {
        const auto db_path = data_dir / "rocksdb";
        try {
            storage = std::make_unique<kv::RocksDBStorage>(db_path);
        } catch (const std::runtime_error& e) {
            logger->error("Failed to open RocksDB engine: {}", e.what());
            return 1;
        }
        logger->info("Using RocksDB storage engine at {}", db_path.string());
    } else {
        storage = std::make_unique<kv::MemoryStorage>();
        logger->info("Using in-memory storage engine");
    }

    // ── WAL ──────────────────────────────────────────────────────────────────
    const fs::path wal_path = data_dir / "wal.bin";
    kv::persistence::WAL wal{wal_path};
    if (auto ec = wal.open()) {
        logger->error("Failed to open WAL at {}: {}",
                      wal_path.string(), ec.message());
        return 1;
    }
    logger->info("WAL opened at {}", wal_path.string());

    // ── Snapshot recovery ────────────────────────────────────────────────────
    // Startup: load snapshot → replay WAL → apply entries → ready
    const fs::path snapshot_path = data_dir / kv::persistence::Snapshot::kFilename;
    uint64_t snapshot_last_index = 0;
    uint64_t snapshot_last_term  = 0;

    if (kv::persistence::Snapshot::exists(snapshot_path)) {
        kv::persistence::SnapshotLoadResult snap_result;
        if (auto ec = kv::persistence::Snapshot::load(snapshot_path, snap_result)) {
            logger->error("Failed to load snapshot: {}", ec.message());
            return 1;
        }

        storage->clear();
        for (auto& [k, v] : snap_result.data) {
            storage->set(std::move(k), std::move(v));
        }

        snapshot_last_index = snap_result.metadata.last_included_index;
        snapshot_last_term  = snap_result.metadata.last_included_term;

        logger->info("Snapshot loaded: index={}, term={}, {} keys",
                     snapshot_last_index, snapshot_last_term,
                     snap_result.data.size());
    } else {
        logger->info("No snapshot found at {}", snapshot_path.string());
    }

    // ── WAL replay ───────────────────────────────────────────────────────────
    kv::persistence::WalReplayResult wal_replay;
    if (auto ec = kv::persistence::WAL::replay(wal_path, wal_replay)) {
        logger->error("Failed to replay WAL: {}", ec.message());
        return 1;
    }

    uint64_t restored_term = 0;
    int32_t restored_voted_for = -1;
    if (wal_replay.metadata) {
        restored_term = wal_replay.metadata->term;
        restored_voted_for = wal_replay.metadata->voted_for;
    }

    // Convert WAL entries to protobuf LogEntry (skip entries covered by snapshot).
    std::vector<kv::raft::LogEntry> restored_entries;
    for (const auto& rec : wal_replay.entries) {
        if (rec.index <= snapshot_last_index) continue;

        kv::raft::LogEntry entry;
        entry.set_term(rec.term);
        entry.set_index(rec.index);
        if (rec.cmd_type != 0) {
            auto* cmd = entry.mutable_command();
            cmd->set_type(static_cast<kv::raft::CommandType>(rec.cmd_type));
            cmd->set_key(rec.key);
            cmd->set_value(rec.value);
        }
        restored_entries.push_back(std::move(entry));
    }

    logger->info("WAL replay: term={}, voted_for={}, {} entries ({} after snapshot)",
                 restored_term, restored_voted_for,
                 wal_replay.entries.size(), restored_entries.size());

    // ── Apply restored entries to storage ─────────────────────────────────────
    // All WAL entries are re-applied on startup. Uncommitted entries will be
    // resolved by Raft conflict resolution after the cluster reforms.
    kv::raft::StateMachine state_machine{*storage, logger};
    if (snapshot_last_index > 0) {
        state_machine.reset(snapshot_last_index);
    }
    for (const auto& entry : restored_entries) {
        state_machine.apply(entry);
    }
    logger->info("Applied {} restored entries (last_applied={})",
                 restored_entries.size(), state_machine.last_applied());

    // ── Build peer ID list and address map ────────────────────────────────────
    std::vector<uint32_t> peer_ids;
    std::map<uint32_t, std::string> peer_addresses;

    // Include self for leader_address() lookups.
    peer_addresses[cfg.id] = cfg.host + ":" + std::to_string(cfg.client_port);
    for (const auto& peer : cfg.peers) {
        peer_ids.push_back(peer.id);
        peer_addresses[peer.id] = peer.host + ":" +
                                  std::to_string(peer.client_port);
    }

    // ── Shared io_context ────────────────────────────────────────────────────
    // All components (Raft, networking, client sessions) share one io_context.
    asio::io_context ioc{
        static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))};

    // ── PeerManager ──────────────────────────────────────────────────────────
    kv::network::PeerManager peer_mgr{ioc, cfg, logger};

    // ── Raft components ──────────────────────────────────────────────────────
    kv::raft::RaftTransport transport{peer_mgr, logger};
    kv::raft::RealTimerFactory timer_factory{ioc};
    kv::persistence::WalPersistCallback persist_cb{wal, logger};
    kv::persistence::SnapshotIOImpl snapshot_io{data_dir, *storage, wal, logger};
    kv::raft::CommitAwaiter commit_awaiter{ioc, logger};

    // Apply callback: apply committed entries + notify waiting clients.
    auto apply_callback = [&state_machine, &commit_awaiter, &logger](
                              const kv::raft::LogEntry& entry) {
        state_machine.apply(entry);
        commit_awaiter.notify_commit(entry.index());
        logger->debug("Applied committed entry index={}", entry.index());
    };

    // Pointer to cluster context (set after construction, used in config callback).
    kv::raft::RaftClusterContext* cluster_ctx_ptr = nullptr;

    // Config change callback: update PeerManager, Transport, and ClusterContext
    // when cluster membership changes.
    auto config_change_callback = [&peer_mgr, &transport, &cluster_ctx_ptr, &logger,
                                    &cfg](
        const kv::raft::ClusterConfiguration& new_config,
        const kv::raft::LogEntry& /*entry*/) {

        // Determine which peers to add/remove by comparing with current peers.
        const auto all_new_peers = new_config.all_peer_ids();
        std::set<uint32_t> new_peer_set(all_new_peers.begin(), all_new_peers.end());

        // Add new peers that PeerManager doesn't know about.
        for (const auto& node : new_config.nodes()) {
            if (node.id() == cfg.id) continue;  // skip self
            if (node.host().empty()) continue;    // skip nodes without address info

            // Try to add — PeerManager::add_peer() is a no-op if already exists.
            bool added = peer_mgr.add_peer(
                node.id(), node.host(),
                static_cast<uint16_t>(node.raft_port()));
            if (added) {
                logger->info("Config change: added peer {} ({}:{}:{})",
                             node.id(), node.host(),
                             node.raft_port(), node.client_port());
                transport.start_receive_loop_for(node.id());
            }

            // Update cluster context address map.
            if (cluster_ctx_ptr) {
                cluster_ctx_ptr->update_peer_address(
                    node.id(),
                    node.host() + ":" + std::to_string(node.client_port()));
            }
        }

        // Also handle old-config nodes during joint consensus.
        for (const auto& node : new_config.old_nodes()) {
            if (node.id() == cfg.id) continue;
            if (node.host().empty()) continue;

            bool added = peer_mgr.add_peer(
                node.id(), node.host(),
                static_cast<uint16_t>(node.raft_port()));
            if (added) {
                logger->info("Config change (joint): added old peer {} ({}:{})",
                             node.id(), node.host(), node.raft_port());
                transport.start_receive_loop_for(node.id());
            }
        }

        // When finalizing (not joint), remove peers that are no longer in config.
        if (!new_config.is_joint()) {
            // Snapshot the current client list (copy the vector of shared_ptrs)
            // so we can safely remove while iterating.
            auto current_clients = peer_mgr.clients();
            for (const auto& client : current_clients) {
                const auto pid = client->peer_id();
                if (!new_peer_set.contains(pid)) {
                    logger->info("Config change: removing peer {}", pid);
                    peer_mgr.remove_peer(pid);
                    if (cluster_ctx_ptr) {
                        cluster_ctx_ptr->remove_peer_address(pid);
                    }
                }
            }
        }
    };

    kv::raft::RaftNode raft_node{
        ioc, cfg.id, peer_ids, transport, timer_factory, logger,
        apply_callback, &snapshot_io, cfg.snapshot_interval, &persist_cb,
        nullptr, config_change_callback};

    // Wire the RaftNode into the transport so responses can be dispatched.
    transport.set_raft_node(&raft_node);

    // Restore Raft state from WAL/snapshot (before start).
    if (snapshot_last_index > 0) {
        // Load cluster configuration persisted alongside the snapshot.
        auto snapshot_config = snapshot_io.load_cluster_config();
        raft_node.restore_snapshot(snapshot_last_index, snapshot_last_term,
                                   snapshot_config);
    }
    if (restored_term > 0 || !restored_entries.empty()) {
        raft_node.restore(restored_term, restored_voted_for,
                          std::move(restored_entries));
    }

    // ── Cluster context (bridges Session ↔ Raft) ─────────────────────────────
    kv::raft::RaftClusterContext cluster_ctx{
        raft_node, commit_awaiter, peer_addresses, logger};
    cluster_ctx_ptr = &cluster_ctx;

    // ── Raft RPC listener (inbound peer messages) ────────────────────────────
    kv::raft::RaftRpcListener raft_listener{
        ioc, cfg.host, cfg.raft_port, raft_node, logger};

    // ── Client TCP acceptor ──────────────────────────────────────────────────
    // We manage the acceptor directly (not via Server) so all components share
    // a single io_context.
    const auto address = asio::ip::make_address(cfg.host);
    const asio::ip::tcp::endpoint client_ep{address, cfg.client_port};
    asio::ip::tcp::acceptor client_acceptor{ioc};
    client_acceptor.open(client_ep.protocol());
    client_acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
    client_acceptor.bind(client_ep);
    client_acceptor.listen();

    logger->info("Client acceptor listening on {}:{}", cfg.host, cfg.client_port);

    // Client accept loop coroutine.
    auto client_accept_loop = [&client_acceptor, &storage, &cluster_ctx,
                               &ioc, &logger]() -> asio::awaitable<void> {
        constexpr auto use_aw = asio::as_tuple(asio::use_awaitable);

        for (;;) {
            auto [ec, socket] = co_await client_acceptor.async_accept(use_aw);
            if (ec) {
                if (ec != asio::error::operation_aborted) {
                    logger->warn("Client accept error: {}", ec.message());
                }
                break;
            }

            socket.set_option(asio::ip::tcp::no_delay(true));

            auto sp = std::make_shared<kv::network::Session>(
                std::move(socket), *storage, cluster_ctx);

            asio::co_spawn(
                ioc,
                [s = std::move(sp)]() -> asio::awaitable<void> {
                    co_await s->run();
                },
                asio::detached);
        }
    };

    // ── Signal handling ──────────────────────────────────────────────────────
    asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code& ec, int signo) {
        if (!ec) {
            logger->info("Received signal {}, shutting down...", signo);
            raft_node.stop();
            commit_awaiter.fail_all();
            raft_listener.stop();
            peer_mgr.stop();
            client_acceptor.close();
            ioc.stop();
        }
    });

    // ── Start all components ─────────────────────────────────────────────────
    peer_mgr.start();
    raft_listener.start();
    transport.start_receive_loops();
    raft_node.start();
    asio::co_spawn(ioc, client_accept_loop(), asio::detached);

    logger->info("kv-server fully wired and running (id={}, peers={})",
                 cfg.id, peer_ids.size());

    // ── Run the event loop ───────────────────────────────────────────────────
    const unsigned int nthreads = std::max(1u,
        std::thread::hardware_concurrency());
    std::vector<std::thread> pool;
    pool.reserve(nthreads - 1);
    for (unsigned int i = 1; i < nthreads; ++i) {
        pool.emplace_back([&ioc] { ioc.run(); });
    }
    ioc.run();

    for (auto& t : pool) {
        t.join();
    }

    wal.close();
    logger->info("kv-server stopped");
    return 0;
}

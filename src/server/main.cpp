#include "common/logger.hpp"
#include "common/node_config.hpp"
#include "network/peer_manager.hpp"
#include "network/server.hpp"
#include "storage/storage.hpp"

#include <spdlog/spdlog.h>

#include <cstdio>

int main(int argc, char* argv[]) {
    // ── Parse and validate CLI arguments ──────────────────────────────────────
    kv::NodeConfig cfg;
    try {
        cfg = kv::parse_config(argc, argv);
    } catch (const std::runtime_error& e) {
        // Logger not yet initialised – use fprintf.
        fprintf(stderr, "%s\n", e.what());
        return 1;
    }

    // ── Logging ───────────────────────────────────────────────────────────────
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

    // ── Storage + Network ─────────────────────────────────────────────────────
    kv::Storage storage;
    kv::network::Server server{cfg.host, cfg.client_port, storage};

    // PeerManager needs the io_context from the Server.
    // We start the peer manager before running the server event loop.
    kv::network::PeerManager peer_mgr{server.io_context(), cfg, logger};
    peer_mgr.start();

    server.run(); // blocks until SIGINT/SIGTERM

    peer_mgr.stop();
    logger->info("kv-server stopped");
    return 0;
}

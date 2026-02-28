#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

namespace kv {

// ── PeerInfo ──────────────────────────────────────────────────────────────────
// Describes a single peer node in the cluster.

struct PeerInfo {
    uint32_t    id;          // Peer node ID (must be unique, > 0)
    std::string host;        // Peer bind host / IP
    uint16_t    raft_port;   // Peer Raft RPC port
    uint16_t    client_port; // Peer client-facing port (for REDIRECT responses)
};

// ── NodeConfig ────────────────────────────────────────────────────────────────
// Full configuration for one kv-server node.
// Populated by parse_config() from CLI arguments.

struct NodeConfig {
    uint32_t    id;                 // This node's ID (must be > 0, unique in cluster)
    std::string host;               // Bind address for client connections
    uint16_t    client_port;        // Port for client (text protocol) connections
    uint16_t    raft_port;          // Port for Raft RPC connections
    std::string data_dir;           // Directory for WAL and snapshot files
    uint32_t    snapshot_interval;  // Committed entries between snapshots
    std::string log_level;          // spdlog level string
    std::string engine;             // Storage engine: "memory" (default) or "rocksdb"

    std::vector<PeerInfo> peers;    // All other nodes in the cluster (>= 2 required)
};

// ── parse_config ──────────────────────────────────────────────────────────────
// Parse CLI arguments into a NodeConfig.
//
// On success: returns a fully validated NodeConfig.
// On error  : throws std::runtime_error with a human-readable message.
//
// Validates:
//   - id > 0
//   - client_port and raft_port in [1, 65535]
//   - peers list has at least 2 entries
//   - each peer id > 0, peer raft_port and client_port in [1, 65535]
//   - no peer has the same id as this node
//
// Peers format: --peers id:host:raft_port:client_port[,id:host:raft_port:client_port,...]
//   Example: --peers 2:127.0.0.1:7002:6380,3:127.0.0.1:7003:6381

[[nodiscard]] NodeConfig parse_config(int argc, char* argv[]);

// ── add_options ───────────────────────────────────────────────────────────────
// Populate a boost::program_options::options_description with kv-server options.
// Exposed for testing and help-text generation.

void add_options(boost::program_options::options_description& desc);

} // namespace kv

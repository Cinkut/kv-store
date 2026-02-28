#include "common/node_config.hpp"

#include <charconv>
#include <format>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

namespace kv {

namespace {

// ── Helpers ───────────────────────────────────────────────────────────────────

// Parse an unsigned integer from string_view.
// Returns the value or throws std::runtime_error on failure.
template <typename T>
[[nodiscard]] T parse_uint(std::string_view sv, std::string_view field_name) {
    T value{};
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), value);
    if (ec != std::errc{} || ptr != sv.data() + sv.size()) {
        throw std::runtime_error(
            std::format("Invalid integer for {}: '{}'", field_name, sv));
    }
    return value;
}

// Validate that a port number is in [1, 65535].
void validate_port(uint16_t port, std::string_view field_name) {
    if (port == 0) {
        throw std::runtime_error(
            std::format("Port for {} must be in [1, 65535], got 0", field_name));
    }
    // uint16_t max is 65535 by definition – no upper bound check needed.
}

// Parse the --peers string into a vector of PeerInfo.
// Format: id:host:raft_port:client_port[,id:host:raft_port:client_port,...]
[[nodiscard]] std::vector<PeerInfo> parse_peers(const std::string& peers_str) {
    if (peers_str.empty()) {
        return {};
    }

    std::vector<PeerInfo> result;

    // Split by comma
    std::string_view remaining{peers_str};
    while (!remaining.empty()) {
        // Find next comma (or end of string)
        auto comma_pos = remaining.find(',');
        std::string_view entry = (comma_pos == std::string_view::npos)
            ? remaining
            : remaining.substr(0, comma_pos);

        if (comma_pos == std::string_view::npos) {
            remaining = {};
        } else {
            remaining = remaining.substr(comma_pos + 1);
        }

        // Split entry by ':'  → id : host : raft_port : client_port
        // We need exactly 4 colon-separated fields.
        // host may contain '.' but not ':', so find colons by position.
        auto first_colon = entry.find(':');
        if (first_colon == std::string_view::npos) {
            throw std::runtime_error(
                std::format("Malformed peer entry (expected id:host:raft_port:client_port): '{}'", entry));
        }

        // Find second colon (after host)
        auto second_colon = entry.find(':', first_colon + 1);
        if (second_colon == std::string_view::npos) {
            throw std::runtime_error(
                std::format("Malformed peer entry (expected id:host:raft_port:client_port): '{}'", entry));
        }

        // Find third colon (after raft_port)
        auto third_colon = entry.find(':', second_colon + 1);
        if (third_colon == std::string_view::npos) {
            throw std::runtime_error(
                std::format("Malformed peer entry (expected id:host:raft_port:client_port): '{}'", entry));
        }

        // Ensure no extra colons
        if (entry.find(':', third_colon + 1) != std::string_view::npos) {
            throw std::runtime_error(
                std::format("Malformed peer entry (too many fields): '{}'", entry));
        }

        std::string_view id_sv          = entry.substr(0, first_colon);
        std::string_view host_sv        = entry.substr(first_colon + 1, second_colon - first_colon - 1);
        std::string_view raft_port_sv   = entry.substr(second_colon + 1, third_colon - second_colon - 1);
        std::string_view client_port_sv = entry.substr(third_colon + 1);

        PeerInfo peer;
        peer.id          = parse_uint<uint32_t>(id_sv,          "peer id");
        peer.host        = std::string(host_sv);
        peer.raft_port   = parse_uint<uint16_t>(raft_port_sv,   "peer raft_port");
        peer.client_port = parse_uint<uint16_t>(client_port_sv, "peer client_port");

        if (peer.id == 0) {
            throw std::runtime_error(
                std::format("Peer id must be > 0, got 0 in entry '{}'", entry));
        }
        validate_port(peer.raft_port,   "peer raft_port");
        validate_port(peer.client_port, "peer client_port");

        if (peer.host.empty()) {
            throw std::runtime_error(
                std::format("Peer host must not be empty in entry '{}'", entry));
        }

        result.push_back(std::move(peer));
    }

    return result;
}

// Validate the fully populated NodeConfig.
void validate(const NodeConfig& cfg) {
    if (cfg.id == 0) {
        throw std::runtime_error("Node id must be > 0");
    }
    validate_port(cfg.client_port, "--client-port");
    validate_port(cfg.raft_port,   "--raft-port");

    if (cfg.data_dir.empty()) {
        throw std::runtime_error("--data-dir must not be empty");
    }
    if (cfg.snapshot_interval == 0) {
        throw std::runtime_error("--snapshot-interval must be > 0");
    }

    if (cfg.engine != "memory" && cfg.engine != "rocksdb") {
        throw std::runtime_error(
            std::format("--engine must be 'memory' or 'rocksdb', got '{}'", cfg.engine));
    }

    // At least 2 peers required (for a majority quorum of 3 nodes).
    if (cfg.peers.size() < 2) {
        throw std::runtime_error(
            std::format("At least 2 peers required, got {}", cfg.peers.size()));
    }

    // No peer may share this node's id.
    for (const auto& peer : cfg.peers) {
        if (peer.id == cfg.id) {
            throw std::runtime_error(
                std::format("Peer id {} is the same as this node's id", peer.id));
        }
    }

    // No duplicate peer ids.
    for (std::size_t i = 0; i < cfg.peers.size(); ++i) {
        for (std::size_t j = i + 1; j < cfg.peers.size(); ++j) {
            if (cfg.peers[i].id == cfg.peers[j].id) {
                throw std::runtime_error(
                    std::format("Duplicate peer id {} in --peers", cfg.peers[i].id));
            }
        }
    }
}

} // anonymous namespace

// ── add_options ───────────────────────────────────────────────────────────────

void add_options(po::options_description& desc) {
    desc.add_options()
        ("help,h",
            "Show this help message and exit")
        ("id",
            po::value<uint32_t>()->required(),
            "Node ID – unique positive integer in the cluster")
        ("host",
            po::value<std::string>()->default_value("0.0.0.0"),
            "Bind address for client connections")
        ("client-port",
            po::value<uint16_t>()->default_value(6379),
            "Port for client (text protocol) connections")
        ("raft-port",
            po::value<uint16_t>()->default_value(7001),
            "Port for Raft RPC connections")
        ("peers",
            po::value<std::string>()->required(),
            "Comma-separated peer list: id:host:raft_port:client_port[,...]")
        ("data-dir",
            po::value<std::string>()->default_value("./data"),
            "Directory for WAL and snapshot files")
        ("snapshot-interval",
            po::value<uint32_t>()->default_value(1000),
            "Number of committed entries between snapshots")
        ("log-level",
            po::value<std::string>()->default_value("info"),
            "Log level: trace|debug|info|warn|error|critical")
        ("engine",
            po::value<std::string>()->default_value("memory"),
            "Storage engine: memory (default) or rocksdb");
}

// ── parse_config ──────────────────────────────────────────────────────────────

NodeConfig parse_config(int argc, char* argv[]) {
    po::options_description desc("kv-server options");
    add_options(desc);

    po::variables_map vm;
    try {
        po::store(
            po::parse_command_line(argc, argv, desc),
            vm);

        // Handle --help before notify() so missing required options don't error.
        if (vm.count("help")) {
            std::ostringstream oss;
            oss << desc;
            throw std::runtime_error(oss.str());
        }

        po::notify(vm);
    } catch (const po::error& e) {
        throw std::runtime_error(std::format("Argument error: {}", e.what()));
    }

    NodeConfig cfg;
    cfg.id                = vm["id"].as<uint32_t>();
    cfg.host              = vm["host"].as<std::string>();
    cfg.client_port       = vm["client-port"].as<uint16_t>();
    cfg.raft_port         = vm["raft-port"].as<uint16_t>();
    cfg.data_dir          = vm["data-dir"].as<std::string>();
    cfg.snapshot_interval = vm["snapshot-interval"].as<uint32_t>();
    cfg.log_level         = vm["log-level"].as<std::string>();
    cfg.engine            = vm["engine"].as<std::string>();
    cfg.peers             = parse_peers(vm["peers"].as<std::string>());

    validate(cfg);
    return cfg;
}

} // namespace kv

#pragma once

#include "network/session.hpp"
#include "raft/commit_awaiter.hpp"
#include "raft/raft_node.hpp"
#include "common/node_config.hpp"

#include <boost/asio/awaitable.hpp>
#include <spdlog/spdlog.h>

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace kv::raft {

// ── RaftClusterContext ───────────────────────────────────────────────────────
//
// Production implementation of kv::network::ClusterContext that bridges
// the client-facing Session layer with the Raft consensus layer.
//
// - is_leader() queries RaftNode::state()
// - leader_address() resolves leader_id to host:client_port using peer info
// - submit_write() submits to RaftNode::submit(), then awaits CommitAwaiter
// - submit_config_change() builds the new NodeInfo list and calls
//   RaftNode::submit_config_change()
//
// NOT thread-safe — all methods must be called from the Raft strand or
// the session coroutine (which runs on the same io_context).

class RaftClusterContext : public network::ClusterContext {
public:
    // peer_info maps peer_id → "host:client_port" for all nodes including self.
    RaftClusterContext(RaftNode& node,
                       CommitAwaiter& awaiter,
                       std::map<uint32_t, std::string> peer_addresses,
                       std::shared_ptr<spdlog::logger> logger = {})
        : node_(node)
        , awaiter_(awaiter)
        , peer_addresses_(std::move(peer_addresses))
        , logger_(std::move(logger))
    {}

    [[nodiscard]] bool is_leader() const noexcept override {
        return node_.state() == NodeState::Leader;
    }

    [[nodiscard]] bool has_read_lease() const noexcept override {
        return node_.has_read_lease();
    }

    [[nodiscard]] std::optional<std::string> leader_address() const override {
        auto lid = node_.leader_id();
        if (!lid) return std::nullopt;
        auto it = peer_addresses_.find(*lid);
        if (it == peer_addresses_.end()) return std::nullopt;
        return it->second;
    }

    [[nodiscard]] boost::asio::awaitable<bool>
    submit_write(const std::string& key, const std::string& value,
                 int type) override;

    [[nodiscard]] bool
    submit_config_change(std::vector<AddServerCmd> add_nodes,
                         std::vector<uint32_t> remove_node_ids) override;

    [[nodiscard]] std::vector<NodeEntry> current_nodes() const override;

    // Update the peer_addresses map when nodes are added/removed.
    void update_peer_address(uint32_t node_id, const std::string& address) {
        peer_addresses_[node_id] = address;
    }

    void remove_peer_address(uint32_t node_id) {
        peer_addresses_.erase(node_id);
    }

private:
    RaftNode& node_;
    CommitAwaiter& awaiter_;
    std::map<uint32_t, std::string> peer_addresses_; // node_id → "host:client_port"
    std::shared_ptr<spdlog::logger> logger_;
};

} // namespace kv::raft

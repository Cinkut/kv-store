#pragma once

#include "raft.pb.h"

#include <algorithm>
#include <cstdint>
#include <set>
#include <vector>

namespace kv::raft {

// ── ClusterConfiguration ────────────────────────────────────────────────────
//
// Manages the active cluster membership for Raft joint consensus.
//
// In a stable configuration, only `nodes_` (C_new) is populated.
// During a membership transition, both `old_nodes_` (C_old) and `nodes_`
// (C_new) are populated — this is the "joint consensus" phase.
//
// Quorum rules:
//   - Stable config:  majority of nodes_ must agree.
//   - Joint config:   majority of old_nodes_ AND majority of nodes_ must
//                      independently agree.
//
// Thread safety: not thread-safe. All access must be on the Raft strand.

class ClusterConfiguration {
public:
    // Construct a stable configuration from a list of peer IDs (not including
    // self).  This matches the original RaftNode constructor interface.
    ClusterConfiguration(uint32_t self_id, std::vector<uint32_t> peer_ids)
        : self_id_{self_id}
        , peer_ids_{std::move(peer_ids)} {}

    // Construct from a protobuf ClusterConfig (e.g. from a log entry or
    // snapshot).  The config's new_nodes define the target membership.
    // If old_nodes is non-empty, we are in joint consensus.
    ClusterConfiguration(uint32_t self_id, const ClusterConfig& proto)
        : self_id_{self_id} {
        for (const auto& node : proto.new_nodes()) {
            if (node.id() != self_id) {
                peer_ids_.push_back(node.id());
            }
            nodes_.push_back(node);
        }
        for (const auto& node : proto.old_nodes()) {
            old_nodes_.push_back(node);
            // Track old peers that aren't already in the new config.
            if (node.id() != self_id) {
                bool in_new = std::any_of(peer_ids_.begin(), peer_ids_.end(),
                    [&](uint32_t id) { return id == node.id(); });
                if (!in_new) {
                    old_only_peer_ids_.push_back(node.id());
                }
            }
        }
        in_joint_ = !old_nodes_.empty();
    }

    // ── Peer accessors ──────────────────────────────────────────────────────

    // All peer IDs in the current (new) configuration, excluding self.
    [[nodiscard]] const std::vector<uint32_t>& peer_ids() const noexcept {
        return peer_ids_;
    }

    // All peer IDs that need to receive RPCs.  During joint consensus this
    // includes peers that are in old_nodes but not in new_nodes.
    [[nodiscard]] std::vector<uint32_t> all_peer_ids() const {
        if (!in_joint_) return peer_ids_;
        std::set<uint32_t> ids(peer_ids_.begin(), peer_ids_.end());
        for (uint32_t id : old_only_peer_ids_) {
            ids.insert(id);
        }
        return {ids.begin(), ids.end()};
    }

    // Peer IDs that exist in the old config but not the new config.
    // Empty when not in joint consensus.
    [[nodiscard]] const std::vector<uint32_t>& old_only_peer_ids() const noexcept {
        return old_only_peer_ids_;
    }

    // ── Quorum checking ─────────────────────────────────────────────────────

    // Check whether a set of node IDs (voters) constitutes a quorum.
    // `voters` should include self_id if this node voted/acked.
    //
    // In stable config: majority of all nodes (self + peer_ids_).
    // In joint config:  majority in BOTH old and new configs independently.
    [[nodiscard]] bool has_quorum(const std::set<uint32_t>& voters) const {
        if (!has_majority(new_member_ids(), voters)) {
            return false;
        }
        if (in_joint_ && !has_majority(old_member_ids(), voters)) {
            return false;
        }
        return true;
    }

    // Convenience: check quorum from a vote/ack count by building the voter
    // set from peer match data.  This is used by try_advance_commit() which
    // checks per-index.  Use has_quorum(set) for simpler cases.

    // ── Joint consensus state ───────────────────────────────────────────────

    [[nodiscard]] bool is_joint() const noexcept { return in_joint_; }

    // Whether this node is a member of the new configuration.
    [[nodiscard]] bool self_in_new_config() const {
        return std::any_of(nodes_.begin(), nodes_.end(),
            [this](const NodeInfo& n) { return n.id() == self_id_; });
    }

    // Whether this node is a member of the old configuration (or if stable,
    // whether it's a member at all).
    [[nodiscard]] bool self_in_old_config() const {
        if (!in_joint_) {
            // In stable config, check new nodes.
            return self_in_new_config();
        }
        return std::any_of(old_nodes_.begin(), old_nodes_.end(),
            [this](const NodeInfo& n) { return n.id() == self_id_; });
    }

    // ── Configuration transitions ───────────────────────────────────────────

    // Begin a joint consensus transition: enter C_old,new.
    // `new_config_nodes` is the full target node list for C_new.
    // The current configuration becomes C_old.
    void begin_joint(const std::vector<NodeInfo>& new_config_nodes) {
        // Current becomes old.
        old_nodes_ = nodes_;

        // Set new.
        nodes_ = new_config_nodes;
        peer_ids_.clear();
        old_only_peer_ids_.clear();

        for (const auto& node : nodes_) {
            if (node.id() != self_id_) {
                peer_ids_.push_back(node.id());
            }
        }
        // Find old-only peers.
        for (const auto& node : old_nodes_) {
            if (node.id() != self_id_) {
                bool in_new = std::any_of(peer_ids_.begin(), peer_ids_.end(),
                    [&](uint32_t id) { return id == node.id(); });
                if (!in_new) {
                    old_only_peer_ids_.push_back(node.id());
                }
            }
        }
        in_joint_ = true;
    }

    // Finalize the transition: commit C_new, drop C_old.
    void finalize() {
        old_nodes_.clear();
        old_only_peer_ids_.clear();
        in_joint_ = false;
    }

    // ── Protobuf serialization ──────────────────────────────────────────────

    // Build a ClusterConfig protobuf for the current state.
    [[nodiscard]] ClusterConfig to_proto() const {
        ClusterConfig cfg;
        for (const auto& node : nodes_) {
            *cfg.add_new_nodes() = node;
        }
        for (const auto& node : old_nodes_) {
            *cfg.add_old_nodes() = node;
        }
        return cfg;
    }

    // Build a ClusterConfig protobuf for only the new (stable) config.
    [[nodiscard]] ClusterConfig to_stable_proto() const {
        ClusterConfig cfg;
        for (const auto& node : nodes_) {
            *cfg.add_new_nodes() = node;
        }
        // old_nodes deliberately empty — stable config.
        return cfg;
    }

    // ── Node info accessors ─────────────────────────────────────────────────

    [[nodiscard]] const std::vector<NodeInfo>& nodes() const noexcept {
        return nodes_;
    }

    [[nodiscard]] const std::vector<NodeInfo>& old_nodes() const noexcept {
        return old_nodes_;
    }

    // Total cluster size (new config).
    [[nodiscard]] int cluster_size() const noexcept {
        return static_cast<int>(nodes_.size());
    }

    // Initialize from the startup peer list (builds NodeInfo with just IDs).
    // Used during initial construction when we only have peer IDs, not full
    // NodeInfo.  Full NodeInfo will be populated once the first config entry
    // is received or when the admin provides it.
    void init_from_peer_ids(uint32_t self_id, const std::vector<uint32_t>& peer_ids) {
        nodes_.clear();
        // Add self.
        NodeInfo self_node;
        self_node.set_id(self_id);
        nodes_.push_back(self_node);
        // Add peers.
        for (uint32_t peer_id : peer_ids) {
            NodeInfo node;
            node.set_id(peer_id);
            nodes_.push_back(node);
        }
    }

private:
    // All member IDs (including self) for the new config.
    [[nodiscard]] std::vector<uint32_t> new_member_ids() const {
        std::vector<uint32_t> ids;
        ids.reserve(nodes_.size());
        for (const auto& node : nodes_) {
            ids.push_back(node.id());
        }
        // If nodes_ is empty (initial construction from peer_ids only),
        // fall back to self + peer_ids_.
        if (ids.empty()) {
            ids.push_back(self_id_);
            ids.insert(ids.end(), peer_ids_.begin(), peer_ids_.end());
        }
        return ids;
    }

    // All member IDs (including self) for the old config.
    [[nodiscard]] std::vector<uint32_t> old_member_ids() const {
        std::vector<uint32_t> ids;
        ids.reserve(old_nodes_.size());
        for (const auto& node : old_nodes_) {
            ids.push_back(node.id());
        }
        return ids;
    }

    // Check if `voters` contains a strict majority of `members`.
    [[nodiscard]] static bool has_majority(const std::vector<uint32_t>& members,
                                           const std::set<uint32_t>& voters) {
        int count = 0;
        for (uint32_t id : members) {
            if (voters.contains(id)) ++count;
        }
        int required = static_cast<int>(members.size()) / 2 + 1;
        return count >= required;
    }

    uint32_t self_id_;
    std::vector<uint32_t> peer_ids_;           // new-config peers (excluding self)
    std::vector<uint32_t> old_only_peer_ids_;  // peers in old but not new config
    std::vector<NodeInfo> nodes_;              // full new-config node list
    std::vector<NodeInfo> old_nodes_;          // full old-config node list (empty if stable)
    bool in_joint_ = false;
};

} // namespace kv::raft

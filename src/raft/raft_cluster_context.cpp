#include "raft/raft_cluster_context.hpp"
#include "raft.pb.h"

namespace kv::raft {

boost::asio::awaitable<bool>
RaftClusterContext::submit_write(const std::string& key,
                                 const std::string& value,
                                 int type) {
    if (node_.state() != NodeState::Leader) {
        if (logger_) {
            logger_->warn("RaftClusterContext: submit_write called on non-leader");
        }
        co_return false;
    }

    // Build the Raft Command protobuf.
    Command cmd;
    cmd.set_type(static_cast<CommandType>(type));
    cmd.set_key(key);
    cmd.set_value(value);

    // The next log index will be log.last_index() + 1 after submit.
    const uint64_t expected_index = node_.log().last_index() + 1;

    bool submitted = node_.submit(std::move(cmd));
    if (!submitted) {
        if (logger_) {
            logger_->warn("RaftClusterContext: submit() failed (not leader?)");
        }
        co_return false;
    }

    if (logger_) {
        logger_->debug("RaftClusterContext: submitted write at index {}", expected_index);
    }

    // Wait for the entry to be committed.
    bool committed = co_await awaiter_.wait_for_commit(expected_index);

    if (logger_) {
        logger_->debug("RaftClusterContext: index {} {}",
                        expected_index, committed ? "committed" : "failed");
    }

    co_return committed;
}

bool RaftClusterContext::submit_config_change(
    std::vector<kv::AddServerCmd> add_nodes,
    std::vector<uint32_t> remove_node_ids) {

    if (node_.state() != NodeState::Leader) {
        if (logger_) {
            logger_->warn("RaftClusterContext: config change called on non-leader");
        }
        return false;
    }

    // Build the new node list from current config + additions - removals.
    const auto& current_config = node_.config();
    std::vector<NodeInfo> new_nodes;

    // Keep existing nodes that aren't being removed.
    std::set<uint32_t> remove_set(remove_node_ids.begin(), remove_node_ids.end());
    for (const auto& node : current_config.nodes()) {
        if (!remove_set.contains(node.id())) {
            new_nodes.push_back(node);
        }
    }

    // Add new nodes.
    for (const auto& add : add_nodes) {
        // Check if this node already exists.
        bool exists = false;
        for (const auto& existing : new_nodes) {
            if (existing.id() == add.node_id) {
                exists = true;
                break;
            }
        }
        if (exists) {
            if (logger_) {
                logger_->warn("RaftClusterContext: node {} already in config", add.node_id);
            }
            return false;
        }
        NodeInfo info;
        info.set_id(add.node_id);
        info.set_host(add.host);
        info.set_raft_port(add.raft_port);
        info.set_client_port(add.client_port);
        new_nodes.push_back(std::move(info));
    }

    if (logger_) {
        logger_->info("RaftClusterContext: submitting config change ({} nodes → {} nodes)",
                       current_config.cluster_size(), new_nodes.size());
    }

    return node_.submit_config_change(std::move(new_nodes));
}

std::vector<network::ClusterContext::NodeEntry>
RaftClusterContext::current_nodes() const {
    std::vector<NodeEntry> result;
    const auto& current_config = node_.config();
    for (const auto& node : current_config.nodes()) {
        result.push_back({
            node.id(),
            node.host(),
            static_cast<uint16_t>(node.raft_port()),
            static_cast<uint16_t>(node.client_port())
        });
    }
    return result;
}

} // namespace kv::raft

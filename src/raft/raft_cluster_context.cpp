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

} // namespace kv::raft

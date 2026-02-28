#include "raft/commit_awaiter.hpp"

#include <boost/asio/this_coro.hpp>

namespace kv::raft {

using boost::asio::awaitable;
using boost::asio::steady_timer;
using boost::asio::as_tuple;
using boost::asio::use_awaitable;

awaitable<bool>
CommitAwaiter::wait_for_commit(uint64_t index,
                               std::chrono::milliseconds timeout) {
    // If already committed by the time we call, return immediately.
    // (Shouldn't normally happen, but handle gracefully.)

    auto [it, inserted] = pending_.emplace(index, PendingEntry{});
    if (!inserted) {
        // Duplicate wait on the same index — shouldn't happen.
        if (logger_) {
            logger_->warn("CommitAwaiter: duplicate wait for index {}", index);
        }
        co_return false;
    }

    // Create a timer that serves as the signal mechanism.
    it->second.timer = std::make_unique<steady_timer>(ioc_, timeout);

    auto [ec] = co_await it->second.timer->async_wait(as_tuple(use_awaitable));

    // ec will be operation_aborted if timer was cancelled (by notify_commit or fail_all).
    // Check the committed flag to distinguish success vs failure.
    bool committed = it->second.committed;
    pending_.erase(it);

    if (committed) {
        if (logger_) {
            logger_->debug("CommitAwaiter: index {} committed", index);
        }
        co_return true;
    }

    // Timed out or cancelled via fail_all.
    if (logger_) {
        logger_->warn("CommitAwaiter: index {} {}", index,
                       ec ? "cancelled" : "timed out");
    }
    co_return false;
}

void CommitAwaiter::notify_commit(uint64_t index) {
    auto it = pending_.find(index);
    if (it == pending_.end()) {
        return; // No waiter for this index — normal for follower-applied entries.
    }

    it->second.committed = true;
    it->second.timer->cancel();
}

void CommitAwaiter::fail_all() {
    for (auto& [index, entry] : pending_) {
        // committed stays false — waiter will see failure.
        entry.timer->cancel();
    }
    if (logger_ && !pending_.empty()) {
        logger_->info("CommitAwaiter: failed {} pending waiters",
                       pending_.size());
    }
}

} // namespace kv::raft

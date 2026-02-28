#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

namespace kv::raft {

// ── CommitAwaiter ────────────────────────────────────────────────────────────
//
// Allows client coroutines to wait for a specific Raft log index to be
// committed.  Used by the integration layer when a client submits a write:
//
//   1. Leader appends entry at index N via RaftNode::submit()
//   2. Client coroutine calls wait_for_commit(N) → suspends
//   3. When ApplyCallback fires for index N, notify_commit(N) wakes the waiter
//   4. The awaitable resolves and the client gets a response
//
// On leadership loss, fail_all() cancels all pending waiters.
//
// NOT thread-safe — must be used on a single strand (the Raft strand).

class CommitAwaiter {
public:
    // Timeout for waiting on a commit.
    static constexpr auto kDefaultTimeout = std::chrono::seconds{5};

    explicit CommitAwaiter(boost::asio::io_context& ioc,
                           std::shared_ptr<spdlog::logger> logger = {})
        : ioc_(ioc)
        , logger_(std::move(logger))
    {}

    // Wait for the entry at `index` to be committed.
    // Returns true if committed, false if timed out or cancelled (e.g. step-down).
    [[nodiscard]] boost::asio::awaitable<bool>
    wait_for_commit(uint64_t index,
                    std::chrono::milliseconds timeout = kDefaultTimeout);

    // Notify that the entry at `index` has been committed.
    // Called from the ApplyCallback.
    void notify_commit(uint64_t index);

    // Cancel all pending waiters (e.g. on leadership loss).
    void fail_all();

    // Number of pending waiters.
    [[nodiscard]] std::size_t pending_count() const noexcept {
        return pending_.size();
    }

private:
    struct PendingEntry {
        std::unique_ptr<boost::asio::steady_timer> timer;
        bool committed = false;
    };

    boost::asio::io_context& ioc_;
    std::shared_ptr<spdlog::logger> logger_;
    std::map<uint64_t, PendingEntry> pending_;
};

} // namespace kv::raft

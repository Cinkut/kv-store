#pragma once

#include "raft/raft_node.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

namespace kv::raft {

// ── RealTimer ────────────────────────────────────────────────────────────────
//
// Production Timer implementation backed by a real boost::asio::steady_timer.
// Used in the running server; tests use the DeterministicTimer instead.

class RealTimer final : public Timer {
public:
    explicit RealTimer(boost::asio::io_context& ioc);

    boost::asio::awaitable<bool> async_wait() override;
    void expires_after(std::chrono::milliseconds duration) override;
    void cancel() override;

private:
    boost::asio::steady_timer timer_;
};

// ── RealTimerFactory ─────────────────────────────────────────────────────────
//
// Production TimerFactory that creates RealTimer instances.

class RealTimerFactory final : public TimerFactory {
public:
    explicit RealTimerFactory(boost::asio::io_context& ioc);

    std::unique_ptr<Timer> create_timer() override;

private:
    boost::asio::io_context& ioc_;
};

} // namespace kv::raft

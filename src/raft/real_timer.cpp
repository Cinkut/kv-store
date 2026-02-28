#include "raft/real_timer.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace kv::raft {

// ── RealTimer ────────────────────────────────────────────────────────────────

RealTimer::RealTimer(boost::asio::io_context& ioc)
    : timer_{ioc}
{}

boost::asio::awaitable<bool> RealTimer::async_wait()
{
    auto [ec] = co_await timer_.async_wait(
        boost::asio::as_tuple(boost::asio::use_awaitable));

    // operation_aborted means the timer was cancelled (reset or stop).
    co_return !ec;
}

void RealTimer::expires_after(std::chrono::milliseconds duration)
{
    timer_.expires_after(duration);
}

void RealTimer::cancel()
{
    timer_.cancel();
}

// ── RealTimerFactory ─────────────────────────────────────────────────────────

RealTimerFactory::RealTimerFactory(boost::asio::io_context& ioc)
    : ioc_{ioc}
{}

std::unique_ptr<Timer> RealTimerFactory::create_timer()
{
    return std::make_unique<RealTimer>(ioc_);
}

} // namespace kv::raft

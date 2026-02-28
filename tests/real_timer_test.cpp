#include "raft/real_timer.hpp"

#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>

#include <chrono>
#include <memory>

namespace {

using namespace kv::raft;
namespace asio = boost::asio;

// ════════════════════════════════════════════════════════════════════════════
//  RealTimer tests
// ════════════════════════════════════════════════════════════════════════════

class RealTimerTest : public ::testing::Test {
protected:
    asio::io_context ioc_;
};

// Timer fires after the specified duration, returning true.
TEST_F(RealTimerTest, FiresAfterDuration) {
    bool fired = false;

    asio::co_spawn(ioc_, [&]() -> asio::awaitable<void> {
        RealTimer timer(ioc_);
        timer.expires_after(std::chrono::milliseconds{10});
        fired = co_await timer.async_wait();
    }, asio::detached);

    ioc_.run();
    EXPECT_TRUE(fired);
}

// Cancelled timer returns false from async_wait.
TEST_F(RealTimerTest, CancelReturnsFalse) {
    bool result = true;

    auto timer = std::make_shared<RealTimer>(ioc_);
    timer->expires_after(std::chrono::milliseconds{5000});

    asio::co_spawn(ioc_, [&, timer]() -> asio::awaitable<void> {
        result = co_await timer->async_wait();
    }, asio::detached);

    // Post a cancel after the coroutine has started waiting.
    asio::co_spawn(ioc_, [&, timer]() -> asio::awaitable<void> {
        timer->cancel();
        co_return;
    }, asio::detached);

    ioc_.run();
    EXPECT_FALSE(result);
}

// Resetting expires_after while waiting cancels the old wait.
TEST_F(RealTimerTest, ResetCancelsOldWait) {
    bool first_result = true;
    bool second_result = false;

    auto timer = std::make_shared<RealTimer>(ioc_);
    timer->expires_after(std::chrono::milliseconds{5000});

    // First wait — will be cancelled by the reset.
    asio::co_spawn(ioc_, [&, timer]() -> asio::awaitable<void> {
        first_result = co_await timer->async_wait();

        // After reset, wait again — this one should fire.
        second_result = co_await timer->async_wait();
    }, asio::detached);

    // Reset the timer to a short duration.
    asio::co_spawn(ioc_, [&, timer]() -> asio::awaitable<void> {
        timer->expires_after(std::chrono::milliseconds{10});
        co_return;
    }, asio::detached);

    ioc_.run();
    // The first wait was cancelled by expires_after().
    EXPECT_FALSE(first_result);
    // The second wait fires after 10ms.
    EXPECT_TRUE(second_result);
}

// Multiple timers from the factory are independent.
TEST_F(RealTimerTest, MultipleTimersIndependent) {
    RealTimerFactory factory(ioc_);
    bool timer1_fired = false;
    bool timer2_cancelled = true;

    auto t1 = factory.create_timer();
    auto t2 = factory.create_timer();

    t1->expires_after(std::chrono::milliseconds{10});
    t2->expires_after(std::chrono::milliseconds{5000});

    // Share ownership for the coroutine captures.
    auto t1_ptr = std::shared_ptr<Timer>(std::move(t1));
    auto t2_ptr = std::shared_ptr<Timer>(std::move(t2));

    asio::co_spawn(ioc_, [&, t1_ptr]() -> asio::awaitable<void> {
        timer1_fired = co_await t1_ptr->async_wait();
    }, asio::detached);

    asio::co_spawn(ioc_, [&, t2_ptr]() -> asio::awaitable<void> {
        timer2_cancelled = co_await t2_ptr->async_wait();
    }, asio::detached);

    // Cancel t2 after starting.
    asio::co_spawn(ioc_, [&, t2_ptr]() -> asio::awaitable<void> {
        t2_ptr->cancel();
        co_return;
    }, asio::detached);

    ioc_.run();
    EXPECT_TRUE(timer1_fired);
    EXPECT_FALSE(timer2_cancelled);
}

// ════════════════════════════════════════════════════════════════════════════
//  RealTimerFactory tests
// ════════════════════════════════════════════════════════════════════════════

TEST_F(RealTimerTest, FactoryCreatesUniqueTimers) {
    RealTimerFactory factory(ioc_);

    auto t1 = factory.create_timer();
    auto t2 = factory.create_timer();

    EXPECT_NE(t1.get(), t2.get());
    EXPECT_NE(t1, nullptr);
    EXPECT_NE(t2, nullptr);
}

// RealTimer can be used with a RaftNode via the TimerFactory interface
// (compile-time verification — no runtime assertion needed beyond construction).
TEST_F(RealTimerTest, ImplementsTimerInterface) {
    RealTimerFactory factory(ioc_);

    // Verify the factory produces Timer*-compatible objects.
    std::unique_ptr<Timer> timer = factory.create_timer();
    ASSERT_NE(timer, nullptr);

    // Verify we can call the interface methods without crashing.
    timer->expires_after(std::chrono::milliseconds{100});
    timer->cancel();
}

} // namespace

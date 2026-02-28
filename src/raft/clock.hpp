#pragma once

#include <chrono>

namespace kv::raft {

// ── Clock abstraction ────────────────────────────────────────────────────────
//
// Provides a virtual time source for RaftNode so that tests can use a
// deterministic, manually-advanceable clock instead of wall-clock time.

class Clock {
public:
    using time_point = std::chrono::steady_clock::time_point;

    virtual ~Clock() = default;

    [[nodiscard]] virtual time_point now() const = 0;
};

// ── SteadyClock ──────────────────────────────────────────────────────────────
//
// Production implementation: delegates to std::chrono::steady_clock.

class SteadyClock final : public Clock {
public:
    [[nodiscard]] time_point now() const override {
        return std::chrono::steady_clock::now();
    }
};

// ── MockClock ────────────────────────────────────────────────────────────────
//
// Test implementation: time only advances via explicit advance() calls.

class MockClock final : public Clock {
public:
    [[nodiscard]] time_point now() const override {
        return now_;
    }

    void advance(std::chrono::milliseconds delta) {
        now_ += delta;
    }

    void set(time_point tp) {
        now_ = tp;
    }

private:
    time_point now_{};
};

} // namespace kv::raft

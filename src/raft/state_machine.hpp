#pragma once

#include <cstdint>
#include <memory>

#include <spdlog/spdlog.h>

#include "raft.pb.h"
#include "storage/storage.hpp"

namespace kv::raft {

// ── StateMachine ─────────────────────────────────────────────────────────────
//
// Applies committed Raft log entries to the underlying kv::Storage.
// Tracks lastApplied to prevent double-application.
//
// NOT thread-safe — all access must happen on the Raft strand.

class StateMachine {
public:
    explicit StateMachine(Storage& storage,
                          std::shared_ptr<spdlog::logger> logger = {});

    // Apply a single committed log entry.
    // Skips NOOP entries.  Updates last_applied_.
    void apply(const LogEntry& entry);

    // Reset the state machine: clear storage and set last_applied.
    // Used when installing a snapshot — caller loads snapshot data into
    // storage separately.
    void reset(uint64_t new_last_applied);

    // Last log index that was applied.
    [[nodiscard]] uint64_t last_applied() const noexcept { return last_applied_; }

    // Direct access to underlying storage (for reads).
    [[nodiscard]] Storage& storage() noexcept { return storage_; }
    [[nodiscard]] const Storage& storage() const noexcept { return storage_; }

private:
    Storage& storage_;
    std::shared_ptr<spdlog::logger> logger_;
    uint64_t last_applied_ = 0;
};

} // namespace kv::raft

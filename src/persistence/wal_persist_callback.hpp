#pragma once

#include "persistence/wal.hpp"
#include "raft/raft_node.hpp"

#include <spdlog/spdlog.h>

#include <memory>

namespace kv::persistence {

// ── WalPersistCallback ──────────────────────────────────────────────────────
//
// Production PersistCallback implementation that writes Raft state changes
// to the Write-Ahead Log before in-memory updates occur.
//
// NOT thread-safe — all calls must happen on the Raft strand.

class WalPersistCallback final : public kv::raft::PersistCallback {
public:
    WalPersistCallback(WAL& wal, std::shared_ptr<spdlog::logger> logger);

    // Persist term and votedFor to WAL.
    void persist_metadata(uint64_t term, int32_t voted_for) override;

    // Persist a single log entry to WAL.
    void persist_entry(const kv::raft::LogEntry& entry) override;

private:
    WAL& wal_;
    std::shared_ptr<spdlog::logger> logger_;
};

} // namespace kv::persistence

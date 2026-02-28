#pragma once

#include "raft/raft_node.hpp"
#include "persistence/snapshot.hpp"
#include "persistence/wal.hpp"
#include "storage/storage_engine.hpp"

#include <spdlog/spdlog.h>

#include <filesystem>
#include <memory>

namespace kv::persistence {

// ── SnapshotIOImpl ──────────────────────────────────────────────────────────
//
// Production implementation of kv::raft::SnapshotIO.
//
// Ties together Snapshot (binary file I/O), WAL (log rewriting after snapshot),
// and Storage (the live key-value state machine).
//
// Thread-safety: NOT thread-safe — all calls must happen on the Raft strand.

class SnapshotIOImpl final : public kv::raft::SnapshotIO {
public:
    // Constructor.
    //   data_dir – node's data directory (snapshot.bin lives here)
    //   storage  – the live kv::StorageEngine (state machine backing store)
    //   wal      – the Write-Ahead Log (rewritten after snapshot creation)
    //   logger   – per-node spdlog logger
    SnapshotIOImpl(const std::filesystem::path& data_dir,
                   kv::StorageEngine& storage,
                   WAL& wal,
                   std::shared_ptr<spdlog::logger> logger);

    // Load the snapshot file from disk for sending via InstallSnapshot RPC.
    // Returns empty data if no snapshot exists.
    SnapshotData load_snapshot_for_sending() override;

    // Install a snapshot received from the leader.
    //   1. Save snapshot bytes to disk.
    //   2. Load snapshot, clear Storage, repopulate from snapshot data.
    //   3. Rewrite WAL (metadata only, no entries — they're in the snapshot).
    bool install_snapshot(const std::string& data,
                          uint64_t last_included_index,
                          uint64_t last_included_term) override;

    // Create a snapshot of the current Storage state and rewrite WAL.
    //   1. Save snapshot from Storage to disk.
    //   2. Rewrite WAL: keep metadata, drop entries ≤ last_included_index.
    bool create_snapshot(uint64_t last_included_index,
                         uint64_t last_included_term) override;

private:
    std::filesystem::path snapshot_path_;
    kv::StorageEngine& storage_;
    WAL& wal_;
    std::shared_ptr<spdlog::logger> logger_;
};

} // namespace kv::persistence

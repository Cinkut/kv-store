#pragma once

#include "raft/raft_node.hpp"
#include "persistence/snapshot.hpp"
#include "persistence/wal.hpp"
#include "storage/storage_engine.hpp"

#include <spdlog/spdlog.h>

#include <filesystem>
#include <memory>
#include <optional>

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
    //   4. Persist the cluster configuration alongside the snapshot.
    bool install_snapshot(const std::string& data,
                          uint64_t last_included_index,
                          uint64_t last_included_term,
                          const std::optional<kv::raft::ClusterConfig>& config = {}) override;

    // Create a snapshot of the current Storage state and rewrite WAL.
    //   1. Save snapshot from Storage to disk.
    //   2. Rewrite WAL: keep metadata, drop entries ≤ last_included_index.
    //   3. Persist the cluster configuration alongside the snapshot.
    bool create_snapshot(uint64_t last_included_index,
                         uint64_t last_included_term,
                         const std::optional<kv::raft::ClusterConfig>& config = {}) override;

    // Load cluster configuration from the persisted cluster_config.pb file.
    // Returns nullopt if no config was persisted.
    std::optional<kv::raft::ClusterConfig> load_cluster_config() override;

private:
    // Atomically write a ClusterConfig protobuf to config_path_.
    void save_config(const kv::raft::ClusterConfig& config);

    std::filesystem::path snapshot_path_;
    std::filesystem::path config_path_;    // cluster_config.pb alongside snapshot.bin
    kv::StorageEngine& storage_;
    WAL& wal_;
    std::shared_ptr<spdlog::logger> logger_;
};

} // namespace kv::persistence

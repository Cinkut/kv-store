#include "persistence/snapshot_io_impl.hpp"

#include <fstream>
#include <iterator>

namespace kv::persistence {

SnapshotIOImpl::SnapshotIOImpl(const std::filesystem::path& data_dir,
                               kv::Storage& storage,
                               WAL& wal,
                               std::shared_ptr<spdlog::logger> logger)
    : snapshot_path_{data_dir / Snapshot::kFilename}
    , storage_{storage}
    , wal_{wal}
    , logger_{std::move(logger)}
{}

// ── load_snapshot_for_sending ────────────────────────────────────────────────

SnapshotIOImpl::SnapshotData SnapshotIOImpl::load_snapshot_for_sending()
{
    if (!Snapshot::exists(snapshot_path_)) {
        logger_->debug("SnapshotIOImpl: no snapshot file at {}",
                       snapshot_path_.string());
        return {};
    }

    // Read the raw file bytes for sending over the wire.
    std::ifstream ifs(snapshot_path_, std::ios::binary);
    if (!ifs) {
        logger_->error("SnapshotIOImpl: failed to open snapshot file {}",
                       snapshot_path_.string());
        return {};
    }

    std::string raw_bytes(
        (std::istreambuf_iterator<char>(ifs)),
        std::istreambuf_iterator<char>());

    // Also parse the metadata so we can include index/term.
    SnapshotLoadResult result;
    auto ec = Snapshot::load(snapshot_path_, result);
    if (ec) {
        logger_->error("SnapshotIOImpl: failed to parse snapshot: {}",
                       ec.message());
        return {};
    }

    logger_->debug("SnapshotIOImpl: loaded snapshot for sending "
                   "(index={}, term={}, {} bytes)",
                   result.metadata.last_included_index,
                   result.metadata.last_included_term,
                   raw_bytes.size());

    return {
        .data = std::move(raw_bytes),
        .last_included_index = result.metadata.last_included_index,
        .last_included_term  = result.metadata.last_included_term,
    };
}

// ── install_snapshot ─────────────────────────────────────────────────────────

bool SnapshotIOImpl::install_snapshot(const std::string& data,
                                      uint64_t last_included_index,
                                      uint64_t last_included_term)
{
    // Step 1: Write the raw snapshot bytes to disk.
    //         The data is the full snapshot file content as produced by
    //         the leader's load_snapshot_for_sending().
    {
        auto tmp_path = snapshot_path_;
        tmp_path += ".tmp";

        std::ofstream ofs(tmp_path, std::ios::binary | std::ios::trunc);
        if (!ofs) {
            logger_->error("SnapshotIOImpl: failed to create temp snapshot {}",
                           tmp_path.string());
            return false;
        }
        ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
        ofs.flush();
        if (!ofs.good()) {
            logger_->error("SnapshotIOImpl: failed to write temp snapshot");
            return false;
        }
        ofs.close();

        std::error_code ec;
        std::filesystem::rename(tmp_path, snapshot_path_, ec);
        if (ec) {
            logger_->error("SnapshotIOImpl: failed to rename snapshot: {}",
                           ec.message());
            return false;
        }
    }

    // Step 2: Load the snapshot back and repopulate Storage.
    SnapshotLoadResult result;
    auto ec = Snapshot::load(snapshot_path_, result);
    if (ec) {
        logger_->error("SnapshotIOImpl: failed to load installed snapshot: {}",
                       ec.message());
        return false;
    }

    // Verify metadata matches what we expected.
    if (result.metadata.last_included_index != last_included_index ||
        result.metadata.last_included_term  != last_included_term) {
        logger_->warn("SnapshotIOImpl: snapshot metadata mismatch "
                      "(expected index={} term={}, got index={} term={})",
                      last_included_index, last_included_term,
                      result.metadata.last_included_index,
                      result.metadata.last_included_term);
    }

    // Clear and repopulate Storage.
    storage_.clear();
    for (auto& [k, v] : result.data) {
        storage_.set(std::move(k), std::move(v));
    }

    // Step 3: Rewrite WAL — keep only metadata (term/vote), drop all entries.
    //         After snapshot install, all entries are covered by the snapshot.
    auto wal_ec = wal_.rewrite(std::nullopt, {});
    if (wal_ec) {
        logger_->error("SnapshotIOImpl: failed to rewrite WAL: {}",
                       wal_ec.message());
        return false;
    }

    logger_->info("SnapshotIOImpl: installed snapshot "
                  "(index={}, term={}, {} keys)",
                  last_included_index, last_included_term,
                  result.data.size());

    return true;
}

// ── create_snapshot ──────────────────────────────────────────────────────────

bool SnapshotIOImpl::create_snapshot(uint64_t last_included_index,
                                     uint64_t last_included_term)
{
    // Step 1: Save current Storage state as a snapshot.
    auto data = storage_.snapshot();

    auto ec = Snapshot::save(snapshot_path_, data,
                             last_included_index, last_included_term);
    if (ec) {
        logger_->error("SnapshotIOImpl: failed to save snapshot: {}",
                       ec.message());
        return false;
    }

    // Step 2: Rewrite WAL — drop entries ≤ last_included_index.
    //         Replay current WAL, keep only entries after the snapshot point.
    WalReplayResult replay;
    auto replay_ec = WAL::replay(wal_.path(), replay);
    if (replay_ec) {
        logger_->error("SnapshotIOImpl: failed to replay WAL for rewrite: {}",
                       replay_ec.message());
        return false;
    }

    // Filter entries: keep only those with index > last_included_index.
    std::vector<LogEntryRecord> remaining;
    for (auto& entry : replay.entries) {
        if (entry.index > last_included_index) {
            remaining.push_back(std::move(entry));
        }
    }

    auto rewrite_ec = wal_.rewrite(replay.metadata, remaining);
    if (rewrite_ec) {
        logger_->error("SnapshotIOImpl: failed to rewrite WAL: {}",
                       rewrite_ec.message());
        return false;
    }

    logger_->info("SnapshotIOImpl: created snapshot "
                  "(index={}, term={}, {} keys, {} WAL entries remaining)",
                  last_included_index, last_included_term,
                  data.size(), remaining.size());

    return true;
}

} // namespace kv::persistence

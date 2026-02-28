#include "persistence/snapshot_io_impl.hpp"
#include "persistence/snapshot.hpp"
#include "persistence/wal.hpp"
#include "storage/storage.hpp"
#include "common/logger.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace {

using namespace kv::persistence;
namespace fs = std::filesystem;

// ── Test fixture ─────────────────────────────────────────────────────────────

class SnapshotIOImplTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("node-1");
        spdlog::drop("kv");
        kv::init_default_logger(spdlog::level::debug);
        logger_ = kv::make_node_logger(1, spdlog::level::debug);

        // Create a fresh temp directory for each test.
        data_dir_ = fs::temp_directory_path() / "kv_snapshot_io_test";
        fs::remove_all(data_dir_);
        fs::create_directories(data_dir_);

        wal_path_ = data_dir_ / "wal.bin";
    }

    void TearDown() override {
        fs::remove_all(data_dir_);
    }

    fs::path data_dir_;
    fs::path wal_path_;
    kv::Storage storage_;
    std::shared_ptr<spdlog::logger> logger_;
};

// ── Tests ────────────────────────────────────────────────────────────────────

TEST_F(SnapshotIOImplTest, LoadSnapshotReturnsEmptyWhenNoFile) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    SnapshotIOImpl sio(data_dir_, storage_, wal, logger_);
    auto result = sio.load_snapshot_for_sending();

    EXPECT_TRUE(result.data.empty());
    EXPECT_EQ(result.last_included_index, 0u);
    EXPECT_EQ(result.last_included_term, 0u);
}

TEST_F(SnapshotIOImplTest, CreateAndLoadSnapshot) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    // Put some data in Storage.
    storage_.set("k1", "v1");
    storage_.set("k2", "v2");
    storage_.set("k3", "v3");

    SnapshotIOImpl sio(data_dir_, storage_, wal, logger_);

    // Create a snapshot at index 5, term 2.
    ASSERT_TRUE(sio.create_snapshot(5, 2));

    // Load it back.
    auto result = sio.load_snapshot_for_sending();

    EXPECT_FALSE(result.data.empty());
    EXPECT_EQ(result.last_included_index, 5u);
    EXPECT_EQ(result.last_included_term, 2u);

    // Verify the file is a valid snapshot.
    auto snap_path = data_dir_ / Snapshot::kFilename;
    EXPECT_TRUE(Snapshot::exists(snap_path));

    SnapshotLoadResult loaded;
    ASSERT_FALSE(Snapshot::load(snap_path, loaded));
    EXPECT_EQ(loaded.data.size(), 3u);
    EXPECT_EQ(loaded.data["k1"], "v1");
    EXPECT_EQ(loaded.data["k2"], "v2");
    EXPECT_EQ(loaded.data["k3"], "v3");
}

TEST_F(SnapshotIOImplTest, CreateSnapshotRewritesWAL) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    // Write some WAL entries.
    ASSERT_FALSE(wal.append_metadata({.term = 1, .voted_for = -1}));
    ASSERT_FALSE(wal.append_entry({.term = 1, .index = 1, .cmd_type = 1,
                                   .key = "a", .value = "1"}));
    ASSERT_FALSE(wal.append_entry({.term = 1, .index = 2, .cmd_type = 1,
                                   .key = "b", .value = "2"}));
    ASSERT_FALSE(wal.append_entry({.term = 1, .index = 3, .cmd_type = 1,
                                   .key = "c", .value = "3"}));

    storage_.set("a", "1");
    storage_.set("b", "2");
    storage_.set("c", "3");

    SnapshotIOImpl sio(data_dir_, storage_, wal, logger_);

    // Create snapshot at index 2 — entries 1,2 should be removed from WAL.
    ASSERT_TRUE(sio.create_snapshot(2, 1));

    // Replay WAL — should have only entry 3 remaining + metadata.
    WalReplayResult replay;
    ASSERT_FALSE(WAL::replay(wal_path_, replay));

    ASSERT_TRUE(replay.metadata.has_value());
    EXPECT_EQ(replay.metadata->term, 1u);

    ASSERT_EQ(replay.entries.size(), 1u);
    EXPECT_EQ(replay.entries[0].index, 3u);
    EXPECT_EQ(replay.entries[0].key, "c");
}

TEST_F(SnapshotIOImplTest, InstallSnapshotFromLeader) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    // Write some pre-existing data.
    storage_.set("old_key", "old_value");
    ASSERT_FALSE(wal.append_entry({.term = 1, .index = 1, .cmd_type = 1,
                                   .key = "old_key", .value = "old_value"}));

    // Prepare a snapshot file (as the leader would send it).
    // Create a temporary snapshot to get the raw bytes.
    auto leader_snap_path = data_dir_ / "leader_snap.bin";
    std::unordered_map<std::string, std::string> leader_data = {
        {"x", "10"}, {"y", "20"},
    };
    ASSERT_FALSE(Snapshot::save(leader_snap_path, leader_data, 10, 3));

    // Read the raw bytes.
    std::ifstream ifs(leader_snap_path, std::ios::binary);
    std::string raw_bytes(
        (std::istreambuf_iterator<char>(ifs)),
        std::istreambuf_iterator<char>());
    ifs.close();

    SnapshotIOImpl sio(data_dir_, storage_, wal, logger_);

    // Install the snapshot.
    ASSERT_TRUE(sio.install_snapshot(raw_bytes, 10, 3));

    // Storage should now have leader's data, not old data.
    EXPECT_FALSE(storage_.get("old_key").has_value());
    EXPECT_EQ(storage_.get("x").value(), "10");
    EXPECT_EQ(storage_.get("y").value(), "20");
    EXPECT_EQ(storage_.size(), 2u);

    // WAL should be rewritten (empty entries).
    WalReplayResult replay;
    ASSERT_FALSE(WAL::replay(wal_path_, replay));
    EXPECT_TRUE(replay.entries.empty());

    // Snapshot file should exist on disk.
    EXPECT_TRUE(Snapshot::exists(data_dir_ / Snapshot::kFilename));
}

TEST_F(SnapshotIOImplTest, LoadAfterInstall) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    // Create + install a snapshot, then verify load_snapshot_for_sending works.
    auto leader_snap_path = data_dir_ / "leader_snap.bin";
    std::unordered_map<std::string, std::string> leader_data = {
        {"hello", "world"},
    };
    ASSERT_FALSE(Snapshot::save(leader_snap_path, leader_data, 7, 2));

    std::ifstream ifs(leader_snap_path, std::ios::binary);
    std::string raw_bytes(
        (std::istreambuf_iterator<char>(ifs)),
        std::istreambuf_iterator<char>());
    ifs.close();

    SnapshotIOImpl sio(data_dir_, storage_, wal, logger_);
    ASSERT_TRUE(sio.install_snapshot(raw_bytes, 7, 2));

    auto loaded = sio.load_snapshot_for_sending();
    EXPECT_FALSE(loaded.data.empty());
    EXPECT_EQ(loaded.last_included_index, 7u);
    EXPECT_EQ(loaded.last_included_term, 2u);
}

TEST_F(SnapshotIOImplTest, CreateSnapshotWithEmptyStorage) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    SnapshotIOImpl sio(data_dir_, storage_, wal, logger_);

    // Create a snapshot with empty storage — should still work.
    ASSERT_TRUE(sio.create_snapshot(1, 1));

    auto loaded = sio.load_snapshot_for_sending();
    EXPECT_FALSE(loaded.data.empty());  // file has headers even if no keys
    EXPECT_EQ(loaded.last_included_index, 1u);
    EXPECT_EQ(loaded.last_included_term, 1u);

    // Verify the snapshot contains 0 keys.
    SnapshotLoadResult result;
    ASSERT_FALSE(Snapshot::load(data_dir_ / Snapshot::kFilename, result));
    EXPECT_EQ(result.data.size(), 0u);
}

TEST_F(SnapshotIOImplTest, CreateSnapshotPreservesMetadata) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    // Write metadata and entries.
    ASSERT_FALSE(wal.append_metadata({.term = 5, .voted_for = 3}));
    ASSERT_FALSE(wal.append_entry({.term = 5, .index = 1, .cmd_type = 1,
                                   .key = "a", .value = "1"}));

    storage_.set("a", "1");

    SnapshotIOImpl sio(data_dir_, storage_, wal, logger_);
    ASSERT_TRUE(sio.create_snapshot(1, 5));

    // After rewrite, metadata should be preserved.
    WalReplayResult replay;
    ASSERT_FALSE(WAL::replay(wal_path_, replay));
    ASSERT_TRUE(replay.metadata.has_value());
    EXPECT_EQ(replay.metadata->term, 5u);
    EXPECT_EQ(replay.metadata->voted_for, 3);
    EXPECT_TRUE(replay.entries.empty());
}

} // namespace

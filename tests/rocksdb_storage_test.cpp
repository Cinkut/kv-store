#include "storage/rocksdb_storage.hpp"
#include "storage/storage_engine.hpp"

#include <algorithm>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace kv {

namespace fs = std::filesystem;

// ── Fixture ─────────────────────────────────────────────────────────────────
// Creates a temporary directory for each test, opens a RocksDBStorage in it,
// and cleans up afterwards.

class RocksDBStorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        db_path_ = fs::temp_directory_path() / ("kv_rocksdb_test_" +
            std::to_string(std::hash<std::thread::id>{}(
                std::this_thread::get_id())) +
            "_" + std::to_string(counter_++));
        fs::create_directories(db_path_);
        storage_ = std::make_unique<RocksDBStorage>(db_path_);
    }

    void TearDown() override {
        storage_.reset(); // close DB before removing files
        std::error_code ec;
        fs::remove_all(db_path_, ec);
    }

    // Re-open the DB at the same path (for persistence tests).
    void reopen() {
        storage_.reset();
        storage_ = std::make_unique<RocksDBStorage>(db_path_);
    }

    fs::path db_path_;
    std::unique_ptr<RocksDBStorage> storage_;
    static inline int counter_ = 0;
};

// ── get() ─────────────────────────────────────────────────────────────────────

TEST_F(RocksDBStorageTest, GetReturnsNulloptForMissingKey) {
    EXPECT_FALSE(storage_->get("nonexistent").has_value());
}

TEST_F(RocksDBStorageTest, GetReturnsNulloptOnEmptyStore) {
    EXPECT_FALSE(storage_->get("").has_value());
}

// ── set() / get() ─────────────────────────────────────────────────────────────

TEST_F(RocksDBStorageTest, SetAndGetReturnsStoredValue) {
    storage_->set("key1", "value1");
    auto result = storage_->get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value1");
}

TEST_F(RocksDBStorageTest, SetOverwritesExistingKey) {
    storage_->set("key", "first");
    storage_->set("key", "second");
    auto result = storage_->get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "second");
}

TEST_F(RocksDBStorageTest, SetHandlesEmptyValue) {
    storage_->set("key", "");
    auto result = storage_->get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "");
}

TEST_F(RocksDBStorageTest, SetHandlesEmptyKey) {
    storage_->set("", "value");
    auto result = storage_->get("");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value");
}

TEST_F(RocksDBStorageTest, SetHandlesValueWithSpaces) {
    storage_->set("greeting", "hello world foo bar");
    auto result = storage_->get("greeting");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "hello world foo bar");
}

TEST_F(RocksDBStorageTest, SetHandlesLargeValues) {
    const std::string large_value(1024 * 1024, 'x'); // 1 MB
    storage_->set("big", large_value);
    auto result = storage_->get("big");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, large_value);
}

TEST_F(RocksDBStorageTest, MultipleKeysAreIndependent) {
    storage_->set("a", "1");
    storage_->set("b", "2");
    storage_->set("c", "3");
    EXPECT_EQ(*storage_->get("a"), "1");
    EXPECT_EQ(*storage_->get("b"), "2");
    EXPECT_EQ(*storage_->get("c"), "3");
}

// ── del() ─────────────────────────────────────────────────────────────────────

TEST_F(RocksDBStorageTest, DelReturnsTrueForExistingKey) {
    storage_->set("key", "value");
    EXPECT_TRUE(storage_->del("key"));
}

TEST_F(RocksDBStorageTest, DelReturnsFalseForMissingKey) {
    EXPECT_FALSE(storage_->del("nonexistent"));
}

TEST_F(RocksDBStorageTest, DelMakesKeyUnavailable) {
    storage_->set("key", "value");
    storage_->del("key");
    EXPECT_FALSE(storage_->get("key").has_value());
}

TEST_F(RocksDBStorageTest, DelIdempotentOnMissingKey) {
    storage_->set("key", "value");
    EXPECT_TRUE(storage_->del("key"));
    EXPECT_FALSE(storage_->del("key"));
    EXPECT_EQ(storage_->size(), 0u);
}

// ── keys() ────────────────────────────────────────────────────────────────────

TEST_F(RocksDBStorageTest, KeysReturnsEmptyVectorWhenEmpty) {
    EXPECT_TRUE(storage_->keys().empty());
}

TEST_F(RocksDBStorageTest, KeysReturnsAllKeys) {
    storage_->set("alpha", "1");
    storage_->set("beta", "2");
    storage_->set("gamma", "3");

    auto ks = storage_->keys();
    ASSERT_EQ(ks.size(), 3u);

    std::sort(ks.begin(), ks.end());
    EXPECT_EQ(ks[0], "alpha");
    EXPECT_EQ(ks[1], "beta");
    EXPECT_EQ(ks[2], "gamma");
}

TEST_F(RocksDBStorageTest, KeysDoesNotIncludeDeletedKey) {
    storage_->set("keep", "1");
    storage_->set("remove", "2");
    storage_->del("remove");

    auto ks = storage_->keys();
    ASSERT_EQ(ks.size(), 1u);
    EXPECT_EQ(ks[0], "keep");
}

// ── size() ────────────────────────────────────────────────────────────────────

TEST_F(RocksDBStorageTest, SizeIsZeroInitially) {
    EXPECT_EQ(storage_->size(), 0u);
}

TEST_F(RocksDBStorageTest, SizeTracksSetAndDel) {
    storage_->set("a", "1");
    EXPECT_EQ(storage_->size(), 1u);
    storage_->set("b", "2");
    EXPECT_EQ(storage_->size(), 2u);
    storage_->del("a");
    EXPECT_EQ(storage_->size(), 1u);
}

TEST_F(RocksDBStorageTest, SizeDoesNotGrowOnOverwrite) {
    storage_->set("key", "v1");
    storage_->set("key", "v2");
    EXPECT_EQ(storage_->size(), 1u);
}

// ── snapshot() ───────────────────────────────────────────────────────────────

TEST_F(RocksDBStorageTest, SnapshotReturnsFullCopy) {
    storage_->set("x", "10");
    storage_->set("y", "20");

    auto snap = storage_->snapshot();
    EXPECT_EQ(snap.size(), 2u);
    EXPECT_EQ(snap.at("x"), "10");
    EXPECT_EQ(snap.at("y"), "20");
}

TEST_F(RocksDBStorageTest, SnapshotIsCopyNotReference) {
    storage_->set("key", "original");
    auto snap = storage_->snapshot();

    storage_->set("key", "modified");

    EXPECT_EQ(snap.at("key"), "original");
}

TEST_F(RocksDBStorageTest, SnapshotReturnsEmptyMapWhenEmpty) {
    EXPECT_TRUE(storage_->snapshot().empty());
}

// ── clear() ──────────────────────────────────────────────────────────────────

TEST_F(RocksDBStorageTest, ClearRemovesAllEntries) {
    storage_->set("a", "1");
    storage_->set("b", "2");
    storage_->clear();
    EXPECT_EQ(storage_->size(), 0u);
    EXPECT_FALSE(storage_->get("a").has_value());
}

// ── Persistence (RocksDB-specific) ──────────────────────────────────────────

TEST_F(RocksDBStorageTest, DataSurvivesReopen) {
    storage_->set("persistent_key", "persistent_value");
    storage_->set("another", "data");
    ASSERT_EQ(storage_->size(), 2u);

    reopen();

    EXPECT_EQ(storage_->size(), 2u);
    auto val = storage_->get("persistent_key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "persistent_value");
    EXPECT_EQ(*storage_->get("another"), "data");
}

TEST_F(RocksDBStorageTest, DeletedKeysStayDeletedAfterReopen) {
    storage_->set("keep", "yes");
    storage_->set("remove", "no");
    storage_->del("remove");

    reopen();

    EXPECT_TRUE(storage_->get("keep").has_value());
    EXPECT_FALSE(storage_->get("remove").has_value());
    EXPECT_EQ(storage_->size(), 1u);
}

TEST_F(RocksDBStorageTest, ClearPersistsAfterReopen) {
    storage_->set("a", "1");
    storage_->set("b", "2");
    storage_->clear();

    reopen();

    EXPECT_EQ(storage_->size(), 0u);
    EXPECT_TRUE(storage_->keys().empty());
}

TEST_F(RocksDBStorageTest, OverwritePersistsAfterReopen) {
    storage_->set("key", "old");
    storage_->set("key", "new");

    reopen();

    auto val = storage_->get("key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "new");
    EXPECT_EQ(storage_->size(), 1u);
}

// ── Concurrent access ─────────────────────────────────────────────────────────

TEST_F(RocksDBStorageTest, ConcurrentWritersNoDataLoss) {
    constexpr int kThreads   = 4;
    constexpr int kPerThread = 250;

    std::vector<std::thread> writers;
    writers.reserve(kThreads);

    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                const std::string key   = "t" + std::to_string(t) + "_k" + std::to_string(i);
                const std::string value = std::to_string(t * kPerThread + i);
                storage_->set(key, value);
            }
        });
    }
    for (auto& w : writers) w.join();

    EXPECT_EQ(storage_->size(), static_cast<std::size_t>(kThreads * kPerThread));
}

TEST_F(RocksDBStorageTest, ConcurrentWritesToDifferentKeysDontInterfere) {
    constexpr int kThreads = 4;
    constexpr int kIters   = 250;

    for (int t = 0; t < kThreads; ++t) {
        storage_->set("slot" + std::to_string(t), "0");
    }

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            const std::string key = "slot" + std::to_string(t);
            for (int i = 1; i <= kIters; ++i) {
                storage_->set(key, std::to_string(i));
            }
        });
    }
    for (auto& th : threads) th.join();

    for (int t = 0; t < kThreads; ++t) {
        auto val = storage_->get("slot" + std::to_string(t));
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(*val, std::to_string(kIters));
    }
}

} // namespace kv

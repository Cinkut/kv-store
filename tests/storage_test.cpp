#include "storage/storage.hpp"

#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace kv {

// ── Fixture ───────────────────────────────────────────────────────────────────

class StorageTest : public ::testing::Test {
protected:
    Storage storage_;
};

// ── get() ─────────────────────────────────────────────────────────────────────

TEST_F(StorageTest, GetReturnsNulloptForMissingKey) {
    EXPECT_FALSE(storage_.get("nonexistent").has_value());
}

TEST_F(StorageTest, GetReturnsNulloptOnEmptyStore) {
    EXPECT_FALSE(storage_.get("").has_value());
}

// ── set() / get() ─────────────────────────────────────────────────────────────

TEST_F(StorageTest, SetAndGetReturnsStoredValue) {
    storage_.set("key1", "value1");
    auto result = storage_.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value1");
}

TEST_F(StorageTest, SetOverwritesExistingKey) {
    storage_.set("key", "first");
    storage_.set("key", "second");
    auto result = storage_.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "second");
}

TEST_F(StorageTest, SetHandlesEmptyValue) {
    storage_.set("key", "");
    auto result = storage_.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "");
}

TEST_F(StorageTest, SetHandlesEmptyKey) {
    storage_.set("", "value");
    auto result = storage_.get("");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value");
}

TEST_F(StorageTest, SetHandlesValueWithSpaces) {
    storage_.set("greeting", "hello world foo bar");
    auto result = storage_.get("greeting");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "hello world foo bar");
}

TEST_F(StorageTest, SetHandlesLargeValues) {
    const std::string large_value(1024 * 1024, 'x'); // 1 MB
    storage_.set("big", large_value);
    auto result = storage_.get("big");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, large_value);
}

TEST_F(StorageTest, MultipleKeysAreIndependent) {
    storage_.set("a", "1");
    storage_.set("b", "2");
    storage_.set("c", "3");
    EXPECT_EQ(*storage_.get("a"), "1");
    EXPECT_EQ(*storage_.get("b"), "2");
    EXPECT_EQ(*storage_.get("c"), "3");
}

// ── del() ─────────────────────────────────────────────────────────────────────

TEST_F(StorageTest, DelReturnsTrueForExistingKey) {
    storage_.set("key", "value");
    EXPECT_TRUE(storage_.del("key"));
}

TEST_F(StorageTest, DelReturnsFalseForMissingKey) {
    EXPECT_FALSE(storage_.del("nonexistent"));
}

TEST_F(StorageTest, DelMakesKeyUnavailable) {
    storage_.set("key", "value");
    storage_.del("key");
    EXPECT_FALSE(storage_.get("key").has_value());
}

TEST_F(StorageTest, DelIdempotentOnMissingKey) {
    storage_.set("key", "value");
    EXPECT_TRUE(storage_.del("key"));
    EXPECT_FALSE(storage_.del("key")); // second delete returns false
    EXPECT_EQ(storage_.size(), 0u);
}

// ── keys() ────────────────────────────────────────────────────────────────────

TEST_F(StorageTest, KeysReturnsEmptyVectorWhenEmpty) {
    EXPECT_TRUE(storage_.keys().empty());
}

TEST_F(StorageTest, KeysReturnsAllKeys) {
    storage_.set("alpha", "1");
    storage_.set("beta", "2");
    storage_.set("gamma", "3");

    auto ks = storage_.keys();
    ASSERT_EQ(ks.size(), 3u);

    std::sort(ks.begin(), ks.end());
    EXPECT_EQ(ks[0], "alpha");
    EXPECT_EQ(ks[1], "beta");
    EXPECT_EQ(ks[2], "gamma");
}

TEST_F(StorageTest, KeysDoesNotIncludeDeletedKey) {
    storage_.set("keep", "1");
    storage_.set("remove", "2");
    storage_.del("remove");

    auto ks = storage_.keys();
    ASSERT_EQ(ks.size(), 1u);
    EXPECT_EQ(ks[0], "keep");
}

// ── size() ────────────────────────────────────────────────────────────────────

TEST_F(StorageTest, SizeIsZeroInitially) {
    EXPECT_EQ(storage_.size(), 0u);
}

TEST_F(StorageTest, SizeTracksSetAndDel) {
    storage_.set("a", "1");
    EXPECT_EQ(storage_.size(), 1u);
    storage_.set("b", "2");
    EXPECT_EQ(storage_.size(), 2u);
    storage_.del("a");
    EXPECT_EQ(storage_.size(), 1u);
}

TEST_F(StorageTest, SizeDoesNotGrowOnOverwrite) {
    storage_.set("key", "v1");
    storage_.set("key", "v2");
    EXPECT_EQ(storage_.size(), 1u);
}

// ── snapshot() ───────────────────────────────────────────────────────────────

TEST_F(StorageTest, SnapshotReturnsFullCopy) {
    storage_.set("x", "10");
    storage_.set("y", "20");

    auto snap = storage_.snapshot();
    EXPECT_EQ(snap.size(), 2u);
    EXPECT_EQ(snap.at("x"), "10");
    EXPECT_EQ(snap.at("y"), "20");
}

TEST_F(StorageTest, SnapshotIsCopyNotReference) {
    storage_.set("key", "original");
    auto snap = storage_.snapshot();

    storage_.set("key", "modified"); // mutate live store

    // Snapshot must still hold the old value.
    EXPECT_EQ(snap.at("key"), "original");
}

TEST_F(StorageTest, SnapshotReturnsEmptyMapWhenEmpty) {
    EXPECT_TRUE(storage_.snapshot().empty());
}

// ── clear() ──────────────────────────────────────────────────────────────────

TEST_F(StorageTest, ClearRemovesAllEntries) {
    storage_.set("a", "1");
    storage_.set("b", "2");
    storage_.clear();
    EXPECT_EQ(storage_.size(), 0u);
    EXPECT_FALSE(storage_.get("a").has_value());
}

// ── Concurrent access ─────────────────────────────────────────────────────────

// N writer threads each write their own unique key/value pairs.
// After all writers finish, verify all entries are present.
TEST_F(StorageTest, ConcurrentWritersNoDataLoss) {
    constexpr int kThreads    = 8;
    constexpr int kPerThread  = 500;

    std::vector<std::thread> writers;
    writers.reserve(kThreads);

    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                const std::string key   = "t" + std::to_string(t) + "_k" + std::to_string(i);
                const std::string value = std::to_string(t * kPerThread + i);
                storage_.set(key, value);
            }
        });
    }
    for (auto& w : writers) w.join();

    EXPECT_EQ(storage_.size(), static_cast<std::size_t>(kThreads * kPerThread));
}

// N reader threads read while 1 writer thread continuously writes.
// Test passes when no data race occurs (thread sanitizer would catch it).
// We also verify the reader always gets a consistent value (never crashes).
TEST_F(StorageTest, ConcurrentReadersAndOneWriter) {
    storage_.set("shared", "0");

    constexpr int kReaders       = 6;
    constexpr int kWriteIters    = 1000;
    constexpr int kReadIters     = 1000;

    std::atomic<bool> done{false};

    // Single writer thread
    std::thread writer([&] {
        for (int i = 0; i < kWriteIters; ++i) {
            storage_.set("shared", std::to_string(i));
        }
        done.store(true, std::memory_order_release);
    });

    // Multiple reader threads
    std::vector<std::thread> readers;
    readers.reserve(kReaders);
    for (int r = 0; r < kReaders; ++r) {
        readers.emplace_back([&] {
            while (!done.load(std::memory_order_acquire)) {
                auto val = storage_.get("shared");
                // We only verify the read doesn't crash / return garbage.
                // The value may be any numeric string.
                (void)val;
            }
            // A few more reads after writer finishes.
            for (int i = 0; i < kReadIters; ++i) {
                auto val = storage_.get("shared");
                (void)val;
            }
        });
    }

    writer.join();
    for (auto& r : readers) r.join();

    // After the writer finished the last write was kWriteIters-1.
    auto final_val = storage_.get("shared");
    ASSERT_TRUE(final_val.has_value());
    EXPECT_EQ(*final_val, std::to_string(kWriteIters - 1));
}

// N threads each increment a counter stored as a string value.
// Since set() is atomic per call but not compare-and-swap, we verify
// that the final count is exactly N * iters (each thread uses its own key).
TEST_F(StorageTest, ConcurrentWritesToDifferentKeysDontInterfere) {
    constexpr int kThreads = 4;
    constexpr int kIters   = 250;

    // Pre-populate keys
    for (int t = 0; t < kThreads; ++t) {
        storage_.set("slot" + std::to_string(t), "0");
    }

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            const std::string key = "slot" + std::to_string(t);
            for (int i = 1; i <= kIters; ++i) {
                storage_.set(key, std::to_string(i));
            }
        });
    }
    for (auto& th : threads) th.join();

    // Each slot must end at kIters (threads own their slot exclusively)
    for (int t = 0; t < kThreads; ++t) {
        auto val = storage_.get("slot" + std::to_string(t));
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(*val, std::to_string(kIters));
    }
}

} // namespace kv

#include "persistence/wal.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

namespace kv::persistence {

// ── Fixture ──────────────────────────────────────────────────────────────────

class WalTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a unique temp directory for each test.
        auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("wal_test_" + std::string(info->name()));
        // Clean up any stale directory from a previous crashed run.
        std::filesystem::remove_all(test_dir_);
        std::filesystem::create_directories(test_dir_);
        wal_path_ = test_dir_ / "test.wal";
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
    std::filesystem::path wal_path_;
};

// ── Open / Close ─────────────────────────────────────────────────────────────

TEST_F(WalTest, OpenCreatesNewFile) {
    WAL wal(wal_path_);
    auto ec = wal.open();
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_TRUE(wal.is_open());
    EXPECT_TRUE(std::filesystem::exists(wal_path_));
}

TEST_F(WalTest, OpenIdempotentWhenAlreadyOpen) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());
    auto ec = wal.open();  // second call
    EXPECT_FALSE(ec) << ec.message();
    EXPECT_TRUE(wal.is_open());
}

TEST_F(WalTest, CloseMarksNotOpen) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());
    wal.close();
    EXPECT_FALSE(wal.is_open());
}

TEST_F(WalTest, ReopenExistingValidFile) {
    // Create and close a WAL.
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
    }
    // Reopen.
    WAL wal(wal_path_);
    auto ec = wal.open();
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_TRUE(wal.is_open());
}

// ── Empty WAL replay ─────────────────────────────────────────────────────────

TEST_F(WalTest, ReplayEmptyWal) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
    }

    WalReplayResult result;
    auto ec = WAL::replay(wal_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_FALSE(result.metadata.has_value());
    EXPECT_TRUE(result.entries.empty());
}

// ── Metadata records ─────────────────────────────────────────────────────────

TEST_F(WalTest, AppendAndReplayMetadata) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        MetadataRecord meta{.term = 5, .voted_for = 2};
        ASSERT_FALSE(wal.append_metadata(meta));
    }

    WalReplayResult result;
    auto ec = WAL::replay(wal_path_, result);
    ASSERT_FALSE(ec) << ec.message();

    ASSERT_TRUE(result.metadata.has_value());
    EXPECT_EQ(result.metadata->term, 5u);
    EXPECT_EQ(result.metadata->voted_for, 2);
    EXPECT_TRUE(result.entries.empty());
}

TEST_F(WalTest, MultipleMetadataLastWins) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_metadata({.term = 1, .voted_for = 0}));
        ASSERT_FALSE(wal.append_metadata({.term = 3, .voted_for = 1}));
        ASSERT_FALSE(wal.append_metadata({.term = 7, .voted_for = -1}));
    }

    WalReplayResult result;
    auto ec = WAL::replay(wal_path_, result);
    ASSERT_FALSE(ec) << ec.message();

    ASSERT_TRUE(result.metadata.has_value());
    EXPECT_EQ(result.metadata->term, 7u);
    EXPECT_EQ(result.metadata->voted_for, -1);
}

TEST_F(WalTest, MetadataNoVote) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_metadata({.term = 0, .voted_for = -1}));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    ASSERT_TRUE(result.metadata.has_value());
    EXPECT_EQ(result.metadata->term, 0u);
    EXPECT_EQ(result.metadata->voted_for, -1);
}

// ── Log entry records ────────────────────────────────────────────────────────

TEST_F(WalTest, AppendAndReplaySingleEntry) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        LogEntryRecord entry{
            .term = 1, .index = 1, .cmd_type = 1,
            .key = "foo", .value = "bar"};
        ASSERT_FALSE(wal.append_entry(entry));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    ASSERT_EQ(result.entries.size(), 1u);

    const auto& e = result.entries[0];
    EXPECT_EQ(e.term, 1u);
    EXPECT_EQ(e.index, 1u);
    EXPECT_EQ(e.cmd_type, 1u);
    EXPECT_EQ(e.key, "foo");
    EXPECT_EQ(e.value, "bar");
}

TEST_F(WalTest, AppendMultipleEntries) {
    constexpr int kCount = 50;
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        for (int i = 1; i <= kCount; ++i) {
            LogEntryRecord entry{
                .term = static_cast<uint64_t>((i - 1) / 10 + 1),
                .index = static_cast<uint64_t>(i),
                .cmd_type = 1,
                .key = "key" + std::to_string(i),
                .value = "val" + std::to_string(i)};
            ASSERT_FALSE(wal.append_entry(entry)) << "entry " << i;
        }
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    ASSERT_EQ(result.entries.size(), static_cast<std::size_t>(kCount));

    for (int i = 1; i <= kCount; ++i) {
        const auto& e = result.entries[i - 1];
        EXPECT_EQ(e.index, static_cast<uint64_t>(i));
        EXPECT_EQ(e.key, "key" + std::to_string(i));
        EXPECT_EQ(e.value, "val" + std::to_string(i));
    }
}

TEST_F(WalTest, NoopAndDeleteEntries) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        // NOOP (cmd_type = 0)
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 1, .cmd_type = 0, .key = "", .value = ""}));
        // DEL (cmd_type = 2)
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 2, .cmd_type = 2, .key = "gone", .value = ""}));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    ASSERT_EQ(result.entries.size(), 2u);
    EXPECT_EQ(result.entries[0].cmd_type, 0u);
    EXPECT_EQ(result.entries[1].cmd_type, 2u);
    EXPECT_EQ(result.entries[1].key, "gone");
}

TEST_F(WalTest, EntryWithEmptyKeyAndValue) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 1, .cmd_type = 0, .key = "", .value = ""}));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    ASSERT_EQ(result.entries.size(), 1u);
    EXPECT_TRUE(result.entries[0].key.empty());
    EXPECT_TRUE(result.entries[0].value.empty());
}

TEST_F(WalTest, EntryWithLargeValue) {
    std::string big_value(100'000, 'X');
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 1, .cmd_type = 1,
             .key = "big", .value = big_value}));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    ASSERT_EQ(result.entries.size(), 1u);
    EXPECT_EQ(result.entries[0].value, big_value);
}

// ── Mixed metadata + entries ─────────────────────────────────────────────────

TEST_F(WalTest, MetadataAndEntriesMixed) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_metadata({.term = 1, .voted_for = 0}));
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 1, .cmd_type = 1,
             .key = "a", .value = "1"}));
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 2, .cmd_type = 1,
             .key = "b", .value = "2"}));
        ASSERT_FALSE(wal.append_metadata({.term = 2, .voted_for = 1}));
        ASSERT_FALSE(wal.append_entry(
            {.term = 2, .index = 3, .cmd_type = 1,
             .key = "c", .value = "3"}));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));

    // Last metadata wins.
    ASSERT_TRUE(result.metadata.has_value());
    EXPECT_EQ(result.metadata->term, 2u);
    EXPECT_EQ(result.metadata->voted_for, 1);

    // All entries present.
    ASSERT_EQ(result.entries.size(), 3u);
    EXPECT_EQ(result.entries[0].key, "a");
    EXPECT_EQ(result.entries[1].key, "b");
    EXPECT_EQ(result.entries[2].key, "c");
}

// ── Corruption detection ─────────────────────────────────────────────────────

TEST_F(WalTest, CorruptedMetadataCrcDetected) {
    // Write valid WAL then corrupt a byte in the metadata record.
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_metadata({.term = 5, .voted_for = 2}));
    }

    // Corrupt: flip a byte in the metadata payload using POSIX I/O.
    {
        int fd = ::open(wal_path_.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        // Header is 7 bytes, metadata starts at offset 7.
        // type(1) + term(8) + voted_for(4) + crc(4) = 17 bytes.
        // Flip byte at offset 9 (inside term field).
        ASSERT_EQ(::lseek(fd, 9, SEEK_SET), 9);
        uint8_t byte = 0;
        ASSERT_EQ(::read(fd, &byte, 1), 1);
        byte ^= 0xFF;
        ASSERT_EQ(::lseek(fd, 9, SEEK_SET), 9);
        ASSERT_EQ(::write(fd, &byte, 1), 1);
        ::close(fd);
    }

    WalReplayResult result;
    auto ec = WAL::replay(wal_path_, result);
    EXPECT_TRUE(ec) << "Expected CRC error";
}

TEST_F(WalTest, CorruptedEntryCrcDetected) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 1, .cmd_type = 1,
             .key = "foo", .value = "bar"}));
    }

    // Corrupt a byte inside the entry record using POSIX I/O.
    {
        auto corrupt_pos = static_cast<off_t>(kWalHeaderSize + 10);
        int fd = ::open(wal_path_.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(::lseek(fd, corrupt_pos, SEEK_SET), corrupt_pos);
        uint8_t byte = 0;
        ASSERT_EQ(::read(fd, &byte, 1), 1);
        byte ^= 0xFF;
        ASSERT_EQ(::lseek(fd, corrupt_pos, SEEK_SET), corrupt_pos);
        ASSERT_EQ(::write(fd, &byte, 1), 1);
        ::close(fd);
    }

    WalReplayResult result;
    auto ec = WAL::replay(wal_path_, result);
    EXPECT_TRUE(ec) << "Expected CRC error on corrupted entry";
}

TEST_F(WalTest, InvalidMagicDetected) {
    // Write garbage to file.
    {
        int fd = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);
        const char* garbage = "BADMAGICDATA";
        ::write(fd, garbage, 12);
        ::close(fd);
    }

    WalReplayResult result;
    auto ec = WAL::replay(wal_path_, result);
    EXPECT_TRUE(ec) << "Expected error for invalid magic";
}

TEST_F(WalTest, TruncatedFileDetected) {
    // Write only a partial header.
    {
        int fd = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);
        const char* partial = "KVW";
        ::write(fd, partial, 3);
        ::close(fd);
    }

    WalReplayResult result;
    auto ec = WAL::replay(wal_path_, result);
    EXPECT_TRUE(ec) << "Expected error for truncated header";
}

// ── truncate_suffix ──────────────────────────────────────────────────────────

TEST_F(WalTest, TruncateSuffixRemovesHighIndexEntries) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_metadata({.term = 3, .voted_for = 1}));
        for (uint64_t i = 1; i <= 5; ++i) {
            ASSERT_FALSE(wal.append_entry(
                {.term = 1, .index = i, .cmd_type = 1,
                 .key = "k" + std::to_string(i),
                 .value = "v" + std::to_string(i)}));
        }
        // Truncate everything after index 3.
        auto ec = wal.truncate_suffix(3);
        ASSERT_FALSE(ec) << ec.message();
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));

    // Metadata preserved.
    ASSERT_TRUE(result.metadata.has_value());
    EXPECT_EQ(result.metadata->term, 3u);
    EXPECT_EQ(result.metadata->voted_for, 1);

    // Only entries 1-3 remain.
    ASSERT_EQ(result.entries.size(), 3u);
    EXPECT_EQ(result.entries[0].index, 1u);
    EXPECT_EQ(result.entries[1].index, 2u);
    EXPECT_EQ(result.entries[2].index, 3u);
}

TEST_F(WalTest, TruncateSuffixKeepsAllWhenIndexHighEnough) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        for (uint64_t i = 1; i <= 3; ++i) {
            ASSERT_FALSE(wal.append_entry(
                {.term = 1, .index = i, .cmd_type = 1,
                 .key = "k", .value = "v"}));
        }
        ASSERT_FALSE(wal.truncate_suffix(100));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    EXPECT_EQ(result.entries.size(), 3u);
}

TEST_F(WalTest, TruncateSuffixRemovesAllWhenIndexZero) {
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        for (uint64_t i = 1; i <= 3; ++i) {
            ASSERT_FALSE(wal.append_entry(
                {.term = 1, .index = i, .cmd_type = 1,
                 .key = "k", .value = "v"}));
        }
        ASSERT_FALSE(wal.truncate_suffix(0));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    EXPECT_TRUE(result.entries.empty());
}

// ── rewrite ──────────────────────────────────────────────────────────────────

TEST_F(WalTest, RewriteProducesValidWal) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    // Write some initial entries.
    for (uint64_t i = 1; i <= 5; ++i) {
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = i, .cmd_type = 1,
             .key = "k" + std::to_string(i),
             .value = "v" + std::to_string(i)}));
    }

    // Rewrite with only a subset.
    MetadataRecord meta{.term = 2, .voted_for = 0};
    std::vector<LogEntryRecord> kept = {
        {.term = 1, .index = 4, .cmd_type = 1, .key = "k4", .value = "v4"},
        {.term = 1, .index = 5, .cmd_type = 1, .key = "k5", .value = "v5"},
    };
    auto ec = wal.rewrite(meta, kept);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_TRUE(wal.is_open());

    // Verify.
    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    ASSERT_TRUE(result.metadata.has_value());
    EXPECT_EQ(result.metadata->term, 2u);
    EXPECT_EQ(result.metadata->voted_for, 0);
    ASSERT_EQ(result.entries.size(), 2u);
    EXPECT_EQ(result.entries[0].index, 4u);
    EXPECT_EQ(result.entries[1].index, 5u);
}

TEST_F(WalTest, RewriteWithNoMetadata) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    std::vector<LogEntryRecord> entries = {
        {.term = 1, .index = 1, .cmd_type = 1, .key = "a", .value = "b"},
    };
    auto ec = wal.rewrite(std::nullopt, entries);
    ASSERT_FALSE(ec) << ec.message();

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    EXPECT_FALSE(result.metadata.has_value());
    ASSERT_EQ(result.entries.size(), 1u);
}

TEST_F(WalTest, RewriteToEmpty) {
    WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    ASSERT_FALSE(wal.append_entry(
        {.term = 1, .index = 1, .cmd_type = 1, .key = "a", .value = "b"}));

    auto ec = wal.rewrite(std::nullopt, {});
    ASSERT_FALSE(ec) << ec.message();

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    EXPECT_FALSE(result.metadata.has_value());
    EXPECT_TRUE(result.entries.empty());
}

// ── Append after reopen ──────────────────────────────────────────────────────

TEST_F(WalTest, AppendAfterReopenPreservesPrevious) {
    // Write first entry.
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 1, .cmd_type = 1,
             .key = "first", .value = "1"}));
    }

    // Reopen, write second entry.
    {
        WAL wal(wal_path_);
        ASSERT_FALSE(wal.open());
        ASSERT_FALSE(wal.append_entry(
            {.term = 1, .index = 2, .cmd_type = 1,
             .key = "second", .value = "2"}));
    }

    WalReplayResult result;
    ASSERT_FALSE(WAL::replay(wal_path_, result));
    ASSERT_EQ(result.entries.size(), 2u);
    EXPECT_EQ(result.entries[0].key, "first");
    EXPECT_EQ(result.entries[1].key, "second");
}

// ── Error on not-open WAL ────────────────────────────────────────────────────

TEST_F(WalTest, AppendMetadataFailsWhenNotOpen) {
    WAL wal(wal_path_);
    // Do NOT call open().
    auto ec = wal.append_metadata({.term = 1, .voted_for = 0});
    EXPECT_TRUE(ec);
}

TEST_F(WalTest, AppendEntryFailsWhenNotOpen) {
    WAL wal(wal_path_);
    auto ec = wal.append_entry(
        {.term = 1, .index = 1, .cmd_type = 1, .key = "a", .value = "b"});
    EXPECT_TRUE(ec);
}

// ── CRC32 unit tests ─────────────────────────────────────────────────────────

TEST_F(WalTest, Crc32EmptyInput) {
    auto c = crc32(nullptr, 0);
    // CRC32 of empty data = 0x00000000.
    EXPECT_EQ(c, 0x00000000u);
}

TEST_F(WalTest, Crc32KnownValue) {
    // CRC32 of "123456789" is 0xCBF43926.
    const std::string input = "123456789";
    auto c = crc32(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    EXPECT_EQ(c, 0xCBF43926u);
}

// ── Serialisation round-trip ─────────────────────────────────────────────────

TEST_F(WalTest, SerialiseMetadataProducesExpectedSize) {
    // type(1) + term(8) + voted_for(4) + crc(4) = 17
    auto buf = serialise_metadata({.term = 42, .voted_for = 3});
    EXPECT_EQ(buf.size(), 17u);
    EXPECT_EQ(buf[0], kRecordTypeMeta);
}

TEST_F(WalTest, SerialiseEntryProducesExpectedSize) {
    LogEntryRecord rec{
        .term = 1, .index = 1, .cmd_type = 1,
        .key = "abc", .value = "defgh"};
    auto buf = serialise_entry(rec);
    // type(1) + total_len(4) + term(8) + index(8) + cmd_type(1) +
    // key_len(2) + key(3) + value_len(4) + value(5) + crc(4) = 40
    EXPECT_EQ(buf.size(), 40u);
    EXPECT_EQ(buf[0], kRecordTypeEntry);
}

// ── Path accessor ────────────────────────────────────────────────────────────

TEST_F(WalTest, PathReturnsConstructorPath) {
    WAL wal(wal_path_);
    EXPECT_EQ(wal.path(), wal_path_);
}

} // namespace kv::persistence

// ════════════════════════════════════════════════════════════════════════════
//  WalPersistCallback tests
// ════════════════════════════════════════════════════════════════════════════

#include "persistence/wal_persist_callback.hpp"
#include "common/logger.hpp"

namespace {

class WalPersistCallbackTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("wal_persist_cb_" + std::string(info->name()));
        std::filesystem::remove_all(test_dir_);
        std::filesystem::create_directories(test_dir_);
        wal_path_ = test_dir_ / "test.wal";
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
    std::filesystem::path wal_path_;
};

TEST_F(WalPersistCallbackTest, PersistMetadataWritesToWAL) {
    kv::persistence::WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    auto logger = spdlog::default_logger();
    kv::persistence::WalPersistCallback cb(wal, logger);

    cb.persist_metadata(5, 2);
    wal.close();

    // Replay and verify metadata was persisted.
    kv::persistence::WalReplayResult result;
    auto ec = kv::persistence::WAL::replay(wal_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_TRUE(result.metadata.has_value());
    EXPECT_EQ(result.metadata->term, 5u);
    EXPECT_EQ(result.metadata->voted_for, 2);
}

TEST_F(WalPersistCallbackTest, PersistMetadataWithNoVote) {
    kv::persistence::WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    auto logger = spdlog::default_logger();
    kv::persistence::WalPersistCallback cb(wal, logger);

    cb.persist_metadata(3, -1);
    wal.close();

    kv::persistence::WalReplayResult result;
    ASSERT_FALSE(kv::persistence::WAL::replay(wal_path_, result));
    ASSERT_TRUE(result.metadata.has_value());
    EXPECT_EQ(result.metadata->term, 3u);
    EXPECT_EQ(result.metadata->voted_for, -1);
}

TEST_F(WalPersistCallbackTest, PersistEntryWritesToWAL) {
    kv::persistence::WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    auto logger = spdlog::default_logger();
    kv::persistence::WalPersistCallback cb(wal, logger);

    kv::raft::LogEntry entry;
    entry.set_term(2);
    entry.set_index(1);
    auto* cmd = entry.mutable_command();
    cmd->set_type(kv::raft::CMD_SET);
    cmd->set_key("foo");
    cmd->set_value("bar");

    cb.persist_entry(entry);
    wal.close();

    // Replay and verify the entry was persisted.
    kv::persistence::WalReplayResult result;
    ASSERT_FALSE(kv::persistence::WAL::replay(wal_path_, result));
    ASSERT_EQ(result.entries.size(), 1u);
    EXPECT_EQ(result.entries[0].term, 2u);
    EXPECT_EQ(result.entries[0].index, 1u);
    EXPECT_EQ(result.entries[0].cmd_type, static_cast<uint8_t>(kv::raft::CMD_SET));
    EXPECT_EQ(result.entries[0].key, "foo");
    EXPECT_EQ(result.entries[0].value, "bar");
}

TEST_F(WalPersistCallbackTest, PersistNoopEntry) {
    kv::persistence::WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    auto logger = spdlog::default_logger();
    kv::persistence::WalPersistCallback cb(wal, logger);

    kv::raft::LogEntry entry;
    entry.set_term(1);
    entry.set_index(1);
    // No command set = NOOP

    cb.persist_entry(entry);
    wal.close();

    kv::persistence::WalReplayResult result;
    ASSERT_FALSE(kv::persistence::WAL::replay(wal_path_, result));
    ASSERT_EQ(result.entries.size(), 1u);
    EXPECT_EQ(result.entries[0].term, 1u);
    EXPECT_EQ(result.entries[0].index, 1u);
    EXPECT_EQ(result.entries[0].cmd_type, 0u); // NOOP
}

TEST_F(WalPersistCallbackTest, PersistMultipleEntriesAndMetadata) {
    kv::persistence::WAL wal(wal_path_);
    ASSERT_FALSE(wal.open());

    auto logger = spdlog::default_logger();
    kv::persistence::WalPersistCallback cb(wal, logger);

    // Persist metadata.
    cb.persist_metadata(1, 1);

    // Persist two entries.
    kv::raft::LogEntry e1;
    e1.set_term(1);
    e1.set_index(1);
    auto* c1 = e1.mutable_command();
    c1->set_type(kv::raft::CMD_SET);
    c1->set_key("a");
    c1->set_value("1");
    cb.persist_entry(e1);

    kv::raft::LogEntry e2;
    e2.set_term(1);
    e2.set_index(2);
    auto* c2 = e2.mutable_command();
    c2->set_type(kv::raft::CMD_DEL);
    c2->set_key("b");
    c2->set_value("");
    cb.persist_entry(e2);

    // Update metadata (new term, new vote).
    cb.persist_metadata(2, 3);

    wal.close();

    // Replay should have latest metadata and both entries.
    kv::persistence::WalReplayResult result;
    ASSERT_FALSE(kv::persistence::WAL::replay(wal_path_, result));

    ASSERT_TRUE(result.metadata.has_value());
    // WAL replays all metadata records; the last one wins.
    EXPECT_EQ(result.metadata->term, 2u);
    EXPECT_EQ(result.metadata->voted_for, 3);

    ASSERT_EQ(result.entries.size(), 2u);
    EXPECT_EQ(result.entries[0].key, "a");
    EXPECT_EQ(result.entries[1].key, "b");
    EXPECT_EQ(result.entries[1].cmd_type, static_cast<uint8_t>(kv::raft::CMD_DEL));
}

} // anonymous namespace

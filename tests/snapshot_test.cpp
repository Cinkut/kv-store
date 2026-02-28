#include "persistence/snapshot.hpp"
#include "persistence/wal.hpp"  // crc32()

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

namespace kv::persistence {

// ── Fixture ──────────────────────────────────────────────────────────────────

class SnapshotTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("snapshot_test_" + std::string(info->name()));
        std::filesystem::remove_all(test_dir_);
        std::filesystem::create_directories(test_dir_);
        snap_path_ = test_dir_ / Snapshot::kFilename;
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
    std::filesystem::path snap_path_;
};

// ── Save / Load basics ──────────────────────────────────────────────────────

TEST_F(SnapshotTest, SaveAndLoadEmpty) {
    std::unordered_map<std::string, std::string> data;
    auto ec = Snapshot::save(snap_path_, data, 10, 3);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_TRUE(std::filesystem::exists(snap_path_));

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(result.metadata.last_included_index, 10u);
    EXPECT_EQ(result.metadata.last_included_term, 3u);
    EXPECT_TRUE(result.data.empty());
}

TEST_F(SnapshotTest, SaveAndLoadSingleEntry) {
    std::unordered_map<std::string, std::string> data;
    data["hello"] = "world";

    auto ec = Snapshot::save(snap_path_, data, 5, 1);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(result.metadata.last_included_index, 5u);
    EXPECT_EQ(result.metadata.last_included_term, 1u);
    ASSERT_EQ(result.data.size(), 1u);
    EXPECT_EQ(result.data.at("hello"), "world");
}

TEST_F(SnapshotTest, SaveAndLoadMultipleEntries) {
    std::unordered_map<std::string, std::string> data;
    data["key1"] = "value1";
    data["key2"] = "value2";
    data["key3"] = "value3";
    data["foo"]  = "bar";
    data["baz"]  = "qux";

    auto ec = Snapshot::save(snap_path_, data, 100, 7);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(result.metadata.last_included_index, 100u);
    EXPECT_EQ(result.metadata.last_included_term, 7u);
    ASSERT_EQ(result.data.size(), 5u);
    EXPECT_EQ(result.data.at("key1"), "value1");
    EXPECT_EQ(result.data.at("key2"), "value2");
    EXPECT_EQ(result.data.at("key3"), "value3");
    EXPECT_EQ(result.data.at("foo"), "bar");
    EXPECT_EQ(result.data.at("baz"), "qux");
}

TEST_F(SnapshotTest, SaveAndLoadEmptyValues) {
    std::unordered_map<std::string, std::string> data;
    data["novalue"] = "";
    data["also_empty"] = "";

    auto ec = Snapshot::save(snap_path_, data, 42, 2);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_EQ(result.data.size(), 2u);
    EXPECT_EQ(result.data.at("novalue"), "");
    EXPECT_EQ(result.data.at("also_empty"), "");
}

TEST_F(SnapshotTest, SaveAndLoadLargeValues) {
    std::unordered_map<std::string, std::string> data;
    // 10 KB value
    std::string large_val(10'000, 'X');
    data["big"] = large_val;

    auto ec = Snapshot::save(snap_path_, data, 999, 5);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_EQ(result.data.size(), 1u);
    EXPECT_EQ(result.data.at("big"), large_val);
}

// ── Metadata preservation ────────────────────────────────────────────────────

TEST_F(SnapshotTest, MetadataZeroIndexAndTerm) {
    std::unordered_map<std::string, std::string> data;
    data["a"] = "b";

    auto ec = Snapshot::save(snap_path_, data, 0, 0);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(result.metadata.last_included_index, 0u);
    EXPECT_EQ(result.metadata.last_included_term, 0u);
}

TEST_F(SnapshotTest, MetadataLargeValues) {
    std::unordered_map<std::string, std::string> data;
    uint64_t large_index = 0xFFFF'FFFF'FFFF'FFFFull;
    uint64_t large_term  = 0x0123'4567'89AB'CDEFull;

    auto ec = Snapshot::save(snap_path_, data, large_index, large_term);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(result.metadata.last_included_index, large_index);
    EXPECT_EQ(result.metadata.last_included_term, large_term);
}

// ── Overwrite ────────────────────────────────────────────────────────────────

TEST_F(SnapshotTest, SaveOverwritesPreviousSnapshot) {
    // First snapshot.
    std::unordered_map<std::string, std::string> data1;
    data1["old"] = "data";
    auto ec = Snapshot::save(snap_path_, data1, 10, 1);
    ASSERT_FALSE(ec) << ec.message();

    // Second snapshot overwrites.
    std::unordered_map<std::string, std::string> data2;
    data2["new"] = "data";
    data2["extra"] = "stuff";
    ec = Snapshot::save(snap_path_, data2, 20, 3);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(result.metadata.last_included_index, 20u);
    EXPECT_EQ(result.metadata.last_included_term, 3u);
    ASSERT_EQ(result.data.size(), 2u);
    EXPECT_EQ(result.data.at("new"), "data");
    EXPECT_EQ(result.data.at("extra"), "stuff");
    // "old" key must not be present.
    EXPECT_EQ(result.data.count("old"), 0u);
}

// ── Atomic write (no .tmp left behind) ──────────────────────────────────────

TEST_F(SnapshotTest, NoTmpFileLeftAfterSave) {
    std::unordered_map<std::string, std::string> data;
    data["k"] = "v";
    auto ec = Snapshot::save(snap_path_, data, 1, 1);
    ASSERT_FALSE(ec) << ec.message();

    auto tmp_path = snap_path_;
    tmp_path += ".tmp";
    EXPECT_FALSE(std::filesystem::exists(tmp_path));
}

// ── Exists ───────────────────────────────────────────────────────────────────

TEST_F(SnapshotTest, ExistsReturnsFalseWhenNoFile) {
    EXPECT_FALSE(Snapshot::exists(snap_path_));
}

TEST_F(SnapshotTest, ExistsReturnsTrueAfterSave) {
    std::unordered_map<std::string, std::string> data;
    auto ec = Snapshot::save(snap_path_, data, 1, 1);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_TRUE(Snapshot::exists(snap_path_));
}

TEST_F(SnapshotTest, ExistsReturnsFalseForDirectory) {
    // snap_path_ parent already exists as a directory — checking a dir path
    EXPECT_FALSE(Snapshot::exists(test_dir_));
}

// ── Corruption detection ─────────────────────────────────────────────────────

TEST_F(SnapshotTest, LoadFailsOnEmptyFile) {
    // Create an empty file.
    int fd = ::open(snap_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    ::close(fd);

    SnapshotLoadResult result;
    auto ec = Snapshot::load(snap_path_, result);
    EXPECT_TRUE(ec);
}

TEST_F(SnapshotTest, LoadFailsOnBadMagic) {
    // Save a valid snapshot, then corrupt the magic bytes.
    std::unordered_map<std::string, std::string> data;
    data["k"] = "v";
    auto ec = Snapshot::save(snap_path_, data, 1, 1);
    ASSERT_FALSE(ec) << ec.message();

    // Overwrite first 4 bytes.
    int fd = ::open(snap_path_.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    const char bad_magic[] = "XXXX";
    ::write(fd, bad_magic, 4);
    ::close(fd);

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    EXPECT_TRUE(ec);
}

TEST_F(SnapshotTest, LoadFailsOnBadVersion) {
    // Save then corrupt version field (bytes 4-5).
    std::unordered_map<std::string, std::string> data;
    data["k"] = "v";
    auto ec = Snapshot::save(snap_path_, data, 1, 1);
    ASSERT_FALSE(ec) << ec.message();

    int fd = ::open(snap_path_.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ::lseek(fd, 4, SEEK_SET);
    uint8_t bad_version[2] = {0xFF, 0xFF};  // version 65535
    ::write(fd, bad_version, 2);
    ::close(fd);

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    EXPECT_TRUE(ec);
}

TEST_F(SnapshotTest, LoadFailsOnCorruptedCrc) {
    std::unordered_map<std::string, std::string> data;
    data["key"] = "value";
    auto ec = Snapshot::save(snap_path_, data, 50, 4);
    ASSERT_FALSE(ec) << ec.message();

    // Get file size, flip a byte in the CRC (last 4 bytes).
    auto fsize = std::filesystem::file_size(snap_path_);
    int fd = ::open(snap_path_.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ::lseek(fd, static_cast<off_t>(fsize) - 4, SEEK_SET);
    uint8_t crc_bytes[4];
    ::read(fd, crc_bytes, 4);
    crc_bytes[0] ^= 0xFF;  // flip bits
    ::lseek(fd, static_cast<off_t>(fsize) - 4, SEEK_SET);
    ::write(fd, crc_bytes, 4);
    ::close(fd);

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    EXPECT_TRUE(ec);
}

TEST_F(SnapshotTest, LoadFailsOnCorruptedData) {
    std::unordered_map<std::string, std::string> data;
    data["key"] = "value";
    auto ec = Snapshot::save(snap_path_, data, 50, 4);
    ASSERT_FALSE(ec) << ec.message();

    // Flip a byte in the middle of the data area.
    auto fsize = std::filesystem::file_size(snap_path_);
    auto corrupt_offset = fsize / 2;
    int fd = ::open(snap_path_.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ::lseek(fd, static_cast<off_t>(corrupt_offset), SEEK_SET);
    uint8_t byte;
    ::read(fd, &byte, 1);
    byte ^= 0xFF;
    ::lseek(fd, static_cast<off_t>(corrupt_offset), SEEK_SET);
    ::write(fd, &byte, 1);
    ::close(fd);

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    EXPECT_TRUE(ec);
}

TEST_F(SnapshotTest, LoadFailsOnTruncatedFile) {
    std::unordered_map<std::string, std::string> data;
    data["key"] = "value";
    auto ec = Snapshot::save(snap_path_, data, 50, 4);
    ASSERT_FALSE(ec) << ec.message();

    // Truncate to half the file.
    auto fsize = std::filesystem::file_size(snap_path_);
    std::filesystem::resize_file(snap_path_, fsize / 2);

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    EXPECT_TRUE(ec);
}

TEST_F(SnapshotTest, LoadFailsOnNonexistentFile) {
    SnapshotLoadResult result;
    auto ec = Snapshot::load(test_dir_ / "nonexistent.bin", result);
    EXPECT_TRUE(ec);
}

// ── Binary format validation ─────────────────────────────────────────────────

TEST_F(SnapshotTest, FileHasCorrectMagicAndVersion) {
    std::unordered_map<std::string, std::string> data;
    data["a"] = "b";
    auto ec = Snapshot::save(snap_path_, data, 1, 1);
    ASSERT_FALSE(ec) << ec.message();

    int fd = ::open(snap_path_.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);

    // Read magic (4 bytes).
    char magic[4];
    ASSERT_EQ(::read(fd, magic, 4), 4);
    EXPECT_EQ(std::memcmp(magic, "KVSS", 4), 0);

    // Read version (2 bytes LE).
    uint8_t ver[2];
    ASSERT_EQ(::read(fd, ver, 2), 2);
    uint16_t version = static_cast<uint16_t>(ver[0]) |
                       (static_cast<uint16_t>(ver[1]) << 8);
    EXPECT_EQ(version, 1u);

    ::close(fd);
}

TEST_F(SnapshotTest, FileHasCorrectMetadataFields) {
    std::unordered_map<std::string, std::string> data;
    auto ec = Snapshot::save(snap_path_, data, 0x0102030405060708ull,
                             0x0A0B0C0D0E0F1011ull);
    ASSERT_FALSE(ec) << ec.message();

    int fd = ::open(snap_path_.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);

    // Skip header (6 bytes).
    ::lseek(fd, 6, SEEK_SET);

    // Read last_included_index (8 bytes LE).
    uint8_t idx_bytes[8];
    ASSERT_EQ(::read(fd, idx_bytes, 8), 8);
    uint64_t idx = 0;
    for (int i = 0; i < 8; ++i) {
        idx |= static_cast<uint64_t>(idx_bytes[i]) << (i * 8);
    }
    EXPECT_EQ(idx, 0x0102030405060708ull);

    // Read last_included_term (8 bytes LE).
    uint8_t term_bytes[8];
    ASSERT_EQ(::read(fd, term_bytes, 8), 8);
    uint64_t term = 0;
    for (int i = 0; i < 8; ++i) {
        term |= static_cast<uint64_t>(term_bytes[i]) << (i * 8);
    }
    EXPECT_EQ(term, 0x0A0B0C0D0E0F1011ull);

    ::close(fd);
}

TEST_F(SnapshotTest, FileHasCorrectEntryCount) {
    std::unordered_map<std::string, std::string> data;
    data["a"] = "1";
    data["b"] = "2";
    data["c"] = "3";

    auto ec = Snapshot::save(snap_path_, data, 1, 1);
    ASSERT_FALSE(ec) << ec.message();

    int fd = ::open(snap_path_.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);

    // Skip header(6) + metadata(16) = 22 bytes.
    ::lseek(fd, 22, SEEK_SET);

    uint8_t cnt[4];
    ASSERT_EQ(::read(fd, cnt, 4), 4);
    uint32_t entry_count = static_cast<uint32_t>(cnt[0]) |
                           (static_cast<uint32_t>(cnt[1]) << 8) |
                           (static_cast<uint32_t>(cnt[2]) << 16) |
                           (static_cast<uint32_t>(cnt[3]) << 24);
    EXPECT_EQ(entry_count, 3u);

    ::close(fd);
}

// ── Deterministic output ─────────────────────────────────────────────────────

TEST_F(SnapshotTest, SaveProducesDeterministicOutput) {
    std::unordered_map<std::string, std::string> data;
    data["z"] = "last";
    data["a"] = "first";
    data["m"] = "middle";

    // Save twice, files should be identical (sorted by key).
    auto path1 = test_dir_ / "snap1.bin";
    auto path2 = test_dir_ / "snap2.bin";

    auto ec = Snapshot::save(path1, data, 42, 7);
    ASSERT_FALSE(ec) << ec.message();
    ec = Snapshot::save(path2, data, 42, 7);
    ASSERT_FALSE(ec) << ec.message();

    auto size1 = std::filesystem::file_size(path1);
    auto size2 = std::filesystem::file_size(path2);
    ASSERT_EQ(size1, size2);

    // Read both files and compare byte-by-byte.
    std::ifstream f1(path1, std::ios::binary);
    std::ifstream f2(path2, std::ios::binary);
    std::vector<char> buf1(size1), buf2(size2);
    f1.read(buf1.data(), static_cast<std::streamsize>(size1));
    f2.read(buf2.data(), static_cast<std::streamsize>(size2));
    EXPECT_EQ(buf1, buf2);
}

// ── CRC32 covers full payload ────────────────────────────────────────────────

TEST_F(SnapshotTest, CrcCoversEntirePayload) {
    // Save a snapshot, read the file, verify CRC manually.
    std::unordered_map<std::string, std::string> data;
    data["x"] = "y";
    auto ec = Snapshot::save(snap_path_, data, 7, 2);
    ASSERT_FALSE(ec) << ec.message();

    auto fsize = std::filesystem::file_size(snap_path_);
    std::vector<uint8_t> buf(fsize);
    int fd = ::open(snap_path_.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::read(fd, buf.data(), fsize), static_cast<ssize_t>(fsize));
    ::close(fd);

    // CRC is last 4 bytes, covers everything before it.
    std::size_t data_len = fsize - 4;
    uint32_t stored_crc = static_cast<uint32_t>(buf[data_len]) |
                          (static_cast<uint32_t>(buf[data_len + 1]) << 8) |
                          (static_cast<uint32_t>(buf[data_len + 2]) << 16) |
                          (static_cast<uint32_t>(buf[data_len + 3]) << 24);
    uint32_t computed_crc = crc32(buf.data(), data_len);
    EXPECT_EQ(stored_crc, computed_crc);
}

// ── Edge cases ───────────────────────────────────────────────────────────────

TEST_F(SnapshotTest, SaveToNonexistentDirectoryFails) {
    std::unordered_map<std::string, std::string> data;
    auto bad_path = test_dir_ / "nonexistent" / "dir" / "snapshot.bin";
    auto ec = Snapshot::save(bad_path, data, 1, 1);
    EXPECT_TRUE(ec);
}

TEST_F(SnapshotTest, BinaryKeysAndValues) {
    // Keys and values with embedded NUL bytes and non-ASCII.
    std::unordered_map<std::string, std::string> data;
    std::string key_with_nul("key\0with\0nuls", 13);
    std::string val_with_nul("val\0ue", 6);
    data[key_with_nul] = val_with_nul;

    std::string binary_key;
    binary_key.push_back('\x01');
    binary_key.push_back('\xFF');
    binary_key.push_back('\x80');
    data[binary_key] = "normal_value";

    auto ec = Snapshot::save(snap_path_, data, 1, 1);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_EQ(result.data.size(), 2u);
    EXPECT_EQ(result.data.at(key_with_nul), val_with_nul);
    EXPECT_EQ(result.data.at(binary_key), "normal_value");
}

TEST_F(SnapshotTest, ManyEntries) {
    std::unordered_map<std::string, std::string> data;
    for (int i = 0; i < 1000; ++i) {
        data["key_" + std::to_string(i)] = "value_" + std::to_string(i);
    }

    auto ec = Snapshot::save(snap_path_, data, 5000, 10);
    ASSERT_FALSE(ec) << ec.message();

    SnapshotLoadResult result;
    ec = Snapshot::load(snap_path_, result);
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_EQ(result.data.size(), 1000u);
    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(result.data.at("key_" + std::to_string(i)),
                  "value_" + std::to_string(i));
    }
}

// ── kFilename constant ───────────────────────────────────────────────────────

TEST_F(SnapshotTest, DefaultFilenameIsSnapshotBin) {
    EXPECT_STREQ(Snapshot::kFilename, "snapshot.bin");
}

} // namespace kv::persistence

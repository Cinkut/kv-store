#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace kv::persistence {

// ── WAL record types ─────────────────────────────────────────────────────────

static constexpr uint8_t kRecordTypeMeta = 0x01;
static constexpr uint8_t kRecordTypeEntry = 0x02;

// ── WAL header constants ─────────────────────────────────────────────────────

static constexpr char kWalMagic[] = "KVWAL";          // 5 bytes (no NUL)
static constexpr std::size_t kWalMagicSize = 5;
static constexpr uint16_t kWalVersion = 1;
static constexpr std::size_t kWalHeaderSize = kWalMagicSize + sizeof(uint16_t);

// ── Metadata record ──────────────────────────────────────────────────────────
//
// Persists currentTerm + votedFor.
// [type: u8 = 0x01][term: u64 LE][voted_for: i32 LE][crc32: u32 LE]

struct MetadataRecord {
    uint64_t term = 0;
    int32_t voted_for = -1;  // -1 = no vote
};

// ── Log entry record ─────────────────────────────────────────────────────────
//
// [type: u8 = 0x02][total_length: u32 LE][term: u64 LE][index: u64 LE]
// [cmd_type: u8][key_len: u16 LE][key][value_len: u32 LE][value]
// [crc32: u32 LE]

struct LogEntryRecord {
    uint64_t term = 0;
    uint64_t index = 0;
    uint8_t cmd_type = 0;   // 0=NOOP, 1=SET, 2=DEL
    std::string key;
    std::string value;
};

// ── Replay result ────────────────────────────────────────────────────────────

struct WalReplayResult {
    std::optional<MetadataRecord> metadata;
    std::vector<LogEntryRecord> entries;
};

// ── Write-Ahead Log ──────────────────────────────────────────────────────────
//
// Append-only binary file. All writes are flushed (fsynced) immediately.
// Thread-safety: NOT thread-safe. Caller must serialise access (Raft strand).

class WAL {
public:
    // Opens (or creates) the WAL file at `path`.
    // Returns an error_code on failure.
    explicit WAL(const std::filesystem::path& path);
    ~WAL();

    // Non-copyable, non-movable.
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;
    WAL(WAL&&) = delete;
    WAL& operator=(WAL&&) = delete;

    // Opens the WAL file. Creates it with a fresh header if it doesn't exist.
    [[nodiscard]] std::error_code open();

    // Close the WAL file.
    void close();

    // Append a metadata record (term + votedFor).
    [[nodiscard]] std::error_code append_metadata(const MetadataRecord& rec);

    // Append a log entry record.
    [[nodiscard]] std::error_code append_entry(const LogEntryRecord& rec);

    // Replay all records from the WAL file.
    // Returns the latest metadata + all log entries, or an error.
    // Only the *last* metadata record is returned (latest term/vote).
    [[nodiscard]] static std::error_code replay(
        const std::filesystem::path& path,
        WalReplayResult& result);

    // Truncate the WAL: remove all entries with index > after_index.
    // Rewrites the file keeping only the header, latest metadata, and
    // entries with index <= after_index.
    [[nodiscard]] std::error_code truncate_suffix(uint64_t after_index);

    // Rewrite the WAL from scratch with the given metadata and entries.
    // Used after snapshot to keep only entries after the snapshot point.
    [[nodiscard]] std::error_code rewrite(
        const std::optional<MetadataRecord>& metadata,
        const std::vector<LogEntryRecord>& entries);

    [[nodiscard]] bool is_open() const { return fd_ != -1; }

    [[nodiscard]] const std::filesystem::path& path() const { return path_; }

private:
    // Write raw bytes to the file and fsync.
    [[nodiscard]] std::error_code write_bytes(const std::vector<uint8_t>& data);

    // Write just the file header ("KVWAL" + version).
    [[nodiscard]] std::error_code write_header();

    // Validate the file header on open.
    [[nodiscard]] static std::error_code validate_header(int fd);

    std::filesystem::path path_;
    int fd_ = -1;
};

// ── CRC32 utility ────────────────────────────────────────────────────────────

// Compute CRC32 (ISO 3309 / ITU-T V.42, same polynomial as zlib).
[[nodiscard]] uint32_t crc32(const uint8_t* data, std::size_t length);

// ── Serialisation helpers ────────────────────────────────────────────────────

// Serialise a metadata record to bytes (including type byte and CRC).
[[nodiscard]] std::vector<uint8_t> serialise_metadata(const MetadataRecord& rec);

// Serialise a log entry record to bytes (including type byte and CRC).
[[nodiscard]] std::vector<uint8_t> serialise_entry(const LogEntryRecord& rec);

} // namespace kv::persistence

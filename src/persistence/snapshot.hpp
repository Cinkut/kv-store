#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>
#include <unordered_map>

namespace kv::persistence {

// ── Snapshot header constants ────────────────────────────────────────────────

static constexpr char kSnapshotMagic[] = "KVSS";          // 4 bytes (no NUL)
static constexpr std::size_t kSnapshotMagicSize = 4;
static constexpr uint16_t kSnapshotVersion = 1;
static constexpr std::size_t kSnapshotHeaderSize =
    kSnapshotMagicSize + sizeof(uint16_t);                // 6 bytes

// ── Snapshot metadata ────────────────────────────────────────────────────────

struct SnapshotMetadata {
    uint64_t last_included_index = 0;
    uint64_t last_included_term  = 0;
};

// ── Snapshot load result ─────────────────────────────────────────────────────

struct SnapshotLoadResult {
    SnapshotMetadata metadata;
    std::unordered_map<std::string, std::string> data;
};

// ── Snapshot ─────────────────────────────────────────────────────────────────
//
// Full-state snapshot of the key-value store.  Binary format (from raft-spec):
//
//   [magic: "KVSS" (4B)][version: u16 LE = 1]
//   [last_included_index: u64 LE][last_included_term: u64 LE]
//   [entry_count: u32 LE]
//     [key_length: u16 LE][key][value_length: u32 LE][value]  × entry_count
//   [crc32: u32 LE]     // CRC of everything from magic through last value
//
// File path: <data_dir>/snapshot.bin
// Atomic write: write to .tmp, then rename.
//
// Thread-safety: static methods, no mutable state. Caller must serialise
// with respect to WAL operations (Raft strand).

class Snapshot {
public:
    // Default snapshot filename.
    static constexpr const char* kFilename = "snapshot.bin";

    // Save a snapshot atomically to `path`.
    // Writes to `<path>.tmp` first, then renames.
    [[nodiscard]] static std::error_code save(
        const std::filesystem::path& path,
        const std::unordered_map<std::string, std::string>& data,
        uint64_t last_included_index,
        uint64_t last_included_term);

    // Load a snapshot from `path`.
    // Validates magic, version, and CRC32.
    [[nodiscard]] static std::error_code load(
        const std::filesystem::path& path,
        SnapshotLoadResult& result);

    // Check if a snapshot file exists at `path`.
    [[nodiscard]] static bool exists(const std::filesystem::path& path);
};

} // namespace kv::persistence

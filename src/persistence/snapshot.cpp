#include "persistence/snapshot.hpp"
#include "persistence/wal.hpp"  // crc32()

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

namespace kv::persistence {

namespace {

// ── Little-endian serialisation helpers ──────────────────────────────────────

void append_raw(std::vector<uint8_t>& buf, const void* data, std::size_t len) {
    const auto* p = static_cast<const uint8_t*>(data);
    buf.insert(buf.end(), p, p + len);
}

void append_u16(std::vector<uint8_t>& buf, uint16_t v) {
    uint8_t b[2];
    b[0] = static_cast<uint8_t>(v);
    b[1] = static_cast<uint8_t>(v >> 8);
    buf.insert(buf.end(), b, b + 2);
}

void append_u32(std::vector<uint8_t>& buf, uint32_t v) {
    uint8_t b[4];
    b[0] = static_cast<uint8_t>(v);
    b[1] = static_cast<uint8_t>(v >> 8);
    b[2] = static_cast<uint8_t>(v >> 16);
    b[3] = static_cast<uint8_t>(v >> 24);
    buf.insert(buf.end(), b, b + 4);
}

void append_u64(std::vector<uint8_t>& buf, uint64_t v) {
    uint8_t b[8];
    for (int i = 0; i < 8; ++i) {
        b[i] = static_cast<uint8_t>(v >> (i * 8));
    }
    buf.insert(buf.end(), b, b + 8);
}

uint16_t read_u16(const uint8_t* p) {
    return static_cast<uint16_t>(p[0]) |
           (static_cast<uint16_t>(p[1]) << 8);
}

uint32_t read_u32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) |
           (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

uint64_t read_u64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(p[i]) << (i * 8);
    }
    return v;
}

// Write all bytes to fd. Returns error_code on failure.
[[nodiscard]] std::error_code write_all(int fd, const uint8_t* data,
                                        std::size_t len) {
    std::size_t written = 0;
    while (written < len) {
        auto n = ::write(fd, data + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return {errno, std::system_category()};
        }
        written += static_cast<std::size_t>(n);
    }
    return {};
}

// Read exactly `len` bytes from fd into `buf`. Returns error_code on failure.
[[nodiscard]] std::error_code read_all(int fd, uint8_t* buf, std::size_t len) {
    std::size_t total = 0;
    while (total < len) {
        auto n = ::read(fd, buf + total, len - total);
        if (n < 0) {
            if (errno == EINTR) continue;
            return {errno, std::system_category()};
        }
        if (n == 0) {
            return std::make_error_code(std::errc::io_error);  // unexpected EOF
        }
        total += static_cast<std::size_t>(n);
    }
    return {};
}

}  // namespace

// ── Snapshot::save ───────────────────────────────────────────────────────────

std::error_code Snapshot::save(
    const std::filesystem::path& path,
    const std::unordered_map<std::string, std::string>& data,
    uint64_t last_included_index,
    uint64_t last_included_term) {

    // Build the binary payload (everything except the trailing CRC).
    std::vector<uint8_t> buf;

    // Reserve a rough estimate.
    buf.reserve(kSnapshotHeaderSize + 16 + 4 + data.size() * 64 + 4);

    // Magic.
    append_raw(buf, kSnapshotMagic, kSnapshotMagicSize);

    // Version.
    append_u16(buf, kSnapshotVersion);

    // Metadata.
    append_u64(buf, last_included_index);
    append_u64(buf, last_included_term);

    // Entry count.
    append_u32(buf, static_cast<uint32_t>(data.size()));

    // Entries: sorted by key for deterministic output.
    std::vector<std::pair<std::string, std::string>> sorted(data.begin(),
                                                            data.end());
    std::sort(sorted.begin(), sorted.end());

    for (const auto& [key, value] : sorted) {
        append_u16(buf, static_cast<uint16_t>(key.size()));
        append_raw(buf, key.data(), key.size());
        append_u32(buf, static_cast<uint32_t>(value.size()));
        append_raw(buf, value.data(), value.size());
    }

    // CRC32 of everything so far.
    uint32_t checksum = crc32(buf.data(), buf.size());
    append_u32(buf, checksum);

    // Atomic write: write to .tmp, fsync, rename.
    auto tmp_path = path;
    tmp_path += ".tmp";

    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        auto ec = std::error_code{errno, std::system_category()};
        spdlog::error("Snapshot: failed to open tmp file {}: {}",
                      tmp_path.string(), ec.message());
        return ec;
    }

    auto ec = write_all(fd, buf.data(), buf.size());
    if (ec) {
        spdlog::error("Snapshot: write failed: {}", ec.message());
        ::close(fd);
        std::filesystem::remove(tmp_path);
        return ec;
    }

    if (::fsync(fd) < 0) {
        ec = {errno, std::system_category()};
        spdlog::error("Snapshot: fsync failed: {}", ec.message());
        ::close(fd);
        std::filesystem::remove(tmp_path);
        return ec;
    }

    ::close(fd);

    // Rename .tmp → final path.
    std::error_code rename_ec;
    std::filesystem::rename(tmp_path, path, rename_ec);
    if (rename_ec) {
        spdlog::error("Snapshot: rename failed: {}", rename_ec.message());
        std::filesystem::remove(tmp_path);
        return rename_ec;
    }

    spdlog::info("Snapshot: saved {} entries at index={} term={} to {}",
                 data.size(), last_included_index, last_included_term,
                 path.string());

    return {};
}

// ── Snapshot::load ───────────────────────────────────────────────────────────

std::error_code Snapshot::load(
    const std::filesystem::path& path,
    SnapshotLoadResult& result) {

    // Read entire file into memory.
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        auto ec = std::error_code{errno, std::system_category()};
        spdlog::error("Snapshot: failed to open {}: {}", path.string(),
                      ec.message());
        return ec;
    }

    // Get file size.
    auto file_size = ::lseek(fd, 0, SEEK_END);
    if (file_size < 0) {
        auto ec = std::error_code{errno, std::system_category()};
        ::close(fd);
        return ec;
    }
    ::lseek(fd, 0, SEEK_SET);

    // Minimum valid snapshot: header(6) + metadata(16) + entry_count(4) + crc(4) = 30
    static constexpr std::size_t kMinSize =
        kSnapshotHeaderSize + 16 + 4 + 4;  // 30 bytes

    if (static_cast<std::size_t>(file_size) < kMinSize) {
        ::close(fd);
        spdlog::error("Snapshot: file too small ({} bytes)", file_size);
        return std::make_error_code(std::errc::invalid_argument);
    }

    std::vector<uint8_t> buf(static_cast<std::size_t>(file_size));
    auto ec = read_all(fd, buf.data(), buf.size());
    ::close(fd);
    if (ec) {
        spdlog::error("Snapshot: read failed: {}", ec.message());
        return ec;
    }

    const uint8_t* p = buf.data();
    const uint8_t* end = p + buf.size();

    // Validate magic.
    if (std::memcmp(p, kSnapshotMagic, kSnapshotMagicSize) != 0) {
        spdlog::error("Snapshot: invalid magic");
        return std::make_error_code(std::errc::invalid_argument);
    }
    p += kSnapshotMagicSize;

    // Validate version.
    uint16_t version = read_u16(p);
    p += 2;
    if (version != kSnapshotVersion) {
        spdlog::error("Snapshot: unsupported version {}", version);
        return std::make_error_code(std::errc::invalid_argument);
    }

    // Read metadata.
    if (p + 16 > end) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    result.metadata.last_included_index = read_u64(p);
    p += 8;
    result.metadata.last_included_term = read_u64(p);
    p += 8;

    // Read entry count.
    if (p + 4 > end) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    uint32_t entry_count = read_u32(p);
    p += 4;

    // Read entries.
    result.data.clear();
    result.data.reserve(entry_count);

    for (uint32_t i = 0; i < entry_count; ++i) {
        // key_length (u16)
        if (p + 2 > end) {
            spdlog::error("Snapshot: truncated at entry {} key_length", i);
            return std::make_error_code(std::errc::invalid_argument);
        }
        uint16_t key_len = read_u16(p);
        p += 2;

        // key
        if (p + key_len > end) {
            spdlog::error("Snapshot: truncated at entry {} key", i);
            return std::make_error_code(std::errc::invalid_argument);
        }
        std::string key(reinterpret_cast<const char*>(p), key_len);
        p += key_len;

        // value_length (u32)
        if (p + 4 > end) {
            spdlog::error("Snapshot: truncated at entry {} value_length", i);
            return std::make_error_code(std::errc::invalid_argument);
        }
        uint32_t val_len = read_u32(p);
        p += 4;

        // value
        if (p + val_len > end) {
            spdlog::error("Snapshot: truncated at entry {} value", i);
            return std::make_error_code(std::errc::invalid_argument);
        }
        std::string value(reinterpret_cast<const char*>(p), val_len);
        p += val_len;

        result.data.emplace(std::move(key), std::move(value));
    }

    // CRC32 check: CRC covers everything from start through the last value.
    // The CRC itself is the last 4 bytes.
    if (p + 4 > end) {
        spdlog::error("Snapshot: truncated at CRC");
        return std::make_error_code(std::errc::invalid_argument);
    }

    uint32_t stored_crc = read_u32(p);
    std::size_t data_len = static_cast<std::size_t>(p - buf.data());
    uint32_t computed_crc = crc32(buf.data(), data_len);

    if (stored_crc != computed_crc) {
        spdlog::error("Snapshot: CRC mismatch (stored={:#010x}, computed={:#010x})",
                      stored_crc, computed_crc);
        return std::make_error_code(std::errc::invalid_argument);
    }

    spdlog::info("Snapshot: loaded {} entries at index={} term={} from {}",
                 result.data.size(),
                 result.metadata.last_included_index,
                 result.metadata.last_included_term,
                 path.string());

    return {};
}

// ── Snapshot::exists ─────────────────────────────────────────────────────────

bool Snapshot::exists(const std::filesystem::path& path) {
    return std::filesystem::exists(path) &&
           std::filesystem::is_regular_file(path);
}

} // namespace kv::persistence

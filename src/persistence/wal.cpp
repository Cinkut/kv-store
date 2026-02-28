#include "persistence/wal.hpp"

#include <array>
#include <cerrno>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

namespace kv::persistence {

// ── CRC32 (ISO 3309 polynomial 0xEDB88320) ──────────────────────────────────

namespace {

constexpr std::array<uint32_t, 256> make_crc32_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0xEDB88320;
            else
                crc >>= 1;
        }
        table[i] = crc;
    }
    return table;
}

constexpr auto kCrc32Table = make_crc32_table();

} // anonymous namespace

uint32_t crc32(const uint8_t* data, std::size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (std::size_t i = 0; i < length; ++i) {
        crc = kCrc32Table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// ── Little-endian helpers ────────────────────────────────────────────────────

namespace {

void write_u8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

void write_u16_le(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
}

void write_u32_le(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void write_i32_le(std::vector<uint8_t>& buf, int32_t v) {
    write_u32_le(buf, static_cast<uint32_t>(v));
}

void write_u64_le(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    }
}

void append_raw(std::vector<uint8_t>& buf, const void* data, std::size_t len) {
    const auto* p = static_cast<const uint8_t*>(data);
    buf.insert(buf.end(), p, p + len);
}

// Read helpers: return false if not enough data.
bool read_u8(const uint8_t*& ptr, const uint8_t* end, uint8_t& out) {
    if (ptr + 1 > end) return false;
    out = *ptr++;
    return true;
}

bool read_u16_le(const uint8_t*& ptr, const uint8_t* end, uint16_t& out) {
    if (ptr + 2 > end) return false;
    out = static_cast<uint16_t>(ptr[0]) |
          (static_cast<uint16_t>(ptr[1]) << 8);
    ptr += 2;
    return true;
}

bool read_u32_le(const uint8_t*& ptr, const uint8_t* end, uint32_t& out) {
    if (ptr + 4 > end) return false;
    out = static_cast<uint32_t>(ptr[0]) |
          (static_cast<uint32_t>(ptr[1]) << 8) |
          (static_cast<uint32_t>(ptr[2]) << 16) |
          (static_cast<uint32_t>(ptr[3]) << 24);
    ptr += 4;
    return true;
}

bool read_i32_le(const uint8_t*& ptr, const uint8_t* end, int32_t& out) {
    uint32_t v = 0;
    if (!read_u32_le(ptr, end, v)) return false;
    out = static_cast<int32_t>(v);
    return true;
}

bool read_u64_le(const uint8_t*& ptr, const uint8_t* end, uint64_t& out) {
    if (ptr + 8 > end) return false;
    out = 0;
    for (int i = 0; i < 8; ++i) {
        out |= static_cast<uint64_t>(ptr[i]) << (i * 8);
    }
    ptr += 8;
    return true;
}

std::error_code make_errno_error() {
    return {errno, std::system_category()};
}

std::error_code make_error(std::errc e) {
    return std::make_error_code(e);
}

// Read all bytes from fd. Returns empty on error.
std::vector<uint8_t> read_all(int fd) {
    std::vector<uint8_t> data;
    uint8_t buf[8192];
    while (true) {
        auto n = ::read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) continue;
            return {};
        }
        if (n == 0) break;
        data.insert(data.end(), buf, buf + n);
    }
    return data;
}

} // anonymous namespace

// ── Serialisation ────────────────────────────────────────────────────────────

std::vector<uint8_t> serialise_metadata(const MetadataRecord& rec) {
    // [type: u8 = 0x01][term: u64 LE][voted_for: i32 LE][crc32: u32 LE]
    std::vector<uint8_t> buf;
    buf.reserve(1 + 8 + 4 + 4);

    write_u8(buf, kRecordTypeMeta);
    write_u64_le(buf, rec.term);
    write_i32_le(buf, rec.voted_for);

    // CRC covers type through voted_for.
    uint32_t c = crc32(buf.data(), buf.size());
    write_u32_le(buf, c);

    return buf;
}

std::vector<uint8_t> serialise_entry(const LogEntryRecord& rec) {
    // Compute total_length: everything from term through value (before CRC).
    // term(8) + index(8) + cmd_type(1) + key_len(2) + key + value_len(4) + value
    uint32_t total_length = 8 + 8 + 1 + 2 +
                            static_cast<uint32_t>(rec.key.size()) +
                            4 +
                            static_cast<uint32_t>(rec.value.size());

    std::vector<uint8_t> buf;
    buf.reserve(1 + 4 + total_length + 4);

    write_u8(buf, kRecordTypeEntry);
    write_u32_le(buf, total_length);
    write_u64_le(buf, rec.term);
    write_u64_le(buf, rec.index);
    write_u8(buf, rec.cmd_type);
    write_u16_le(buf, static_cast<uint16_t>(rec.key.size()));
    append_raw(buf, rec.key.data(), rec.key.size());
    write_u32_le(buf, static_cast<uint32_t>(rec.value.size()));
    append_raw(buf, rec.value.data(), rec.value.size());

    // CRC covers type through value.
    uint32_t c = crc32(buf.data(), buf.size());
    write_u32_le(buf, c);

    return buf;
}

// ── WAL implementation ───────────────────────────────────────────────────────

WAL::WAL(const std::filesystem::path& path) : path_(path) {}

WAL::~WAL() {
    close();
}

std::error_code WAL::open() {
    if (fd_ != -1) {
        return {};  // Already open.
    }

    bool exists = std::filesystem::exists(path_);

    // Create parent directories if needed.
    if (!exists) {
        std::error_code ec;
        std::filesystem::create_directories(path_.parent_path(), ec);
        if (ec) return ec;
    }

    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        return make_errno_error();
    }

    if (!exists || std::filesystem::file_size(path_) == 0) {
        // New file — write header.
        auto ec = write_header();
        if (ec) {
            close();
            return ec;
        }
    } else {
        // Existing file — validate header.
        auto ec = validate_header(fd_);
        if (ec) {
            close();
            return ec;
        }
        // Seek to end for appending.
        if (::lseek(fd_, 0, SEEK_END) < 0) {
            auto e = make_errno_error();
            close();
            return e;
        }
    }

    return {};
}

void WAL::close() {
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
}

std::error_code WAL::write_header() {
    std::vector<uint8_t> hdr;
    hdr.reserve(kWalHeaderSize);
    append_raw(hdr, kWalMagic, kWalMagicSize);
    write_u16_le(hdr, kWalVersion);
    return write_bytes(hdr);
}

std::error_code WAL::validate_header(int fd) {
    // Seek to start.
    if (::lseek(fd, 0, SEEK_SET) < 0) {
        return make_errno_error();
    }

    uint8_t hdr[kWalHeaderSize];
    auto n = ::read(fd, hdr, kWalHeaderSize);
    if (n < 0) return make_errno_error();
    if (static_cast<std::size_t>(n) < kWalHeaderSize) {
        return make_error(std::errc::invalid_argument);
    }

    // Check magic.
    if (std::memcmp(hdr, kWalMagic, kWalMagicSize) != 0) {
        return make_error(std::errc::invalid_argument);
    }

    // Check version.
    uint16_t version = static_cast<uint16_t>(hdr[5]) |
                       (static_cast<uint16_t>(hdr[6]) << 8);
    if (version != kWalVersion) {
        return make_error(std::errc::not_supported);
    }

    return {};
}

std::error_code WAL::write_bytes(const std::vector<uint8_t>& data) {
    const uint8_t* ptr = data.data();
    std::size_t remaining = data.size();

    while (remaining > 0) {
        auto n = ::write(fd_, ptr, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return make_errno_error();
        }
        ptr += n;
        remaining -= static_cast<std::size_t>(n);
    }

    // fsync to ensure durability.
    if (::fdatasync(fd_) < 0) {
        return make_errno_error();
    }

    return {};
}

std::error_code WAL::append_metadata(const MetadataRecord& rec) {
    if (fd_ == -1) return make_error(std::errc::bad_file_descriptor);
    auto data = serialise_metadata(rec);
    return write_bytes(data);
}

std::error_code WAL::append_entry(const LogEntryRecord& rec) {
    if (fd_ == -1) return make_error(std::errc::bad_file_descriptor);
    auto data = serialise_entry(rec);
    return write_bytes(data);
}

std::error_code WAL::replay(
    const std::filesystem::path& path,
    WalReplayResult& result)
{
    result = {};

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return make_errno_error();
    }

    auto data = read_all(fd);
    ::close(fd);

    if (data.empty()) {
        return make_error(std::errc::invalid_argument);
    }

    const uint8_t* ptr = data.data();
    const uint8_t* end = data.data() + data.size();

    // Validate header.
    if (data.size() < kWalHeaderSize) {
        return make_error(std::errc::invalid_argument);
    }
    if (std::memcmp(ptr, kWalMagic, kWalMagicSize) != 0) {
        return make_error(std::errc::invalid_argument);
    }
    uint16_t version = 0;
    ptr += kWalMagicSize;
    if (!read_u16_le(ptr, end, version)) {
        return make_error(std::errc::invalid_argument);
    }
    if (version != kWalVersion) {
        return make_error(std::errc::not_supported);
    }

    // Parse records.
    while (ptr < end) {
        const uint8_t* record_start = ptr;

        uint8_t type = 0;
        if (!read_u8(ptr, end, type)) break;

        if (type == kRecordTypeMeta) {
            // [type(1)][term(8)][voted_for(4)][crc(4)] = 17 bytes total
            if (ptr + 8 + 4 + 4 > end) {
                spdlog::warn("WAL: truncated metadata record");
                break;
            }

            MetadataRecord rec;
            if (!read_u64_le(ptr, end, rec.term)) break;
            if (!read_i32_le(ptr, end, rec.voted_for)) break;

            uint32_t stored_crc = 0;
            if (!read_u32_le(ptr, end, stored_crc)) break;

            // Verify CRC: type through voted_for.
            std::size_t payload_len = static_cast<std::size_t>(ptr - record_start) - 4;
            uint32_t computed_crc = crc32(record_start, payload_len);
            if (computed_crc != stored_crc) {
                spdlog::warn("WAL: CRC mismatch in metadata record");
                return make_error(std::errc::io_error);
            }

            result.metadata = rec;

        } else if (type == kRecordTypeEntry) {
            uint32_t total_length = 0;
            if (!read_u32_le(ptr, end, total_length)) {
                spdlog::warn("WAL: truncated entry record (total_length)");
                break;
            }

            // Ensure enough data for total_length + crc(4).
            if (ptr + total_length + 4 > end) {
                spdlog::warn("WAL: truncated entry record (payload)");
                break;
            }

            LogEntryRecord rec;
            if (!read_u64_le(ptr, end, rec.term)) break;
            if (!read_u64_le(ptr, end, rec.index)) break;
            if (!read_u8(ptr, end, rec.cmd_type)) break;

            uint16_t key_len = 0;
            if (!read_u16_le(ptr, end, key_len)) break;
            if (ptr + key_len > end) break;
            rec.key.assign(reinterpret_cast<const char*>(ptr), key_len);
            ptr += key_len;

            uint32_t value_len = 0;
            if (!read_u32_le(ptr, end, value_len)) break;
            if (ptr + value_len > end) break;
            rec.value.assign(reinterpret_cast<const char*>(ptr), value_len);
            ptr += value_len;

            uint32_t stored_crc = 0;
            if (!read_u32_le(ptr, end, stored_crc)) break;

            // CRC covers type through value.
            std::size_t payload_len = static_cast<std::size_t>(ptr - record_start) - 4;
            uint32_t computed_crc = crc32(record_start, payload_len);
            if (computed_crc != stored_crc) {
                spdlog::warn("WAL: CRC mismatch in entry record at index {}", rec.index);
                return make_error(std::errc::io_error);
            }

            result.entries.push_back(std::move(rec));

        } else {
            spdlog::warn("WAL: unknown record type 0x{:02X}", type);
            break;
        }
    }

    return {};
}

std::error_code WAL::truncate_suffix(uint64_t after_index) {
    if (fd_ == -1) return make_error(std::errc::bad_file_descriptor);

    // Read current WAL, filter entries, rewrite.
    WalReplayResult current;
    close();  // close for replay

    auto ec = replay(path_, current);
    if (ec) return ec;

    // Filter entries.
    std::vector<LogEntryRecord> kept;
    for (auto& e : current.entries) {
        if (e.index <= after_index) {
            kept.push_back(std::move(e));
        }
    }

    return rewrite(current.metadata, kept);
}

std::error_code WAL::rewrite(
    const std::optional<MetadataRecord>& metadata,
    const std::vector<LogEntryRecord>& entries)
{
    close();

    // Write to a temporary file, then rename.
    auto tmp_path = path_;
    tmp_path += ".tmp";

    int tmp_fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (tmp_fd < 0) {
        return make_errno_error();
    }

    // Write header.
    std::vector<uint8_t> hdr;
    hdr.reserve(kWalHeaderSize);
    append_raw(hdr, kWalMagic, kWalMagicSize);
    write_u16_le(hdr, kWalVersion);

    auto write_all = [](int fd, const std::vector<uint8_t>& data) -> std::error_code {
        const uint8_t* ptr = data.data();
        std::size_t remaining = data.size();
        while (remaining > 0) {
            auto n = ::write(fd, ptr, remaining);
            if (n < 0) {
                if (errno == EINTR) continue;
                return {errno, std::system_category()};
            }
            ptr += n;
            remaining -= static_cast<std::size_t>(n);
        }
        return {};
    };

    auto ec = write_all(tmp_fd, hdr);
    if (ec) { ::close(tmp_fd); return ec; }

    // Write metadata if present.
    if (metadata) {
        auto data = serialise_metadata(*metadata);
        ec = write_all(tmp_fd, data);
        if (ec) { ::close(tmp_fd); return ec; }
    }

    // Write entries.
    for (const auto& entry : entries) {
        auto data = serialise_entry(entry);
        ec = write_all(tmp_fd, data);
        if (ec) { ::close(tmp_fd); return ec; }
    }

    // fsync + close.
    if (::fdatasync(tmp_fd) < 0) {
        auto e = make_errno_error();
        ::close(tmp_fd);
        return e;
    }
    ::close(tmp_fd);

    // Atomic rename.
    std::error_code fs_ec;
    std::filesystem::rename(tmp_path, path_, fs_ec);
    if (fs_ec) return fs_ec;

    // Reopen for appending.
    return open();
}

} // namespace kv::persistence

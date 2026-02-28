#include "network/binary_protocol.hpp"

#include <cstdint>
#include <cstring>
#include <string>

namespace kv::network {

// ── Big-endian helpers ────────────────────────────────────────────────────────

namespace {

void write_u8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

void write_u16_be(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

void write_u32_be(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

void write_bytes(std::vector<uint8_t>& buf, const void* data, std::size_t len) {
    const auto* p = static_cast<const uint8_t*>(data);
    buf.insert(buf.end(), p, p + len);
}

bool read_u16_be(const uint8_t*& ptr, const uint8_t* end, uint16_t& out) {
    if (ptr + 2 > end) return false;
    out = static_cast<uint16_t>(
        (static_cast<uint16_t>(ptr[0]) << 8) |
         static_cast<uint16_t>(ptr[1]));
    ptr += 2;
    return true;
}

bool read_u32_be(const uint8_t*& ptr, const uint8_t* end, uint32_t& out) {
    if (ptr + 4 > end) return false;
    out = (static_cast<uint32_t>(ptr[0]) << 24) |
          (static_cast<uint32_t>(ptr[1]) << 16) |
          (static_cast<uint32_t>(ptr[2]) << 8) |
           static_cast<uint32_t>(ptr[3]);
    ptr += 4;
    return true;
}

// Build the response payload (without header), returning the status byte.
struct ResponseParts {
    uint8_t status;
    std::vector<uint8_t> payload;
};

ResponseParts build_response_parts(const Response& response) {
    return std::visit(
        [](const auto& r) -> ResponseParts {
            using T = std::decay_t<decltype(r)>;

            if constexpr (std::is_same_v<T, OkResp>) {
                return {binary::kStatusOk, {}};

            } else if constexpr (std::is_same_v<T, PongResp>) {
                return {binary::kStatusPong, {}};

            } else if constexpr (std::is_same_v<T, NotFoundResp>) {
                return {binary::kStatusNotFound, {}};

            } else if constexpr (std::is_same_v<T, DeletedResp>) {
                return {binary::kStatusDeleted, {}};

            } else if constexpr (std::is_same_v<T, ValueResp>) {
                std::vector<uint8_t> payload;
                write_u32_be(payload, static_cast<uint32_t>(r.value.size()));
                write_bytes(payload, r.value.data(), r.value.size());
                return {binary::kStatusValue, std::move(payload)};

            } else if constexpr (std::is_same_v<T, KeysResp>) {
                std::vector<uint8_t> payload;
                write_u32_be(payload, static_cast<uint32_t>(r.keys.size()));
                for (const auto& k : r.keys) {
                    write_u16_be(payload, static_cast<uint16_t>(k.size()));
                    write_bytes(payload, k.data(), k.size());
                }
                return {binary::kStatusKeys, std::move(payload)};

            } else if constexpr (std::is_same_v<T, ErrorResp>) {
                std::vector<uint8_t> payload;
                write_u16_be(payload, static_cast<uint16_t>(r.message.size()));
                write_bytes(payload, r.message.data(), r.message.size());
                return {binary::kStatusError, std::move(payload)};

            } else if constexpr (std::is_same_v<T, RedirectResp>) {
                std::vector<uint8_t> payload;
                write_u16_be(payload, static_cast<uint16_t>(r.address.size()));
                write_bytes(payload, r.address.data(), r.address.size());
                return {binary::kStatusRedirect, std::move(payload)};
            }
        },
        response);
}

} // anonymous namespace

// ── read_binary_header ────────────────────────────────────────────────────────

bool read_binary_header(
    std::span<const uint8_t> data,
    uint8_t& msg_type_out,
    uint32_t& payload_length_out) noexcept
{
    if (data.size() < binary::kHeaderSize) return false;

    msg_type_out = data[0];
    payload_length_out =
        (static_cast<uint32_t>(data[1]) << 24) |
        (static_cast<uint32_t>(data[2]) << 16) |
        (static_cast<uint32_t>(data[3]) << 8) |
         static_cast<uint32_t>(data[4]);
    return true;
}

// ── parse_binary_request ──────────────────────────────────────────────────────

std::variant<Command, ErrorResp> parse_binary_request(
    uint8_t msg_type, std::span<const uint8_t> payload)
{
    const uint8_t* ptr = payload.data();
    const uint8_t* end = payload.data() + payload.size();

    switch (msg_type) {
        case binary::kMsgPing: {
            return PingCmd{};
        }

        case binary::kMsgKeys: {
            return KeysCmd{};
        }

        case binary::kMsgGet: {
            uint16_t key_len = 0;
            if (!read_u16_be(ptr, end, key_len)) {
                return ErrorResp{"binary GET: truncated key_len"};
            }
            if (ptr + key_len > end) {
                return ErrorResp{"binary GET: truncated key"};
            }
            if (key_len == 0) {
                return ErrorResp{"binary GET: empty key"};
            }
            std::string key(reinterpret_cast<const char*>(ptr), key_len);
            return GetCmd{std::move(key)};
        }

        case binary::kMsgDel: {
            uint16_t key_len = 0;
            if (!read_u16_be(ptr, end, key_len)) {
                return ErrorResp{"binary DEL: truncated key_len"};
            }
            if (ptr + key_len > end) {
                return ErrorResp{"binary DEL: truncated key"};
            }
            if (key_len == 0) {
                return ErrorResp{"binary DEL: empty key"};
            }
            std::string key(reinterpret_cast<const char*>(ptr), key_len);
            return DelCmd{std::move(key)};
        }

        case binary::kMsgSet: {
            uint16_t key_len = 0;
            if (!read_u16_be(ptr, end, key_len)) {
                return ErrorResp{"binary SET: truncated key_len"};
            }
            if (ptr + key_len > end) {
                return ErrorResp{"binary SET: truncated key"};
            }
            if (key_len == 0) {
                return ErrorResp{"binary SET: empty key"};
            }
            std::string key(reinterpret_cast<const char*>(ptr), key_len);
            ptr += key_len;

            uint32_t value_len = 0;
            if (!read_u32_be(ptr, end, value_len)) {
                return ErrorResp{"binary SET: truncated value_len"};
            }
            if (ptr + value_len > end) {
                return ErrorResp{"binary SET: truncated value"};
            }
            std::string value(reinterpret_cast<const char*>(ptr), value_len);
            return SetCmd{std::move(key), std::move(value)};
        }

        default:
            return ErrorResp{"binary: unknown msg_type 0x" +
                             std::string(1, "0123456789abcdef"[(msg_type >> 4) & 0xF]) +
                             std::string(1, "0123456789abcdef"[msg_type & 0xF])};
    }
}

// ── serialize_binary_response ─────────────────────────────────────────────────

std::vector<uint8_t> serialize_binary_response(const Response& response) {
    auto [status, payload] = build_response_parts(response);

    std::vector<uint8_t> buf;
    buf.reserve(binary::kHeaderSize + payload.size());

    write_u8(buf, status);
    write_u32_be(buf, static_cast<uint32_t>(payload.size()));
    buf.insert(buf.end(), payload.begin(), payload.end());

    return buf;
}

// ── serialize_binary_request ──────────────────────────────────────────────────

std::vector<uint8_t> serialize_binary_request(const Command& cmd) {
    return std::visit(
        [](const auto& c) -> std::vector<uint8_t> {
            using T = std::decay_t<decltype(c)>;

            std::vector<uint8_t> buf;

            if constexpr (std::is_same_v<T, PingCmd>) {
                write_u8(buf, binary::kMsgPing);
                write_u32_be(buf, 0);

            } else if constexpr (std::is_same_v<T, KeysCmd>) {
                write_u8(buf, binary::kMsgKeys);
                write_u32_be(buf, 0);

            } else if constexpr (std::is_same_v<T, GetCmd>) {
                auto key_len = static_cast<uint16_t>(c.key.size());
                uint32_t payload_len = 2 + key_len;
                write_u8(buf, binary::kMsgGet);
                write_u32_be(buf, payload_len);
                write_u16_be(buf, key_len);
                write_bytes(buf, c.key.data(), c.key.size());

            } else if constexpr (std::is_same_v<T, DelCmd>) {
                auto key_len = static_cast<uint16_t>(c.key.size());
                uint32_t payload_len = 2 + key_len;
                write_u8(buf, binary::kMsgDel);
                write_u32_be(buf, payload_len);
                write_u16_be(buf, key_len);
                write_bytes(buf, c.key.data(), c.key.size());

            } else if constexpr (std::is_same_v<T, SetCmd>) {
                auto key_len = static_cast<uint16_t>(c.key.size());
                auto value_len = static_cast<uint32_t>(c.value.size());
                uint32_t payload_len = 2 + key_len + 4 + value_len;
                write_u8(buf, binary::kMsgSet);
                write_u32_be(buf, payload_len);
                write_u16_be(buf, key_len);
                write_bytes(buf, c.key.data(), c.key.size());
                write_u32_be(buf, value_len);
                write_bytes(buf, c.value.data(), c.value.size());
            }

            return buf;
        },
        cmd);
}

// ── parse_binary_response ─────────────────────────────────────────────────────

std::variant<Response, ErrorResp> parse_binary_response(
    uint8_t status, std::span<const uint8_t> payload)
{
    const uint8_t* ptr = payload.data();
    const uint8_t* end = payload.data() + payload.size();

    switch (status) {
        case binary::kStatusOk:
            return OkResp{};

        case binary::kStatusPong:
            return PongResp{};

        case binary::kStatusNotFound:
            return NotFoundResp{};

        case binary::kStatusDeleted:
            return DeletedResp{};

        case binary::kStatusValue: {
            uint32_t value_len = 0;
            if (!read_u32_be(ptr, end, value_len)) {
                return ErrorResp{"binary response: truncated value_len"};
            }
            if (ptr + value_len > end) {
                return ErrorResp{"binary response: truncated value"};
            }
            return ValueResp{std::string(reinterpret_cast<const char*>(ptr), value_len)};
        }

        case binary::kStatusKeys: {
            uint32_t count = 0;
            if (!read_u32_be(ptr, end, count)) {
                return ErrorResp{"binary response: truncated keys count"};
            }
            std::vector<std::string> keys;
            keys.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint16_t key_len = 0;
                if (!read_u16_be(ptr, end, key_len)) {
                    return ErrorResp{"binary response: truncated key_len"};
                }
                if (ptr + key_len > end) {
                    return ErrorResp{"binary response: truncated key"};
                }
                keys.emplace_back(reinterpret_cast<const char*>(ptr), key_len);
                ptr += key_len;
            }
            return KeysResp{std::move(keys)};
        }

        case binary::kStatusError: {
            uint16_t msg_len = 0;
            if (!read_u16_be(ptr, end, msg_len)) {
                return ErrorResp{"binary response: truncated error msg_len"};
            }
            if (ptr + msg_len > end) {
                return ErrorResp{"binary response: truncated error message"};
            }
            return ErrorResp{std::string(reinterpret_cast<const char*>(ptr), msg_len)};
        }

        case binary::kStatusRedirect: {
            uint16_t addr_len = 0;
            if (!read_u16_be(ptr, end, addr_len)) {
                return ErrorResp{"binary response: truncated redirect addr_len"};
            }
            if (ptr + addr_len > end) {
                return ErrorResp{"binary response: truncated redirect address"};
            }
            return RedirectResp{std::string(reinterpret_cast<const char*>(ptr), addr_len)};
        }

        default:
            return ErrorResp{"binary response: unknown status 0x" +
                             std::string(1, "0123456789abcdef"[(status >> 4) & 0xF]) +
                             std::string(1, "0123456789abcdef"[status & 0xF])};
    }
}

} // namespace kv::network

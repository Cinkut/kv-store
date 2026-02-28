#pragma once

#include "network/protocol.hpp"

#include <cstdint>
#include <span>
#include <variant>
#include <vector>

namespace kv::network {

// ── Binary message type constants ─────────────────────────────────────────────

namespace binary {

// Request msg_type values.
inline constexpr uint8_t kMsgSet  = 0x01;
inline constexpr uint8_t kMsgGet  = 0x02;
inline constexpr uint8_t kMsgDel  = 0x03;
inline constexpr uint8_t kMsgKeys = 0x04;
inline constexpr uint8_t kMsgPing = 0x05;

// Response status values.
inline constexpr uint8_t kStatusOk       = 0x00;
inline constexpr uint8_t kStatusValue    = 0x01;
inline constexpr uint8_t kStatusNotFound = 0x02;
inline constexpr uint8_t kStatusDeleted  = 0x03;
inline constexpr uint8_t kStatusKeys     = 0x04;
inline constexpr uint8_t kStatusPong     = 0x05;
inline constexpr uint8_t kStatusError    = 0x10;
inline constexpr uint8_t kStatusRedirect = 0x20;

// Header size: msg_type/status (1 byte) + payload_length (4 bytes).
inline constexpr std::size_t kHeaderSize = 5;

} // namespace binary

// ── Auto-detection ────────────────────────────────────────────────────────────

// Returns true if the first byte indicates a binary-protocol connection.
// Binary: 0x00–0x1F, Text: 0x20–0x7F.
[[nodiscard]] constexpr bool is_binary_protocol(uint8_t first_byte) noexcept {
    return first_byte <= 0x1F;
}

// ── Binary protocol functions ─────────────────────────────────────────────────

// Parse a binary request payload (everything AFTER the 5-byte header).
// `msg_type` is the first byte from the header.
// `payload` is the payload bytes (payload_length bytes from the header).
//
// Returns a Command on success, or an ErrorResp on malformed input.
// Thread-safe: pure function, no shared state.
[[nodiscard]] std::variant<Command, ErrorResp> parse_binary_request(
    uint8_t msg_type, std::span<const uint8_t> payload);

// Serialize a Response into a binary wire-ready buffer.
// The returned buffer includes the 5-byte header (status + payload_length)
// followed by the payload bytes.
// Thread-safe: pure function, no shared state.
[[nodiscard]] std::vector<uint8_t> serialize_binary_response(const Response& response);

// ── Header helpers ────────────────────────────────────────────────────────────

// Read a 5-byte binary header, extracting msg_type and payload_length.
// Returns false if data is smaller than 5 bytes.
[[nodiscard]] bool read_binary_header(
    std::span<const uint8_t> data,
    uint8_t& msg_type_out,
    uint32_t& payload_length_out) noexcept;

// Build a binary request (header + payload) for a given Command.
// Used by kv-cli in binary mode.
[[nodiscard]] std::vector<uint8_t> serialize_binary_request(const Command& cmd);

// Parse a binary response header + payload into a Response.
// `status` is the first byte from the header.
// `payload` is the payload bytes.
[[nodiscard]] std::variant<Response, ErrorResp> parse_binary_response(
    uint8_t status, std::span<const uint8_t> payload);

} // namespace kv::network

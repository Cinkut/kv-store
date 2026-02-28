#pragma once

#include "network/protocol.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <string>
#include <variant>
#include <vector>

namespace kv::network {

// ── RESP parser (streaming, reads from socket) ───────────────────────────────

// Read one RESP request (array of bulk strings) from the socket and convert
// to a Command.  The `first_byte` is the already-consumed auto-detect byte.
// `buf` is a persistent read buffer shared across calls on the same connection.
//
// Expects: *N\r\n$len\r\nverb\r\n$len\r\narg\r\n...
// Also handles inline commands (single line terminated by \r\n).
//
// Returns Command on success, ErrorResp on parse/protocol error.
[[nodiscard]] boost::asio::awaitable<std::variant<Command, ErrorResp>>
parse_resp_request(boost::asio::ip::tcp::socket& socket, uint8_t first_byte,
                   std::string& buf);

// ── RESP serializer ──────────────────────────────────────────────────────────

// Serialize a Response into RESP wire format.
// Thread-safe: pure function, no shared state.
[[nodiscard]] std::string serialize_resp_response(const Response& response);

// ── RESP client-side helpers (for kv-cli) ────────────────────────────────────

// Serialize a Command into a RESP array request.
[[nodiscard]] std::string serialize_resp_request(const Command& cmd);

// Read one RESP response from the socket and convert to a Response.
[[nodiscard]] boost::asio::awaitable<std::variant<Response, ErrorResp>>
read_resp_response(boost::asio::ip::tcp::socket& socket);

} // namespace kv::network

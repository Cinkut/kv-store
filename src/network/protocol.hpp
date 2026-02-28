#pragma once

#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace kv {

// ── Commands ──────────────────────────────────────────────────────────────────
//
// Parsed representation of a single client command.  Each command type is a
// plain struct; the whole thing is wrapped in a std::variant so callers can
// std::visit over it without inheritance.

struct SetCmd {
    std::string key;
    std::string value;
};

struct GetCmd {
    std::string key;
};

struct DelCmd {
    std::string key;
};

struct KeysCmd {};

struct PingCmd {};

using Command = std::variant<SetCmd, GetCmd, DelCmd, KeysCmd, PingCmd>;

// ── Responses ─────────────────────────────────────────────────────────────────

struct OkResp {};
struct PongResp {};
struct NotFoundResp {};
struct DeletedResp {};

struct ValueResp {
    std::string value;
};

struct KeysResp {
    std::vector<std::string> keys;
};

struct ErrorResp {
    std::string message;
};

struct RedirectResp {
    std::string address; // "host:port" of the current leader
};

using Response =
    std::variant<OkResp, PongResp, NotFoundResp, DeletedResp, ValueResp, KeysResp, ErrorResp,
                 RedirectResp>;

// ── Protocol ──────────────────────────────────────────────────────────────────

namespace network {

// Stateless helper: parse one line (without the trailing '\n') into a Command.
// Returns ErrorResp-producing variant on malformed input, so callers can
// directly serialize the error back to the client.
//
// Thread-safe: pure function, no shared state.
[[nodiscard]] std::variant<Command, ErrorResp> parse_command(std::string_view line);

// Serialize a Response into a wire-ready string (always ends with '\n').
// Thread-safe: pure function, no shared state.
[[nodiscard]] std::string serialize_response(const Response& response);

} // namespace network
} // namespace kv

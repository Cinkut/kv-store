#include "network/protocol.hpp"

#include <sstream>
#include <string>

namespace kv::network {

// ── Helpers ───────────────────────────────────────────────────────────────────

namespace {

// Split `line` on the first space, returning {head, rest}.
// If there is no space, rest is empty.
std::pair<std::string_view, std::string_view> split_once(std::string_view line) {
    const auto pos = line.find(' ');
    if (pos == std::string_view::npos) {
        return {line, {}};
    }
    return {line.substr(0, pos), line.substr(pos + 1)};
}

} // namespace

// ── parse_command ─────────────────────────────────────────────────────────────

std::variant<Command, ErrorResp> parse_command(std::string_view line) {
    // Strip trailing '\r' so the parser is CRLF-tolerant.
    if (!line.empty() && line.back() == '\r') {
        line.remove_suffix(1);
    }

    if (line.empty()) {
        return ErrorResp{"empty command"};
    }

    auto [verb, rest] = split_once(line);

    // ── PING ──────────────────────────────────────────────────────────────────
    if (verb == "PING") {
        if (!rest.empty()) {
            return ErrorResp{"PING takes no arguments"};
        }
        return PingCmd{};
    }

    // ── KEYS ──────────────────────────────────────────────────────────────────
    if (verb == "KEYS") {
        if (!rest.empty()) {
            return ErrorResp{"KEYS takes no arguments"};
        }
        return KeysCmd{};
    }

    // ── GET key ───────────────────────────────────────────────────────────────
    if (verb == "GET") {
        if (rest.empty()) {
            return ErrorResp{"GET requires a key"};
        }
        // The key must be a single token (no embedded spaces allowed).
        auto [key, extra] = split_once(rest);
        if (!extra.empty()) {
            return ErrorResp{"GET takes exactly one argument"};
        }
        return GetCmd{std::string(key)};
    }

    // ── DEL key ───────────────────────────────────────────────────────────────
    if (verb == "DEL") {
        if (rest.empty()) {
            return ErrorResp{"DEL requires a key"};
        }
        auto [key, extra] = split_once(rest);
        if (!extra.empty()) {
            return ErrorResp{"DEL takes exactly one argument"};
        }
        return DelCmd{std::string(key)};
    }

    // ── SET key value ─────────────────────────────────────────────────────────
    //
    // The value is everything after "SET <key> "; it may contain spaces.
    if (verb == "SET") {
        if (rest.empty()) {
            return ErrorResp{"SET requires a key and a value"};
        }
        auto [key, value] = split_once(rest);
        if (key.empty()) {
            return ErrorResp{"SET: key must not be empty after tokenisation"};
        }
        if (value.empty()) {
            return ErrorResp{"SET requires a value"};
        }
        return SetCmd{std::string(key), std::string(value)};
    }

    return ErrorResp{"unknown command: " + std::string(verb)};
}

// ── serialize_response ────────────────────────────────────────────────────────

std::string serialize_response(const Response& response) {
    return std::visit(
        [](const auto& r) -> std::string {
            using T = std::decay_t<decltype(r)>;

            if constexpr (std::is_same_v<T, OkResp>) {
                return "OK\n";
            } else if constexpr (std::is_same_v<T, PongResp>) {
                return "PONG\n";
            } else if constexpr (std::is_same_v<T, NotFoundResp>) {
                return "NOT_FOUND\n";
            } else if constexpr (std::is_same_v<T, DeletedResp>) {
                return "DELETED\n";
            } else if constexpr (std::is_same_v<T, ValueResp>) {
                return "VALUE " + r.value + "\n";
            } else if constexpr (std::is_same_v<T, KeysResp>) {
                if (r.keys.empty()) {
                    return "KEYS\n";
                }
                std::string out = "KEYS";
                for (const auto& k : r.keys) {
                    out += ' ';
                    out += k;
                }
                out += '\n';
                return out;
            } else if constexpr (std::is_same_v<T, ErrorResp>) {
                return "ERROR " + r.message + "\n";
            } else if constexpr (std::is_same_v<T, RedirectResp>) {
                return "REDIRECT " + r.address + "\n";
            }
        },
        response);
}

} // namespace kv::network

#include "network/protocol.hpp"

#include <charconv>
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

    // ── ADDSERVER id host raft_port client_port ───────────────────────────────
    if (verb == "ADDSERVER") {
        if (rest.empty()) {
            return ErrorResp{"ADDSERVER requires: id host raft_port client_port"};
        }
        auto [id_tok, after_id] = split_once(rest);
        if (after_id.empty()) {
            return ErrorResp{"ADDSERVER requires: id host raft_port client_port"};
        }
        auto [host_tok, after_host] = split_once(after_id);
        if (after_host.empty()) {
            return ErrorResp{"ADDSERVER requires: id host raft_port client_port"};
        }
        auto [rport_tok, after_rport] = split_once(after_host);
        if (after_rport.empty()) {
            return ErrorResp{"ADDSERVER requires: id host raft_port client_port"};
        }
        auto [cport_tok, extra] = split_once(after_rport);
        if (!extra.empty()) {
            return ErrorResp{"ADDSERVER takes exactly 4 arguments"};
        }

        uint32_t id = 0;
        auto [p1, e1] = std::from_chars(id_tok.data(), id_tok.data() + id_tok.size(), id);
        if (e1 != std::errc{} || p1 != id_tok.data() + id_tok.size() || id == 0) {
            return ErrorResp{"ADDSERVER: invalid node id"};
        }

        uint16_t raft_port = 0;
        auto [p2, e2] = std::from_chars(rport_tok.data(), rport_tok.data() + rport_tok.size(), raft_port);
        if (e2 != std::errc{} || p2 != rport_tok.data() + rport_tok.size() || raft_port == 0) {
            return ErrorResp{"ADDSERVER: invalid raft_port"};
        }

        uint16_t client_port = 0;
        auto [p3, e3] = std::from_chars(cport_tok.data(), cport_tok.data() + cport_tok.size(), client_port);
        if (e3 != std::errc{} || p3 != cport_tok.data() + cport_tok.size() || client_port == 0) {
            return ErrorResp{"ADDSERVER: invalid client_port"};
        }

        return AddServerCmd{id, std::string(host_tok), raft_port, client_port};
    }

    // ── REMOVESERVER id ──────────────────────────────────────────────────────
    if (verb == "REMOVESERVER") {
        if (rest.empty()) {
            return ErrorResp{"REMOVESERVER requires a node id"};
        }
        auto [id_tok, extra] = split_once(rest);
        if (!extra.empty()) {
            return ErrorResp{"REMOVESERVER takes exactly one argument"};
        }

        uint32_t id = 0;
        auto [p1, e1] = std::from_chars(id_tok.data(), id_tok.data() + id_tok.size(), id);
        if (e1 != std::errc{} || p1 != id_tok.data() + id_tok.size() || id == 0) {
            return ErrorResp{"REMOVESERVER: invalid node id"};
        }

        return RemoveServerCmd{id};
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

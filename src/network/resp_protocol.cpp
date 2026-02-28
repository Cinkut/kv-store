#include "network/resp_protocol.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace kv::network {

namespace {

constexpr auto use_awaitable = boost::asio::as_tuple(boost::asio::use_awaitable);

// Read a CRLF-terminated line from the socket.  Returns the line content
// (without the trailing \r\n).  On error, returns nullopt via the bool.
boost::asio::awaitable<std::pair<std::string, bool>>
read_line(boost::asio::ip::tcp::socket& socket, std::string& buf) {
    auto [ec, n] = co_await boost::asio::async_read_until(
        socket, boost::asio::dynamic_buffer(buf), "\r\n", use_awaitable);

    if (ec) {
        co_return std::pair<std::string, bool>{"", false};
    }

    // n includes the \r\n
    std::string line = buf.substr(0, n - 2);
    buf.erase(0, n);
    co_return std::pair<std::string, bool>{std::move(line), true};
}

// Read exactly `count` bytes + trailing \r\n from the socket.
boost::asio::awaitable<std::pair<std::string, bool>>
read_bulk(boost::asio::ip::tcp::socket& socket, std::string& buf, std::size_t count) {
    const std::size_t need = count + 2; // data + \r\n

    // Ensure buf has enough data.
    while (buf.size() < need) {
        std::size_t before = buf.size();
        // Read more data.
        auto [ec, n] = co_await boost::asio::async_read_until(
            socket, boost::asio::dynamic_buffer(buf), "\r\n", use_awaitable);
        if (ec) {
            co_return std::pair<std::string, bool>{"", false};
        }
    }

    std::string data = buf.substr(0, count);
    buf.erase(0, need);
    co_return std::pair<std::string, bool>{std::move(data), true};
}

// Parse an integer from a string_view (used for array count and bulk length).
bool parse_int(std::string_view sv, int& out) {
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out);
    return ec == std::errc{} && ptr == sv.data() + sv.size();
}

// Map a verb + args into a Command.
std::variant<Command, ErrorResp>
map_command(const std::vector<std::string>& args) {
    if (args.empty()) {
        return ErrorResp{"ERR empty command"};
    }

    // Uppercase the verb for case-insensitive matching.
    std::string verb = args[0];
    std::transform(verb.begin(), verb.end(), verb.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });

    if (verb == "PING") {
        return PingCmd{};
    } else if (verb == "GET") {
        if (args.size() != 2) {
            return ErrorResp{"ERR wrong number of arguments for 'GET' command"};
        }
        return GetCmd{args[1]};
    } else if (verb == "SET") {
        if (args.size() != 3) {
            return ErrorResp{"ERR wrong number of arguments for 'SET' command"};
        }
        return SetCmd{args[1], args[2]};
    } else if (verb == "DEL") {
        if (args.size() != 2) {
            return ErrorResp{"ERR wrong number of arguments for 'DEL' command"};
        }
        return DelCmd{args[1]};
    } else if (verb == "KEYS") {
        // KEYS * — we ignore the pattern and return all keys.
        return KeysCmd{};
    } else if (verb == "COMMAND") {
        // Redis clients send COMMAND DOCS on connect — reply with OK.
        return PingCmd{};  // We'll respond with PONG, which is harmless.
    } else {
        return ErrorResp{"ERR unknown command '" + verb + "'"};
    }
}

} // anonymous namespace

// ── RESP parser ──────────────────────────────────────────────────────────────

boost::asio::awaitable<std::variant<Command, ErrorResp>>
parse_resp_request(boost::asio::ip::tcp::socket& socket, uint8_t first_byte,
                   std::string& buf) {

    if (first_byte != '*') {
        // Inline command: read until \r\n.  first_byte is the start.
        buf.push_back(static_cast<char>(first_byte));
        auto [line, ok] = co_await read_line(socket, buf);
        if (!ok) {
            co_return ErrorResp{"ERR connection closed during inline read"};
        }
        // Prepend first_byte to line.
        std::string full = std::string(1, static_cast<char>(first_byte)) + line;
        // Split by spaces.
        std::vector<std::string> parts;
        std::string token;
        for (char c : full) {
            if (c == ' ') {
                if (!token.empty()) {
                    parts.push_back(std::move(token));
                    token.clear();
                }
            } else {
                token.push_back(c);
            }
        }
        if (!token.empty()) {
            parts.push_back(std::move(token));
        }
        co_return map_command(parts);
    }

    // RESP array: *N\r\n — we already consumed '*', read rest of line.
    auto [count_line, ok1] = co_await read_line(socket, buf);
    if (!ok1) {
        co_return ErrorResp{"ERR connection closed during array count"};
    }

    int count = 0;
    if (!parse_int(count_line, count) || count < 0) {
        co_return ErrorResp{"ERR invalid array count"};
    }

    if (count == 0) {
        co_return ErrorResp{"ERR empty array"};
    }

    std::vector<std::string> args;
    args.reserve(static_cast<std::size_t>(count));

    for (int i = 0; i < count; ++i) {
        // Read $len\r\n
        auto [dollar_line, ok2] = co_await read_line(socket, buf);
        if (!ok2) {
            co_return ErrorResp{"ERR connection closed during bulk header"};
        }
        if (dollar_line.empty() || dollar_line[0] != '$') {
            co_return ErrorResp{"ERR expected bulk string, got: " + dollar_line};
        }

        int bulk_len = 0;
        if (!parse_int(std::string_view{dollar_line}.substr(1), bulk_len) || bulk_len < 0) {
            co_return ErrorResp{"ERR invalid bulk string length"};
        }

        // Read exactly bulk_len bytes + \r\n.
        auto [data, ok3] = co_await read_bulk(socket, buf, static_cast<std::size_t>(bulk_len));
        if (!ok3) {
            co_return ErrorResp{"ERR connection closed during bulk data"};
        }

        args.push_back(std::move(data));
    }

    co_return map_command(args);
}

// ── RESP serializer ──────────────────────────────────────────────────────────

std::string serialize_resp_response(const Response& response) {
    return std::visit(
        [](const auto& r) -> std::string {
            using T = std::decay_t<decltype(r)>;

            if constexpr (std::is_same_v<T, OkResp>) {
                return "+OK\r\n";
            } else if constexpr (std::is_same_v<T, PongResp>) {
                return "+PONG\r\n";
            } else if constexpr (std::is_same_v<T, ValueResp>) {
                return "$" + std::to_string(r.value.size()) + "\r\n" + r.value + "\r\n";
            } else if constexpr (std::is_same_v<T, NotFoundResp>) {
                return "$-1\r\n";  // null bulk string
            } else if constexpr (std::is_same_v<T, DeletedResp>) {
                return ":1\r\n";  // integer 1
            } else if constexpr (std::is_same_v<T, KeysResp>) {
                std::string out = "*" + std::to_string(r.keys.size()) + "\r\n";
                for (const auto& k : r.keys) {
                    out += "$" + std::to_string(k.size()) + "\r\n" + k + "\r\n";
                }
                return out;
            } else if constexpr (std::is_same_v<T, ErrorResp>) {
                return "-ERR " + r.message + "\r\n";
            } else if constexpr (std::is_same_v<T, RedirectResp>) {
                return "-MOVED 0 " + r.address + "\r\n";
            }
        },
        response);
}

// ── RESP client-side helpers ─────────────────────────────────────────────────

std::string serialize_resp_request(const Command& cmd) {
    return std::visit(
        [](const auto& c) -> std::string {
            using T = std::decay_t<decltype(c)>;

            auto bulk = [](const std::string& s) -> std::string {
                return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
            };

            if constexpr (std::is_same_v<T, PingCmd>) {
                return "*1\r\n$4\r\nPING\r\n";
            } else if constexpr (std::is_same_v<T, GetCmd>) {
                return "*2\r\n" + bulk("GET") + bulk(c.key);
            } else if constexpr (std::is_same_v<T, SetCmd>) {
                return "*3\r\n" + bulk("SET") + bulk(c.key) + bulk(c.value);
            } else if constexpr (std::is_same_v<T, DelCmd>) {
                return "*2\r\n" + bulk("DEL") + bulk(c.key);
            } else if constexpr (std::is_same_v<T, KeysCmd>) {
                return "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n";
            }
        },
        cmd);
}

boost::asio::awaitable<std::variant<Response, ErrorResp>>
read_resp_response(boost::asio::ip::tcp::socket& socket) {
    std::string buf;

    auto [line, ok] = co_await read_line(socket, buf);
    if (!ok) {
        co_return ErrorResp{"connection closed"};
    }

    if (line.empty()) {
        co_return ErrorResp{"empty response"};
    }

    char type = line[0];
    std::string_view payload{line.data() + 1, line.size() - 1};

    switch (type) {
        case '+': {
            // Simple string.
            std::string s{payload};
            if (s == "OK") co_return Response{OkResp{}};
            if (s == "PONG") co_return Response{PongResp{}};
            co_return Response{ValueResp{std::move(s)}};
        }
        case '-': {
            // Error string.
            std::string msg{payload};
            // Check for MOVED redirect: -MOVED 0 host:port
            if (msg.starts_with("MOVED ")) {
                // Extract address (after "MOVED slot ")
                auto space1 = msg.find(' ', 6);
                if (space1 != std::string::npos) {
                    co_return Response{RedirectResp{msg.substr(space1 + 1)}};
                }
            }
            // Strip "ERR " prefix if present.
            if (msg.starts_with("ERR ")) {
                msg = msg.substr(4);
            }
            co_return Response{ErrorResp{std::move(msg)}};
        }
        case ':': {
            // Integer.
            int val = 0;
            parse_int(payload, val);
            if (val >= 1) {
                co_return Response{DeletedResp{}};
            }
            co_return Response{NotFoundResp{}};
        }
        case '$': {
            // Bulk string.
            int len = 0;
            if (!parse_int(payload, len)) {
                co_return ErrorResp{"invalid bulk length"};
            }
            if (len < 0) {
                co_return Response{NotFoundResp{}};  // null bulk string
            }
            auto [data, ok2] = co_await read_bulk(socket, buf, static_cast<std::size_t>(len));
            if (!ok2) {
                co_return ErrorResp{"connection closed during bulk read"};
            }
            co_return Response{ValueResp{std::move(data)}};
        }
        case '*': {
            // Array.
            int count = 0;
            if (!parse_int(payload, count) || count < 0) {
                co_return ErrorResp{"invalid array count"};
            }
            std::vector<std::string> keys;
            keys.reserve(static_cast<std::size_t>(count));
            for (int i = 0; i < count; ++i) {
                auto [elem_line, ok3] = co_await read_line(socket, buf);
                if (!ok3) {
                    co_return ErrorResp{"connection closed during array element"};
                }
                if (elem_line.empty() || elem_line[0] != '$') {
                    co_return ErrorResp{"expected bulk string in array"};
                }
                int elem_len = 0;
                if (!parse_int(std::string_view{elem_line}.substr(1), elem_len) || elem_len < 0) {
                    co_return ErrorResp{"invalid bulk length in array"};
                }
                auto [elem_data, ok4] = co_await read_bulk(socket, buf,
                                                            static_cast<std::size_t>(elem_len));
                if (!ok4) {
                    co_return ErrorResp{"connection closed during array bulk read"};
                }
                keys.push_back(std::move(elem_data));
            }
            co_return Response{KeysResp{std::move(keys)}};
        }
        default:
            co_return ErrorResp{"unknown RESP type: " + std::string(1, type)};
    }
}

} // namespace kv::network

#include "common/logger.hpp"
#include "network/binary_protocol.hpp"
#include "network/protocol.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/program_options.hpp>
#include <boost/system/error_code.hpp>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

namespace po = boost::program_options;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;

namespace {

constexpr auto use_awaitable = asio::as_tuple(asio::use_awaitable);

// Format a Response as a human-readable string for display.
std::string format_response(const kv::Response& resp) {
    return std::visit(
        [](const auto& r) -> std::string {
            using T = std::decay_t<decltype(r)>;

            if constexpr (std::is_same_v<T, kv::OkResp>) {
                return "OK";
            } else if constexpr (std::is_same_v<T, kv::PongResp>) {
                return "PONG";
            } else if constexpr (std::is_same_v<T, kv::NotFoundResp>) {
                return "NOT_FOUND";
            } else if constexpr (std::is_same_v<T, kv::DeletedResp>) {
                return "DELETED";
            } else if constexpr (std::is_same_v<T, kv::ValueResp>) {
                return "VALUE " + r.value;
            } else if constexpr (std::is_same_v<T, kv::KeysResp>) {
                std::string out = "KEYS";
                for (const auto& k : r.keys) {
                    out += ' ';
                    out += k;
                }
                return out;
            } else if constexpr (std::is_same_v<T, kv::ErrorResp>) {
                return "ERROR " + r.message;
            } else if constexpr (std::is_same_v<T, kv::RedirectResp>) {
                return "REDIRECT " + r.address;
            }
        },
        resp);
}

} // anonymous namespace

// ── Text REPL coroutine ───────────────────────────────────────────────────────

asio::awaitable<void> repl_text(tcp::socket socket) {
    std::string recv_buf;
    recv_buf.reserve(512);

    std::string line;
    while (true) {
        fprintf(stdout, "> ");
        fflush(stdout);

        if (!std::getline(std::cin, line)) {
            fprintf(stdout, "\n");
            break;
        }

        if (line.empty()) {
            continue;
        }

        // Send as text protocol (newline-terminated).
        const std::string request = line + "\n";

        auto [wec, _] = co_await asio::async_write(
            socket, asio::buffer(request), use_awaitable);

        if (wec) {
            spdlog::error("kv-cli: send error: {}", wec.message());
            break;
        }

        // Receive response (newline-terminated).
        auto [rec, n] = co_await asio::async_read_until(
            socket, asio::dynamic_buffer(recv_buf), '\n', use_awaitable);

        if (rec) {
            if (rec == asio::error::eof) {
                fprintf(stdout, "Server disconnected.\n");
            } else {
                spdlog::error("kv-cli: recv error: {}", rec.message());
            }
            break;
        }

        // Print response (strip trailing '\n').
        std::string response = recv_buf.substr(0, n);
        recv_buf.erase(0, n);
        if (!response.empty() && response.back() == '\n') {
            response.pop_back();
        }
        fprintf(stdout, "%s\n", response.c_str());
    }
}

// ── Binary REPL coroutine ─────────────────────────────────────────────────────

asio::awaitable<void> repl_binary(tcp::socket socket) {
    using namespace kv;
    using namespace kv::network;

    std::string line;
    while (true) {
        fprintf(stdout, "> ");
        fflush(stdout);

        if (!std::getline(std::cin, line)) {
            fprintf(stdout, "\n");
            break;
        }

        if (line.empty()) {
            continue;
        }

        // Parse the text input into a Command.
        auto parse_result = parse_command(line);

        if (std::holds_alternative<ErrorResp>(parse_result)) {
            fprintf(stdout, "ERROR %s\n",
                    std::get<ErrorResp>(parse_result).message.c_str());
            continue;
        }

        const auto& cmd = std::get<Command>(parse_result);

        // Serialize to binary wire format.
        auto wire = serialize_binary_request(cmd);

        auto [wec, _] = co_await asio::async_write(
            socket, asio::buffer(wire), use_awaitable);

        if (wec) {
            spdlog::error("kv-cli: send error: {}", wec.message());
            break;
        }

        // Read the 5-byte response header.
        std::vector<uint8_t> header(binary::kHeaderSize);
        auto [hec, hn] = co_await asio::async_read(
            socket, asio::buffer(header), use_awaitable);

        if (hec) {
            if (hec == asio::error::eof) {
                fprintf(stdout, "Server disconnected.\n");
            } else {
                spdlog::error("kv-cli: recv header error: {}", hec.message());
            }
            break;
        }

        uint8_t status = 0;
        uint32_t payload_len = 0;
        if (!read_binary_header(header, status, payload_len)) {
            spdlog::error("kv-cli: invalid response header");
            break;
        }

        // Read the payload.
        std::vector<uint8_t> payload(payload_len);
        if (payload_len > 0) {
            auto [pec, pn] = co_await asio::async_read(
                socket, asio::buffer(payload), use_awaitable);

            if (pec) {
                spdlog::error("kv-cli: recv payload error: {}", pec.message());
                break;
            }
        }

        // Parse the binary response.
        auto resp_result = parse_binary_response(status, payload);

        if (std::holds_alternative<ErrorResp>(resp_result)) {
            // Outer parse error (malformed response from server).
            fprintf(stdout, "ERROR %s\n",
                    std::get<ErrorResp>(resp_result).message.c_str());
            continue;
        }

        const auto& resp = std::get<Response>(resp_result);
        fprintf(stdout, "%s\n", format_response(resp).c_str());
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    po::options_description desc("kv-cli options");
    desc.add_options()
        ("help,h",                                            "Show this help")
        ("host",   po::value<std::string>()->default_value("127.0.0.1"), "Server host")
        ("port,p", po::value<std::uint16_t>()->default_value(6379),      "Server port")
        ("binary,b",                                          "Use binary protocol")
        ("log-level,l", po::value<std::string>()->default_value("warn"), "Log level");

    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const po::error& e) {
        fprintf(stderr, "Argument error: %s\n", e.what());
        return 1;
    }

    if (vm.count("help")) {
        std::ostringstream oss;
        oss << desc;
        fprintf(stdout, "%s\n", oss.str().c_str());
        return 0;
    }

    const auto host      = vm["host"].as<std::string>();
    const auto port      = vm["port"].as<std::uint16_t>();
    const auto log_level = vm["log-level"].as<std::string>();
    const bool binary    = vm.count("binary") > 0;

    kv::init_default_logger(kv::parse_log_level(log_level));

    spdlog::debug("kv-cli connecting to {}:{} ({})", host, port,
                  binary ? "binary" : "text");

    try {
        asio::io_context ioc;
        tcp::resolver resolver{ioc};
        auto endpoints = resolver.resolve(host, std::to_string(port));

        tcp::socket socket{ioc};
        boost::system::error_code ec;
        asio::connect(socket, endpoints, ec);

        if (ec) {
            spdlog::error("kv-cli: failed to connect to {}:{} – {}", host, port, ec.message());
            return 1;
        }

        socket.set_option(tcp::no_delay(true));

        fprintf(stdout, "Connected to %s:%u (%s mode). "
                "Type commands (PING, SET k v, GET k, DEL k, KEYS). Ctrl+D to quit.\n",
                host.c_str(), port, binary ? "binary" : "text");

        if (binary) {
            asio::co_spawn(ioc, repl_binary(std::move(socket)), asio::detached);
        } else {
            asio::co_spawn(ioc, repl_text(std::move(socket)), asio::detached);
        }
        ioc.run();

    } catch (const std::exception& ex) {
        spdlog::error("kv-cli: exception: {}", ex.what());
        return 1;
    }

    return 0;
}

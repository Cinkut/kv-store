#include "common/logger.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
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

namespace po = boost::program_options;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;

// ── REPL coroutine ────────────────────────────────────────────────────────────

asio::awaitable<void> repl(tcp::socket socket) {
    constexpr auto use_awaitable = asio::as_tuple(asio::use_awaitable);

    std::string recv_buf;
    recv_buf.reserve(512);

    std::string line;
    while (true) {
        // Print prompt.
        fprintf(stdout, "> ");
        fflush(stdout);

        // Read a line from stdin (blocking – intentional for REPL simplicity).
        if (!std::getline(std::cin, line)) {
            // EOF on stdin – exit cleanly.
            fprintf(stdout, "\n");
            break;
        }

        if (line.empty()) {
            continue;
        }

        // Append newline for the server protocol.
        const std::string request = line + "\n";

        // Send to server.
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

// ── main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    po::options_description desc("kv-cli options");
    desc.add_options()
        ("help,h",                                            "Show this help")
        ("host",   po::value<std::string>()->default_value("127.0.0.1"), "Server host")
        ("port,p", po::value<std::uint16_t>()->default_value(6379),      "Server port")
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

    kv::init_default_logger(kv::parse_log_level(log_level));

    spdlog::debug("kv-cli connecting to {}:{}", host, port);

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

        fprintf(stdout, "Connected to %s:%u. Type commands (PING, SET k v, GET k, DEL k, KEYS). Ctrl+D to quit.\n",
                host.c_str(), port);

        // The REPL reads stdin synchronously, so we run it directly in a coroutine.
        asio::co_spawn(ioc, repl(std::move(socket)), asio::detached);
        ioc.run();

    } catch (const std::exception& ex) {
        spdlog::error("kv-cli: exception: {}", ex.what());
        return 1;
    }

    return 0;
}

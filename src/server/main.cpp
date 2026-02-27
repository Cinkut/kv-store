#include "common/logger.hpp"
#include "network/server.hpp"
#include "storage/storage.hpp"

#include <boost/program_options.hpp>
#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstdio>
#include <sstream>
#include <string>

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    // ── CLI argument parsing ───────────────────────────────────────────────────
    po::options_description desc("kv-server options");
    desc.add_options()
        ("help,h",                                       "Show this help message")
        ("host",   po::value<std::string>()->default_value("0.0.0.0"), "Bind host")
        ("port,p", po::value<std::uint16_t>()->default_value(6379),    "Bind port")
        ("log-level,l", po::value<std::string>()->default_value("info"), "Log level (trace/debug/info/warn/error)");

    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const po::error& e) {
        // Can't use spdlog yet – logger not initialised.
        fprintf(stderr, "Argument error: %s\n", e.what());
        return 1;
    }

    if (vm.count("help")) {
        // spdlog is not yet initialised here; use fprintf to stdout.
        std::ostringstream oss;
        oss << desc;
        fprintf(stdout, "%s\n", oss.str().c_str());
        return 0;
    }

    const auto host      = vm["host"].as<std::string>();
    const auto port      = vm["port"].as<std::uint16_t>();
    const auto log_level = vm["log-level"].as<std::string>();

    // ── Logging ───────────────────────────────────────────────────────────────
    kv::init_default_logger(kv::parse_log_level(log_level));
    auto logger = kv::make_node_logger(0, kv::parse_log_level(log_level));
    logger->info("kv-server starting – {}:{}", host, port);

    // ── Storage + Server ──────────────────────────────────────────────────────
    kv::Storage storage;
    kv::network::Server server{host, port, storage};

    server.run(); // blocks until SIGINT/SIGTERM

    logger->info("kv-server stopped");
    return 0;
}

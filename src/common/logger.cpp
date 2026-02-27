#include "common/logger.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace kv {

void init_default_logger(spdlog::level::level_enum level) {
    auto logger = spdlog::stdout_color_mt("kv");
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] %v");
    logger->set_level(level);
    spdlog::set_default_logger(logger);
}

std::shared_ptr<spdlog::logger> make_node_logger(
    uint32_t node_id,
    spdlog::level::level_enum level)
{
    const std::string name = "node-" + std::to_string(node_id);

    // Return existing logger if already created (idempotent).
    if (auto existing = spdlog::get(name)) {
        return existing;
    }

    auto logger = spdlog::stdout_color_mt(name);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] %v");
    logger->set_level(level);
    return logger;
}

spdlog::level::level_enum parse_log_level(const std::string& s) {
    if (s == "trace")    return spdlog::level::trace;
    if (s == "debug")    return spdlog::level::debug;
    if (s == "info")     return spdlog::level::info;
    if (s == "warn")     return spdlog::level::warn;
    if (s == "error")    return spdlog::level::err;
    if (s == "critical") return spdlog::level::critical;
    return spdlog::level::info;
}

} // namespace kv

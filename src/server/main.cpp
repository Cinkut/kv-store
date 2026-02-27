#include "common/logger.hpp"

#include <spdlog/spdlog.h>

int main(int /*argc*/, char* /*argv*/[]) {
    kv::init_default_logger(spdlog::level::debug);

    auto logger = kv::make_node_logger(1, spdlog::level::debug);
    logger->info("kv-server starting (stub)");
    logger->debug("Log level: debug");
    logger->warn("This is a warning example");

    return 0;
}

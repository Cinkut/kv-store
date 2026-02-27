#include "common/logger.hpp"

#include <spdlog/spdlog.h>

int main(int /*argc*/, char* /*argv*/[]) {
    kv::init_default_logger(spdlog::level::debug);

    auto logger = kv::make_node_logger(0, spdlog::level::debug);
    logger->info("kv-cli starting (stub)");

    return 0;
}

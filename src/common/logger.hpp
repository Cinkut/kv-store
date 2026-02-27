#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <spdlog/spdlog.h>

namespace kv {

// ── Node state (used for log color-coding) ────────────────────────────────────
enum class NodeState : uint8_t {
    Follower  = 0,
    Candidate = 1,
    Leader    = 2,
};

// ── Logger façade ─────────────────────────────────────────────────────────────

// Initialize the global default logger (for components that don't belong to a
// specific node: CLI, early startup messages, tests).
// Call once at program start before any logging.
void init_default_logger(spdlog::level::level_enum level = spdlog::level::info);

// Create (or retrieve if already exists) a per-node logger.
//   node_id  – numeric node ID embedded in every log line as [node-<id>]
//   level    – initial log level
// Returns a shared_ptr to the created logger.
std::shared_ptr<spdlog::logger> make_node_logger(
    uint32_t node_id,
    spdlog::level::level_enum level = spdlog::level::info);

// Parse a log-level string from CLI args ("trace", "debug", "info", …).
// Returns spdlog::level::info on unrecognised input.
spdlog::level::level_enum parse_log_level(const std::string& s);

} // namespace kv

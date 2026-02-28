#include "raft/state_machine.hpp"

#include <spdlog/spdlog.h>

namespace kv::raft {

StateMachine::StateMachine(Storage& storage,
                           std::shared_ptr<spdlog::logger> logger)
    : storage_(storage)
    , logger_(std::move(logger))
{
}

void StateMachine::apply(const LogEntry& entry) {
    if (entry.index() <= last_applied_) {
        return;  // Already applied.
    }

    const auto& cmd = entry.command();
    switch (cmd.type()) {
        case CMD_SET:
            storage_.set(cmd.key(), cmd.value());
            if (logger_) {
                logger_->debug("[apply] SET {} at index {}", cmd.key(), entry.index());
            }
            break;

        case CMD_DEL:
            storage_.del(cmd.key());
            if (logger_) {
                logger_->debug("[apply] DEL {} at index {}", cmd.key(), entry.index());
            }
            break;

        case CMD_NOOP:
            if (logger_) {
                logger_->debug("[apply] NOOP at index {}", entry.index());
            }
            break;

        default:
            if (logger_) {
                logger_->warn("[apply] Unknown command type {} at index {}",
                              static_cast<int>(cmd.type()), entry.index());
            }
            break;
    }

    last_applied_ = entry.index();
}

} // namespace kv::raft

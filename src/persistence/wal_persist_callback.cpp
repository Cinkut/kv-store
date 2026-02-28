#include "persistence/wal_persist_callback.hpp"

namespace kv::persistence {

WalPersistCallback::WalPersistCallback(WAL& wal,
                                       std::shared_ptr<spdlog::logger> logger)
    : wal_(wal)
    , logger_(std::move(logger))
{}

void WalPersistCallback::persist_metadata(uint64_t term, int32_t voted_for) {
    MetadataRecord rec;
    rec.term = term;
    rec.voted_for = voted_for;

    auto ec = wal_.append_metadata(rec);
    if (ec) {
        logger_->error("WalPersistCallback: failed to persist metadata "
                       "(term={}, voted_for={}): {}",
                       term, voted_for, ec.message());
    } else {
        logger_->debug("WalPersistCallback: persisted metadata "
                       "(term={}, voted_for={})",
                       term, voted_for);
    }
}

void WalPersistCallback::persist_entry(const kv::raft::LogEntry& entry) {
    LogEntryRecord rec;
    rec.term = entry.term();
    rec.index = entry.index();

    if (entry.has_command()) {
        rec.cmd_type = static_cast<uint8_t>(entry.command().type());
        rec.key = entry.command().key();
        rec.value = entry.command().value();
    } else {
        rec.cmd_type = 0; // NOOP
    }

    auto ec = wal_.append_entry(rec);
    if (ec) {
        logger_->error("WalPersistCallback: failed to persist entry "
                       "(index={}, term={}): {}",
                       rec.index, rec.term, ec.message());
    } else {
        logger_->debug("WalPersistCallback: persisted entry "
                       "(index={}, term={})",
                       rec.index, rec.term);
    }
}

} // namespace kv::persistence

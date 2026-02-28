#include "raft/raft_log.hpp"

#include <algorithm>

namespace kv::raft {

// ── Queries ──────────────────────────────────────────────────────────────────

uint64_t RaftLog::last_index() const noexcept {
    if (entries_.empty()) return offset_;
    return offset_ + static_cast<uint64_t>(entries_.size());
}

uint64_t RaftLog::last_term() const noexcept {
    if (entries_.empty()) return 0;
    return entries_.back().term();
}

std::optional<uint64_t> RaftLog::term_at(uint64_t index) const noexcept {
    if (index == 0) return 0;  // sentinel: term 0 at index 0
    auto pos = to_pos(index);
    if (!pos) return std::nullopt;
    return entries_[*pos].term();
}

const LogEntry* RaftLog::entry_at(uint64_t index) const noexcept {
    auto pos = to_pos(index);
    if (!pos) return nullptr;
    return &entries_[*pos];
}

std::size_t RaftLog::size() const noexcept {
    return entries_.size();
}

bool RaftLog::empty() const noexcept {
    return entries_.empty();
}

// ── Mutations ────────────────────────────────────────────────────────────────

void RaftLog::append(LogEntry entry) {
    entries_.push_back(std::move(entry));
}

bool RaftLog::try_append(uint64_t prev_log_index,
                          uint64_t prev_log_term,
                          const std::vector<LogEntry>& entries) {
    // Consistency check: entry at prev_log_index must have matching term.
    // Special case: prev_log_index == 0 always succeeds (empty prefix).
    if (prev_log_index > 0) {
        auto t = term_at(prev_log_index);
        if (!t || *t != prev_log_term) {
            return false;
        }
    }

    // Walk through incoming entries; handle conflicts.
    uint64_t insert_index = prev_log_index + 1;
    for (std::size_t i = 0; i < entries.size(); ++i, ++insert_index) {
        auto existing_term = term_at(insert_index);
        if (existing_term && *existing_term == entries[i].term()) {
            // Entry already present and matches — skip.
            continue;
        }
        if (existing_term) {
            // Conflict: truncate from this point.
            truncate_after(insert_index - 1);
        }
        // Append remaining entries.
        for (std::size_t j = i; j < entries.size(); ++j) {
            entries_.push_back(entries[j]);
        }
        break;
    }

    return true;
}

void RaftLog::truncate_after(uint64_t last_kept_index) {
    if (last_kept_index >= last_index()) return;
    if (last_kept_index <= offset_) {
        entries_.clear();
        return;
    }
    auto pos = to_pos(last_kept_index);
    if (pos) {
        entries_.resize(*pos + 1);
    }
}

std::vector<LogEntry> RaftLog::entries_from(uint64_t from_index,
                                             uint64_t to_index) const {
    std::vector<LogEntry> result;
    if (from_index > to_index) return result;
    auto from_pos = to_pos(from_index);
    auto to_pos_v = to_pos(to_index);
    if (!from_pos || !to_pos_v) return result;
    result.reserve(*to_pos_v - *from_pos + 1);
    for (std::size_t i = *from_pos; i <= *to_pos_v; ++i) {
        result.push_back(entries_[i]);
    }
    return result;
}

std::vector<LogEntry> RaftLog::entries_from(uint64_t from_index) const {
    if (empty()) return {};
    return entries_from(from_index, last_index());
}

// ── Private ──────────────────────────────────────────────────────────────────

std::optional<std::size_t> RaftLog::to_pos(uint64_t index) const noexcept {
    if (index <= offset_ || index > last_index()) return std::nullopt;
    return static_cast<std::size_t>(index - offset_ - 1);
}

} // namespace kv::raft

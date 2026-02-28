#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "raft.pb.h"

namespace kv::raft {

// ── RaftLog ──────────────────────────────────────────────────────────────────
//
// In-memory replicated log.  Indices are 1-based (Raft convention).
// Index 0 is a sentinel and never stored; last_index() == 0 means empty log.
//
// NOT thread-safe – all access must happen on the Raft strand.

class RaftLog {
public:
    RaftLog() = default;

    // ── Queries ──────────────────────────────────────────────────────────────

    // Index of the last entry (0 = empty log).
    [[nodiscard]] uint64_t last_index() const noexcept;

    // Term of the last entry (0 = empty log).
    [[nodiscard]] uint64_t last_term() const noexcept;

    // Term of the entry at `index`.  Returns nullopt if out of range.
    [[nodiscard]] std::optional<uint64_t> term_at(uint64_t index) const noexcept;

    // Entry at `index`.  Returns nullptr if out of range.
    [[nodiscard]] const LogEntry* entry_at(uint64_t index) const noexcept;

    // Number of entries in the log.
    [[nodiscard]] std::size_t size() const noexcept;

    // True if the log is empty.
    [[nodiscard]] bool empty() const noexcept;

    // ── Mutations ────────────────────────────────────────────────────────────

    // Append a single entry.  The entry's index field MUST equal last_index()+1.
    void append(LogEntry entry);

    // Append entries received from a leader.
    //   prev_log_index – index immediately before the first new entry
    //   prev_log_term  – expected term at prev_log_index
    //   entries        – entries to append (may overlap existing tail)
    //
    // Returns false if the consistency check fails (no entry at prev_log_index
    // with matching term).  On conflict the log is truncated at the first
    // divergence point and the remaining entries are appended.
    [[nodiscard]] bool try_append(uint64_t prev_log_index,
                                  uint64_t prev_log_term,
                                  const std::vector<LogEntry>& entries);

    // Remove all entries with index > `last_kept_index`.
    void truncate_after(uint64_t last_kept_index);

    // Discard all entries with index <= `new_offset`.
    // After this call, offset_ == new_offset, and only entries after new_offset
    // remain.  Used after snapshot installation to trim prefix.
    // `snapshot_last_term` is the term of the last included entry in the
    // snapshot, used for term_at(offset_) and last_term() when log is empty.
    void truncate_prefix(uint64_t new_offset, uint64_t snapshot_last_term);

    // Return entries in the half-open range [from_index, to_index].
    // Both bounds are inclusive.  Returns empty vector if range is invalid.
    [[nodiscard]] std::vector<LogEntry> entries_from(uint64_t from_index,
                                                     uint64_t to_index) const;

    // Return all entries from from_index to end.
    [[nodiscard]] std::vector<LogEntry> entries_from(uint64_t from_index) const;

    // ── Snapshot-aware queries ───────────────────────────────────────────────

    // The log offset (entries with index <= offset have been snapshotted).
    [[nodiscard]] uint64_t offset() const noexcept { return offset_; }

    // The term of the snapshot's last included entry (0 if no snapshot).
    [[nodiscard]] uint64_t snapshot_last_term() const noexcept { return snapshot_last_term_; }

private:
    // entries_[0] corresponds to log index (offset_ + 1).
    // With no snapshot truncation, offset_ == 0 and entries_[i] has index i+1.
    std::vector<LogEntry> entries_;
    uint64_t offset_ = 0;            // entries with index <= offset_ are snapshotted
    uint64_t snapshot_last_term_ = 0; // term of the entry at offset_ (from snapshot)

    // Convert Raft 1-based index to vector position.  Returns nullopt if OOB.
    [[nodiscard]] std::optional<std::size_t> to_pos(uint64_t index) const noexcept;
};

} // namespace kv::raft

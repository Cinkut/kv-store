#pragma once

#include "storage/storage_engine.hpp"

#include <shared_mutex>
#include <unordered_map>

namespace kv {

// ── MemoryStorage ────────────────────────────────────────────────────────────
//
// Thread-safe key-value store backed by std::unordered_map.
//
// Concurrency model:
//   - get() / keys() / size() / snapshot() acquire a shared (read) lock.
//   - set() / del() / clear() acquire an exclusive (write) lock.
//   Multiple concurrent readers are allowed; writers are exclusive.

class MemoryStorage final : public StorageEngine {
public:
    MemoryStorage() = default;

    // Not copyable – copies of a live store would silently race.
    MemoryStorage(const MemoryStorage&)            = delete;
    MemoryStorage& operator=(const MemoryStorage&) = delete;

    // Movable (useful in tests; move is not thread-safe, caller must ensure
    // no concurrent access while moving).
    MemoryStorage(MemoryStorage&&)            = default;
    MemoryStorage& operator=(MemoryStorage&&) = default;

    ~MemoryStorage() override = default;

    [[nodiscard]] std::optional<std::string> get(std::string_view key) const override;
    void set(std::string key, std::string value) override;
    bool del(std::string_view key) override;
    [[nodiscard]] std::vector<std::string> keys() const override;
    [[nodiscard]] std::size_t size() const override;
    [[nodiscard]] std::unordered_map<std::string, std::string> snapshot() const override;
    void clear() override;

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> map_;
};

// Backward-compatible alias – existing code that uses kv::Storage will
// continue to compile.  New code should use StorageEngine& or MemoryStorage.
using Storage = MemoryStorage;

} // namespace kv

#pragma once

#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace kv {

// Thread-safe key-value store backed by std::unordered_map.
//
// Concurrency model:
//   - get() / keys() / size() / snapshot() acquire a shared (read) lock.
//   - set() / del() / clear() acquire an exclusive (write) lock.
//   Multiple concurrent readers are allowed; writers are exclusive.
class Storage {
public:
    Storage() = default;

    // Not copyable – copies of a live store would silently race.
    Storage(const Storage&)            = delete;
    Storage& operator=(const Storage&) = delete;

    // Movable (useful in tests; move is not thread-safe, caller must ensure
    // no concurrent access while moving).
    Storage(Storage&&)            = default;
    Storage& operator=(Storage&&) = default;

    // Returns the value for `key`, or std::nullopt if not present.
    [[nodiscard]] std::optional<std::string> get(std::string_view key) const;

    // Inserts or overwrites `key` with `value`.
    void set(std::string key, std::string value);

    // Removes `key`. Returns true if the key existed, false otherwise.
    bool del(std::string_view key);

    // Returns a snapshot of all keys (order is unspecified).
    [[nodiscard]] std::vector<std::string> keys() const;

    // Returns the number of stored key-value pairs.
    [[nodiscard]] std::size_t size() const;

    // Returns a full copy of the current state (used for snapshotting).
    [[nodiscard]] std::unordered_map<std::string, std::string> snapshot() const;

    // Removes all entries.
    void clear();

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> map_;
};

} // namespace kv

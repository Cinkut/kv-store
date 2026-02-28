#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace kv {

// ── StorageEngine ────────────────────────────────────────────────────────────
//
// Abstract interface for a key-value storage backend.
//
// Implementations must be thread-safe: multiple concurrent readers are allowed,
// writers are exclusive.  The concrete backend (in-memory hash map, RocksDB,
// etc.) is selected at startup.
//
// All methods use the same signatures as the original Storage class so that
// existing consumers (Session, StateMachine, SnapshotIOImpl) can switch to
// this interface with minimal changes.

class StorageEngine {
public:
    virtual ~StorageEngine() = default;

    // Returns the value for `key`, or std::nullopt if not present.
    [[nodiscard]] virtual std::optional<std::string> get(std::string_view key) const = 0;

    // Inserts or overwrites `key` with `value`.
    virtual void set(std::string key, std::string value) = 0;

    // Removes `key`.  Returns true if the key existed, false otherwise.
    virtual bool del(std::string_view key) = 0;

    // Returns all keys (order is unspecified).
    [[nodiscard]] virtual std::vector<std::string> keys() const = 0;

    // Returns the number of stored key-value pairs.
    [[nodiscard]] virtual std::size_t size() const = 0;

    // Returns a full copy of the current state (used for snapshotting).
    [[nodiscard]] virtual std::unordered_map<std::string, std::string> snapshot() const = 0;

    // Removes all entries.
    virtual void clear() = 0;
};

} // namespace kv

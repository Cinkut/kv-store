#pragma once

#include "storage/storage_engine.hpp"

#include <filesystem>
#include <memory>
#include <string>

namespace rocksdb {
class DB;
} // namespace rocksdb

namespace kv {

// ── RocksDBStorage ──────────────────────────────────────────────────────────
//
// Persistent key-value storage backed by RocksDB.
//
// Thread safety is delegated to RocksDB itself: RocksDB::Get/Put/Delete are
// safe for concurrent use from multiple threads.  The snapshot()/clear()
// methods acquire a RocksDB snapshot or iterate the full keyspace.
//
// The database directory is created on construction; the destructor closes
// the database cleanly.

class RocksDBStorage final : public StorageEngine {
public:
    // Opens (or creates) a RocksDB database at `db_path`.
    // Throws std::runtime_error if the database cannot be opened.
    explicit RocksDBStorage(const std::filesystem::path& db_path);

    ~RocksDBStorage() override;

    // Not copyable or movable – RocksDB owns internal state.
    RocksDBStorage(const RocksDBStorage&)            = delete;
    RocksDBStorage& operator=(const RocksDBStorage&) = delete;
    RocksDBStorage(RocksDBStorage&&)                 = delete;
    RocksDBStorage& operator=(RocksDBStorage&&)      = delete;

    [[nodiscard]] std::optional<std::string> get(std::string_view key) const override;
    void set(std::string key, std::string value) override;
    bool del(std::string_view key) override;
    [[nodiscard]] std::vector<std::string> keys() const override;
    [[nodiscard]] std::size_t size() const override;
    [[nodiscard]] std::unordered_map<std::string, std::string> snapshot() const override;
    void clear() override;

private:
    std::unique_ptr<rocksdb::DB> db_;
};

} // namespace kv

#include "storage/rocksdb_storage.hpp"

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <spdlog/spdlog.h>

#include <stdexcept>

namespace kv {

RocksDBStorage::RocksDBStorage(const std::filesystem::path& db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;

    // Optimise for small-to-medium working sets typical of a KV store.
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    rocksdb::DB* raw_db = nullptr;
    auto status = rocksdb::DB::Open(options, db_path.string(), &raw_db);
    if (!status.ok()) {
        throw std::runtime_error(
            "Failed to open RocksDB at " + db_path.string() + ": " +
            status.ToString());
    }
    db_.reset(raw_db);
    spdlog::info("RocksDB opened at {}", db_path.string());
}

RocksDBStorage::~RocksDBStorage() {
    if (db_) {
        spdlog::info("Closing RocksDB");
    }
    // unique_ptr<rocksdb::DB> destructor calls delete, which closes the DB.
}

std::optional<std::string> RocksDBStorage::get(std::string_view key) const {
    std::string value;
    auto status = db_->Get(
        rocksdb::ReadOptions{}, rocksdb::Slice{key.data(), key.size()}, &value);
    if (status.IsNotFound()) {
        return std::nullopt;
    }
    if (!status.ok()) {
        spdlog::error("RocksDB Get failed: {}", status.ToString());
        return std::nullopt;
    }
    return value;
}

void RocksDBStorage::set(std::string key, std::string value) {
    auto status = db_->Put(rocksdb::WriteOptions{}, key, value);
    if (!status.ok()) {
        spdlog::error("RocksDB Put failed: {}", status.ToString());
    }
}

bool RocksDBStorage::del(std::string_view key) {
    // Check existence first — RocksDB Delete succeeds even if key missing.
    std::string existing;
    auto get_status = db_->Get(
        rocksdb::ReadOptions{}, rocksdb::Slice{key.data(), key.size()},
        &existing);
    if (get_status.IsNotFound()) {
        return false;
    }

    auto status = db_->Delete(
        rocksdb::WriteOptions{}, rocksdb::Slice{key.data(), key.size()});
    if (!status.ok()) {
        spdlog::error("RocksDB Delete failed: {}", status.ToString());
        return false;
    }
    return true;
}

std::vector<std::string> RocksDBStorage::keys() const {
    std::vector<std::string> result;
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(rocksdb::ReadOptions{}));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        result.emplace_back(it->key().ToString());
    }
    return result;
}

std::size_t RocksDBStorage::size() const {
    std::size_t count = 0;
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(rocksdb::ReadOptions{}));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ++count;
    }
    return count;
}

std::unordered_map<std::string, std::string> RocksDBStorage::snapshot() const {
    std::unordered_map<std::string, std::string> result;
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(rocksdb::ReadOptions{}));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        result.emplace(it->key().ToString(), it->value().ToString());
    }
    return result;
}

void RocksDBStorage::clear() {
    // Delete all keys via a WriteBatch.
    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(rocksdb::ReadOptions{}));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        batch.Delete(it->key());
    }
    auto status = db_->Write(rocksdb::WriteOptions{}, &batch);
    if (!status.ok()) {
        spdlog::error("RocksDB clear() failed: {}", status.ToString());
    }
}

} // namespace kv

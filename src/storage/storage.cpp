#include "storage/storage.hpp"

#include <mutex>
#include <shared_mutex>

namespace kv {

std::optional<std::string> Storage::get(std::string_view key) const {
    std::shared_lock lock(mutex_);
    // std::unordered_map supports heterogeneous lookup via find(string_view)
    // only with a transparent hash. We use a local string for now – it's a
    // single allocation on the hot path, acceptable for correctness first.
    auto it = map_.find(std::string(key));
    if (it == map_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void Storage::set(std::string key, std::string value) {
    std::unique_lock lock(mutex_);
    map_.insert_or_assign(std::move(key), std::move(value));
}

bool Storage::del(std::string_view key) {
    std::unique_lock lock(mutex_);
    return map_.erase(std::string(key)) > 0;
}

std::vector<std::string> Storage::keys() const {
    std::shared_lock lock(mutex_);
    std::vector<std::string> result;
    result.reserve(map_.size());
    for (const auto& [k, _] : map_) {
        result.push_back(k);
    }
    return result;
}

std::size_t Storage::size() const {
    std::shared_lock lock(mutex_);
    return map_.size();
}

std::unordered_map<std::string, std::string> Storage::snapshot() const {
    std::shared_lock lock(mutex_);
    return map_; // full copy under read lock
}

void Storage::clear() {
    std::unique_lock lock(mutex_);
    map_.clear();
}

} // namespace kv

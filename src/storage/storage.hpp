#pragma once

#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kv {

class Storage {
public:
    Storage() = default;

    [[nodiscard]] std::optional<std::string> get(std::string_view key) const;
    void set(std::string key, std::string value);
    bool del(std::string_view key);
    [[nodiscard]] std::vector<std::string> keys() const;
    [[nodiscard]] std::size_t size() const;

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> map_;
};

} // namespace kv

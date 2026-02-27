# C++20 Coding Guidelines

These rules ensure consistency across the entire codebase. Follow them strictly.

---

## 1. Language Standard

- **C++20** (`-std=c++20`).
- Use C++20 features where they improve clarity. Do not use features just because they exist.
- GCC 15 is the target compiler. Do not use MSVC or Clang-specific extensions.

### C++20 Features to Use

| Feature             | Where / How                                                         |
|---------------------|---------------------------------------------------------------------|
| `co_await`          | All Boost.Asio async operations. Return `awaitable<T>`.            |
| `co_return`         | Return values from coroutines.                                      |
| Concepts            | Constrain template parameters (e.g., `Serializable`, `Transport`). |
| `std::span`         | Non-owning views over contiguous data (WAL buffers, protobuf bytes).|
| `std::format`       | String formatting (GCC 15 supports it). Prefer over `fmt::format`. |
| Designated init     | `LogEntry{.term = 1, .index = 5, .command = cmd}`                  |
| `[[nodiscard]]`     | On functions where ignoring the return value is likely a bug.       |
| `[[likely]]`/`[[unlikely]]` | In hot paths (e.g., AppendEntries success path is `[[likely]]`). |
| Ranges              | Prefer `std::ranges::find`, `std::views::filter` over raw loops where readable. |
| `std::jthread`      | Thread pool threads – automatic join on destruction.                |
| Structured bindings | `auto [key, value] = *map.find(k);`                                |

### Features to Avoid

| Feature              | Reason                                                    |
|----------------------|-----------------------------------------------------------|
| `std::expected`      | C++23. Use `std::optional<T>` + spdlog error instead.     |
| Modules (`import`)   | GCC 15 support is incomplete. Stick with `#include`.      |
| `std::print`         | C++23. Use spdlog for all output.                         |

---

## 2. Naming Conventions

```
namespace kv                    // lowercase
namespace kv::raft              // nested, lowercase

class RaftNode                  // PascalCase for types
struct LogEntry                 // PascalCase for types
enum class NodeState            // PascalCase for types

void handle_request()           // snake_case for functions
bool is_leader() const          // snake_case for functions

int current_term_;              // snake_case with trailing underscore for private members
int commit_index_;              // snake_case with trailing underscore for private members

static constexpr int kMaxRetries = 5;     // k-prefix PascalCase for constants
static constexpr auto kHeartbeatInterval  // k-prefix PascalCase for constants
    = std::chrono::milliseconds{50};

template <typename T>           // Single letter for simple templates
template <Serializable Msg>     // Concept-constrained where applicable

// File names: snake_case.hpp, snake_case.cpp
raft_node.hpp
raft_node.cpp
```

---

## 3. File Organization

### Headers (`.hpp`)

```cpp
#pragma once

#include <system_headers>       // 1. Standard library
#include <boost/asio.hpp>       // 2. Third-party
#include "project/header.hpp"   // 3. Project headers (relative to src/)

namespace kv {

class Foo {
public:
    // Public interface only. Keep it minimal.
    explicit Foo(int x);        // Always explicit for single-arg constructors
    [[nodiscard]] int value() const noexcept;

private:
    int value_;                 // Data members at the bottom
};

} // namespace kv
```

### Source files (`.cpp`)

```cpp
#include "storage/storage.hpp"  // Own header first (catches missing includes)

#include <unordered_map>        // Then std
#include <shared_mutex>

#include <spdlog/spdlog.h>      // Then third-party

namespace kv {

// Implementation here

} // namespace kv
```

### Rules
- **One class per file** (with exceptions for small helper types).
- Headers expose **only the public interface**. Implementation in `.cpp`.
- No `using namespace` in headers. Allowed in `.cpp` files only for `boost::asio` (it's pervasive).

---

## 4. Error Handling

### Strategy by Layer

| Layer            | Error strategy                                           |
|------------------|----------------------------------------------------------|
| Storage          | `std::optional` for lookups. Exceptions never thrown.    |
| Protocol parser  | Return `std::optional<Command>`. Log malformed input.    |
| Network (Asio)   | `boost::system::error_code`. Log + close connection.     |
| Raft             | Log errors. Step down to Follower on inconsistency.      |
| WAL / Snapshot   | `std::error_code` return. Fatal errors → log + `std::terminate`. |

### Rules
- **No exceptions in hot paths**. Use `std::optional`, error codes, or status enums.
- **Never silently ignore errors**. Every error path must log via spdlog.
- **Fatal errors** (corrupt WAL, out of disk): log at `critical` level, then terminate. Do not attempt recovery from unrecoverable state.
- **Network errors** (connection reset, timeout): log at `warn` level, close connection, continue serving.

```cpp
// Good: explicit error handling
auto result = storage.get(key);
if (!result) {
    spdlog::debug("Key not found: {}", key);
    co_return Response::not_found();
}

// Bad: throwing for expected conditions
try {
    auto val = storage.at(key);  // throws if missing – don't do this
} catch (...) { ... }
```

---

## 5. Async Patterns (Boost.Asio Coroutines)

### Coroutine Signature

```cpp
// Every async operation returns awaitable<T>
boost::asio::awaitable<void> Session::run() {
    auto [ec, bytes] = co_await async_read_until(
        socket_, buffer_, '\n',
        as_tuple(use_awaitable)   // ALWAYS use as_tuple to get error_code
    );
    if (ec) {
        spdlog::warn("Read error: {}", ec.message());
        co_return;
    }
    // process...
}
```

### Rules

1. **Always use `as_tuple(use_awaitable)`** to receive `error_code` instead of exceptions. Exceptions from async ops are expensive and hard to debug.
2. **Never block in a coroutine**. No `std::mutex::lock()`, no `sleep()`. Use `co_await steady_timer.async_wait(...)` for delays.
3. **Strand for shared state**: Raft state machine runs on a dedicated strand. All access to Raft state is dispatched via `co_spawn(strand_, ...)` or `dispatch(strand_, ...)`.
4. **Cancellation**: Use `boost::asio::cancellation_signal` or cancel timers explicitly. Do not destroy timers from outside their strand.

### Thread Model

```
Main thread
  ├── io_context.run()  ─── Thread pool (hardware_concurrency threads)
  │     ├── Client sessions (one coroutine per TCP connection)
  │     ├── Raft strand (one strand, all Raft state here)
  │     └── Peer client coroutines
  └── signal_set (SIGINT/SIGTERM → graceful shutdown)
```

---

## 6. Memory & Performance

- **No raw `new`/`delete`**. Use `std::unique_ptr`, `std::shared_ptr`, or stack allocation.
- **Prefer `std::string_view`** for read-only string parameters. Never store a `string_view` that outlives its source.
- **Prefer `std::span<const std::byte>`** for binary buffers.
- **Move semantics**: Accept large objects by value and `std::move` into storage. Don't pass by `const&` and then copy internally.
- **Reserve containers**: If the size is known or estimable, `reserve()` before filling.
- **Avoid `shared_ptr` in hot paths**. Use `unique_ptr` or raw references with clear ownership.

```cpp
// Good: move into storage
void RaftLog::append(LogEntry entry) {   // by value
    entries_.push_back(std::move(entry)); // move in
}

// Good: string_view for lookups
std::optional<std::string> Storage::get(std::string_view key) const {
    std::shared_lock lock(mutex_);
    auto it = map_.find(key);  // C++20: heterogeneous lookup works if map supports it
    if (it == map_.end()) return std::nullopt;
    return it->second;
}
```

---

## 7. Logging (spdlog)

### Setup

```cpp
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

// On startup, create a per-node logger:
auto logger = spdlog::stdout_color_mt("node-1");
logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] %v");
logger->set_level(spdlog::level::debug);
```

### Log Levels

| Level    | Use for                                                |
|----------|--------------------------------------------------------|
| trace    | Byte-level data: raw packets, buffer contents          |
| debug    | Per-request flow: "Received GET foo", "Sending AppendEntries" |
| info     | State changes: "Became Leader for term 5", "Node 2 connected" |
| warn     | Recoverable issues: connection reset, vote rejected    |
| error    | Unexpected failures: WAL write failed, protobuf parse error |
| critical | Fatal: corrupt data, unrecoverable state → terminate   |

### Rules
- **Every state transition must be logged at `info`**: Follower→Candidate, Candidate→Leader, *→Follower.
- **Every RPC must be logged at `debug`**: what was sent, what was received.
- **Never log sensitive data** (values stored in KV — they could be passwords).
- **Include context**: term, node id, peer id in every Raft log message.

```cpp
spdlog::info("[term={}] Became Candidate, requesting votes", current_term_);
spdlog::debug("[term={}] Sending RequestVote to node {}", current_term_, peer_id);
spdlog::warn("[term={}] Vote rejected by node {} (already voted for {})", current_term_, peer_id, voted_for);
```

---

## 8. Testing Conventions

- Test file: `tests/<module>_test.cpp`
- Test fixture: `class StorageTest : public ::testing::Test`
- Test names: `TEST_F(StorageTest, SetAndGetReturnsValue)`
  - Pattern: `<Action><Condition>` or `<Scenario><ExpectedResult>`
- Use `EXPECT_*` (not `ASSERT_*`) unless the test cannot continue after failure.
- Raft tests: use a mock transport that operates in-memory with deterministic timers. No real TCP.

```cpp
TEST_F(StorageTest, GetReturnsNulloptForMissingKey) {
    auto result = storage_.get("nonexistent");
    EXPECT_FALSE(result.has_value());
}

TEST_F(StorageTest, SetAndGetReturnsStoredValue) {
    storage_.set("key1", "value1");
    auto result = storage_.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value1");
}
```

---

## 9. CMake Conventions

- Minimum version: `cmake_minimum_required(VERSION 3.31)`
- Each `src/<module>/` has its own `CMakeLists.txt` that defines a library target.
- `kvstore_lib` links all module libraries together.
- Use `target_link_libraries` with `PUBLIC`/`PRIVATE` correctly:
  - `PUBLIC`: dependency exposed in headers
  - `PRIVATE`: dependency used only in `.cpp`
- Protobuf: use `protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS proto/raft.proto)`. Generated files go to `${CMAKE_CURRENT_BINARY_DIR}`.
- Compiler flags via presets, not hardcoded in CMakeLists.txt.

```cmake
# Example module CMakeLists.txt
add_library(kv_storage
    storage.cpp
)
target_include_directories(kv_storage PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kv_storage PRIVATE spdlog::spdlog)
```

---

## 10. Git Practices

- Commit messages: imperative mood, short first line.
  - `Add Storage class with shared_mutex`
  - `Implement RequestVote RPC handler`
  - `Fix election timer reset on AppendEntries`
- One logical change per commit. Do not mix refactoring with feature work.
- Branch per etap: `etap-1-foundation`, `etap-2-cluster`, etc. Merge to `main` when stable.

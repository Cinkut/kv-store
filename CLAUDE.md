# KV Store – Distributed Key-Value Store with Raft Consensus (C++20)

## What Is This

A distributed, fault-tolerant key-value store in C++20. Cluster of 3+ nodes, Raft leader election, replicated log, persistent WAL + snapshots.

## Tech Stack

| Component        | Choice                     | Notes                              |
|------------------|----------------------------|------------------------------------|
| Language         | C++20                      | Coroutines, concepts, ranges       |
| Networking       | Boost.Asio (`co_await`)    | `boost::asio::awaitable<T>`        |
| Raft RPC         | Protocol Buffers (protobuf)| Generated C++ classes for all RPCs |
| Client protocol  | Custom text (→ binary E5)  | `SET key val\n` / `OK\n`           |
| Logging          | spdlog                     | Mandatory. No cout/printf.         |
| Testing          | Google Test                | Every public class has tests       |
| CLI args         | boost::program_options     |                                    |
| Build            | CMake 3.31 + CMakePresets  | vcpkg manifest mode                |
| Compiler         | GCC 15 / Fedora 43         |                                    |

## Build Targets

- `kvstore_lib` – static library (all logic)
- `kv-server` – cluster node executable
- `kv-cli` – interactive client REPL
- `kvstore_tests` – Google Test binary

## Critical Design Rules

1. **Protobuf for Raft RPC** – never manual binary parsers for inter-node messages.
2. **Single strand for Raft** – all Raft state runs on one `boost::asio::strand`. Not thread-safe by design.
3. **WAL before in-memory** – always persist to disk first, then update memory.
4. **No dynamic membership** – cluster size fixed at startup.
5. **Leader handles all writes** – followers redirect clients via `REDIRECT <addr>\n`.
6. **spdlog only** – no `std::cout`, no `printf`. All output through spdlog.

## Detailed Documentation

For full details, read the specific file you need:

- `docs/architecture.md` – Full implementation plan (5 stages), directory structure, roadmap with checklist
- `docs/cpp-guidelines.md` – C++20 coding standards, naming, error handling, async patterns, CMake conventions
- `docs/raft-spec.md` – Raft protocol details, protobuf schema, WAL/snapshot formats, node CLI args

## Namespace Layout

- `kv::` – top-level (Storage, Server, Session, Protocol)
- `kv::raft::` – Raft consensus (RaftNode, RaftLog, StateMachine)
- `kv::persistence::` – WAL, Snapshot

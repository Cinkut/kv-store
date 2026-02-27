# Architecture – Full Implementation Plan

## Directory Structure

```
kv_store/
├── CLAUDE.md                       # AI context (short, references docs/)
├── CMakeLists.txt                  # Root CMake
├── CMakePresets.json               # Debug / Release presets
├── vcpkg.json                      # boost-asio, boost-program-options, protobuf, spdlog, gtest
├── proto/
│   └── raft.proto                  # Protobuf definitions for all Raft RPCs
├── src/
│   ├── CMakeLists.txt
│   ├── storage/
│   │   ├── CMakeLists.txt
│   │   ├── storage.hpp
│   │   └── storage.cpp
│   ├── network/
│   │   ├── CMakeLists.txt
│   │   ├── server.hpp / .cpp       # TCP acceptor, spawns sessions
│   │   ├── session.hpp / .cpp      # Per-connection coroutine
│   │   ├── peer_client.hpp / .cpp  # Async client to other Raft nodes
│   │   └── protocol.hpp / .cpp     # Text command parser / serializer
│   ├── raft/
│   │   ├── CMakeLists.txt
│   │   ├── raft_node.hpp / .cpp    # Core state machine (Follower/Candidate/Leader)
│   │   ├── raft_log.hpp / .cpp     # In-memory log (vector<LogEntry>)
│   │   ├── raft_rpc.hpp / .cpp     # Send/receive protobuf RPCs
│   │   └── state_machine.hpp/.cpp  # Applies committed entries to Storage
│   ├── persistence/
│   │   ├── CMakeLists.txt
│   │   ├── wal.hpp / .cpp          # Write-Ahead Log
│   │   └── snapshot.hpp / .cpp     # Full state snapshots
│   ├── server/
│   │   ├── CMakeLists.txt
│   │   └── main.cpp                # kv-server entry point
│   └── cli/
│       ├── CMakeLists.txt
│       └── main.cpp                # kv-cli entry point
├── tests/
│   ├── CMakeLists.txt
│   ├── storage_test.cpp
│   ├── protocol_test.cpp
│   ├── raft_election_test.cpp
│   ├── raft_replication_test.cpp
│   ├── wal_test.cpp
│   └── integration_test.cpp
└── scripts/
    └── run_cluster.sh              # Launch 3 nodes locally
```

## Build Targets

| Target           | Type           | Links Against   | Description                     |
|------------------|----------------|-----------------|---------------------------------|
| `kvstore_lib`    | STATIC library | boost, protobuf, spdlog | All logic in one lib   |
| `kv-server`      | Executable     | `kvstore_lib`   | Single cluster node             |
| `kv-cli`         | Executable     | boost-asio      | Interactive REPL client         |
| `kvstore_tests`  | Executable     | `kvstore_lib`, gtest | All unit + integration tests |

## vcpkg Dependencies

```json
{
  "name": "kv-store",
  "version": "0.1.0",
  "dependencies": [
    "boost-asio",
    "boost-program-options",
    "protobuf",
    "spdlog",
    "gtest"
  ]
}
```

---

## Roadmap

### Etap 1 – Single Node (Foundation)

Goal: A working single-node KV server that accepts TCP connections and responds to text commands.

- [x] **1.1 Project scaffolding**
  - CMakeLists.txt (root + per-module), FetchContent for Boost/spdlog/gtest/protobuf
  - CMakePresets.json (Debug / Release / debug-asan), Unix Makefiles generator
  - vcpkg.json manifest (declarative, FetchContent is the actual mechanism)
  - `.gitignore`
  - Verified: all 3 binaries build, 2/2 placeholder tests pass

- [x] **1.2 spdlog integration**
  - `src/common/logger.hpp` + `logger.cpp` → `kv_common` static library
  - `init_default_logger(level)` – global default logger (CLI, tests, early startup)
  - `make_node_logger(node_id, level)` – per-node logger, name = `node-<id>`
  - `parse_log_level(string)` – parses CLI string to `spdlog::level::level_enum`
  - Pattern: `[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] %v` (color per level)
  - `NodeState` enum (`Follower`/`Candidate`/`Leader`) defined in `logger.hpp`
  - All other modules link `kv_common` (not `spdlog::spdlog` directly)

- [ ] **1.3 Storage engine**
  - Class `kv::Storage`
  - Internal: `std::unordered_map<std::string, std::string>` + `std::shared_mutex`
  - Methods: `get(key)`, `set(key, value)`, `del(key)`, `keys()`, `size()`, `snapshot()` (returns full copy)
  - `get` returns `std::optional<std::string>`
  - Unit tests: basic CRUD, concurrent reads/writes (multiple threads)

- [ ] **1.4 Text protocol**
  - Class `kv::Protocol`
  - Parse: `std::string_view` → `kv::Command` (variant: SetCmd, GetCmd, DelCmd, KeysCmd, PingCmd)
  - Serialize: `kv::Response` → `std::string`
  - Commands: `SET key value\n`, `GET key\n`, `DEL key\n`, `KEYS\n`, `PING\n`
  - Responses: `OK\n`, `VALUE <v>\n`, `NOT_FOUND\n`, `DELETED\n`, `KEYS <k1> <k2>...\n`, `PONG\n`, `ERROR <msg>\n`
  - Unit tests: valid commands, malformed input, edge cases (empty key, spaces in value)

- [ ] **1.5 TCP server**
  - Class `kv::Server` – owns `io_context`, `tcp::acceptor`
  - `co_spawn` per accepted connection → `kv::Session`
  - `kv::Session::run()` – `boost::asio::awaitable<void>`, reads lines, parses, executes on Storage, writes response
  - Graceful shutdown on SIGINT/SIGTERM (`boost::asio::signal_set`)
  - Thread pool: `std::thread::hardware_concurrency()` threads running `io_context`

- [ ] **1.6 kv-cli**
  - CLI args: `--host`, `--port` (defaults: 127.0.0.1:6379)
  - REPL loop: read stdin → send to server → print response
  - Handle disconnect gracefully

- [ ] **1.7 Integration test**
  - Start server in test, connect via TCP, run SET/GET/DEL/KEYS/PING sequence
  - Verify correct responses

### Etap 2 – Cluster Communication

Goal: 3 node processes that know about each other and can exchange messages.

- [ ] **2.1 Node configuration**
  - `kv::NodeConfig` struct: `id`, `host`, `client_port`, `raft_port`, `data_dir`, `peers[]`, `snapshot_interval`
  - `boost::program_options` parsing (see `docs/raft-spec.md` for full CLI args)
  - Validate: id > 0, ports in range, at least 2 peers

- [ ] **2.2 Peer client**
  - Class `kv::network::PeerClient`
  - Async TCP client with `co_await`
  - Auto-reconnect with exponential backoff (100ms → 200ms → 400ms → ... → 5s cap)
  - Send/receive length-prefixed protobuf messages
  - Connection states: `Disconnected` → `Connecting` → `Connected`

- [ ] **2.3 Peer discovery**
  - On startup: attempt connection to all peers from config
  - Log connection state changes
  - Periodic connectivity check (heartbeat before Raft is active)
  - Test: launch 3 nodes, verify all-to-all connectivity logged

### Etap 3 – Raft Consensus (Core)

Goal: Leader election + log replication. The hard part.

- [ ] **3.1 Protobuf schema**
  - Define `proto/raft.proto` (see `docs/raft-spec.md` for full schema)
  - CMake: `protobuf_generate_cpp()` → generated code in build dir

- [ ] **3.2 Raft data structures**
  - `kv::raft::LogEntry` – wraps protobuf LogEntry
  - `kv::raft::RaftLog` – in-memory `std::vector<LogEntry>`, index/term accessors
  - `kv::raft::RaftNode` skeleton – enum `NodeState { Follower, Candidate, Leader }`

- [ ] **3.3 Leader election**
  - Election timer: `boost::asio::steady_timer`, random [150ms, 300ms]
  - Follower → Candidate on timeout: increment term, vote for self, send RequestVote to all peers
  - Vote logic: grant if candidate's log is at least as up-to-date, haven't voted for anyone else this term
  - Step-down: any RPC with higher term → revert to Follower
  - Tests: mock transport, deterministic timers. Scenarios: normal election, split vote, stale candidate

- [ ] **3.4 Log replication**
  - Leader receives client command → append to local log → send AppendEntries to all peers
  - AppendEntries consistency check: prevLogIndex + prevLogTerm must match
  - Follower: append entries, update commitIndex to min(leaderCommit, last new entry index)
  - Leader: track `nextIndex[]`, `matchIndex[]` per peer. Decrement nextIndex on rejection, retry.
  - Commit rule: majority of matchIndex >= N AND log[N].term == currentTerm → commit
  - Tests: basic replication, follower catch-up, leader failover

- [ ] **3.5 State machine**
  - `kv::raft::StateMachine` wraps `kv::Storage`
  - Applies committed log entries to storage: SET → storage.set(), DEL → storage.del()
  - Tracks `lastApplied` index

- [ ] **3.6 Client redirect**
  - Follower receiving write command → `REDIRECT <leader_host>:<leader_port>\n`
  - Leader ID tracked in RaftNode (received via AppendEntries)

### Etap 4 – Persistence (Disk)

Goal: Data survives node restarts.

- [ ] **4.1 Write-Ahead Log**
  - Class `kv::persistence::WAL`
  - Append-only binary file (see `docs/raft-spec.md` for format)
  - `append(LogEntry)` → write + fsync
  - `replay()` → iterator over stored entries
  - CRC32 validation on read
  - Persist `currentTerm` + `votedFor` as special WAL entries
  - Tests: write/read/replay, corruption detection (bad CRC)

- [ ] **4.2 Snapshots**
  - Class `kv::persistence::Snapshot`
  - Serialize full `Storage` state to file (format in `docs/raft-spec.md`)
  - Trigger: every N committed entries (configurable)
  - After snapshot: truncate WAL entries ≤ snapshot's last_included_index
  - Startup sequence: load snapshot → replay WAL from snapshot point → ready
  - Tests: snapshot + restore, snapshot + WAL replay

- [ ] **4.3 InstallSnapshot RPC**
  - Leader sends snapshot to followers that are too far behind (nextIndex < first log index)
  - Follower: receive snapshot, write to disk, reset state machine, reply

### Etap 5 – Optimization

Goal: Performance improvements.

- [ ] **5.1 Binary client protocol**
  - Header: `[msg_type: 1B][payload_length: 4B]`
  - Auto-detect: first byte of connection determines text vs binary mode
  - Benchmark: compare throughput text vs binary

- [ ] **5.2 io_uring (optional)**
  - Replace fsync in WAL with io_uring submission
  - Potentially: io_uring for network accept/read/write
  - Benchmark: compare latency/throughput vs Boost.Asio

---

## Testing Strategy

| File                        | Scope              | What It Tests                                      |
|-----------------------------|--------------------|----------------------------------------------------|
| `storage_test.cpp`          | Unit               | CRUD, concurrent access, edge cases                |
| `protocol_test.cpp`         | Unit               | Parse/serialize all commands, malformed input       |
| `raft_election_test.cpp`    | Unit (mock)        | Election scenarios: normal, split vote, stale       |
| `raft_replication_test.cpp` | Unit (mock)        | Replication, catch-up, commit, leader change        |
| `wal_test.cpp`              | Unit               | Write, replay, CRC corruption                      |
| `integration_test.cpp`      | Integration        | 3-node cluster, SET on leader, read on follower     |

Raft tests use a **mock transport layer** (no real TCP) with **deterministic timers** (manually advance time). This makes tests fast and reproducible.

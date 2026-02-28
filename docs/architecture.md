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

- [x] **1.3 Storage engine**
  - Class `kv::Storage`
  - Internal: `std::unordered_map<std::string, std::string>` + `std::shared_mutex`
  - Methods: `get(key)`, `set(key, value)`, `del(key)`, `keys()`, `size()`, `snapshot()` (returns full copy)
  - `get` returns `std::optional<std::string>`
  - Unit tests: basic CRUD, concurrent reads/writes (multiple threads)
  - Verified: 26/26 tests pass

- [x] **1.4 Text protocol**
  - Class `kv::Protocol`
  - Parse: `std::string_view` → `kv::Command` (variant: SetCmd, GetCmd, DelCmd, KeysCmd, PingCmd)
  - Serialize: `kv::Response` → `std::string`
  - Commands: `SET key value\n`, `GET key\n`, `DEL key\n`, `KEYS\n`, `PING\n`
  - Responses: `OK\n`, `VALUE <v>\n`, `NOT_FOUND\n`, `DELETED\n`, `KEYS <k1> <k2>...\n`, `PONG\n`, `ERROR <msg>\n`
  - Unit tests: valid commands, malformed input, edge cases (empty key, spaces in value)
  - Verified: 31/31 tests pass

- [x] **1.5 TCP server**
  - Class `kv::Server` – owns `io_context`, `tcp::acceptor`
  - `co_spawn` per accepted connection → `kv::Session`
  - `kv::Session::run()` – `boost::asio::awaitable<void>`, reads lines, parses, executes on Storage, writes response
  - Graceful shutdown on SIGINT/SIGTERM (`boost::asio::signal_set`)
  - Thread pool: `std::thread::hardware_concurrency()` threads running `io_context`
  - All targets build cleanly; 57/57 tests pass

- [x] **1.6 kv-cli**
  - CLI args: `--host`, `--port` (defaults: 127.0.0.1:6379)
  - REPL loop: read stdin → send to server → print response
  - Handle disconnect gracefully
  - Verified: builds successfully; async TCP I/O with sync stdin using `as_tuple(use_awaitable)` pattern

- [x] **1.7 Integration test**
  - Start server in test, connect via TCP, run SET/GET/DEL/KEYS/PING sequence
  - Verify correct responses
  - Tests: Ping, SetAndGet, GetNotFound, Overwrite, Del, DelNotFound, KeysEmpty, KeysAfterInserts, ValueWithSpaces, MultipleConnections, MalformedCommand, SetEmptyValueRejected, PipelineMultipleCommands
  - Verified: 70/70 tests pass (26 storage + 31 protocol + 13 integration)

### Etap 2 – Cluster Communication

Goal: 3 node processes that know about each other and can exchange messages.

- [x] **2.1 Node configuration**
  - `kv::NodeConfig` struct: `id`, `host`, `client_port`, `raft_port`, `data_dir`, `peers[]`, `snapshot_interval`
  - `boost::program_options` parsing (see `docs/raft-spec.md` for full CLI args)
  - Validate: id > 0, ports in range, at least 2 peers

- [x] **2.2 Peer client**
  - Class `kv::network::PeerClient`
  - Async TCP client with `co_await`
  - Auto-reconnect with exponential backoff (100ms → 200ms → 400ms → ... → 5s cap)
  - Send/receive length-prefixed protobuf messages
  - Connection states: `Disconnected` → `Connecting` → `Connected`

- [x] **2.3 Peer discovery**
  - On startup: attempt connection to all peers from config
  - Log connection state changes
  - Periodic connectivity check (heartbeat before Raft is active)
  - Test: launch 3 nodes, verify all-to-all connectivity logged

### Etap 3 – Raft Consensus (Core)

Goal: Leader election + log replication. The hard part.

- [x] **3.1 Protobuf schema**
  - Define `proto/raft.proto` (see `docs/raft-spec.md` for full schema)
  - CMake: `protobuf_generate_cpp()` → generated code in build dir

- [x] **3.2 Raft data structures**
  - `kv::raft::LogEntry` – wraps protobuf LogEntry
  - `kv::raft::RaftLog` – in-memory `std::vector<LogEntry>`, index/term accessors
  - `kv::raft::RaftNode` skeleton – enum `NodeState { Follower, Candidate, Leader }`

- [x] **3.3 Leader election**
  - Election timer: `boost::asio::steady_timer`, random [150ms, 300ms]
  - Follower → Candidate on timeout: increment term, vote for self, send RequestVote to all peers
  - Vote logic: grant if candidate's log is at least as up-to-date, haven't voted for anyone else this term
  - Step-down: any RPC with higher term → revert to Follower
  - Tests: mock transport, deterministic timers. Scenarios: normal election, split vote, stale candidate

- [x] **3.4 Log replication**
  - Leader receives client command → append to local log → send AppendEntries to all peers
  - AppendEntries consistency check: prevLogIndex + prevLogTerm must match
  - Follower: append entries, update commitIndex to min(leaderCommit, last new entry index)
  - Leader: track `nextIndex[]`, `matchIndex[]` per peer. Decrement nextIndex on rejection, retry.
  - Commit rule: majority of matchIndex >= N AND log[N].term == currentTerm → commit
  - Tests: basic replication, follower catch-up, leader failover

- [x] **3.5 State machine**
  - `kv::raft::StateMachine` wraps `kv::Storage`
  - Applies committed log entries to storage: SET → storage.set(), DEL → storage.del()
  - Tracks `lastApplied` index

- [x] **3.6 Client redirect**
  - Follower receiving write command → `REDIRECT <leader_host>:<leader_port>\n`
  - Leader ID tracked in RaftNode (received via AppendEntries)

### Etap 4 – Persistence (Disk)

Goal: Data survives node restarts.

- [x] **4.1 Write-Ahead Log**
  - Class `kv::persistence::WAL`
  - Append-only binary file (see `docs/raft-spec.md` for format)
  - `append(LogEntry)` → write + fsync
  - `replay()` → iterator over stored entries
  - CRC32 validation on read
  - Persist `currentTerm` + `votedFor` as special WAL entries
  - Tests: write/read/replay, corruption detection (bad CRC)
  - Verified: 32 new tests, 195/195 total pass

- [x] **4.2 Snapshots**
  - Class `kv::persistence::Snapshot`
  - Serialize full `Storage` state to file (format in `docs/raft-spec.md`)
  - Trigger: every N committed entries (configurable)
  - After snapshot: truncate WAL entries ≤ snapshot's last_included_index
  - Startup sequence: load snapshot → replay WAL from snapshot point → ready
  - Tests: snapshot + restore, snapshot + WAL replay
  - Verified: 28 new tests, 223/223 total pass

- [x] **4.3 InstallSnapshot RPC**
  - Leader sends snapshot to followers that are too far behind (nextIndex < first log index)
  - Follower: receive snapshot, write to disk, reset state machine, reply
  - `SnapshotIO` abstract interface in `raft_node.hpp` (decouples from file I/O)
  - `RaftLog::truncate_prefix()` for log compaction after snapshot
  - `StateMachine::reset()` for snapshot installation
  - Auto-trigger via `maybe_trigger_snapshot()` after applying committed entries
  - Verified: 23 new tests, 246/246 total pass

### Etap 5 – Optimization

Goal: Performance improvements via binary client protocol.

- [ ] **5.1 Binary client protocol**

  Wire format defined in `docs/raft-spec.md` section 9. All integers big-endian.

  - [ ] **5.1.1 Binary protocol parser/serializer**
    - New files: `src/network/binary_protocol.hpp`, `src/network/binary_protocol.cpp`
    - `parse_binary_request(span<const uint8_t>)` → `variant<Command, ErrorResp>`
    - `serialize_binary_response(const Response&)` → `vector<uint8_t>`
    - Reuse existing `Command` / `Response` variant types — no new domain types
    - Request: `[msg_type: u8][payload_length: u32 BE][payload]`
    - Response: `[status: u8][payload_length: u32 BE][payload]`
    - msg_type: `0x01`=SET, `0x02`=GET, `0x03`=DEL, `0x04`=KEYS, `0x05`=PING
    - status: `0x00`=OK, `0x01`=VALUE, `0x02`=NOT_FOUND, `0x03`=DELETED, `0x04`=KEYS, `0x05`=PONG, `0x10`=ERROR, `0x20`=REDIRECT

  - [ ] **5.1.2 Unit tests for binary protocol**
    - New file: `tests/binary_protocol_test.cpp`
    - Test all 5 request types: encode → decode round-trip
    - Test all 8 response statuses: serialize → verify bytes
    - Test malformed inputs: truncated header, unknown msg_type, payload too short, zero-length key
    - ~20-25 tests

  - [ ] **5.1.3 Dual-mode session (auto-detect text vs binary)**
    - Modify `Session::run()`: peek first byte of connection
    - First byte `0x00–0x1F` → binary mode, `0x20–0x7F` → text mode
    - Split into `run_text()` (existing logic) and `run_binary()` (new)
    - Binary loop: read 5-byte header → read payload → parse → dispatch → serialize → write
    - `dispatch()` unchanged — works on protocol-agnostic `Command`/`Response`

  - [ ] **5.1.4 Integration tests for binary protocol**
    - New `BinarySyncClient` test helper in `tests/integration_test.cpp`
    - Test all operations via binary wire: PING, SET+GET, DEL, KEYS, errors
    - Test auto-detection: text and binary clients on the same server simultaneously

  - [ ] **5.1.5 kv-cli `--binary` flag**
    - Add `--binary` option to `src/cli/main.cpp`
    - When set: encode commands as binary requests, decode binary responses
    - Validates full end-to-end binary path

  - [x] **5.1.6 Throughput benchmark**
    - Standalone benchmark: N SET+GET cycles, measure elapsed time
    - Compare text vs binary protocol throughput
    - Print results summary (ops/sec, latency percentiles)

- [x] **5.2 io_uring** — Skipped (optional, minimal benefit for this project scope)

---

## Testing Strategy

| File                        | Scope              | What It Tests                                      |
|-----------------------------|--------------------|----------------------------------------------------|
| `storage_test.cpp`          | Unit               | CRUD, concurrent access, edge cases                |
| `protocol_test.cpp`         | Unit               | Parse/serialize all commands, malformed input       |
| `binary_protocol_test.cpp`  | Unit               | Binary encode/decode, round-trips, malformed input  |
| `raft_test.cpp`             | Unit (mock)        | Election, replication, InstallSnapshot, snapshots   |
| `wal_test.cpp`              | Unit               | Write, replay, CRC corruption                      |
| `snapshot_test.cpp`         | Unit               | Snapshot save/load, CRC, atomic writes             |
| `integration_test.cpp`      | Integration        | Text + binary protocol, multi-connection            |

Raft tests use a **mock transport layer** (no real TCP) with **deterministic timers** (manually advance time). This makes tests fast and reproducible.

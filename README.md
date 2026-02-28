# kv_store

A distributed, fault-tolerant key-value store built from scratch in C++20. Uses the Raft consensus algorithm for leader election and log replication across a cluster of nodes. Data is persisted via a write-ahead log (WAL) and periodic snapshots.

## Features

- **Raft consensus** -- leader election, log replication, and InstallSnapshot RPC (Ongaro 2014)
- **Fault tolerance** -- cluster continues operating as long as a majority of nodes are alive
- **Persistence** -- WAL with CRC32 integrity checks, atomic snapshots, full recovery on restart
- **Dual client protocol** -- text (`SET key value\n`) and binary (length-prefixed, auto-detected)
- **Follower redirect** -- writes on followers return `REDIRECT <leader_host:port>`
- **Async I/O** -- Boost.Asio coroutines (`co_await`) throughout, single-strand Raft state machine
- **370 tests** -- unit, integration, and end-to-end cluster tests

## Architecture

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node 1  │◄───►│ Node 2  │◄───►│ Node 3  │   Raft RPC (protobuf over TCP)
│ (Leader)│     │(Follower)│    │(Follower)│
└────┬────┘     └────┬────┘     └────┬────┘
     │               │               │
   Client          Client          Client      Text or binary protocol
   :6379           :6380           :6381
```

Each node runs a Raft state machine on a dedicated strand. The leader handles all writes; followers redirect clients to the leader. Reads are served locally by any node.

### Components

| Module | Description |
|--------|-------------|
| `storage` | In-memory `unordered_map` with `shared_mutex` for concurrent reads |
| `network` | TCP server, per-connection sessions, text/binary protocol, peer clients |
| `raft` | RaftNode, RaftLog, StateMachine, transport, timers, commit awaiter |
| `persistence` | Write-ahead log, snapshots, WAL-based persist callbacks |

## Building

Requires **GCC 13+** (tested with GCC 15) and **CMake 3.25+**. All dependencies are fetched automatically via FetchContent.

```bash
# Configure (debug build)
cmake -B build/debug -S . --preset debug

# Build everything
cmake --build build/debug -j$(nproc)

# Build specific targets
cmake --build build/debug --target kv-server kv-cli kvstore_tests -j$(nproc)
```

### Build presets

| Preset | Description |
|--------|-------------|
| `debug` | Debug build with symbols |
| `debug-asan` | Debug with AddressSanitizer + UBSan |
| `release` | Optimized release build |

## Running

### Single node (standalone)

```bash
./build/debug/src/server/kv-server \
    --id 1 \
    --client-port 6379 \
    --raft-port 7001 \
    --peers 2:127.0.0.1:7002:6380,3:127.0.0.1:7003:6381 \
    --data-dir ./data/node1
```

### 3-node local cluster

```bash
./scripts/run_cluster.sh
```

This starts 3 nodes on localhost (client ports 6379-6381, Raft ports 7001-7003). Data is stored in `./data/node{1,2,3}/`. Set `LOG_LEVEL=debug` for verbose output.

### kv-cli

```bash
# Text mode (default)
./build/debug/src/cli/kv-cli --host 127.0.0.1 --port 6379

# Binary protocol mode
./build/debug/src/cli/kv-cli --port 6379 --binary
```

### Client protocol

```
> PING
PONG
> SET mykey hello world
OK
> GET mykey
VALUE hello world
> DEL mykey
DELETED
> KEYS
KEYS key1 key2 key3
```

Followers respond with `REDIRECT 127.0.0.1:6379` for write commands (SET/DEL).

### Server options

| Flag | Default | Description |
|------|---------|-------------|
| `--id` | required | Node ID (positive integer) |
| `--host` | `0.0.0.0` | Bind address |
| `--client-port` | `6379` | Client protocol port |
| `--raft-port` | `7001` | Raft RPC port |
| `--peers` | required | Peer list: `id:host:raft_port:client_port,...` |
| `--data-dir` | `./data` | WAL and snapshot directory |
| `--snapshot-interval` | `1000` | Entries between snapshots |
| `--log-level` | `info` | trace/debug/info/warn/error/critical |

## Testing

```bash
# Run all tests
cmake --build build/debug --target kvstore_tests -j$(nproc)
./build/debug/tests/kvstore_tests

# Run specific test suite
./build/debug/tests/kvstore_tests --gtest_filter='RaftNodeTest.*'
```

370 tests across 15 test files:

| Test file | Scope | What it tests |
|-----------|-------|---------------|
| `storage_test` | Unit | CRUD, concurrent access |
| `protocol_test` | Unit | Text command parse/serialize |
| `binary_protocol_test` | Unit | Binary encode/decode |
| `node_config_test` | Unit | CLI argument validation |
| `peer_client_test` | Unit | TCP peer client, reconnect |
| `peer_manager_test` | Unit | Peer discovery |
| `raft_test` | Unit | Election, replication, snapshots (mock transport) |
| `wal_test` | Unit | WAL write/replay, CRC corruption |
| `snapshot_test` | Unit | Snapshot save/load, atomic writes |
| `real_timer_test` | Unit | Production timer behavior |
| `raft_transport_test` | Unit | Transport send/receive |
| `snapshot_io_impl_test` | Unit | Snapshot I/O implementation |
| `redirect_test` | Integration | Follower redirect, cluster context |
| `integration_test` | Integration | Text + binary protocol over TCP |
| `cluster_integration_test` | End-to-end | 3-node cluster via fork/exec |

## Tech stack

| Component | Choice |
|-----------|--------|
| Language | C++20 (coroutines, concepts, ranges) |
| Async I/O | Boost.Asio with `co_await` |
| Raft RPC | Protocol Buffers v3 |
| Logging | spdlog |
| Testing | Google Test |
| CLI parsing | Boost.Program_options |
| Build | CMake 3.25+ with presets |

## Project structure

```
kv_store/
├── proto/raft.proto              # Protobuf schema for Raft RPCs
├── src/
│   ├── common/                   # Logger, NodeConfig
│   ├── storage/                  # In-memory KV store
│   ├── network/                  # TCP server, sessions, protocols, peers
│   ├── raft/                     # Raft consensus engine
│   ├── persistence/              # WAL, snapshots
│   ├── server/                   # kv-server entry point
│   └── cli/                      # kv-cli entry point
├── tests/                        # 370 tests (unit + integration + e2e)
├── tools/                        # Throughput benchmark
├── scripts/                      # run_cluster.sh
└── docs/
    ├── architecture.md           # Implementation roadmap
    ├── raft-spec.md              # Raft protocol details, wire formats
    └── cpp-guidelines.md         # Coding conventions
```

## Documentation

- [`docs/architecture.md`](docs/architecture.md) -- Full implementation plan and roadmap
- [`docs/raft-spec.md`](docs/raft-spec.md) -- Raft protocol spec, protobuf schema, WAL/snapshot formats, timer values
- [`docs/cpp-guidelines.md`](docs/cpp-guidelines.md) -- C++20 coding standards and async patterns

# Raft Specification – Protocol, Formats, RPC

Reference: Diego Ongaro, "In Search of an Understandable Consensus Algorithm" (2014).

This project implements **Core Raft**: leader election + log replication + persistence. No dynamic membership changes (joint consensus).

---

## 1. Node States

```
                    timeout, start election
    ┌──────────┐  ────────────────────────►  ┌────────────┐
    │ Follower │                              │ Candidate  │
    └──────────┘  ◄────────────────────────  └────────────┘
         ▲         discovers leader or             │
         │         higher term                     │ receives majority votes
         │                                         ▼
         │         higher term           ┌──────────┐
         └────────────────────────────── │  Leader   │
                   discovered            └──────────┘
```

### State Transitions

| From      | To        | Trigger                                              |
|-----------|-----------|------------------------------------------------------|
| Follower  | Candidate | Election timeout expires without hearing from Leader |
| Candidate | Leader    | Receives votes from majority of cluster              |
| Candidate | Follower  | Discovers current Leader or higher term              |
| Candidate | Candidate | Election timeout expires (split vote) → new election |
| Leader    | Follower  | Discovers higher term in any RPC response            |

---

## 2. Protobuf Schema (`proto/raft.proto`)

```protobuf
syntax = "proto3";

package kv.raft;

// --- Log Entry ---

enum CommandType {
    CMD_NOOP = 0;      // Leader's first entry each term (no-op)
    CMD_SET  = 1;
    CMD_DEL  = 2;
}

message Command {
    CommandType type = 1;
    string key       = 2;
    string value     = 3;   // empty for DEL and NOOP
}

message LogEntry {
    uint64  term    = 1;
    uint64  index   = 2;
    Command command = 3;
}

// --- RequestVote RPC ---

message RequestVoteRequest {
    uint64 term           = 1;  // candidate's term
    uint32 candidate_id   = 2;  // candidate requesting vote
    uint64 last_log_index = 3;  // index of candidate's last log entry
    uint64 last_log_term  = 4;  // term of candidate's last log entry
}

message RequestVoteResponse {
    uint64 term         = 1;    // currentTerm, for candidate to update itself
    bool   vote_granted = 2;    // true = candidate received vote
}

// --- AppendEntries RPC ---

message AppendEntriesRequest {
    uint64 term           = 1;  // leader's term
    uint32 leader_id      = 2;  // so follower can redirect clients
    uint64 prev_log_index = 3;  // index of log entry immediately preceding new ones
    uint64 prev_log_term  = 4;  // term of prevLogIndex entry
    repeated LogEntry entries = 5;  // log entries to store (empty for heartbeat)
    uint64 leader_commit  = 6;  // leader's commitIndex
}

message AppendEntriesResponse {
    uint64 term        = 1;     // currentTerm, for leader to update itself
    bool   success     = 2;     // true if follower contained entry matching prevLogIndex/prevLogTerm
    uint64 match_index = 3;     // highest log index known to be replicated on this follower
}

// --- InstallSnapshot RPC (Etap 4) ---

message InstallSnapshotRequest {
    uint64 term                = 1;
    uint32 leader_id           = 2;
    uint64 last_included_index = 3;  // snapshot replaces all entries up through this index
    uint64 last_included_term  = 4;  // term of last_included_index
    bytes  data                = 5;  // raw snapshot bytes (entire KV state)
}

message InstallSnapshotResponse {
    uint64 term = 1;                 // currentTerm, for leader to update itself
}

// --- Wrapper for multiplexing on a single TCP connection ---

message RaftMessage {
    oneof payload {
        RequestVoteRequest       request_vote_req    = 1;
        RequestVoteResponse      request_vote_resp   = 2;
        AppendEntriesRequest     append_entries_req   = 3;
        AppendEntriesResponse    append_entries_resp  = 4;
        InstallSnapshotRequest   install_snapshot_req = 5;
        InstallSnapshotResponse  install_snapshot_resp = 6;
    }
}
```

### Wire Format (over TCP)

Each protobuf message is sent as:
```
[length: uint32 big-endian][serialized RaftMessage: length bytes]
```

This allows the receiver to read exactly `length` bytes and deserialize.

---

## 3. Raft State (per node)

### Persistent State (written to WAL before responding to RPCs)

| Field        | Type                      | Description                          |
|--------------|---------------------------|--------------------------------------|
| currentTerm  | uint64                    | Latest term server has seen          |
| votedFor     | optional<uint32>          | CandidateId voted for in current term|
| log[]        | vector<LogEntry>          | Log entries                          |

### Volatile State (all servers)

| Field        | Type   | Init | Description                                       |
|--------------|--------|------|---------------------------------------------------|
| commitIndex  | uint64 | 0    | Highest log entry known to be committed           |
| lastApplied  | uint64 | 0    | Highest log entry applied to state machine        |

### Volatile State (leaders only, reinitialized after election)

| Field          | Type              | Init                | Description                          |
|----------------|-------------------|---------------------|--------------------------------------|
| nextIndex[]    | map<id, uint64>   | last log index + 1  | Next log entry to send to each peer  |
| matchIndex[]   | map<id, uint64>   | 0                   | Highest entry known replicated       |

---

## 4. Timer Configuration

| Timer              | Value               | Notes                                         |
|--------------------|----------------------|-----------------------------------------------|
| Election timeout   | random [150ms, 300ms]| Reset on: valid AppendEntries, granting vote  |
| Heartbeat interval | 50ms                 | Leader sends empty AppendEntries              |
| RPC timeout        | 100ms                | Timeout for a single RPC call to a peer       |
| Reconnect backoff  | 100ms → 5s (×2 each)| Peer client reconnection attempts             |

Election timeout must be >> heartbeat interval to avoid unnecessary elections.

---

## 5. WAL Binary Format

### Metadata Record (written on term/vote changes)

```
[type: uint8 = 0x01]
[term: uint64 LE]
[voted_for: int32 LE]    // -1 = no vote
[crc32: uint32 LE]
```

### Log Entry Record

```
[type: uint8 = 0x02]
[total_length: uint32 LE]     // length of everything after this field, before CRC
[term: uint64 LE]
[index: uint64 LE]
[cmd_type: uint8]             // 0=NOOP, 1=SET, 2=DEL
[key_length: uint16 LE]
[key: bytes]
[value_length: uint32 LE]
[value: bytes]
[crc32: uint32 LE]            // CRC of all fields from type through value
```

### File Layout

```
[header: "KVWAL" (5 bytes)][version: uint16 LE = 1]
[record 1]
[record 2]
...
```

File path: `<data_dir>/wal.bin`

On startup: validate header, replay all records, verify CRCs.

---

## 6. Snapshot Format

```
[magic: "KVSS" (4 bytes)]
[version: uint16 LE = 1]
[last_included_index: uint64 LE]
[last_included_term: uint64 LE]
[entry_count: uint32 LE]
  [key_length: uint16 LE][key: bytes][value_length: uint32 LE][value: bytes]  × entry_count
[crc32: uint32 LE]           // CRC of everything from magic through last value
```

File path: `<data_dir>/snapshot.bin`

Trigger: when `lastApplied - snapshot_last_index >= snapshot_interval` (default 1000).

After snapshot:
1. Write snapshot file atomically (write to `.tmp`, then rename)
2. Truncate WAL: remove entries with index ≤ last_included_index
3. Write new WAL header + remaining entries

---

## 7. Node CLI Arguments

```
kv-server
    --id <uint32>                   Node ID (must be unique in cluster)
    --host <string>                 Bind address (default: 0.0.0.0)
    --client-port <uint16>          Port for client connections (default: 6379)
    --raft-port <uint16>            Port for Raft RPC connections (default: 7001)
    --peers <id:host:port,...>      Comma-separated peer list
    --data-dir <path>               Directory for WAL + snapshots (default: ./data)
    --snapshot-interval <uint32>    Entries between snapshots (default: 1000)
    --log-level <string>            trace|debug|info|warn|error|critical (default: info)
```

Example: 3-node local cluster

```bash
# Terminal 1
kv-server --id 1 --client-port 6379 --raft-port 7001 \
          --peers 2:127.0.0.1:7002,3:127.0.0.1:7003 \
          --data-dir ./data/node1

# Terminal 2
kv-server --id 2 --client-port 6380 --raft-port 7002 \
          --peers 1:127.0.0.1:7001,3:127.0.0.1:7003 \
          --data-dir ./data/node2

# Terminal 3
kv-server --id 3 --client-port 6381 --raft-port 7003 \
          --peers 1:127.0.0.1:7001,2:127.0.0.1:7002 \
          --data-dir ./data/node3
```

## 8. Client Protocol (Text)

### Commands (client → server)

```
SET <key> <value>\n
GET <key>\n
DEL <key>\n
KEYS\n
PING\n
```

- `<key>`: no spaces, no newlines, max 256 bytes
- `<value>`: may contain spaces (everything after second space on SET line), max 1MB, no newlines

### Responses (server → client)

```
OK\n                            # SET succeeded (committed by Raft majority)
VALUE <value>\n                 # GET found the key
NOT_FOUND\n                     # GET/DEL: key does not exist
DELETED\n                       # DEL succeeded
KEYS <key1> <key2> ...\n        # Space-separated list (empty if no keys: "KEYS\n")
PONG\n                          # Response to PING
ERROR <message>\n               # Parse error, internal error, etc.
REDIRECT <host>:<port>\n        # Client connected to follower, retry on leader
```

### Connection Lifecycle

1. Client opens TCP connection to any node.
2. Client sends commands, one per line.
3. Server responds with one line per command.
4. If server is Follower and receives SET/DEL: responds `REDIRECT <leader_addr>`.
5. Connection stays open (persistent). Client can pipeline commands.
6. Client closes connection when done. Server cleans up session.

---

## 9. Binary Client Protocol (Etap 5)

### Detection

First byte of connection:
- `0x00`–`0x1F` → binary protocol
- `0x20`–`0x7F` → text protocol (printable ASCII = text command)

### Binary Format

**Request:**
```
[msg_type: uint8][payload_length: uint32 BE][payload: bytes]
```

| msg_type | Command | Payload                                          |
|----------|---------|--------------------------------------------------|
| 0x01     | SET     | [key_len: uint16 BE][key][value_len: uint32 BE][value] |
| 0x02     | GET     | [key_len: uint16 BE][key]                        |
| 0x03     | DEL     | [key_len: uint16 BE][key]                        |
| 0x04     | KEYS    | (empty)                                          |
| 0x05     | PING    | (empty)                                          |

**Response:**
```
[status: uint8][payload_length: uint32 BE][payload: bytes]
```

| status | Meaning    | Payload                                          |
|--------|------------|--------------------------------------------------|
| 0x00   | OK         | (empty)                                          |
| 0x01   | VALUE      | [value_len: uint32 BE][value]                    |
| 0x02   | NOT_FOUND  | (empty)                                          |
| 0x03   | DELETED    | (empty)                                          |
| 0x04   | KEYS       | [count: uint32 BE][key_len: uint16 BE][key] × count |
| 0x05   | PONG       | (empty)                                          |
| 0x10   | ERROR      | [msg_len: uint16 BE][message]                    |
| 0x20   | REDIRECT   | [addr_len: uint16 BE][host:port as UTF-8]        |

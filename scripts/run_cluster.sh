#!/usr/bin/env bash
#
# run_cluster.sh — Launch a 3-node kv-server cluster locally.
#
# Usage:
#   ./scripts/run_cluster.sh              # Uses build/debug/src/server/kv-server
#   ./scripts/run_cluster.sh /path/to/kv-server
#
# Ports:
#   Node 1: client=6379, raft=7001
#   Node 2: client=6380, raft=7002
#   Node 3: client=6381, raft=7003
#
# Data directories are created under ./data/{node1,node2,node3}.
# Press Ctrl+C to stop all nodes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Resolve kv-server binary.
KV_SERVER="${1:-$PROJECT_DIR/build/debug/src/server/kv-server}"

if [[ ! -x "$KV_SERVER" ]]; then
    echo "ERROR: kv-server binary not found or not executable: $KV_SERVER" >&2
    echo "Build it first:  cmake --build build/debug --target kv-server -j\$(nproc)" >&2
    exit 1
fi

# Create data directories.
DATA_ROOT="$PROJECT_DIR/data"
mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3"

HOST="127.0.0.1"
LOG_LEVEL="${LOG_LEVEL:-info}"

echo "Starting 3-node kv-server cluster..."
echo "  Node 1: client=$HOST:6379  raft=$HOST:7001  data=$DATA_ROOT/node1"
echo "  Node 2: client=$HOST:6380  raft=$HOST:7002  data=$DATA_ROOT/node2"
echo "  Node 3: client=$HOST:6381  raft=$HOST:7003  data=$DATA_ROOT/node3"
echo ""
echo "Connect with:  kv-cli --host $HOST --port 6379"
echo "Press Ctrl+C to stop all nodes."
echo ""

# Track child PIDs for cleanup.
PIDS=()

cleanup() {
    echo ""
    echo "Stopping cluster..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    # Wait for all children.
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    echo "All nodes stopped."
}

trap cleanup EXIT INT TERM

# Node 1
"$KV_SERVER" \
    --id 1 \
    --host "$HOST" \
    --client-port 6379 \
    --raft-port 7001 \
    --peers "2:$HOST:7002:6380,3:$HOST:7003:6381" \
    --data-dir "$DATA_ROOT/node1" \
    --log-level "$LOG_LEVEL" &
PIDS+=($!)

# Node 2
"$KV_SERVER" \
    --id 2 \
    --host "$HOST" \
    --client-port 6380 \
    --raft-port 7002 \
    --peers "1:$HOST:7001:6379,3:$HOST:7003:6381" \
    --data-dir "$DATA_ROOT/node2" \
    --log-level "$LOG_LEVEL" &
PIDS+=($!)

# Node 3
"$KV_SERVER" \
    --id 3 \
    --host "$HOST" \
    --client-port 6381 \
    --raft-port 7003 \
    --peers "1:$HOST:7001:6379,2:$HOST:7002:6380" \
    --data-dir "$DATA_ROOT/node3" \
    --log-level "$LOG_LEVEL" &
PIDS+=($!)

echo "All 3 nodes launched (PIDs: ${PIDS[*]})"
echo ""

# Wait for any child to exit.
wait -n "${PIDS[@]}" 2>/dev/null || true

echo "A node exited unexpectedly. Shutting down remaining nodes..."

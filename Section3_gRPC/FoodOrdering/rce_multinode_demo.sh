#!/bin/bash
# Multi-node RCE demo: server on one allocated node, client probe on another,
# communicating over the real cluster network (not localhost).
set -e
cd "$(dirname "$0")"

module load python/3.12.5 >/dev/null 2>&1

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
SERVER_NODE=${NODES[0]}
CLIENT_NODE=${NODES[1]}
PORT=50151

echo "Server node: $SERVER_NODE"
echo "Client node: $CLIENT_NODE"

srun --nodes=1 --ntasks=1 -w "$SERVER_NODE" bash -c "module load python/3.12.5; python3 server.py 0.0.0.0:$PORT" &
SERVER_PID=$!

sleep 4

srun --nodes=1 --ntasks=1 -w "$CLIENT_NODE" bash -c "module load python/3.12.5; python3 remote_client_probe.py $SERVER_NODE:$PORT"

kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true

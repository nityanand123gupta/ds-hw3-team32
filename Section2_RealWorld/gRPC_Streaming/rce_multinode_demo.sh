#!/bin/bash
# Multi-node RCE demo: server, streaming client, and dashboard probe each on
# a different allocated compute node, communicating over the cluster network.
set -e
cd "$(dirname "$0")"

module load python/3.12.5 >/dev/null 2>&1

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
SERVER_NODE=${NODES[0]}
CLIENT_NODE=${NODES[1]}
DASH_NODE=${NODES[2]:-${NODES[0]}}
PORT=50252

echo "Server node : $SERVER_NODE"
echo "Client node : $CLIENT_NODE"
echo "Dashboard probe node : $DASH_NODE"

srun --nodes=1 --ntasks=1 -w "$SERVER_NODE" bash -c "module load python/3.12.5; python3 server.py 0.0.0.0:$PORT 5 20 4" &
SERVER_PID=$!
sleep 4

srun --nodes=1 --ntasks=1 -w "$CLIENT_NODE" bash -c "module load python/3.12.5; python3 generate_dataset_helper.py > /tmp/rce_demo_dataset.txt; python3 streaming_client.py $SERVER_NODE:$PORT /tmp/rce_demo_dataset.txt --rate 500"

srun --nodes=1 --ntasks=1 -w "$DASH_NODE" bash -c "module load python/3.12.5; python3 dashboard_probe.py $SERVER_NODE:$PORT"

kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true

# Section 2 - Q2: Server Log Analytics via Real-Time gRPC Streaming

Team 32 - Nityanand Gupta (2024101147), A V Aditya (2024111031)

Continues the HW2 Q7 "Large-Scale Server Log Analytics" problem, now
treating the dataset as a continuous stream and computing the same
analytics with gRPC instead of a batch framework.

## Architecture

- **`streaming_client.py`**: reads a pre-generated dataset file and replays
  its records one gRPC message at a time via a client-streaming RPC
  (`StreamLogs`), at a configurable rate (`--rate records/sec`, 0 =
  unthrottled) and batching granularity (`--batch-size`, how often the
  pacing sleep is checked).
- **`server.py`**: a `LogAnalyticsServicer` holding `num_workers`
  independent `Worker` objects (default 4), each with its own lock and its
  own `PartialStats` accumulator. Incoming records are sharded round-robin
  across workers, so ingestion never contends on one global lock. A
  `GetAnalytics` query briefly locks each worker in turn to fold its
  partial state into a merged snapshot - ingestion into the *other*
  workers is never blocked while one worker is being read.
- **`analytics_core.py`**: the aggregation logic (`PartialStats.update`,
  `merge_into`, `top_servers`, `top_endpoints`, `busiest_interval`,
  `format_output`), written to mirror `analytics_common.hpp` /
  `Hadoop_MapReduce/analytics_common.hpp` field-for-field, so the same
  input produces byte-identical output regardless of which of the four
  implementations (sequential, MPI, Hadoop, gRPC) processed it.
- **`dashboard.py`**: a CLI that polls `GetAnalytics` every `--interval`
  seconds and redraws a live text dashboard while ingestion is in progress.

## .proto interface

`log_analytics.proto` defines `LogAnalyticsService` with:

- `StreamLogs(stream LogRecord) returns (IngestSummary)` - client-streaming
  ingestion (the required "streaming ingestion of records").
- `GetAnalytics(Empty) returns (AnalyticsSnapshot)` - the required "queries
  for the current analytics", returning every field the assignment's output
  format needs (totals, response-time stats, status buckets, busiest
  interval, Top-K servers/endpoints) as structured fields rather than
  preformatted text, so both the dashboard and the correctness test can
  consume it directly.
- `Reset(Empty) returns (Ack)` - clears state; used between test runs.

`K` (Top-K count) and `S` (expected server-id range, for parity with the
other implementations) are supplied once at server startup, matching the
dataset's own `N K S` header.

## Files

| File | Purpose |
|---|---|
| `log_analytics.proto` | Service + message definitions |
| `analytics_core.py` | Aggregation logic shared by the server and the test |
| `server.py` | Sharded-worker servicer |
| `streaming_client.py` | Replays a dataset file as a live stream |
| `dashboard.py` | Interactive CLI dashboard |
| `dashboard_probe.py` | Non-interactive single-query version of the dashboard, used for scripted/RCE demos |
| `generate_dataset_helper.py` | Small pure-Python dataset generator (no build step) for quick demos |
| `test_streaming_analytics.py` | Automated correctness/concurrency test |

## Setup

```bash
pip install --user grpcio grpcio-tools
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. log_analytics.proto
```

## Running locally

```bash
python3 server.py localhost:50052 2 3 4          # K=2, S=3 (matches test_data/sample_input.txt), 4 workers
python3 streaming_client.py localhost:50052 test_data/sample_input.txt --rate 200
python3 dashboard.py localhost:50052              # in another terminal, while streaming
```

## Automated correctness test

```bash
python3 test_streaming_analytics.py
```

Covers:
- Streaming the assignment's sample dataset end-to-end and checking the
  final `GetAnalytics` snapshot matches `test_data/expected_output.txt`
  **exactly**.
- `Reset` clearing all state.
- A 20,000-record synthetic stream with **concurrent** `GetAnalytics`
  queries firing throughout ingestion (no errors), and the final snapshot
  matching a sequential recomputation over the same records.

All 5 checks pass:
```
5 passed, 0 failed
```

## Verified on the actual RCE cluster

Two things were actually executed on the RCE cluster, not just tested locally:

1. **`test_streaming_analytics.py`** (all 5 checks) run inside a `salloc`
   allocation with `module load python/3.12.5` - passed on a real compute
   node.
2. **True 3-node cross-node run** (`rce_multinode_demo.sh`): server on one
   node, `streaming_client.py` replaying 2000 records at 500 rec/s from a
   *second* node, and a dashboard probe querying from a *third* node - all
   connecting over the cluster network (`<server-node>:<port>`, not
   `localhost`):
   ```
   Server node : node01
   Client node : node02
   Dashboard probe node : node03
   [StreamingClient] Ingested 2000 records in 4.003s (499.6 rec/s)
   ... live dashboard output ...
   [Probe] Queried node01:50252 from a different node than the server -- cross-node gRPC OK.
   ```
   Run it yourself with:
   ```bash
   cd ~/HW3/Section2_RealWorld/gRPC_Streaming
   salloc --nodes=3 --ntasks-per-node=1 --time=00:07:00 bash rce_multinode_demo.sh
   ```

## Performance knobs to explore for the report

- **Number of workers**: more shards reduce lock contention on ingestion
  but each `GetAnalytics` call touches every shard, so query latency grows
  linearly with worker count - a tradeoff worth measuring at, e.g., 1/2/4/8
  workers.
- **Streaming rate / batch size**: `--rate` controls records/sec;
  `--batch-size` controls how often the pacing loop checks the clock
  (larger batches = less timer overhead, coarser rate control).
- **Concurrent query load**: spin up multiple `dashboard_probe.py`/`GetAnalytics`
  callers during ingestion and measure query latency vs. ingestion
  throughput as query concurrency increases.

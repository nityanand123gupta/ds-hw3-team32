#!/usr/bin/env python3
"""
Benchmark: effect of "the way records are streamed" -- streaming rate
(records/sec) and message/pacing batch size -- on ingestion throughput and
on query latency observed by a concurrent dashboard poller.

This is the assignment's explicitly required "Effect of streaming/message
granularity" investigation (Section 2 Q2, Performance Evaluation), which
is distinct from the worker-count study in benchmark_workers.py.

Usage:
    python3 benchmark_streaming_granularity.py
"""
import statistics
import subprocess
import sys
import threading
import time

import grpc

import log_analytics_pb2 as pb2
import log_analytics_pb2_grpc as pb2_grpc

N_RECORDS = 20000
K, S = 5, 20
BASE_PORT = 50450


def make_dataset(path, n, seed):
    import random
    rng = random.Random(seed)
    with open(path, "w") as f:
        f.write(f"{n} {K} {S}\n")
        for _ in range(n):
            ts = rng.randint(0, n // 10)
            sid = rng.randint(0, S - 1)
            eid = rng.randint(0, 4 * S - 1)
            uid = rng.randint(0, n // 5)
            sc = rng.choice([200, 200, 200, 301, 404, 404, 500])
            rt = rng.uniform(0.5, 500.0)
            by = rng.randint(0, 20000)
            f.write(f"{ts} {sid} {eid} {uid} {sc} {rt:.6f} {by}\n")


def read_records(path):
    with open(path) as f:
        f.readline()
        for line in f:
            line = line.strip()
            if not line:
                continue
            ts, sid, eid, uid, sc, rt, by = line.split()
            yield pb2.LogRecord(
                timestamp=int(ts), server_id=int(sid), endpoint_id=int(eid),
                user_id=int(uid), status_code=int(sc), response_time=float(rt),
                bytes_sent=int(by),
            )


def paced_replay(path, rate, batch_size):
    """Mirrors streaming_client.py's replay() pacing logic exactly, so this
    benchmark measures the same code path the real streaming client uses."""
    sent = 0
    t0 = time.monotonic()
    interval = (1.0 / rate) if rate > 0 else 0.0
    for record in read_records(path):
        yield record
        sent += 1
        if interval > 0 and sent % batch_size == 0:
            target = t0 + sent * interval
            now = time.monotonic()
            if target > now:
                time.sleep(target - now)


def bench_rate_and_granularity(dataset_path, configs):
    """configs: list of (label, rate, batch_size)"""
    print("== Effect of streaming rate and message/pacing batch size ==")
    print(f"{'config':>22} {'target rate':>12} {'batch':>7} {'elapsed_s':>10} {'actual rec/s':>13}")
    port = BASE_PORT
    addr = f"localhost:{port}"
    for label, rate, batch in configs:
        proc = subprocess.Popen(
            [sys.executable, "server.py", addr, str(K), str(S), "4"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(1.2)
        try:
            stub = pb2_grpc.LogAnalyticsServiceStub(grpc.insecure_channel(addr))
            t0 = time.monotonic()
            summary = stub.StreamLogs(paced_replay(dataset_path, rate, batch))
            elapsed = time.monotonic() - t0
            actual_rate = summary.records_ingested / elapsed
            rate_str = "unlimited" if rate <= 0 else str(rate)
            print(f"{label:>22} {rate_str:>12} {batch:>7} {elapsed:>10.3f} {actual_rate:>13.1f}")
        finally:
            proc.terminate()
            proc.wait(timeout=5)


def bench_query_latency_during_streaming(dataset_path, rate):
    """Measures dashboard-style GetAnalytics latency WHILE a stream at the
    given rate is actively being ingested -- the realistic scenario the
    assignment's "analytics queries while ingestion is in progress"
    requirement describes, as opposed to querying a static, fully-loaded
    server (which is what benchmark_workers.py measures)."""
    port = BASE_PORT + 1
    addr = f"localhost:{port}"
    proc = subprocess.Popen(
        [sys.executable, "server.py", addr, str(K), str(S), "4"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(1.2)
    try:
        stub = pb2_grpc.LogAnalyticsServiceStub(grpc.insecure_channel(addr))
        latencies = []
        lock = threading.Lock()
        stop_flag = {"stop": False}

        def query_loop():
            local_stub = pb2_grpc.LogAnalyticsServiceStub(grpc.insecure_channel(addr))
            while not stop_flag["stop"]:
                t0 = time.monotonic()
                local_stub.GetAnalytics(pb2.Empty())
                dt = (time.monotonic() - t0) * 1000
                with lock:
                    latencies.append(dt)
                time.sleep(0.05)

        qt = threading.Thread(target=query_loop, daemon=True)
        qt.start()

        stub.StreamLogs(paced_replay(dataset_path, rate, 50))

        stop_flag["stop"] = True
        qt.join(timeout=2)

        latencies.sort()
        if latencies:
            p50 = statistics.median(latencies)
            p95 = latencies[int(0.95 * len(latencies)) - 1]
            print(f"\n== Dashboard query latency while a {rate} rec/s stream is being ingested ==")
            print(f"  queries observed: {len(latencies)}   p50: {p50:.2f}ms   p95: {p95:.2f}ms")
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def main():
    dataset_path = "test_data/_bench_granularity.txt"
    make_dataset(dataset_path, N_RECORDS, seed=321)

    configs = [
        ("unthrottled", 0, 100),
        ("500 rec/s, batch=1", 500, 1),
        ("500 rec/s, batch=50", 500, 50),
        ("500 rec/s, batch=200", 500, 200),
        ("2000 rec/s, batch=100", 2000, 100),
    ]
    bench_rate_and_granularity(dataset_path, configs)
    bench_query_latency_during_streaming(dataset_path, rate=500)


if __name__ == "__main__":
    main()

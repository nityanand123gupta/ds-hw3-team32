#!/usr/bin/env python3
"""
Benchmark: streaming ingestion throughput vs. number of server-side workers,
and query latency under concurrent load.

Usage:
    python3 benchmark_workers.py
"""
import statistics
import subprocess
import sys
import threading
import time

import grpc

import log_analytics_pb2 as pb2
import log_analytics_pb2_grpc as pb2_grpc

N_RECORDS = 50000
K, S = 5, 20
BASE_PORT = 50350


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


def bench_worker_count(dataset_path, worker_counts):
    print("== Ingestion throughput vs. worker count ==")
    print(f"{'workers':>8} {'elapsed_s':>10} {'rec/s':>12}")
    results = []
    for i, w in enumerate(worker_counts):
        port = BASE_PORT + i
        addr = f"localhost:{port}"
        proc = subprocess.Popen(
            [sys.executable, "server.py", addr, str(K), str(S), str(w)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(1.2)
        try:
            channel = grpc.insecure_channel(addr)
            stub = pb2_grpc.LogAnalyticsServiceStub(channel)
            t0 = time.monotonic()
            summary = stub.StreamLogs(read_records(dataset_path))
            elapsed = time.monotonic() - t0
            rate = summary.records_ingested / elapsed
            print(f"{w:>8} {elapsed:>10.3f} {rate:>12.0f}")
            results.append((w, elapsed, rate))
        finally:
            proc.terminate()
            proc.wait(timeout=5)
    return results


def bench_query_latency(dataset_path, concurrency_levels):
    print("\n== Query latency vs. concurrent query clients (during steady-state, 8 workers) ==")
    port = BASE_PORT + 100
    addr = f"localhost:{port}"
    proc = subprocess.Popen(
        [sys.executable, "server.py", addr, str(K), str(S), "8"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(1.2)
    try:
        channel = grpc.insecure_channel(addr)
        stub = pb2_grpc.LogAnalyticsServiceStub(channel)
        # pre-load some data so GetAnalytics has real work to merge
        stub.StreamLogs(read_records(dataset_path))

        print(f"{'concurrency':>12} {'p50_ms':>10} {'p95_ms':>10} {'p99_ms':>10}")
        for c in concurrency_levels:
            latencies = []
            lock = threading.Lock()

            def worker():
                local_stub = pb2_grpc.LogAnalyticsServiceStub(grpc.insecure_channel(addr))
                for _ in range(20):
                    t0 = time.monotonic()
                    local_stub.GetAnalytics(pb2.Empty())
                    dt = (time.monotonic() - t0) * 1000
                    with lock:
                        latencies.append(dt)

            threads = [threading.Thread(target=worker) for _ in range(c)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            latencies.sort()
            p50 = statistics.median(latencies)
            p95 = latencies[int(0.95 * len(latencies)) - 1]
            p99 = latencies[int(0.99 * len(latencies)) - 1]
            print(f"{c:>12} {p50:>10.2f} {p95:>10.2f} {p99:>10.2f}")
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def main():
    dataset_path = "test_data/_bench_dataset.txt"
    make_dataset(dataset_path, N_RECORDS, seed=123)

    bench_worker_count(dataset_path, [1, 2, 4, 8])
    bench_query_latency(dataset_path, [1, 2, 4, 8])


if __name__ == "__main__":
    main()

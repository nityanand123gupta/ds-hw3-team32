#!/usr/bin/env python3
"""
Automated correctness test for the gRPC streaming analytics system.

Starts the server as a subprocess, streams the assignment's sample dataset
(and a larger generated one) through streaming_client.py's ingestion path
directly (no subprocess needed for the client, just the gRPC stub), queries
GetAnalytics for the final snapshot, formats it with analytics_core, and
diffs it against the ground-truth expected_output.txt / a sequential
recomputation -- the same correctness bar as Sections 1 and Q1.

Also exercises: querying analytics mid-stream (concurrent ingest + query),
and the Reset RPC.

Usage:
    python3 test_streaming_analytics.py
"""
import subprocess
import sys
import threading
import time

import grpc

import log_analytics_pb2 as pb2
import log_analytics_pb2_grpc as pb2_grpc
from analytics_core import PartialStats, format_output
from streaming_client import read_records

ADDRESS = "localhost:50252"
ADDRESS_BIG = "localhost:50253"
PASS = 0
FAIL = 0


def check(name, condition):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}")


def sequential_reference(path, K):
    st = PartialStats()
    for r in read_records(path):
        st.update(r.timestamp, r.server_id, r.endpoint_id, r.status_code,
                   r.response_time, r.bytes_sent)
    return format_output(st, K)


def snapshot_to_text(snap: pb2.AnalyticsSnapshot) -> str:
    lines = [
        f"TOTAL_REQUESTS {snap.total_requests}",
        f"SUCCESSFUL_REQUESTS {snap.successful_requests}",
        f"FAILED_REQUESTS {snap.failed_requests}",
        f"AVERAGE_RESPONSE_TIME {snap.average_response_time:.6f}",
        f"MIN_RESPONSE_TIME {snap.min_response_time:.6f}",
        f"MAX_RESPONSE_TIME {snap.max_response_time:.6f}",
        f"TOTAL_BYTES {snap.total_bytes}",
        f"STATUS_2XX {snap.status_2xx}",
        f"STATUS_3XX {snap.status_3xx}",
        f"STATUS_4XX {snap.status_4xx}",
        f"STATUS_5XX {snap.status_5xx}",
        f"BUSIEST_INTERVAL {snap.busiest_interval_id} {snap.busiest_interval_count}",
        "TOP_SERVERS",
    ]
    for row in snap.top_servers:
        lines.append(f"{row.server_id} {row.request_count} {row.average_response_time:.6f}")
    lines.append("TOP_ENDPOINTS")
    for row in snap.top_endpoints:
        lines.append(f"{row.endpoint_id} {row.request_count} {row.total_bytes}")
    return "\n".join(lines) + "\n"


def main():
    server_proc = subprocess.Popen(
        [sys.executable, "server.py", ADDRESS, "2", "3", "4"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    # Separate server instance for the big-dataset sub-test below, since it
    # needs a different (K, S) than the sample dataset's header specifies.
    server_proc_big = subprocess.Popen(
        [sys.executable, "server.py", ADDRESS_BIG, "3", "10", "4"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    time.sleep(1.5)

    try:
        channel = grpc.insecure_channel(ADDRESS)
        stub = pb2_grpc.LogAnalyticsServiceStub(channel)

        print("== Stream the assignment sample dataset ==")
        with open("test_data/sample_input.txt") as f:
            header = f.readline().split()
        N, K, S = int(header[0]), int(header[1]), int(header[2])

        summary = stub.StreamLogs(read_records("test_data/sample_input.txt"))
        check("all records ingested", summary.records_ingested == N)

        snap = stub.GetAnalytics(pb2.Empty())
        actual = snapshot_to_text(snap)
        with open("test_data/expected_output.txt") as f:
            expected = f.read().replace("\r\n", "\n")
        check("final snapshot matches assignment's expected_output.txt", actual == expected)
        if actual != expected:
            print("---expected---\n" + expected)
            print("---actual---\n" + actual)

        print("== Reset RPC ==")
        stub.Reset(pb2.Empty())
        snap2 = stub.GetAnalytics(pb2.Empty())
        check("state cleared after Reset", snap2.total_requests == 0 and snap2.records_ingested == 0)

        print("== Concurrent ingest + query on a larger generated stream ==")
        # Build a bigger synthetic dataset in-process (no need for the C++
        # generator here) and confirm querying mid-stream doesn't crash and
        # the final result still matches a sequential recomputation.
        import random
        rng = random.Random(7)
        big_path = "test_data/_tmp_big.txt"
        NB, KB, SB = 20000, 3, 10
        with open(big_path, "w") as f:
            f.write(f"{NB} {KB} {SB}\n")
            for i in range(NB):
                ts = rng.randint(0, 3000)
                sid = rng.randint(0, SB - 1)
                eid = rng.randint(0, 4 * SB - 1)
                uid = rng.randint(0, 1000)
                sc = rng.choice([200, 201, 301, 404, 500])
                rt = rng.uniform(0.5, 500.0)
                by = rng.randint(0, 20000)
                f.write(f"{ts} {sid} {eid} {uid} {sc} {rt:.6f} {by}\n")

        expected_big = sequential_reference(big_path, KB)

        query_errors = []

        def query_loop(stop_flag):
            while not stop_flag["stop"]:
                try:
                    stub.GetAnalytics(pb2.Empty())
                except grpc.RpcError as e:
                    query_errors.append(e)
                time.sleep(0.01)

        stub_big = pb2_grpc.LogAnalyticsServiceStub(grpc.insecure_channel(ADDRESS_BIG))

        def query_loop(stop_flag):
            while not stop_flag["stop"]:
                try:
                    stub_big.GetAnalytics(pb2.Empty())
                except grpc.RpcError as e:
                    query_errors.append(e)
                time.sleep(0.01)

        stop_flag = {"stop": False}
        qt = threading.Thread(target=query_loop, args=(stop_flag,), daemon=True)
        qt.start()

        stub_big.StreamLogs(read_records(big_path))

        stop_flag["stop"] = True
        qt.join(timeout=2)

        check("no errors from concurrent GetAnalytics during ingestion", len(query_errors) == 0)

        final_snap = stub_big.GetAnalytics(pb2.Empty())
        final_text = snapshot_to_text(final_snap)
        check("20000-record stream matches sequential recomputation", final_text == expected_big)

        print(f"\n{PASS} passed, {FAIL} failed")
        sys.exit(1 if FAIL else 0)

    finally:
        for proc in (server_proc, server_proc_big):
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    main()

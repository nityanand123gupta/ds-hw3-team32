#!/usr/bin/env python3
"""
Streaming ingestion client: reads a pre-generated dataset (the same
"N K S" + record format used by the sequential/MPI/Hadoop implementations)
and replays its records over gRPC as if they were arriving live.

Usage:
    python3 streaming_client.py <server_host:port> <dataset_file> [--rate N] [--batch-size N]

    --rate N        target records/sec (0 = as fast as possible, default 0)
    --batch-size N  sleep once per N records instead of once per record,
                    reducing timer overhead at high rates (default 100)
"""
import argparse
import sys
import time

import grpc

import log_analytics_pb2 as pb2
import log_analytics_pb2_grpc as pb2_grpc


def read_records(path):
    with open(path) as f:
        f.readline()  # "N K S" header, not part of the record stream
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


def replay(path, rate, batch_size):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("server_addr")
    ap.add_argument("dataset_file")
    ap.add_argument("--rate", type=float, default=0.0, help="records/sec, 0 = unthrottled")
    ap.add_argument("--batch-size", type=int, default=100)
    args = ap.parse_args()

    with open(args.dataset_file) as f:
        header = f.readline().split()
        N, K, S = int(header[0]), int(header[1]), int(header[2])
    print(f"[StreamingClient] Dataset header: N={N} K={K} S={S}")
    print(f"[StreamingClient] Replaying to {args.server_addr} "
          f"(rate={'unlimited' if args.rate <= 0 else args.rate} rec/s)")

    channel = grpc.insecure_channel(args.server_addr)
    stub = pb2_grpc.LogAnalyticsServiceStub(channel)

    t_start = time.monotonic()
    summary = stub.StreamLogs(replay(args.dataset_file, args.rate, args.batch_size))
    elapsed = time.monotonic() - t_start

    print(f"[StreamingClient] Ingested {summary.records_ingested} records in "
          f"{elapsed:.3f}s ({summary.records_ingested / elapsed:.1f} rec/s)")


if __name__ == "__main__":
    main()

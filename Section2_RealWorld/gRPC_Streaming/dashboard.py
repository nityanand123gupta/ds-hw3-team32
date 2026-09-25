#!/usr/bin/env python3
"""
CLI dashboard: polls GetAnalytics and displays the current state while a
stream is being ingested.

Usage:
    python3 dashboard.py <server_host:port> [--interval SECONDS]
"""
import argparse
import os
import time

import grpc

import log_analytics_pb2 as pb2
import log_analytics_pb2_grpc as pb2_grpc


def render(snap: pb2.AnalyticsSnapshot):
    os.system("cls" if os.name == "nt" else "clear")
    avg = snap.average_response_time
    print("=" * 60)
    print(" SERVER LOG ANALYTICS - LIVE DASHBOARD")
    print("=" * 60)
    print(f" Records ingested so far : {snap.records_ingested}")
    print(f" Total requests          : {snap.total_requests}")
    print(f" Successful / Failed     : {snap.successful_requests} / {snap.failed_requests}")
    print(f" Avg / Min / Max latency : {avg:.3f} / {snap.min_response_time:.3f} / {snap.max_response_time:.3f} ms")
    print(f" Total bytes sent        : {snap.total_bytes}")
    print(f" Status 2xx/3xx/4xx/5xx  : {snap.status_2xx}/{snap.status_3xx}/{snap.status_4xx}/{snap.status_5xx}")
    print(f" Busiest interval        : #{snap.busiest_interval_id} ({snap.busiest_interval_count} reqs)")
    print("-" * 60)
    print(" TOP SERVERS (id, count, avg_response_time)")
    for row in snap.top_servers:
        print(f"   {row.server_id:>6}  {row.request_count:>8}  {row.average_response_time:.3f}")
    print("-" * 60)
    print(" TOP ENDPOINTS (id, count, total_bytes)")
    for row in snap.top_endpoints:
        print(f"   {row.endpoint_id:>6}  {row.request_count:>8}  {row.total_bytes}")
    print("=" * 60)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("server_addr")
    ap.add_argument("--interval", type=float, default=1.0)
    args = ap.parse_args()

    channel = grpc.insecure_channel(args.server_addr)
    stub = pb2_grpc.LogAnalyticsServiceStub(channel)

    try:
        while True:
            snap = stub.GetAnalytics(pb2.Empty())
            render(snap)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[Dashboard] Stopped.")


if __name__ == "__main__":
    main()

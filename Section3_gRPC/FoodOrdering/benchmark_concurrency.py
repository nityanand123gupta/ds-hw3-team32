#!/usr/bin/env python3
"""
Benchmark: PlaceOrder throughput and latency under increasing concurrent
customer load.

Usage:
    python3 benchmark_concurrency.py
"""
import statistics
import subprocess
import sys
import threading
import time

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

ADDRESS = "localhost:50351"


def worker(addr, n_orders, latencies, lock):
    stub = pb2_grpc.FoodOrderingServiceStub(grpc.insecure_channel(addr))
    for _ in range(n_orders):
        t0 = time.monotonic()
        stub.PlaceOrder(pb2.OrderRequest(
            customer_id="bench",
            restaurant_name="Pizza House",
            items=[pb2.OrderItemRequest(item_name="Margherita Pizza", quantity=1)],
        ))
        dt = (time.monotonic() - t0) * 1000
        with lock:
            latencies.append(dt)


def main():
    proc = subprocess.Popen(
        [sys.executable, "server.py", ADDRESS],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(1.2)
    try:
        print(f"{'clients':>8} {'orders':>8} {'elapsed_s':>10} {'orders/s':>10} {'p50_ms':>8} {'p95_ms':>8}")
        for n_clients in [1, 5, 10, 20]:
            orders_per_client = 50
            latencies = []
            lock = threading.Lock()
            threads = [
                threading.Thread(target=worker, args=(ADDRESS, orders_per_client, latencies, lock))
                for _ in range(n_clients)
            ]
            t0 = time.monotonic()
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            elapsed = time.monotonic() - t0
            total_orders = n_clients * orders_per_client
            rate = total_orders / elapsed
            latencies.sort()
            p50 = statistics.median(latencies)
            p95 = latencies[int(0.95 * len(latencies)) - 1]
            print(f"{n_clients:>8} {total_orders:>8} {elapsed:>10.3f} {rate:>10.1f} {p50:>8.2f} {p95:>8.2f}")
    finally:
        proc.terminate()
        proc.wait(timeout=5)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Real-Time Server Log Analytics gRPC server.

Usage:
    python3 server.py <host:port> <K> <S> [num_workers]
    python3 server.py localhost:50052 5 20 4

K and S come from the same "N K S" header used by the sequential/MPI/Hadoop
implementations, so the final snapshot after a full stream matches their
output exactly.

Architecture: incoming records are sharded round-robin across `num_workers`
independent PartialStats accumulators, each behind its own lock. This is
the "multiple analytics workers" the assignment asks for: ingestion never
contends on a single global lock, and a GetAnalytics query briefly locks
each worker in turn to fold its partial state into a merged snapshot,
without blocking ingestion into the other workers.
"""
import sys
import threading
from concurrent import futures

import grpc

import log_analytics_pb2 as pb2
import log_analytics_pb2_grpc as pb2_grpc
from analytics_core import PartialStats, busiest_interval, top_endpoints, top_servers


class Worker:
    def __init__(self):
        self.lock = threading.Lock()
        self.stats = PartialStats()

    def ingest(self, r: pb2.LogRecord):
        with self.lock:
            self.stats.update(r.timestamp, r.server_id, r.endpoint_id,
                               r.status_code, r.response_time, r.bytes_sent)

    def merge_into(self, target: PartialStats):
        with self.lock:
            self.stats.merge_into(target)

    def reset(self):
        with self.lock:
            self.stats = PartialStats()


class LogAnalyticsServicer(pb2_grpc.LogAnalyticsServiceServicer):
    def __init__(self, K: int, S: int, num_workers: int):
        self.K = K
        self.S = S
        self.workers = [Worker() for _ in range(num_workers)]
        self._rr_lock = threading.Lock()
        self._rr_counter = 0
        self.records_ingested = 0
        self._count_lock = threading.Lock()

    def _next_worker(self) -> Worker:
        with self._rr_lock:
            w = self.workers[self._rr_counter % len(self.workers)]
            self._rr_counter += 1
        return w

    # ---------------- StreamLogs (client streaming ingestion) ----------------
    def StreamLogs(self, request_iterator, context):
        count = 0
        for record in request_iterator:
            worker = self._next_worker()
            worker.ingest(record)
            count += 1
        with self._count_lock:
            self.records_ingested += count
        return pb2.IngestSummary(records_ingested=count)

    # ---------------- GetAnalytics ----------------
    def GetAnalytics(self, request, context):
        merged = PartialStats()
        for w in self.workers:
            w.merge_into(merged)

        avg = merged.sum_response_time / merged.total if merged.total > 0 else 0.0
        mn = merged.min_response_time if merged.total > 0 else 0.0
        mx = merged.max_response_time if merged.total > 0 else 0.0
        bi, bc = busiest_interval(merged.intervals)

        snapshot = pb2.AnalyticsSnapshot(
            records_ingested=self.records_ingested,
            total_requests=merged.total,
            successful_requests=merged.success,
            failed_requests=merged.failed,
            average_response_time=avg,
            min_response_time=mn,
            max_response_time=mx,
            total_bytes=merged.total_bytes,
            status_2xx=merged.status2xx,
            status_3xx=merged.status3xx,
            status_4xx=merged.status4xx,
            status_5xx=merged.status5xx,
            busiest_interval_id=bi,
            busiest_interval_count=bc,
        )
        for sid, cnt, avg_rt in top_servers(merged.servers, self.K):
            snapshot.top_servers.add(server_id=sid, request_count=cnt, average_response_time=avg_rt)
        for eid, cnt, by in top_endpoints(merged.endpoints, self.K):
            snapshot.top_endpoints.add(endpoint_id=eid, request_count=cnt, total_bytes=by)
        return snapshot

    # ---------------- Reset ----------------
    def Reset(self, request, context):
        for w in self.workers:
            w.reset()
        with self._count_lock:
            self.records_ingested = 0
        return pb2.Ack(success=True, message="Analytics state reset.")


def serve(address, K, S, num_workers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max(8, num_workers + 4)))
    pb2_grpc.add_LogAnalyticsServiceServicer_to_server(
        LogAnalyticsServicer(K, S, num_workers), server)
    server.add_insecure_port(address)
    server.start()
    print(f"[Server] Log Analytics gRPC server listening on {address} "
          f"(K={K}, S={S}, workers={num_workers})")
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 server.py <host:port> <K> <S> [num_workers]")
        sys.exit(1)
    addr = sys.argv[1]
    K = int(sys.argv[2])
    S = int(sys.argv[3])
    num_workers = int(sys.argv[4]) if len(sys.argv) > 4 else 4
    serve(addr, K, S, num_workers)

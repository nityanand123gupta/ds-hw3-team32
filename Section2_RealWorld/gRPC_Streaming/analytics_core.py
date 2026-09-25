"""
Server log analytics aggregation logic, mirroring analytics_common.hpp
(HW2 / Section 2 Q1) field-for-field so that the same input always produces
the same numbers regardless of which paradigm processed it (sequential /
MPI / Hadoop MapReduce / gRPC streaming).

A PartialStats accumulates one shard's worth of records; merge() combines
two shards with the same associative operations analytics_common.hpp uses
(sum, min, max, dict-merge), so the gRPC server can maintain one PartialStats
per worker thread and cheaply merge them on demand for a snapshot.
"""
import math


class PartialStats:
    __slots__ = (
        "total", "success", "failed",
        "sum_response_time", "min_response_time", "max_response_time",
        "total_bytes", "status2xx", "status3xx", "status4xx", "status5xx",
        "servers", "endpoints", "intervals",
    )

    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.sum_response_time = 0.0
        self.min_response_time = math.inf
        self.max_response_time = -math.inf
        self.total_bytes = 0
        self.status2xx = 0
        self.status3xx = 0
        self.status4xx = 0
        self.status5xx = 0
        self.servers = {}    # server_id -> [count, sum_response_time]
        self.endpoints = {}  # endpoint_id -> [count, bytes]
        self.intervals = {}  # interval_id -> count

    def update(self, timestamp, server_id, endpoint_id, status_code, response_time, bytes_sent):
        self.total += 1
        if status_code < 400:
            self.success += 1
        else:
            self.failed += 1

        self.sum_response_time += response_time
        if response_time < self.min_response_time:
            self.min_response_time = response_time
        if response_time > self.max_response_time:
            self.max_response_time = response_time

        self.total_bytes += bytes_sent

        bucket = status_code // 100
        if bucket == 2:
            self.status2xx += 1
        elif bucket == 3:
            self.status3xx += 1
        elif bucket == 4:
            self.status4xx += 1
        elif bucket == 5:
            self.status5xx += 1

        srv = self.servers.get(server_id)
        if srv is None:
            self.servers[server_id] = [1, response_time]
        else:
            srv[0] += 1
            srv[1] += response_time

        ep = self.endpoints.get(endpoint_id)
        if ep is None:
            self.endpoints[endpoint_id] = [1, bytes_sent]
        else:
            ep[0] += 1
            ep[1] += bytes_sent

        interval = timestamp // 60
        self.intervals[interval] = self.intervals.get(interval, 0) + 1

    def merge_into(self, target: "PartialStats"):
        """Adds self's contents into target (target is mutated)."""
        target.total += self.total
        target.success += self.success
        target.failed += self.failed
        target.sum_response_time += self.sum_response_time
        if self.min_response_time < target.min_response_time:
            target.min_response_time = self.min_response_time
        if self.max_response_time > target.max_response_time:
            target.max_response_time = self.max_response_time
        target.total_bytes += self.total_bytes
        target.status2xx += self.status2xx
        target.status3xx += self.status3xx
        target.status4xx += self.status4xx
        target.status5xx += self.status5xx

        for sid, (cnt, rt) in self.servers.items():
            s = target.servers.get(sid)
            if s is None:
                target.servers[sid] = [cnt, rt]
            else:
                s[0] += cnt
                s[1] += rt

        for eid, (cnt, by) in self.endpoints.items():
            e = target.endpoints.get(eid)
            if e is None:
                target.endpoints[eid] = [cnt, by]
            else:
                e[0] += cnt
                e[1] += by

        for ivl, cnt in self.intervals.items():
            target.intervals[ivl] = target.intervals.get(ivl, 0) + cnt


def top_servers(servers: dict, K: int):
    rows = [(sid, cnt, rt / cnt) for sid, (cnt, rt) in servers.items() if cnt > 0]
    rows.sort(key=lambda r: (-r[1], r[0]))
    if K >= 0:
        rows = rows[:K]
    return rows


def top_endpoints(endpoints: dict, K: int):
    rows = [(eid, cnt, by) for eid, (cnt, by) in endpoints.items()]
    rows.sort(key=lambda r: (-r[1], r[0]))
    if K >= 0:
        rows = rows[:K]
    return rows


def busiest_interval(intervals: dict):
    best_id, best_count = 0, 0
    first = True
    for ivl, cnt in intervals.items():
        if first or cnt > best_count or (cnt == best_count and ivl < best_id):
            best_id, best_count = ivl, cnt
            first = False
    return best_id, best_count


def format_output(st: PartialStats, K: int) -> str:
    """Produces the exact assignment output format, matching
    analytics_common.hpp::formatOutput byte-for-byte."""
    avg = st.sum_response_time / st.total if st.total > 0 else 0.0
    mn = st.min_response_time if st.total > 0 else 0.0
    mx = st.max_response_time if st.total > 0 else 0.0

    lines = [
        f"TOTAL_REQUESTS {st.total}",
        f"SUCCESSFUL_REQUESTS {st.success}",
        f"FAILED_REQUESTS {st.failed}",
        f"AVERAGE_RESPONSE_TIME {avg:.6f}",
        f"MIN_RESPONSE_TIME {mn:.6f}",
        f"MAX_RESPONSE_TIME {mx:.6f}",
        f"TOTAL_BYTES {st.total_bytes}",
        f"STATUS_2XX {st.status2xx}",
        f"STATUS_3XX {st.status3xx}",
        f"STATUS_4XX {st.status4xx}",
        f"STATUS_5XX {st.status5xx}",
    ]
    bi, bc = busiest_interval(st.intervals)
    lines.append(f"BUSIEST_INTERVAL {bi} {bc}")

    lines.append("TOP_SERVERS")
    for sid, cnt, avg_rt in top_servers(st.servers, K):
        lines.append(f"{sid} {cnt} {avg_rt:.6f}")

    lines.append("TOP_ENDPOINTS")
    for eid, cnt, by in top_endpoints(st.endpoints, K):
        lines.append(f"{eid} {cnt} {by}")

    return "\n".join(lines) + "\n"

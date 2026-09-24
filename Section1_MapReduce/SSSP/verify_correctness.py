#!/usr/bin/env python3
"""
Correctness checker: compares a MapReduce SSSP output file against a
reference sequential Dijkstra computation on the same raw graph input.

Usage:
    python3 verify_correctness.py <graph_input.txt> <mapreduce_output.txt>
"""
import heapq
import sys

INF = float("inf")


def dijkstra(V, adj, source=0):
    dist = [INF] * V
    dist[source] = 0
    pq = [(0, source)]
    visited = [False] * V
    while pq:
        d, u = heapq.heappop(pq)
        if visited[u]:
            continue
        visited[u] = True
        for v, w in adj[u]:
            nd = d + w
            if nd < dist[v]:
                dist[v] = nd
                heapq.heappush(pq, (nd, v))
    return dist


def main():
    graph_file, mr_output_file = sys.argv[1], sys.argv[2]

    with open(graph_file) as f:
        data = f.read().split()
    idx = 0
    V = int(data[idx]); idx += 1
    E = int(data[idx]); idx += 1
    adj = [[] for _ in range(V)]
    for _ in range(E):
        u = int(data[idx]); idx += 1
        v = int(data[idx]); idx += 1
        w = int(data[idx]); idx += 1
        adj[u].append((v, w))

    expected = dijkstra(V, adj)

    actual = {}
    with open(mr_output_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            node, dist = line.split()
            actual[int(node)] = INF if dist == "INF" else int(dist)

    mismatches = []
    for node in range(V):
        exp = expected[node]
        act = actual.get(node)
        if act is None:
            mismatches.append((node, exp, "MISSING"))
        elif exp != act:
            mismatches.append((node, exp, act))

    if mismatches:
        print(f"FAIL: {len(mismatches)} mismatches out of {V} nodes")
        for node, exp, act in mismatches[:20]:
            print(f"  node {node}: expected {exp}, got {act}")
        sys.exit(1)
    else:
        print(f"PASS: all {V} node distances match reference Dijkstra")


if __name__ == "__main__":
    main()

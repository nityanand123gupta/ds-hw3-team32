#!/usr/bin/env python3
"""
Builds the initial MapReduce state file from the problem's raw graph input.

Raw input format:
    V E
    u1 v1 w1
    ...
    uE vE wE

Output (one line per node, tab-separated), which is exactly the format
mapper.py / reducer.py operate on:
    node_id <TAB> distance <TAB> adj_list

Node 0 (the source) starts at distance 0; every other node starts at INF.
"""
import sys

INF = 10 ** 15


def main():
    data = sys.stdin.read().split()
    idx = 0
    V = int(data[idx]); idx += 1
    E = int(data[idx]); idx += 1

    adj = [[] for _ in range(V)]
    for _ in range(E):
        u = int(data[idx]); idx += 1
        v = int(data[idx]); idx += 1
        w = int(data[idx]); idx += 1
        adj[u].append((v, w))

    for node in range(V):
        dist = 0 if node == 0 else INF
        adj_str = ",".join(f"{v}:{w}" for v, w in adj[node])
        print(f"{node}\t{dist}\t{adj_str}")


if __name__ == "__main__":
    main()

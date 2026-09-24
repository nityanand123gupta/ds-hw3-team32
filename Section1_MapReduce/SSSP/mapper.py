#!/usr/bin/env python3
"""
SSSP Mapper (iterative MapReduce, Bellman-Ford relaxation style).

Input line format (per node, tab-separated):
    node_id <TAB> distance <TAB> adj_list

    adj_list = "v1:w1,v2:w2,..."   (empty string if no outgoing edges)

For every input line the mapper:
  1. Re-emits the node's own structure record (tagged 'S') so the graph
     topology (adjacency list) survives into the next iteration.
  2. If the node's current distance is finite, relaxes every outgoing
     edge and emits a candidate distance (tagged 'D') for each neighbor.
"""
import sys

INF = 10 ** 15


def parse_adj(adj_str):
    if not adj_str:
        return []
    edges = []
    for part in adj_str.split(","):
        v, w = part.split(":")
        edges.append((v, int(w)))
    return edges


def main():
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line:
            continue
        node, dist_s, adj_s = line.split("\t")
        dist = int(dist_s)

        # Structure record: keeps adjacency list alive for next round.
        print(f"{node}\tS\t{dist}\t{adj_s}")

        if dist < INF:
            for v, w in parse_adj(adj_s):
                nd = dist + w
                if nd < INF:
                    print(f"{v}\tD\t{nd}")


if __name__ == "__main__":
    main()

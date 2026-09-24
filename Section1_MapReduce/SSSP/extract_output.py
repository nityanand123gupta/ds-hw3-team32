#!/usr/bin/env python3
"""
Formats a final state file into the required SSSP output:
    node_id shortest_distance
sorted by node_id ascending, with unreachable nodes printed as INF.
"""
import sys

INF = 10 ** 15


def main():
    rows = []
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line:
            continue
        node, dist, *_ = line.split("\t")
        rows.append((int(node), int(dist)))

    rows.sort(key=lambda x: x[0])
    for node, dist in rows:
        d = "INF" if dist >= INF else dist
        print(f"{node} {d}")


if __name__ == "__main__":
    main()

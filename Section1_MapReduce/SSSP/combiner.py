#!/usr/bin/env python3
"""
SSSP Combiner.

Runs on locally-sorted mapper output (sorted lexicographically by node_id,
so all lines for the same node_id are contiguous). For consecutive 'D'
(candidate distance) records belonging to the same node, only the minimum
is forwarded -- this is the standard MapReduce combiner optimization and
reduces data volume before the global shuffle/sort. 'S' (structure)
records are passed through untouched.
"""
import sys


def main():
    cur_key = None
    cur_min = None

    def flush():
        if cur_key is not None:
            print(f"{cur_key}\tD\t{cur_min}")

    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t")
        node, tag = parts[0], parts[1]

        if tag == "S":
            flush()
            cur_key = None
            cur_min = None
            print(line)
            continue

        # tag == "D"
        dist = int(parts[2])
        if cur_key == node:
            if dist < cur_min:
                cur_min = dist
        else:
            flush()
            cur_key = node
            cur_min = dist

    flush()


if __name__ == "__main__":
    main()

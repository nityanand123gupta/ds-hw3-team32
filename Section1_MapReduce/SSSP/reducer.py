#!/usr/bin/env python3
"""
SSSP Reducer.

Input is globally sorted, so every record for a given node_id is
contiguous: exactly one 'S' (structure) record carrying its adjacency
list, plus zero or more 'D' (candidate distance) records from neighbors
that relaxed an edge toward it this round.

For each node, the reducer outputs the minimum of its previous distance
and all candidate distances, paired with its (unchanged) adjacency list --
i.e. a new state record in the same format consumed by the mapper, ready
to be fed into the next iteration.
"""
import sys

INF = 10 ** 15


def main():
    cur_node = None
    cur_dist = INF
    cur_adj = ""

    def flush():
        if cur_node is not None:
            print(f"{cur_node}\t{cur_dist}\t{cur_adj}")

    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t")
        node, tag = parts[0], parts[1]

        if node != cur_node:
            flush()
            cur_node = node
            cur_dist = INF
            cur_adj = ""

        if tag == "S":
            s_dist = int(parts[2])
            cur_adj = parts[3] if len(parts) > 3 else ""
            if s_dist < cur_dist:
                cur_dist = s_dist
        else:  # tag == "D"
            d = int(parts[2])
            if d < cur_dist:
                cur_dist = d

    flush()


if __name__ == "__main__":
    main()

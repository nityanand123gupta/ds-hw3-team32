#!/usr/bin/env python3
"""
Reproducible random weighted directed graph generator for SSSP benchmarking.

Usage:
    python3 generate_graph.py <V> <E> <seed> > graph.txt

Guarantees every node 1..V-1 is reachable from node 0 by first generating
a random spanning structure out of node 0, then adding (E - (V-1)) extra
random edges.
"""
import random
import sys


def main():
    if len(sys.argv) != 4:
        print("Usage: generate_graph.py <V> <E> <seed>", file=sys.stderr)
        sys.exit(1)

    V = int(sys.argv[1])
    E = int(sys.argv[2])
    seed = int(sys.argv[3])
    rng = random.Random(seed)

    edges = []

    # Random spanning structure rooted at 0 so every node is reachable.
    nodes = list(range(1, V))
    rng.shuffle(nodes)
    connected = [0]
    for v in nodes:
        u = rng.choice(connected)
        w = rng.randint(1, 1000)
        edges.append((u, v, w))
        connected.append(v)

    # Extra random edges up to E.
    while len(edges) < E:
        u = rng.randint(0, V - 1)
        v = rng.randint(0, V - 1)
        if u == v:
            continue
        w = rng.randint(1, 1000)
        edges.append((u, v, w))

    print(V, len(edges))
    for u, v, w in edges:
        print(u, v, w)


if __name__ == "__main__":
    main()

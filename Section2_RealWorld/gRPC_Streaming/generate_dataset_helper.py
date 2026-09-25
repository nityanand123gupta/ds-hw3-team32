#!/usr/bin/env python3
"""Tiny reproducible dataset generator (pure Python, no build step needed),
used by the RCE multi-node demo. Writes to stdout."""
import random
import sys

N, K, S = 2000, 5, 20
seed = int(sys.argv[1]) if len(sys.argv) > 1 else 42
rng = random.Random(seed)

print(f"{N} {K} {S}")
for _ in range(N):
    ts = rng.randint(0, 600)
    sid = rng.randint(0, S - 1)
    eid = rng.randint(0, 4 * S - 1)
    uid = rng.randint(0, N // 5)
    sc = rng.choice([200, 200, 200, 301, 404, 404, 500])
    rt = rng.uniform(0.5, 500.0)
    by = rng.randint(0, 20000)
    print(f"{ts} {sid} {eid} {uid} {sc} {rt:.6f} {by}")

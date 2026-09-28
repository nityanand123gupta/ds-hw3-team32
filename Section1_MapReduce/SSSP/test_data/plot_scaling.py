#!/usr/bin/env python3
"""Generates speedup/efficiency plots from scaling_summary.csv."""
import csv

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

rows = []
with open("scaling_summary.csv") as f:
    for r in csv.DictReader(f):
        rows.append({k: float(v) for k, v in r.items()})

nodes = [r["nodes"] for r in rows]
speedup = [r["speedup"] for r in rows]
efficiency = [r["efficiency"] for r in rows]
ideal = nodes

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))

ax1.plot(nodes, speedup, "o-", label="Measured speedup")
ax1.plot(nodes, ideal, "k--", alpha=0.4, label="Ideal (linear) speedup")
ax1.set_xlabel("Number of SLURM nodes")
ax1.set_ylabel("Speedup (T1 / TN)")
ax1.set_title("SSSP MapReduce: Speedup vs Nodes\n(V=1500, E=5000)")
ax1.legend()
ax1.grid(alpha=0.3)

ax2.plot(nodes, efficiency, "o-", color="tab:orange")
ax2.axhline(1.0, color="k", linestyle="--", alpha=0.4)
ax2.set_xlabel("Number of SLURM nodes")
ax2.set_ylabel("Efficiency (Speedup / N)")
ax2.set_title("SSSP MapReduce: Efficiency vs Nodes")
ax2.grid(alpha=0.3)

fig.tight_layout()
fig.savefig("scaling_plot.png", dpi=150)
print("Wrote scaling_plot.png")

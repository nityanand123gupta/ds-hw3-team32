# Section 1 - Q2: Single Source Shortest Path (SSSP) via Iterative MapReduce

Team 32 - Nityanand Gupta (2024101147), A V Aditya (2024111031)

## Algorithm

Iterative Bellman-Ford relaxation expressed as repeated MapReduce rounds.
Each node's state is a record:

```
node_id <TAB> distance <TAB> adj_list
```

where `adj_list` is `v1:w1,v2:w2,...` (its outgoing edges). Node 0 (source)
starts at distance 0; every other node starts at `INF` (sentinel `10**15`).

Each round:

- **Map**: for every node record, re-emit its own structure (`S`) record
  so the adjacency list survives to the next round. If its distance is
  finite, relax every outgoing edge and emit a candidate distance (`D`
  record) to each neighbor: `neighbor, dist + weight`.
- **Combine**: locally collapse multiple `D` candidates for the same node
  down to their minimum (standard MapReduce combiner optimization);
  `S` records pass through untouched.
- **Reduce**: for each node, take the minimum of its previous distance
  and all incoming `D` candidates, and re-attach its (unchanged)
  adjacency list -> this becomes next round's input record.

By the Bellman-Ford theorem, after `V-1` rounds every shortest path
(which uses at most `V-1` edges) has been discovered. The driver caps
iterations at `V-1` and also stops early if a round produces no distance
changes.

## Files

| File | Purpose |
|---|---|
| `mapper.py` | Map phase (relax edges) |
| `combiner.py` | Local pre-aggregation (min per node) |
| `reducer.py` | Reduce phase (global min, carry adjacency forward) |
| `init_state.py` | Converts raw `V E / u v w` input into round-0 state |
| `extract_output.py` | Formats final state into `node_id distance` (INF for unreachable), sorted |
| `generate_graph.py` | Reproducible random weighted graph generator for benchmarking |
| `verify_correctness.py` | Cross-checks MapReduce output against a reference sequential Dijkstra |
| `run_sssp_local.sh` | Local driver: repeatedly invokes `../../MapreduceForLocalTesting.sh` |
| `run_sssp_distributed.sh` | SLURM/RCE driver: repeatedly runs the distributed split/srun/sort pipeline (same stages as `Mapreduce_distributed.sh`) |
| `test_data/` | Sample input/output from the assignment PDF + generated test graphs |

## Running locally

```bash
cd Section1_MapReduce/SSSP
bash run_sssp_local.sh test_data/sample_input.txt test_data/sample_actual_output.txt
```

Verified against the assignment's sample (V=4):

```
0 0
1 3
2 2
3 7
```

Also verified on generated graphs up to V=1500 against a reference
Dijkstra implementation (`verify_correctness.py`) - all distances match.

Unreachable nodes are correctly reported as `INF`
(see `test_data/unreachable_input.txt`).

## Running on the RCE SLURM cluster

```bash
ssh <username>@rce.iiit.ac.in
cd ~/HW3/Section1_MapReduce/SSSP
sbatch run_sssp_distributed.sh test_data/large_graph.txt
```

This requests 4 nodes/tasks (edit `#SBATCH --nodes` / `--ntasks` as
needed), splits each round's state file across tasks, runs the
mapper/combiner distributedly via `srun`, does the shuffle/sort
centrally, and loops until convergence or the `V-1` bound. Per-round
timings (mapper / shuffle / combiner / shuffle / reducer / total) are
written to `perf_results/sssp_dist_benchmark_summary.csv` for the
report's performance section.

### Verified on the actual RCE cluster

Both runs below were executed for real via `salloc` + `run_sssp_distributed.sh`
on the RCE SLURM cluster (not just locally):

- **Sample graph (V=4)**, 2 nodes/tasks (`node06`,`node07`): completed in
  3 rounds, output `0 0 / 1 3 / 2 2 / 3 7` - exact match with the
  assignment's expected output.
- **Generated graph (V=1500, E=5000)**, 4 nodes/tasks
  (`node01-03,node06`): converged in 14 rounds (well under the
  `V-1=1499` worst-case bound); `verify_correctness.py` confirmed
  `PASS: all 1500 node distances match reference Dijkstra`.
  Per-round timing breakdown saved in
  `test_data/rce_dist_benchmark_summary.csv`.

This confirms the pipeline genuinely runs across multiple SLURM-allocated
compute nodes (mapper/combiner stages executed via `srun` on separate
nodes), not just as a local simulation.

## Scaling study: execution time, speedup, efficiency

Same fixed dataset (V=1500, E=5000, `test_data/large_graph.txt`) run at
1, 2, 4, and 6 SLURM nodes/tasks, each via
`salloc --nodes=N --ntasks=N bash run_sssp_distributed.sh test_data/large_graph.txt`.
"MapReduce time" is the sum of the `total_time_s` column across all 14
convergence rounds (i.e. actual mapper+shuffle+combiner+shuffle+reducer
work), excluding SLURM queue/allocation overhead; raw per-run data is in
`test_data/scaling_summary.csv` (regenerate the plot with
`python3 test_data/plot_scaling.py`).

| Nodes (N) | MapReduce time (s) | Speedup (T1/TN) | Efficiency (Speedup/N) |
|---|---|---|---|
| 1 | 2.920 | 1.000 | 1.000 |
| 2 | 7.508 | 0.389 | 0.194 |
| 4 | 8.465 | 0.345 | 0.086 |
| 6 | 8.375 | 0.349 | 0.058 |

![Speedup and efficiency vs node count](test_data/scaling_plot.png)

**Analysis - why this doesn't speed up (communication vs. computation):**
at V=1500/E=5000, each round's actual per-record work (parsing a state
line, relaxing a handful of edges, taking a min) is on the order of
microseconds. Each round's wall time is instead dominated by `srun`
process-launch and task-coordination overhead - launching N tasks,
having them each open/read/write their chunk files, and rejoining for
the central sort. That per-round coordination cost **grows** with N
(more tasks to spawn and synchronize), while the useful work per task
**shrinks** with N (each task handles a smaller slice of a
constant-size problem). This is a textbook case of Amdahl's-law-style
overhead: the parallel portion of the work is far smaller than the fixed
per-round coordination cost at this problem size, so adding nodes makes
the round slower, not faster - hence efficiency well below 1 and falling
as N grows. This is a fixed-cost-per-round approach: it is not that
MapReduce itself is unsuitable, but that this problem size is far too
small for `srun`-per-round orchestration to pay for itself. It would be
expected to amortize better either with a much larger graph (so
per-task compute dominates the fixed launch cost) or with a real
long-lived MapReduce runtime (e.g. actual Hadoop, where worker JVMs
persist across the job instead of being re-spawned by `srun` every
round).

## Generating benchmark datasets

```bash
python3 generate_graph.py <V> <E> <seed> > test_data/graph_V_E.txt
```

Guarantees every node is reachable from node 0 (random spanning
structure + extra random edges), so runs terminate meaningfully within
the `V-1` bound.

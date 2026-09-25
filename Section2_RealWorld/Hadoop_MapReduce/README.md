# Section 2 - Q1: Server Log Analytics via Hadoop MapReduce (C++ / Hadoop Streaming)

Team 32 - Nityanand Gupta (2024101147), A V Aditya (2024111031)

Continues the HW2 Q7 "Large-Scale Server Log Analytics" problem (see
`../../HW2_reference` for the original problem statement, sequential and
MPI implementations), now re-implemented as a Hadoop Streaming job.

## Design

Two MapReduce stages, matching the assignment's "no prescribed
decomposition, justify your design" guidance:

**Stage 1 (distributed, does the heavy lifting over all N records)**

- `mapper.cpp`: for every log line, emits four kinds of single-record
  partial-aggregate `key\tvalue` pairs:
  - `GLOBAL` -> `count success min max sum bytes s2xx s3xx s4xx s5xx`
  - `SERVER_<id>` -> `count sum_response_time`
  - `ENDPOINT_<id>` -> `count bytes`
  - `INTERVAL_<id>` -> `count`
- `aggregate.cpp`: used as **both** `-combiner` and `-reducer`. Every merge
  here (sum, min, max) is associative and commutative, and a merged line is
  written back in the exact same shape it was read in, so the reducer can
  merge combiner output the same way the combiner merged mapper output.
  With `-numReduceTasks 1`, the single reducer's stdout is the complete,
  globally-merged aggregate: one line per `GLOBAL`/server/endpoint/interval.

**Stage 2 (tiny, local)**

- `finalize.cpp`: by this point the data is only
  O(S + distinct endpoints + distinct intervals) lines - a few thousand at
  most regardless of how large N was. Running that through another
  distributed MR pass would add scheduling overhead for no benefit, so it's
  finished locally. It `#include`s `analytics_common.hpp` - the exact same
  header the HW2 sequential/MPI programs use - so Top-K ordering,
  busiest-interval tie-breaking, and `%.6f` number formatting are
  byte-identical to the rest of the assignment.

This mirrors the MPI version's own split (see `server_log_mpi.cpp`):
fixed-size totals/per-server stats go through a `Reduce`-like path, while
unbounded endpoint/interval keys are merged as complete lists (never
per-shard Top-K, since a middling item on every shard could still be
globally Top-K).

## Files

| File | Purpose |
|---|---|
| `mapper.cpp` | Map phase |
| `aggregate.cpp` | Combiner **and** reducer (same associative merge) |
| `finalize.cpp` | Stage 2: Top-K, busiest interval, final formatting |
| `analytics_common.hpp` | Copied from HW2 - parser/formatter shared with the sequential/MPI reference |
| `generate_dataset.cpp` | Copied from HW2 - reproducible dataset generator |
| `run_local_pipeline.sh` | Simulates the job locally as a Unix pipeline (fast correctness testing, no HDFS/YARN needed) |
| `run_hadoop.sh` | Runs the real Hadoop Streaming job on RCE |
| `test_data/` | HW2's sample input/expected output, plus generated test data |

## Correctness verification

Local pipeline simulation (`mapper | sort | aggregate | sort | aggregate | finalize`):

```bash
bash run_local_pipeline.sh test_data/sample_input.txt test_data/actual_output.txt
diff test_data/expected_output.txt test_data/actual_output.txt   # identical
```

Also verified at scale: generated a 50,000-record dataset
(`generate_dataset 50000 5 20 test_data/big_input.txt 123`), ran it through
this MapReduce pipeline, and diffed against HW2's `server_log_sequential`
binary on the same input - **byte-identical output**.

## Running the real Hadoop Streaming job on RCE

```bash
module load hdfs/hdfs java/11.0.13
salloc --nodes=1 --ntasks=1 --time=00:15:00 bash run_hadoop.sh test_data/sample_input.txt
```

This compiles the three C++ programs on the node itself (avoids ABI
mismatches from cross-compiling), uploads the record body to HDFS, submits
`hadoop jar hadoop-streaming-3.3.0.jar` with `-mapper ./mapper -combiner
./aggregate -reducer ./aggregate -D mapreduce.job.reduces=1`, pulls the
merged output back with `hdfs dfs -getmerge`, and finishes with `./finalize`.

### Status on RCE: blocked by the known Hadoop/YARN outage

Checked directly on a compute node (never the login node):

- `hdfs dfs -ls /` succeeds (NameNode is reachable for reads), but
  `hdfs dfs -mkdir` **fails with "Name node is in safe mode"** - the
  cluster's HDFS cannot currently accept writes.
- `yarn node -list` hangs, retrying
  `Connecting to ResourceManager at /0.0.0.0:8032` indefinitely - YARN's
  ResourceManager is not reachable, so no MapReduce job can actually be
  scheduled.

This matches the course announcement: *"There is currently an issue with
the Hadoop environment on RCE... until then, you may implement and execute
the MapReduce programs using a Slurm-based script."* `run_hadoop.sh` is
complete and ready to run as-is the moment HDFS/YARN are back; until then,
correctness is demonstrated via the local pipeline simulation above (which
exercises the identical mapper/combiner/reducer/finalize binaries, just
without HDFS/YARN as the orchestrator) plus the sequential cross-check at
50,000 records.

## MPI vs MapReduce (design comparison)

| Aspect | MPI (HW2) | Hadoop MapReduce (this) |
|---|---|---|
| Data distribution | Rank 0 computes byte ranges; every rank seeks directly into the file | HDFS splits the input; Hadoop assigns splits to mappers |
| Fixed-size aggregates (totals, per-server) | `MPI_Reduce` (SUM/MIN/MAX) | Combiner + single reducer, same associative merge |
| Unbounded keys (endpoints, intervals) | `MPI_Gatherv` of complete per-rank lists, merged on rank 0 | Emitted as separate keys, merged by the same reducer pass |
| Top-K / busiest interval | Computed once, on rank 0, after merging | Computed once, locally, in `finalize.cpp` after the reducer |
| Fault tolerance | None - a lost rank kills the job | Hadoop can re-run failed map/reduce tasks |
| Programming model | Explicit point-to-point/collective calls | Declarative map/reduce functions; framework handles shuffle/sort |
| Startup/scheduling overhead | Low (`mpirun` starts ranks directly) | Higher (JVM startup per task, HDFS block placement, YARN scheduling) |

Both approaches converge on the same underlying algorithm (map-side
per-record aggregation + associative reduce for everything except
Top-K/busiest-interval, which need one final global pass). MPI's explicit
control over data placement gives it lower overhead for a single-run batch
job at this problem's scale; Hadoop's value is in automatic fault-tolerance
and elastic scheduling across a shared, larger cluster, which matters most
when N is large enough that task/node failures during the job become
likely - not clearly demonstrable here since RCE's Hadoop deployment
itself is currently down for that comparison.

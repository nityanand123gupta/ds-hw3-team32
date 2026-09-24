#!/bin/bash
#SBATCH --job-name=sssp_mapreduce
#SBATCH --output=sssp_dist_%j.out
#SBATCH --error=sssp_dist_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:15:00

# ============================================================
# SSSP via iterative MapReduce -- distributed (SLURM) driver
#
# Same map -> shuffle/sort -> combine -> shuffle/sort -> reduce stages
# as Mapreduce_distributed.sh, but wrapped in a loop that re-runs the
# pipeline once per Bellman-Ford round, feeding each round's reducer
# output back in as the next round's input.
#
# Usage: sbatch run_sssp_distributed.sh <input_graph.txt>
# ============================================================
set -e

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi
cd "$SCRIPT_DIR"

INPUT_GRAPH=${1:-test_data/sample_input.txt}
RESULTS_DIR="$SCRIPT_DIR/perf_results"
mkdir -p "$RESULTS_DIR"

WORK_DIR="$SCRIPT_DIR/sssp_work_$SLURM_JOB_ID"
mkdir -p "$WORK_DIR"

SUMMARY_FILE="$RESULTS_DIR/sssp_dist_benchmark_summary.csv"
echo "round,mapper_time_s,shuffle1_time_s,combiner_time_s,shuffle2_time_s,reducer_time_s,total_time_s" > "$SUMMARY_FILE"

if [ -z "$SLURM_NTASKS" ]; then
    SLURM_NTASKS=4 # fallback for local testing without SLURM
fi

V=$(head -n1 "$INPUT_GRAPH" | awk '{print $1}')
MAX_ITERS=$((V - 1))
if [ "$MAX_ITERS" -lt 1 ]; then
    MAX_ITERS=1
fi

echo "============================================"
echo "Distributed SSSP MapReduce"
echo "Date: $(date)"
echo "Nodes: $SLURM_JOB_NODELIST"
echo "Tasks: $SLURM_NTASKS"
echo "V=$V, Max iterations=$MAX_ITERS"
echo "============================================"

python3 "$SCRIPT_DIR/init_state.py" < "$INPUT_GRAPH" > "$WORK_DIR/state_0.txt"

i=0
while [ "$i" -lt "$MAX_ITERS" ]; do
    NEXT=$((i + 1))
    CUR_STATE="$WORK_DIR/state_$i.txt"
    NEXT_STATE="$WORK_DIR/state_$NEXT.txt"

    echo "--------------------------------------------"
    echo "Round $NEXT/$MAX_ITERS"
    echo "--------------------------------------------"

    TOTAL_START=$(date +%s%N)

    # Split current state across tasks
    split -d -a 2 -n l/"$SLURM_NTASKS" "$CUR_STATE" "$WORK_DIR/chunk_"

    # Stage 1: Mapper (distributed)
    STAGE_START=$(date +%s%N)
    srun --ntasks="$SLURM_NTASKS" bash -c "
        TID=\$(printf '%02d' \$SLURM_PROCID)
        python3 '$SCRIPT_DIR/mapper.py' < '$WORK_DIR/chunk_'\${TID} > '$WORK_DIR/map_'\${TID}.out
    "
    STAGE_END=$(date +%s%N)
    MAPPER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Mapper (Dist):   ${MAPPER_TIME}s"

    # Stage 2: Local sort per chunk
    STAGE_START=$(date +%s%N)
    srun --ntasks="$SLURM_NTASKS" bash -c "
        TID=\$(printf '%02d' \$SLURM_PROCID)
        sort '$WORK_DIR/map_'\${TID}.out > '$WORK_DIR/shuf1_'\${TID}.out
    "
    STAGE_END=$(date +%s%N)
    SHUFFLE1_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Shuffle 1:       ${SHUFFLE1_TIME}s"

    # Stage 3: Combiner (distributed)
    STAGE_START=$(date +%s%N)
    srun --ntasks="$SLURM_NTASKS" bash -c "
        TID=\$(printf '%02d' \$SLURM_PROCID)
        python3 '$SCRIPT_DIR/combiner.py' < '$WORK_DIR/shuf1_'\${TID}.out > '$WORK_DIR/comb_'\${TID}.out
    "
    STAGE_END=$(date +%s%N)
    COMBINER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Combiner (Dist): ${COMBINER_TIME}s"

    # Stage 4: Global shuffle/sort (single node)
    STAGE_START=$(date +%s%N)
    sort "$WORK_DIR"/comb_*.out > "$WORK_DIR/global_shuf2.out"
    STAGE_END=$(date +%s%N)
    SHUFFLE2_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Shuffle 2 (Glob):${SHUFFLE2_TIME}s"

    # Stage 5: Reducer (single node on aggregated data)
    STAGE_START=$(date +%s%N)
    python3 "$SCRIPT_DIR/reducer.py" < "$WORK_DIR/global_shuf2.out" > "$NEXT_STATE"
    STAGE_END=$(date +%s%N)
    REDUCER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Reducer:         ${REDUCER_TIME}s"

    TOTAL_END=$(date +%s%N)
    TOTAL_TIME=$(echo "scale=6; ($TOTAL_END - $TOTAL_START) / 1000000000" | bc)
    echo "  TOTAL round time: ${TOTAL_TIME}s"

    echo "$NEXT,$MAPPER_TIME,$SHUFFLE1_TIME,$COMBINER_TIME,$SHUFFLE2_TIME,$REDUCER_TIME,$TOTAL_TIME" >> "$SUMMARY_FILE"

    rm -f "$WORK_DIR"/chunk_* "$WORK_DIR"/map_*.out "$WORK_DIR"/shuf1_*.out "$WORK_DIR"/comb_*.out "$WORK_DIR/global_shuf2.out"

    if diff -q <(cut -f1,2 "$CUR_STATE") <(cut -f1,2 "$NEXT_STATE") > /dev/null; then
        echo "Converged after $NEXT round(s)."
        i=$NEXT
        break
    fi
    i=$NEXT
done

python3 "$SCRIPT_DIR/extract_output.py" < "$WORK_DIR/state_$i.txt" > "$RESULTS_DIR/sssp_output.txt"

echo ""
echo "============================================"
echo "Distributed SSSP complete!"
echo "Output: $RESULTS_DIR/sssp_output.txt"
echo "Benchmark summary: $SUMMARY_FILE"
echo "============================================"
column -t -s',' "$SUMMARY_FILE"

rm -rf "$WORK_DIR"

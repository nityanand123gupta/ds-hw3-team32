#!/bin/bash
# ============================================================
# SSSP via iterative MapReduce -- local testing driver
#
# Usage: ./run_sssp_local.sh <input_graph.txt> <output.txt>
#
# Reuses the provided MapreduceForLocalTesting.sh single-round pipeline
# (mapper | sort | combiner | sort | reducer) and repeats it for up to
# V-1 rounds (the standard Bellman-Ford bound), stopping early if the
# distances stop changing.
# ============================================================
set -e

INPUT_GRAPH=${1:-test_data/sample_input.txt}
OUTPUT_FILE=${2:-output.txt}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PIPELINE_SCRIPT="$SCRIPT_DIR/../../MapreduceForLocalTesting.sh"

if [ ! -f "$PIPELINE_SCRIPT" ]; then
    echo "ERROR: cannot find MapreduceForLocalTesting.sh at $PIPELINE_SCRIPT"
    exit 1
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

V=$(head -n1 "$INPUT_GRAPH" | awk '{print $1}')
MAX_ITERS=$((V - 1))
if [ "$MAX_ITERS" -lt 1 ]; then
    MAX_ITERS=1
fi

echo "============================================"
echo "SSSP (iterative MapReduce) -- local run"
echo "Input : $INPUT_GRAPH"
echo "V     : $V"
echo "Max iterations (Bellman-Ford bound): $MAX_ITERS"
echo "============================================"

echo "Building initial state..."
python3 "$SCRIPT_DIR/init_state.py" < "$INPUT_GRAPH" > "$WORK_DIR/state_0.txt"

i=0
# cd into this directory so mapper.py/combiner.py/reducer.py resolve
# relative to cwd, as required by MapreduceForLocalTesting.sh
cd "$SCRIPT_DIR"

while [ "$i" -lt "$MAX_ITERS" ]; do
    NEXT=$((i + 1))
    ITER_START=$(date +%s%N)

    bash "$PIPELINE_SCRIPT" "$WORK_DIR/state_$i.txt" "$WORK_DIR/state_$NEXT.txt" > /dev/null

    ITER_END=$(date +%s%N)
    ITER_TIME=$(awk -v a="$ITER_START" -v b="$ITER_END" 'BEGIN{printf "%.3f", (b-a)/1000000000}')
    echo "Round $NEXT/$MAX_ITERS done (${ITER_TIME}s)"

    if diff -q <(cut -f1,2 "$WORK_DIR/state_$i.txt") <(cut -f1,2 "$WORK_DIR/state_$NEXT.txt") > /dev/null; then
        echo "Converged after $NEXT round(s) -- distances stopped changing."
        i=$NEXT
        break
    fi
    i=$NEXT
done

python3 "$SCRIPT_DIR/extract_output.py" < "$WORK_DIR/state_$i.txt" > "$OUTPUT_FILE"
echo "============================================"
echo "SSSP complete. Output written to: $OUTPUT_FILE"
echo "============================================"

#!/bin/bash
# ============================================================
# Server Log Analytics -- local MapReduce-pipeline simulation
#
# Simulates the Hadoop Streaming job (mapper -> combiner -> shuffle/sort ->
# reducer) as a plain Unix pipeline, for fast local correctness testing
# without needing a live HDFS/YARN cluster. run_hadoop.sh runs the real
# thing on RCE.
#
# Usage: ./run_local_pipeline.sh <input_file_with_header> <output_file>
# ============================================================
set -e

INPUT_FILE=${1:-test_data/sample_input.txt}
OUTPUT_FILE=${2:-output.txt}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

for exe in mapper aggregate finalize; do
    if [ ! -f "$exe" ] && [ ! -f "$exe.exe" ]; then
        echo "Building $exe..."
        g++ -O2 -std=c++17 -static -o "$exe" "$exe.cpp" 2>/dev/null || g++ -O2 -std=c++17 -static -o "$exe.exe" "$exe.cpp"
    fi
done
MAPPER=$([ -f mapper.exe ] && echo ./mapper.exe || echo ./mapper)
AGGREGATE=$([ -f aggregate.exe ] && echo ./aggregate.exe || echo ./aggregate)
FINALIZE=$([ -f finalize.exe ] && echo ./finalize.exe || echo ./finalize)

read -r N K S < "$INPUT_FILE"
echo "N=$N K=$K S=$S"

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

tail -n +2 "$INPUT_FILE" > "$WORK_DIR/body.txt"

# mapper -> local sort+combine (per-mapper-task simulation) -> global
# sort+reduce (numReduceTasks=1 simulation)
"$MAPPER" < "$WORK_DIR/body.txt" | sort | "$AGGREGATE" | sort | "$AGGREGATE" > "$WORK_DIR/aggregated.txt"

"$FINALIZE" "$WORK_DIR/aggregated.txt" "$K" "$S" > "$OUTPUT_FILE"
echo "Done. Output written to $OUTPUT_FILE"

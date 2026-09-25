#!/bin/bash
# ============================================================
# Server Log Analytics -- real Hadoop Streaming job on the RCE cluster
#
# Must be run on a COMPUTE NODE (inside salloc/srun), never the login node.
#
# Usage:
#   module load hdfs/hdfs
#   salloc --nodes=1 --ntasks=1 --time=00:15:00 bash run_hadoop.sh test_data/sample_input.txt
# ============================================================
set -e

INPUT_FILE=${1:-test_data/sample_input.txt}
HDFS_DIR=${2:-/user/$USER/q7_server_log_analytics}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building mapper/aggregate/finalize on this node's toolchain..."
g++ -O2 -std=c++17 -o mapper mapper.cpp
g++ -O2 -std=c++17 -o aggregate aggregate.cpp
g++ -O2 -std=c++17 -o finalize finalize.cpp
chmod +x mapper aggregate finalize

read -r N K S < "$INPUT_FILE"
echo "N=$N K=$K S=$S"

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
tail -n +2 "$INPUT_FILE" > "$WORK_DIR/body.txt"

HADOOP_STREAMING_JAR=$(find "${HADOOP_HOME:-/usr/local/apps}" -name 'hadoop-streaming*.jar' 2>/dev/null | head -1)
if [ -z "$HADOOP_STREAMING_JAR" ]; then
    echo "ERROR: could not locate hadoop-streaming*.jar. Is the hdfs/hadoop module loaded?"
    exit 1
fi
echo "Using streaming jar: $HADOOP_STREAMING_JAR"

hdfs dfs -mkdir -p "$HDFS_DIR/input"
hdfs dfs -rm -f -r "$HDFS_DIR/output" 2>/dev/null || true
hdfs dfs -put -f "$WORK_DIR/body.txt" "$HDFS_DIR/input/body.txt"

hadoop jar "$HADOOP_STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -files "$SCRIPT_DIR/mapper,$SCRIPT_DIR/aggregate" \
    -mapper "./mapper" \
    -combiner "./aggregate" \
    -reducer "./aggregate" \
    -input "$HDFS_DIR/input/body.txt" \
    -output "$HDFS_DIR/output"

hdfs dfs -getmerge "$HDFS_DIR/output" "$WORK_DIR/aggregated.txt"

./finalize "$WORK_DIR/aggregated.txt" "$K" "$S"

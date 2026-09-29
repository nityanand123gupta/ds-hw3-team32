#!/bin/bash
# Runs the local-pipeline simulation at one (name, N, K, S) configuration
# and prints "pipeline=... finalize=... total=..." timings.
set -e
name=$1; n=$2; k=$3; s=$4
cd "$(dirname "$0")"

./generate_dataset "$n" "$k" "$s" "/tmp/hw2eq_${name}.txt" 42 >/dev/null 2>&1
tail -n +2 "/tmp/hw2eq_${name}.txt" > "/tmp/hw2eq_${name}_body.txt"

t0=$(date +%s%N)
./mapper < "/tmp/hw2eq_${name}_body.txt" > "/tmp/hw2eq_${name}_map.txt"
sort "/tmp/hw2eq_${name}_map.txt" > "/tmp/hw2eq_${name}_s1.txt"
./aggregate < "/tmp/hw2eq_${name}_s1.txt" > "/tmp/hw2eq_${name}_comb.txt"
sort "/tmp/hw2eq_${name}_comb.txt" > "/tmp/hw2eq_${name}_s2.txt"
./aggregate < "/tmp/hw2eq_${name}_s2.txt" > "/tmp/hw2eq_${name}_agg.txt"
t1=$(date +%s%N)
./finalize "/tmp/hw2eq_${name}_agg.txt" "$k" "$s" > "/tmp/hw2eq_${name}_out.txt"
t2=$(date +%s%N)

awk -v a="$t0" -v b="$t1" -v c="$t2" 'BEGIN{printf "pipeline=%.4fs finalize=%.4fs total=%.4fs\n", (b-a)/1e9, (c-b)/1e9, (c-a)/1e9}'

rm -f "/tmp/hw2eq_${name}"*

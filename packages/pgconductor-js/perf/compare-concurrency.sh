#!/bin/bash

# Compare task-level concurrency impact on performance

echo "========================================"
echo "Task-Level Concurrency Impact Benchmark"
echo "========================================"
echo

scenarios=(
  "task-concurrency-1"
  "task-concurrency-5"
  "task-concurrency-10"
  "task-concurrency-25"
  "task-concurrency-unlimited"
)

results_file=$(mktemp)

for scenario in "${scenarios[@]}"; do
  echo "Running: $scenario"
  bun perf/throughput/run.ts "$scenario" 2>&1 | tee /dev/tty | \
    grep -E "(Task Concurrency:|Overall:|Total time:)" >> "$results_file"
  echo "---" >> "$results_file"
  echo
done

echo
echo "========================================"
echo "Summary"
echo "========================================"
echo
cat "$results_file"
rm "$results_file"

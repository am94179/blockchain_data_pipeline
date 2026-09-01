#!/usr/bin/env bash

# Orchestrate one raw-data delivery through the medallion layers. Each command
# is synchronous: `set -e` prevents later layers from starting after a failure.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: ./run_batch.sh <batch_id>" >&2
  echo "Example: ./run_batch.sh 2026-08-18" >&2
  exit 1
fi

batch_id="$1"
if [[ ! "$batch_id" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Batch ID must use YYYY-MM-DD format: $batch_id" >&2
  exit 1
fi

raw_dir="data/raw/ingest_${batch_id}"
datasets=(blocks transactions inputs outputs)

for dataset in "${datasets[@]}"; do
  if [[ ! -f "${raw_dir}/${dataset}.csv" ]]; then
    echo "Missing batch input: ${raw_dir}/${dataset}.csv" >&2
    exit 1
  fi
done

run_job() {
  local layer="$1"
  local job="$2"
  echo "==> ${layer}/${job}"
  if [[ "$layer" == "bronze" ]]; then
    ./run.sh "$layer" "$job" "$batch_id"
  else
    ./run.sh "$layer" "$job"
  fi
}

echo "Starting batch ${batch_id}"

# All bronze inputs must land before any silver transformation begins.
for dataset in "${datasets[@]}"; do
  run_job bronze "ingest_${dataset}"
done

# Inputs and outputs validate transaction references; reconciliation needs both.
run_job silver ingest_blocks
run_job silver ingest_transactions
run_job silver ingest_inputs
run_job silver ingest_outputs
run_job silver reconcile_utxos

# Gold jobs read the completed silver layer. Serial execution keeps local Spark
# resource use predictable and makes failure handling straightforward.
for dataset in "${datasets[@]}"; do
  run_job gold "ingest_${dataset}"
done

echo "Batch ${batch_id} completed successfully."

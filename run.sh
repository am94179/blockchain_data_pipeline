#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage:" >&2
  echo "  ./run.sh bronze <ingest_job> <batch_id>" >&2
  echo "  ./run.sh silver <job>" >&2
  echo "  ./run.sh gold <job>" >&2
  exit 1
fi

layer="$1"
job="$2"

if [[ "$layer" != "bronze" && "$layer" != "silver" && "$layer" != "gold" ]]; then
  echo "Unsupported layer: $layer. Use bronze, silver, or gold." >&2
  exit 1
fi

job_path="/opt/spark/app/${layer}/${job}.py"

if [[ ! -f "spark/${layer}/${job}.py" ]]; then
  echo "Job not found: spark/${layer}/${job}.py" >&2
  exit 1
fi

submit_args=()

if [[ "$layer" == "bronze" ]]; then
  if [[ $# -ne 3 ]]; then
    echo "Bronze jobs require a batch ID." >&2
    exit 1
  fi

  batch_id="$3"
  if [[ ! "$batch_id" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Batch ID must use YYYY-MM-DD format: $batch_id" >&2
    exit 1
  fi

  dataset="${job#ingest_}"
  if [[ "$dataset" == "$job" ]]; then
    echo "Bronze jobs must use the ingest_<dataset> naming convention: $job" >&2
    exit 1
  fi

  data_path="/opt/spark/data/raw/ingest_${batch_id}/${dataset}.csv"
  submit_args=(
    --input_path "$data_path"
    --batch_id "$batch_id"
  )
elif [[ $# -ne 2 ]]; then
  echo "${layer^} jobs do not take a batch ID." >&2
  exit 1
fi

exec docker compose exec -e PYTHONPATH=/opt/spark/app spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/app \
  "$job_path" \
  "${submit_args[@]}"

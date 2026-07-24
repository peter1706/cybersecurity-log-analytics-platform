#!/usr/bin/env bash
# Run the pipeline end-to-end for one day across all sources, then leave the
# stack up so the pytest e2e smoke test can verify the Silver/Gold output.
#
# Uses `airflow tasks test` to execute each stage synchronously and in order,
# which is deterministic and avoids polling scheduler run state.
set -euo pipefail

DAY="${1:-0}"
LOGICAL_DATE="2026-01-01"

# DockerOperator bind-mounts data/subset from the host, so it needs an absolute
# host path. Override whatever is in .env for this run.
export HOST_PROJECT_DIR="$(pwd)"

echo "==> Building images (airflow, simulator, spark-processor)"
docker compose --profile build build

echo "==> Starting core services"
docker compose up -d minio mc-init airflow-postgres airflow-init \
  airflow-api-server airflow-scheduler airflow-dag-processor

echo "==> Waiting for the Airflow scheduler to be ready"
for _ in $(seq 1 60); do
  if docker compose exec -T airflow-scheduler airflow jobs check --job-type SchedulerJob >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

echo "==> Running pipeline stages for day=${DAY}"
# Each source lands through Silver; auth also builds Gold. Stages are ordered so
# a source's Bronze precedes its Silver.
tasks=()
for source in auth proc flows dns; do
  tasks+=("${source}.simulate" "${source}.land_to_bronze" "${source}.bronze_to_silver")
done
tasks+=("auth.silver_to_gold")

# `airflow tasks test` can exit 0 even when the task fails/retries, so inspect its
# output for failure markers and abort at the offending stage instead of masking it.
for task in "${tasks[@]}"; do
  echo "--- $task ---"
  output="$(docker compose exec -T airflow-scheduler \
    airflow tasks test daily_pipeline "$task" "$LOGICAL_DATE" 2>&1)"
  echo "$output"
  if echo "$output" | grep -Eiq "Task failed with exception|Marking task as (FAILED|UP_FOR_RETRY)|new_state=(failed|up_for_retry)"; then
    echo "==> Stage '$task' failed; aborting pipeline." >&2
    exit 1
  fi
done

echo "==> Pipeline complete. Gold output is in the '${GOLD_BUCKET:-gold}' bucket."

#!/usr/bin/env bash
# Run the Increment 1 thin-slice pipeline end-to-end for one day, then leave the
# stack up so the pytest e2e smoke test can verify the Gold output.
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
for task in simulate_day land_to_bronze bronze_to_silver silver_to_gold; do
  echo "--- $task ---"
  docker compose exec -T airflow-scheduler \
    airflow tasks test daily_pipeline "$task" "$LOGICAL_DATE"
done

echo "==> Pipeline complete. Gold output is in the '${GOLD_BUCKET:-gold}' bucket."

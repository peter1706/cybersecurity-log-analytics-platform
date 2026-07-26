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

echo "==> Initializing container secrets (./secrets)"
# Compose mounts every credential from ./secrets/<name>; create any missing ones.
bash scripts/init_secrets.sh

echo "==> Building images (airflow, simulator, spark-processor, delivery, ml-mock)"
docker compose --profile build build

echo "==> Starting core services"
# --wait blocks until long-running services are healthy and airflow-init has
# exited 0 (it is a service_completed_successfully dependency, so --wait tolerates
# its exit); --wait-timeout fails fast instead of hanging until an outer CI
# timeout. mc-init is deliberately excluded: it is a one-shot that nothing
# depends on via a compose condition, so --wait would treat its clean 0-exit as a
# failure. Dump state + init logs on failure so the cause is visible.
if ! docker compose up -d --wait --wait-timeout 300 \
  minio airflow-postgres postgres-catalog airflow-init \
  airflow-api-server airflow-scheduler airflow-dag-processor; then
  echo "==> Core services failed to become ready; dumping diagnostics." >&2
  docker compose ps -a >&2 || true
  docker compose logs --no-color --tail 200 \
    airflow-init airflow-postgres airflow-api-server >&2 || true
  exit 1
fi
# One-shot bucket + delivered-scoped-account provisioner; runs to completion in
# the background before the first stage touches MinIO.
docker compose up -d mc-init

echo "==> Applying governance-catalog migrations"
# One-shot: waits for postgres-catalog to be healthy, applies every migration in
# order, then exits. Idempotent, so re-running the pipeline is safe. Every task
# below writes lineage/schema/job-run rows, so the schema must exist first.
docker compose run --rm catalog-migrate

echo "==> Waiting for the Airflow scheduler to be ready"
for _ in $(seq 1 60); do
  if docker compose exec -T airflow-scheduler airflow jobs check --job-type SchedulerJob >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

echo "==> Running pipeline stages for day=${DAY}"
# Each source lands through Silver; a single cross-source Gold step follows.
# Stages are ordered so a source's Bronze precedes its Silver.
tasks=()
for source in auth proc flows dns; do
  tasks+=("${source}.simulate" "${source}.land_to_bronze" "${source}.bronze_to_silver")
done
# Single cross-source Gold step, then encrypted delivery + consumer verification.
tasks+=("silver_to_gold" "deliver" "ml_consume")

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

echo "==> Pipeline complete. Gold: '${GOLD_BUCKET:-gold}', delivered: '${DELIVERED_BUCKET:-delivered}'."

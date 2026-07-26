#!/usr/bin/env bash
# Full rolling-window end-to-end: backfill an inclusive range of data days so the
# final anchor day sees a complete Silver -> Gold window, then leave the stack up
# for the pytest full-window assertions (tests/e2e/test_full_window.py).
#
#   scripts/e2e_full_window.sh [START_DAY] [END_DAY]   (defaults: 0 6)
#
# This is the heavyweight counterpart to scripts/e2e_pipeline.sh (which runs a
# single day): it builds the images, brings up the core services, applies the
# catalog migrations, then hands off to scripts/backfill.sh for the multi-day run.
set -euo pipefail

START_DAY="${1:-0}"
END_DAY="${2:-6}"

# DockerOperator / backfill bind-mount data/subset from the host, so an absolute
# host path is required. Override whatever is in .env for this run.
export HOST_PROJECT_DIR="$(pwd)"

echo "==> Initializing container secrets (./secrets)"
# Compose + backfill mount every credential from ./secrets/<name>.
bash scripts/init_secrets.sh

echo "==> Building images (airflow, simulator, spark-processor, delivery, ml-mock)"
docker compose --profile build build

echo "==> Starting core services"
# --wait blocks until long-running services are healthy and airflow-init has
# exited 0 (a service_completed_successfully dependency, so --wait tolerates its
# exit); --wait-timeout fails fast instead of hanging. mc-init is excluded: it is
# a one-shot nothing depends on via a compose condition, so --wait would treat its
# clean 0-exit as a failure. Dump diagnostics on failure so the cause is visible.
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

echo "==> Applying catalog migrations"
# One-shot: waits for postgres-catalog to be healthy, applies every migration in
# order, then exits. Idempotent, so re-running is safe. The backfill below writes
# lineage/schema/checksum rows, so the schema must exist first.
docker compose run --rm catalog-migrate

echo "==> Backfilling days ${START_DAY}..${END_DAY} (seeds the rolling window)"
bash scripts/backfill.sh "$START_DAY" "$END_DAY"

echo "==> Full-window backfill complete. Gold: '${GOLD_BUCKET:-gold}', delivered: '${DELIVERED_BUCKET:-delivered}'."

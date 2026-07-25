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
docker compose up -d minio mc-init airflow-postgres airflow-init \
  airflow-api-server airflow-scheduler airflow-dag-processor postgres-catalog

echo "==> Applying catalog migrations"
# One-shot: waits for postgres-catalog to be healthy, applies every migration in
# order, then exits. Idempotent, so re-running is safe. The backfill below writes
# lineage/schema/checksum rows, so the schema must exist first.
docker compose run --rm catalog-migrate

echo "==> Backfilling days ${START_DAY}..${END_DAY} (seeds the rolling window)"
bash scripts/backfill.sh "$START_DAY" "$END_DAY"

echo "==> Full-window backfill complete. Gold: '${GOLD_BUCKET:-gold}', delivered: '${DELIVERED_BUCKET:-delivered}'."

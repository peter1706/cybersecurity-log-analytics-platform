#!/usr/bin/env bash
# Backfill the Medallion pipeline across an inclusive range of data days so the
# Silver -> Gold rolling window has history to aggregate over.
#
#   scripts/backfill.sh [START_DAY] [END_DAY]   (defaults: 0 6)
#
# It runs the same task images the Airflow DAG launches, directly via `docker
# run` on the platform network (so an arbitrary --day can be passed, which
# `airflow tasks test` cannot easily override). Ordering matters: every source is
# landed through Silver for the whole range first, then auth Gold is built per
# anchor day in ascending order so each anchor's rolling window sees the days
# before it. Re-running is safe -- landing keys are fixed and Delta writes use
# dynamic partition overwrite, so no day is double-counted.
set -euo pipefail

START_DAY="${1:-0}"
END_DAY="${2:-6}"

if [ ! -f .env ]; then
  echo "ERROR: .env not found. Copy .env.example to .env first." >&2
  exit 1
fi
# shellcheck disable=SC1091
set -a; . ./.env; set +a

NETWORK="${NETWORK_NAME:-platform-net}"
SIM_IMG="${IMG_SIMULATOR:-clap-lanl-simulator:dev}"
SPARK_IMG="${IMG_SPARK:-clap-spark-processor:dev}"
SUBSET_DIR="$(pwd)/data/subset"
SOURCES=(auth proc flows dns)

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  echo "ERROR: docker network '$NETWORK' not found. Start the stack (make up)." >&2
  exit 1
fi

run_sim() { docker run --rm --network "$NETWORK" --env-file .env \
  -v "$SUBSET_DIR:/data/subset:ro" "$SIM_IMG" "$@"; }
run_spark() { docker run --rm --network "$NETWORK" --env-file .env "$SPARK_IMG" "$@"; }

echo "==> Seeding landing for days ${START_DAY}..${END_DAY} (all sources, one pass each)"
for source in "${SOURCES[@]}"; do
  run_sim --source "$source" --day "$START_DAY" --day-end "$END_DAY"
done

echo "==> Bronze + Silver per source, per day"
for day in $(seq "$START_DAY" "$END_DAY"); do
  for source in "${SOURCES[@]}"; do
    run_spark land_to_bronze --source "$source" --day "$day"
    run_spark bronze_to_silver --source "$source" --day "$day"
  done
done

echo "==> Gold (computer_features) per anchor day -- rolling window = ${ROLLING_WINDOW_DAYS:-7}d"
for day in $(seq "$START_DAY" "$END_DAY"); do
  run_spark silver_to_gold --day "$day"
done

echo "==> Backfill complete for anchor days ${START_DAY}..${END_DAY}."

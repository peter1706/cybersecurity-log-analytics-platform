#!/usr/bin/env bash
# Backfill the Medallion pipeline across an inclusive range of data days so the
# Silver -> Gold rolling window has history to aggregate over.
#
#   scripts/backfill.sh [START_DAY] [END_DAY]   (defaults: 0 6)
#
# It runs the same task images the Airflow DAG launches, directly via `docker
# run` on the platform network (so an arbitrary --day can be passed, which
# `airflow tasks test` cannot easily override). Ordering matters: every source is
# landed through Silver for the whole range first, then the unified Gold table is
# built per anchor day in ascending order so each anchor's rolling window sees the
# days before it, then each anchor day is delivered and consumed. Re-running is
# safe -- landing/delivered keys are fixed and Delta writes use dynamic partition
# overwrite, so no day is double-counted.
set -euo pipefail

START_DAY="${1:-0}"
END_DAY="${2:-6}"

if [ ! -f .env ]; then
  echo "ERROR: .env not found. Copy .env.example to .env first." >&2
  exit 1
fi
# shellcheck disable=SC1091
set -a; . ./.env; set +a

PIPELINE_NET="${PIPELINE_NETWORK_NAME:-clap-pipeline-net}"
ML_NET="${ML_NETWORK_NAME:-clap-ml-net}"
SIM_IMG="${IMG_SIMULATOR:-clap-lanl-simulator:dev}"
SPARK_IMG="${IMG_SPARK:-clap-spark-processor:dev}"
DELIVERY_IMG="${IMG_DELIVERY:-clap-delivery:dev}"
ML_MOCK_IMG="${IMG_ML_MOCK:-clap-ml-mock:dev}"
SUBSET_DIR="$(pwd)/data/subset"
SECRETS_DIR="$(pwd)/secrets"
SOURCES=(auth proc flows dns)

if [ ! -d "$SECRETS_DIR" ]; then
  echo "ERROR: ./secrets not found. Run scripts/init_secrets.sh first." >&2
  exit 1
fi
if ! docker network inspect "$PIPELINE_NET" >/dev/null 2>&1; then
  echo "ERROR: docker network '$PIPELINE_NET' not found. Start the stack (make up)." >&2
  exit 1
fi

# Credentials are delivered as read-only /run/secrets mounts, never env vars.
# The producer/Spark/delivery tasks run on the data-plane network and get the
# whole secrets dir; the ML consumer runs on the isolated consumer network with
# only its `delivered`-scoped MinIO account + decryption key mounted.
run_sim() { docker run --rm --network "$PIPELINE_NET" --env-file .env \
  -v "$SECRETS_DIR:/run/secrets:ro" -v "$SUBSET_DIR:/data/subset:ro" "$SIM_IMG" "$@"; }
run_spark() { docker run --rm --network "$PIPELINE_NET" --env-file .env \
  -v "$SECRETS_DIR:/run/secrets:ro" "$SPARK_IMG" "$@"; }
run_delivery() { docker run --rm --network "$PIPELINE_NET" --env-file .env \
  -v "$SECRETS_DIR:/run/secrets:ro" "$DELIVERY_IMG" "$@"; }
run_consume() { docker run --rm --network "$ML_NET" --env-file .env \
  -v "$SECRETS_DIR/minio_ml_consumer_key:/run/secrets/minio_ml_consumer_key:ro" \
  -v "$SECRETS_DIR/minio_ml_consumer_secret:/run/secrets/minio_ml_consumer_secret:ro" \
  -v "$SECRETS_DIR/delivery_encryption_key:/run/secrets/delivery_encryption_key:ro" \
  "$ML_MOCK_IMG" "$@"; }

echo "==> Seeding landing for days ${START_DAY}..${END_DAY} (all sources, one pass each)"
for source in "${SOURCES[@]}"; do
  run_sim --source "$source" --day "$START_DAY" --day-end "$END_DAY"
done

echo "==> Bronze then Silver, all sources x days ${START_DAY}..${END_DAY} (one Spark session each)"
# Batch every (source, day) into a single Spark session per stage so the
# JVM/Ivy cold start is paid twice, not 2 x sources x days times. land_to_bronze
# runs fully before bronze_to_silver, so each Silver day reads a Bronze day that
# already exists.
run_spark land_to_bronze --all-sources --day "$START_DAY" --day-end "$END_DAY"
run_spark bronze_to_silver --all-sources --day "$START_DAY" --day-end "$END_DAY"

echo "==> Gold (computer_features) per anchor day ${START_DAY}..${END_DAY} -- rolling window = ${ROLLING_WINDOW_DAYS:-7}d (one Spark session)"
# One session builds every anchor day; ascending order means each anchor's
# rolling window sees the Silver days before it.
run_spark silver_to_gold --day "$START_DAY" --day-end "$END_DAY"

echo "==> Deliver + consume per anchor day"
for day in $(seq "$START_DAY" "$END_DAY"); do
  run_delivery --day "$day"
  run_consume --day "$day"
done

echo "==> Backfill complete for anchor days ${START_DAY}..${END_DAY}."

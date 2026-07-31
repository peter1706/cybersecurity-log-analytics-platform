#!/usr/bin/env bash
# Populate ./secrets with one file per container secret (each mounted at
# /run/secrets/<name>). Idempotent: existing files are kept; only missing ones
# are created -- migrated from the matching .env value when set, otherwise given
# a local default or a freshly generated random value / Fernet key.
#
#   scripts/init_secrets.sh
#
# ./secrets is gitignored and must NEVER be committed. Re-run any time a secret
# file is missing (e.g. after a fresh checkout).
set -euo pipefail

SECRETS_DIR="secrets"
mkdir -p "$SECRETS_DIR"
# 0711 (not 0700): the DAG bind-mounts this whole dir into task containers at
# /run/secrets, and a bind mount carries the host mode through -- the task images
# run as a non-root uid that is not the owner, so they need the traverse bit to
# open the files by name. Others still cannot list the directory.
chmod 711 "$SECRETS_DIR"

# Load current .env values (if present) so an existing local stack keeps its
# credentials when they are migrated out of .env into secret files.
if [ -f .env ]; then
  # shellcheck disable=SC1091
  set -a; . ./.env; set +a
fi

rand() { python3 -c "import secrets;print(secrets.token_hex(${1:-24}))"; }
fernet() { python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"; }

# write_secret <name> <value>: write only when the file is absent (idempotent).
# Mode 0644 (not 0600), and only useful paired with the 0711 dir above: compose
# and the DAG bind-mount these files into containers with the host file's
# owner/mode, and the Airflow services (AIRFLOW_UID:0) and task images (uid 1000)
# are neither the owner nor able to read a 0600 file on Linux -- so they need the
# world-read bit. These files are generated, gitignored, never committed, and
# ephemeral, so world-readable on the host is acceptable.
# Existing files are re-chmod'd so a local stack created with the old 0600 mode
# self-heals on the next run.
write_secret() {
  local name="$1" value="$2" path="$SECRETS_DIR/$1"
  if [ -f "$path" ]; then
    chmod 644 "$path"
    echo "  keep    $name"
    return
  fi
  printf '%s' "$value" > "$path"
  chmod 644 "$path"
  echo "  create  $name"
}

echo "==> Initializing container secrets in ./$SECRETS_DIR"
write_secret minio_root_user           "${MINIO_ROOT_USER:-minioadmin}"
write_secret minio_root_password       "${MINIO_ROOT_PASSWORD:-minioadmin}"
write_secret minio_ml_consumer_key     "${MINIO_ML_CONSUMER_KEY:-ml-consumer}"
write_secret minio_ml_consumer_secret  "${MINIO_ML_CONSUMER_SECRET:-$(rand 20)}"
write_secret postgres_catalog_password "${CATALOG_DB_PASSWORD:-$(rand)}"
write_secret airflow_db_password       "${AIRFLOW_DB_PASSWORD:-$(rand)}"
write_secret airflow_fernet_key        "${AIRFLOW_FERNET_KEY:-$(fernet)}"
write_secret airflow_jwt_secret        "${AIRFLOW_JWT_SECRET:-$(rand)}"
write_secret airflow_admin_password    "${AIRFLOW_ADMIN_PASSWORD:-admin}"
# Opaque master-key material for Parquet Modular Encryption on delivered output;
# catalog.parquet_encryption derives a 256-bit AES key from it via SHA-256, so any
# high-entropy string works (an existing Fernet-format value stays valid too).
write_secret delivery_encryption_key   "${DELIVERY_ENCRYPTION_KEY:-$(rand 32)}"

echo "==> Done. ./$SECRETS_DIR is gitignored -- never commit it."

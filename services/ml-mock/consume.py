#!/usr/bin/env python3
"""ml-mock consumer: verify one delivered partition and simulate retraining.

Runs as the ``ml_consume`` DAG task after ``deliver``. For anchor day ``--day`` and
window ``--window-days`` it:

1. fetches the manifest + encrypted Parquet from the ``delivered`` bucket;
2. decrypts with the provided Fernet key and checks the plaintext SHA-256 against
   the manifest;
3. validates the schema against the consumer's feature contract exactly, rejecting
   any deviation;
4. checks the record count matches the manifest, then logs a simulated retrain.

This is the consumer's least-privilege boundary in miniature: it reads only the
``delivered`` bucket.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import traceback

import boto3
import feature_contract
import pyarrow.parquet as pq
from botocore.client import Config
from cryptography.fernet import Fernet

from catalog import read_secret

DATASET = "computer_features"


def _s3_client():
    # Least privilege: a MinIO service account scoped to the `delivered` bucket
    # only (never the layer buckets), read from container secrets.
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=read_secret("minio_ml_consumer_key", env="MINIO_ML_CONSUMER_KEY"),
        aws_secret_access_key=read_secret(
            "minio_ml_consumer_secret", env="MINIO_ML_CONSUMER_SECRET"
        ),
        config=Config(signature_version="s3v4"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _prefix(anchor_day: int, window_days: int) -> str:
    return f"{DATASET}/window_days={window_days}/anchor_day={anchor_day:02d}"


def consume(anchor_day: int, window_days: int) -> dict:
    """Verify a delivered partition end-to-end; return a small summary dict."""
    bucket = os.environ.get("DELIVERED_BUCKET", "delivered")
    key = read_secret("delivery_encryption_key", env="DELIVERY_ENCRYPTION_KEY")
    prefix = _prefix(anchor_day, window_days)

    client = _s3_client()
    manifest = json.loads(
        client.get_object(Bucket=bucket, Key=f"{prefix}/manifest.json")["Body"].read()
    )
    ciphertext = client.get_object(Bucket=bucket, Key=manifest["data_object"])["Body"].read()

    plaintext = Fernet(key).decrypt(ciphertext)
    checksum = hashlib.sha256(plaintext).hexdigest()
    if checksum != manifest["checksum_sha256"]:
        raise ValueError(
            "delivered checksum mismatch (data corrupted or tampered): "
            f"manifest={manifest['checksum_sha256'][:12]}... actual={checksum[:12]}..."
        )

    table = pq.read_table(io.BytesIO(plaintext))
    feature_contract.validate_schema(table.column_names)

    if table.num_rows != manifest["record_count"]:
        raise ValueError(
            f"record count mismatch: manifest={manifest['record_count']} "
            f"parquet={table.num_rows}"
        )

    n_features = len(feature_contract.EXPECTED_COLUMNS) - 3  # minus id/window keys
    print(
        f"ml-mock: accepted delivery anchor_day={anchor_day} window_days={window_days} "
        f"schema={manifest['schema_version']} rows={table.num_rows} "
        f"checksum={checksum[:12]}... -> simulating retrain on "
        f"{table.num_rows:,} computers x {n_features} features",
        flush=True,
    )
    return {"anchor_day": anchor_day, "window_days": window_days, "rows": table.num_rows}


def main() -> None:
    """Parse args and verify one delivered anchor-day partition."""
    default_window = int(os.environ.get("ROLLING_WINDOW_DAYS", "7"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=int, required=True, help="anchor day to consume")
    parser.add_argument("--window-days", type=int, default=default_window)
    args = parser.parse_args()
    consume(args.day, args.window_days)


if __name__ == "__main__":
    # Exit via os._exit to skip interpreter teardown: pyarrow's native thread
    # pools can abort ("terminate called without an active exception", exit 133)
    # during static destruction, which would otherwise fail this batch task.
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - surface a clear failure to Airflow
        print(f"ml-mock FAILED: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.stderr.flush()
        os._exit(1)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)

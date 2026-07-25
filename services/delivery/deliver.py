#!/usr/bin/env python3
"""delivery: hand one Gold ``computer_features`` partition to the ML consumer.

Reads the Gold Delta partition for ``(window_days, anchor_day)`` directly with the
``deltalake`` (delta-rs) reader -- lightweight and partition-correct (it resolves
the active files from ``_delta_log`` so an overwrite is not double-counted). The
rows are written as columnar Parquet, Fernet-encrypted at rest, and uploaded to
the ``delivered`` bucket together with a delivery manifest.

Object keys are fixed per partition, so re-delivering an anchor day overwrites the
same objects without duplicating anything.

The manifest is written into the ``delivered`` bucket alongside the encrypted data.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import traceback

import boto3
import bundle
import pyarrow.parquet as pq
from botocore.client import Config
from deltalake import DeltaTable


def _storage_options() -> dict[str, str]:
    """delta-rs (object_store) S3 options pointing at MinIO."""
    return {
        "AWS_ENDPOINT_URL": os.environ["MINIO_ENDPOINT"],
        "AWS_ACCESS_KEY_ID": os.environ["MINIO_ROOT_USER"],
        "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_ROOT_PASSWORD"],
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
    }


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def read_gold_partition(anchor_day: int, window_days: int):
    """Return the Gold partition as a pyarrow Table in canonical column order."""
    gold_bucket = os.environ.get("GOLD_BUCKET", "gold")
    uri = f"s3://{gold_bucket}/{bundle.DATASET}"
    table = DeltaTable(uri, storage_options=_storage_options()).to_pyarrow_table(
        partitions=[
            ("window_days", "=", str(window_days)),
            ("anchor_day", "=", str(anchor_day)),
        ]
    )
    # Enforce the binding schema/order at the source; a missing column raises
    # here rather than delivering an off-contract file.
    return table.select(bundle.DELIVERED_COLUMNS)


def _ensure_bucket(client, bucket: str) -> None:
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        print(f"  created bucket {bucket}", flush=True)


def deliver(anchor_day: int, window_days: int) -> dict:
    """Read, encrypt, and upload one partition + manifest; return the manifest."""
    schema_version = os.environ.get("SCHEMA_VERSION", "v1")
    key = os.environ["DELIVERY_ENCRYPTION_KEY"]
    delivered_bucket = os.environ.get("DELIVERED_BUCKET", "delivered")

    table = read_gold_partition(anchor_day, window_days)
    record_count = table.num_rows
    if record_count == 0:
        raise ValueError(
            f"delivery: no Gold rows for anchor_day={anchor_day}, window_days={window_days}; "
            "run silver_to_gold for this day first"
        )

    buf = io.BytesIO()
    pq.write_table(table, buf)
    plaintext = buf.getvalue()

    ciphertext = bundle.encrypt(plaintext, key)
    manifest = bundle.build_manifest(
        schema_version=schema_version,
        window_days=window_days,
        anchor_day=anchor_day,
        record_count=record_count,
        plaintext=plaintext,
        key=key,
        columns=table.column_names,
    )

    client = _s3_client()
    _ensure_bucket(client, delivered_bucket)
    client.put_object(
        Bucket=delivered_bucket,
        Key=bundle.data_key(anchor_day, window_days),
        Body=ciphertext,
    )
    client.put_object(
        Bucket=delivered_bucket,
        Key=bundle.manifest_key(anchor_day, window_days),
        Body=bundle.manifest_bytes(manifest),
    )
    print(
        f"delivery: anchor_day={anchor_day} window_days={window_days} "
        f"-> s3://{delivered_bucket}/{bundle.data_key(anchor_day, window_days)} "
        f"({record_count:,} rows, {len(ciphertext):,} bytes encrypted, "
        f"sha256={manifest['checksum_sha256'][:12]}...)",
        flush=True,
    )
    return manifest


def main() -> None:
    """Parse args and deliver one anchor-day partition."""
    default_window = int(os.environ.get("ROLLING_WINDOW_DAYS", "7"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=int, required=True, help="anchor day to deliver")
    parser.add_argument("--window-days", type=int, default=default_window)
    args = parser.parse_args()
    deliver(args.day, args.window_days)


if __name__ == "__main__":
    # Exit via os._exit to skip interpreter teardown: the native pyarrow/deltalake
    # thread pools can abort ("terminate called without an active exception",
    # exit 133) during static destruction, which would otherwise fail this task.
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - surface a clear failure to Airflow
        print(f"delivery FAILED: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.stderr.flush()
        os._exit(1)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)

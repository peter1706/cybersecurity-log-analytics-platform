#!/usr/bin/env python3
"""Always-on Streamlit dashboard over the delivered store (host port 8501).

Reads *only* the `delivered` bucket: lists every delivered partition, shows its
manifest metadata (schema version, record count, checksum, timestamp), and lets
you decrypt and preview one partition. It never touches landing/bronze/silver/gold
-- the same least-privilege boundary the ml_consume task uses.
"""

from __future__ import annotations

import json
import logging
import os

import boto3
import pandas as pd
import streamlit as st
from botocore.client import BaseClient, Config

import feature_contract
from catalog import read_secret
from catalog.parquet_encryption import read_encrypted_parquet

DATASET = "computer_features"

logger = logging.getLogger(__name__)


def _s3_client() -> BaseClient:
    # Same `delivered`-scoped MinIO service account as the ml_consume task.
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


def _list_manifests(client, bucket: str) -> list[dict]:
    paginator = client.get_paginator("list_objects_v2")
    manifests = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{DATASET}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("manifest.json"):
                body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                manifests.append(json.loads(body))
    return sorted(manifests, key=lambda m: (m["window_days"], m["anchor_day"]))


def _load_preview(client, bucket: str, manifest: dict, key: str) -> pd.DataFrame:
    ciphertext = client.get_object(Bucket=bucket, Key=manifest["data_object"])["Body"].read()
    # Authenticated (AES-GCM) decryption fails on any tampering/corruption.
    return read_encrypted_parquet(ciphertext, key).to_pandas()


def main() -> None:
    st.set_page_config(page_title="Delivered features", layout="wide")
    st.title("Delivered feature store")
    st.caption(
        "Consumer view of the `delivered` bucket only "
        "(computer_features · encrypted at rest · manifest-verified)."
    )

    bucket = os.environ.get("DELIVERED_BUCKET", "delivered")
    key = read_secret("delivery_encryption_key", env="DELIVERY_ENCRYPTION_KEY", default="")
    client = _s3_client()

    try:
        manifests = _list_manifests(client, bucket)
    except Exception as exc:  # noqa: BLE001 - dashboard should show, not crash
        logger.exception("Could not list deliveries in '%s'", bucket)
        st.error(f"Could not list deliveries in '{bucket}': {exc}")
        return

    if not manifests:
        st.info(f"No deliveries yet in '{bucket}'. Run the pipeline's `deliver` task.")
        return

    overview = pd.DataFrame(
        [
            {
                "anchor_day": m["anchor_day"],
                "window_days": m["window_days"],
                "records": m["record_count"],
                "schema": m["schema_version"],
                "created_at": m["created_at"],
                "checksum": m["checksum_sha256"][:12] + "...",
                "key_id": m.get("encryption", {}).get("key_id", ""),
            }
            for m in manifests
        ]
    )
    st.subheader(f"Deliveries ({len(manifests)})")
    st.dataframe(overview, use_container_width=True, hide_index=True)

    labels = {f"anchor_day={m['anchor_day']} · window={m['window_days']}d": m for m in manifests}
    choice = st.selectbox("Preview a partition", list(labels))
    manifest = labels[choice]

    schema_ok = manifest.get("columns") == feature_contract.EXPECTED_COLUMNS
    st.write(
        f"Schema conformance: {'PASS' if schema_ok else 'FAIL'} · "
        f"{manifest['record_count']:,} records"
    )

    if not key:
        st.warning("DELIVERY_ENCRYPTION_KEY not set; cannot decrypt for preview.")
        return
    try:
        df = _load_preview(client, bucket, manifest, key)
    except Exception as exc:  # noqa: BLE001 - dashboard should show, not crash
        logger.exception("Could not decrypt/preview partition")
        st.error(f"Could not decrypt/preview: {exc}")
        return
    st.dataframe(df.head(50), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

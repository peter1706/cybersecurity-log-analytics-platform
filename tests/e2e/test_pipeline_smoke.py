"""End-to-end smoke test for first thin vertical slice.

Assumes the pipeline has already run (see `make e2e` / scripts/e2e_pipeline.sh),
then verifies the Gold Delta table for auth features contains rows by reading its
parquet parts directly from MinIO over the host-mapped S3 port.
"""

import io
import os

import pytest

boto3 = pytest.importorskip("boto3")
pq = pytest.importorskip("pyarrow.parquet")

pytestmark = pytest.mark.e2e

ENDPOINT = os.environ.get("MINIO_ENDPOINT_HOST", "http://localhost:9000")
ACCESS = os.environ.get("MINIO_ROOT_USER", "minioadmin")
SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")
GOLD_TABLE = "auth_features"


def _client():
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def test_gold_auth_features_has_rows():
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    parquet_keys = []
    for page in paginator.paginate(Bucket=GOLD_BUCKET, Prefix=f"{GOLD_TABLE}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and "_delta_log" not in key:
                parquet_keys.append(key)

    assert parquet_keys, f"no parquet parts under {GOLD_BUCKET}/{GOLD_TABLE}/"

    total_rows = 0
    for key in parquet_keys:
        body = client.get_object(Bucket=GOLD_BUCKET, Key=key)["Body"].read()
        total_rows += pq.read_table(io.BytesIO(body)).num_rows

    assert total_rows > 0, "Gold auth_features table has no rows"

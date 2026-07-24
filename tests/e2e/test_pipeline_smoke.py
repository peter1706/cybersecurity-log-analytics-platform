"""End-to-end smoke test for the daily pipeline.

Assumes the pipeline has already run (see `make e2e` / scripts/e2e_pipeline.sh),
then verifies each Silver source table and the auth Gold feature table contain
rows by reading their parquet parts directly from MinIO over the host-mapped S3
port.
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
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")
SILVER_SOURCES = ("auth", "proc", "flows", "dns")
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


def _table_row_count(client, bucket: str, table: str) -> int:
    paginator = client.get_paginator("list_objects_v2")
    parquet_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{table}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and "_delta_log" not in key:
                parquet_keys.append(key)

    assert parquet_keys, f"no parquet parts under {bucket}/{table}/"

    total_rows = 0
    for key in parquet_keys:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        total_rows += pq.read_table(io.BytesIO(body)).num_rows
    return total_rows


@pytest.mark.parametrize("source", SILVER_SOURCES)
def test_silver_source_has_rows(source):
    rows = _table_row_count(_client(), SILVER_BUCKET, source)
    assert rows > 0, f"Silver {source} table has no rows"


def test_gold_auth_features_has_rows():
    rows = _table_row_count(_client(), GOLD_BUCKET, GOLD_TABLE)
    assert rows > 0, "Gold auth_features table has no rows"

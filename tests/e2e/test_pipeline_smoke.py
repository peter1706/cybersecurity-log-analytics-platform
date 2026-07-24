"""End-to-end smoke test for the daily pipeline.

Assumes the pipeline has already run (see `make e2e` / scripts/e2e_pipeline.sh),
then verifies each Silver source table and the unified Gold computer_features
table contain rows by reading their parquet parts directly from MinIO over the
host-mapped S3 port.
"""

import io
import json
import os
import pathlib
import re
import shutil
import subprocess

import pytest

boto3 = pytest.importorskip("boto3")
pq = pytest.importorskip("pyarrow.parquet")

pytestmark = pytest.mark.e2e

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
LOGICAL_DATE = "2026-01-01"

ENDPOINT = os.environ.get("MINIO_ENDPOINT_HOST", "http://localhost:9000")
ACCESS = os.environ.get("MINIO_ROOT_USER", "minioadmin")
SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")
SILVER_SOURCES = ("auth", "proc", "flows", "dns")
FEATURES_TABLE = "computer_features"


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


def _active_delta_files(client, bucket: str, table: str) -> set[str]:
    """Return the data files a Delta table currently references (add - remove).

    Reads the JSON commits under ``_delta_log/`` in version order and applies
    each ``add``/``remove`` action. Raw parquet enumeration double-counts after an
    overwrite (superseded files linger until vacuum); the log tells us which files
    are logically live. Assumes no checkpoint parquet has been written yet, which
    holds for the low commit counts this demo produces.
    """
    paginator = client.get_paginator("list_objects_v2")
    commit_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{table}/_delta_log/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                commit_keys.append(key)
    # Delta commit files are zero-padded (e.g. 00000000000000000001.json), so a
    # lexicographic sort is also the version order.
    commit_keys.sort()

    active: set[str] = set()
    for key in commit_keys:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action:
                active.add(action["add"]["path"])
            elif "remove" in action:
                active.discard(action["remove"]["path"])
    return active


def _delta_row_count(client, bucket: str, table: str) -> int:
    """Logical row count of a Delta table (only files it currently references)."""
    active = _active_delta_files(client, bucket, table)
    assert active, f"no active data files in Delta table {bucket}/{table}/"
    total = 0
    for rel_path in active:
        body = client.get_object(Bucket=bucket, Key=f"{table}/{rel_path}")["Body"].read()
        total += pq.read_table(io.BytesIO(body)).num_rows
    return total


def _run_task(task: str) -> subprocess.CompletedProcess:
    """Re-run one daily_pipeline task synchronously inside the scheduler container."""
    return subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "tasks",
            "test",
            "daily_pipeline",
            task,
            LOGICAL_DATE,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("source", SILVER_SOURCES)
def test_silver_source_has_rows(source):
    rows = _table_row_count(_client(), SILVER_BUCKET, source)
    assert rows > 0, f"Silver {source} table has no rows"


def test_gold_computer_features_has_rows():
    rows = _table_row_count(_client(), GOLD_BUCKET, FEATURES_TABLE)
    assert rows > 0, "Gold computer_features table has no rows"


def test_gold_rerun_is_idempotent():
    """Re-running the Gold job for the same anchor day must not double-count.

    Dynamic partition overwrite replaces only the anchor-day partition, so the
    logical row count (read via the Delta log) is unchanged across a re-run.
    """
    if shutil.which("docker") is None:
        pytest.skip("docker not available to re-run the Gold task")

    table = FEATURES_TABLE
    client = _client()
    before = _delta_row_count(client, GOLD_BUCKET, table)
    assert before > 0, f"Gold {table} table is empty before the re-run"

    result = _run_task("silver_to_gold")
    combined = result.stdout + result.stderr
    if re.search(r"Task failed with exception|new_state=(failed|up_for_retry)", combined, re.I):
        pytest.fail(f"silver_to_gold re-run reported failure:\n{combined}")

    after = _delta_row_count(client, GOLD_BUCKET, table)
    assert after == before, f"{table} row count changed on re-run: {before} -> {after}"

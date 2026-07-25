"""Shared helpers for the end-to-end tests (not collected as a test module).

Reused by both the day-0 smoke test and the full 7-day rolling-window test so the
MinIO/Delta/catalog access logic lives in one place. Import from a test module,
which guards ``boto3``/``pyarrow`` with ``importorskip`` first.
"""

import io
import json
import os
import pathlib
import shutil
import subprocess

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
LOGICAL_DATE = "2026-01-01"

ENDPOINT = os.environ.get("MINIO_ENDPOINT_HOST", "http://localhost:9000")
ACCESS = os.environ.get("MINIO_ROOT_USER", "minioadmin")
SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")
DELIVERED_BUCKET = os.environ.get("DELIVERED_BUCKET", "delivered")
SILVER_SOURCES = ("auth", "proc", "flows", "dns")
FEATURES_TABLE = "computer_features"
WINDOW_DAYS = int(os.environ.get("ROLLING_WINDOW_DAYS", "7"))
CATALOG_DB_USER = os.environ.get("CATALOG_DB_USER", "catalog")
CATALOG_DB_NAME = os.environ.get("CATALOG_DB_NAME", "catalog")


def client():
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def table_row_count(s3, bucket: str, table: str) -> int:
    """Sum rows across every parquet part under a table prefix (ignores _delta_log)."""
    paginator = s3.get_paginator("list_objects_v2")
    parquet_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{table}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and "_delta_log" not in key:
                parquet_keys.append(key)

    assert parquet_keys, f"no parquet parts under {bucket}/{table}/"

    total_rows = 0
    for key in parquet_keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        total_rows += pq.read_table(io.BytesIO(body)).num_rows
    return total_rows


def active_delta_files(s3, bucket: str, table: str) -> set[str]:
    """Return the data files a Delta table currently references (add - remove).

    Reads the JSON commits under ``_delta_log/`` in version order and applies
    each ``add``/``remove`` action. Raw parquet enumeration double-counts after an
    overwrite (superseded files linger until vacuum); the log tells us which files
    are logically live. Assumes no checkpoint parquet has been written yet, which
    holds for the low commit counts this demo produces.
    """
    paginator = s3.get_paginator("list_objects_v2")
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
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action:
                active.add(action["add"]["path"])
            elif "remove" in action:
                active.discard(action["remove"]["path"])
    return active


def delta_row_count(s3, bucket: str, table: str) -> int:
    """Logical row count of a Delta table (only files it currently references)."""
    active = active_delta_files(s3, bucket, table)
    assert active, f"no active data files in Delta table {bucket}/{table}/"
    total = 0
    for rel_path in active:
        body = s3.get_object(Bucket=bucket, Key=f"{table}/{rel_path}")["Body"].read()
        total += pq.read_table(io.BytesIO(body)).num_rows
    return total


def delta_column_sum_by_partition(s3, bucket: str, table: str, column: str, partition: str) -> dict:
    """Sum ``column`` grouped by a Hive partition value parsed from the file path.

    Partition columns are encoded in the Delta file path (e.g.
    ``.../anchor_day=06/part-*.parquet``), not in the parquet payload, so the
    value is read from the path and the metric summed from the data file.
    Returns ``{partition_value(int): sum}``.
    """
    import re

    active = active_delta_files(s3, bucket, table)
    assert active, f"no active data files in Delta table {bucket}/{table}/"
    sums: dict[int, float] = {}
    pattern = re.compile(rf"{re.escape(partition)}=(\d+)")
    for rel_path in active:
        match = pattern.search(rel_path)
        if match is None:
            continue
        value = int(match.group(1))
        body = s3.get_object(Bucket=bucket, Key=f"{table}/{rel_path}")["Body"].read()
        col = pq.read_table(io.BytesIO(body), columns=[column])[column]
        total = pc.sum(col).as_py() or 0
        sums[value] = sums.get(value, 0) + total
    return sums


def read_delta_table(s3, bucket: str, table: str) -> "pa.Table":
    """Read a whole Delta table's active files into one pyarrow Table."""
    active = sorted(active_delta_files(s3, bucket, table))
    assert active, f"no active data files in Delta table {bucket}/{table}/"
    tables = []
    for rel_path in active:
        body = s3.get_object(Bucket=bucket, Key=f"{table}/{rel_path}")["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    return pa.concat_tables(tables)


def read_delta_partition(s3, bucket: str, table: str, partition: str, value: int) -> "pa.Table":
    """Read only the active files of one Hive partition (matched by integer value).

    Matches ``partition=<n>`` in the file path irrespective of zero-padding, so it
    works for both Spark's unpadded Delta paths and padded delivered keys.
    """
    import re

    active = active_delta_files(s3, bucket, table)
    pattern = re.compile(rf"{re.escape(partition)}=(\d+)")
    rel_paths = [p for p in active if (m := pattern.search(p)) and int(m.group(1)) == value]
    assert rel_paths, f"no active files for {partition}={value} in {bucket}/{table}/"
    tables = []
    for rel_path in sorted(rel_paths):
        body = s3.get_object(Bucket=bucket, Key=f"{table}/{rel_path}")["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    return pa.concat_tables(tables)


def run_task(task: str) -> subprocess.CompletedProcess:
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


def catalog_count(table: str, where: str = "") -> int:
    """Row count of a governance-catalog table via psql in postgres-catalog."""
    if shutil.which("docker") is None:
        pytest.skip("docker not available to query the governance catalog")
    clause = f" WHERE {where}" if where else ""
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres-catalog",
            "psql",
            "-U",
            CATALOG_DB_USER,
            "-d",
            CATALOG_DB_NAME,
            "-tAc",
            f"SELECT count(*) FROM {table}{clause};",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(f"psql count on {table} failed:\n{result.stdout}\n{result.stderr}")
    return int(result.stdout.strip())

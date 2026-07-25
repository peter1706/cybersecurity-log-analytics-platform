"""End-to-end smoke test for the daily pipeline.

Assumes the pipeline has already run (see `make e2e` / scripts/e2e_pipeline.sh),
then verifies each Silver source table, the unified Gold computer_features table,
and the encrypted delivered partition + manifest by reading MinIO directly over
the host-mapped S3 port.
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
DELIVERED_BUCKET = os.environ.get("DELIVERED_BUCKET", "delivered")
SILVER_SOURCES = ("auth", "proc", "flows", "dns")
FEATURES_TABLE = "computer_features"
WINDOW_DAYS = int(os.environ.get("ROLLING_WINDOW_DAYS", "7"))
ANCHOR_DAY = 0  # scripts/e2e_pipeline.sh runs the pipeline for day 0
CATALOG_DB_USER = os.environ.get("CATALOG_DB_USER", "catalog")
CATALOG_DB_NAME = os.environ.get("CATALOG_DB_NAME", "catalog")


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


def _catalog_count(table: str, where: str = "") -> int:
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


def test_governance_lineage_rows_persisted():
    """Each layer-crossing task records lineage (landing + Bronze/Silver/Gold)."""
    assert _catalog_count("lineage", "to_layer = 'landing'") > 0
    assert _catalog_count("lineage", "to_layer = 'bronze'") > 0
    assert _catalog_count("lineage", "to_layer = 'silver'") > 0
    assert _catalog_count("lineage", "to_layer = 'gold'") > 0


def test_governance_schema_registry_rows_persisted():
    """The Spark jobs register the written schema for Bronze/Silver/Gold."""
    assert _catalog_count("schema_registry", "layer = 'gold'") > 0


def test_governance_delivery_manifest_persisted():
    """The deliver task writes a manifest row for the delivered partition."""
    assert (
        _catalog_count(
            "delivery_manifests",
            f"window_days = {WINDOW_DAYS} AND anchor_day = {ANCHOR_DAY}",
        )
        == 1
    )


def test_governance_job_runs_persisted():
    """Airflow callbacks log a job-run row per executed task."""
    assert _catalog_count("job_runs", "status = 'success'") > 0


@pytest.mark.parametrize("source", SILVER_SOURCES)
def test_silver_source_has_rows(source):
    rows = _table_row_count(_client(), SILVER_BUCKET, source)
    assert rows > 0, f"Silver {source} table has no rows"


def test_gold_computer_features_has_rows():
    rows = _table_row_count(_client(), GOLD_BUCKET, FEATURES_TABLE)
    assert rows > 0, "Gold computer_features table has no rows"


def _delivered_prefix() -> str:
    return f"{FEATURES_TABLE}/window_days={WINDOW_DAYS}/anchor_day={ANCHOR_DAY:02d}"


def test_delivery_produces_encrypted_object_and_manifest():
    """The deliver task must write an encrypted Parquet + a contract-conformant manifest."""
    import feature_contract

    client = _client()
    prefix = _delivered_prefix()

    manifest = json.loads(
        client.get_object(Bucket=DELIVERED_BUCKET, Key=f"{prefix}/manifest.json")["Body"].read()
    )
    assert manifest["record_count"] > 0
    assert manifest["window_days"] == WINDOW_DAYS
    assert manifest["anchor_day"] == ANCHOR_DAY
    assert manifest["columns"] == feature_contract.EXPECTED_COLUMNS
    assert manifest["encryption"]["scheme"] == "fernet"

    head = client.head_object(Bucket=DELIVERED_BUCKET, Key=manifest["data_object"])
    assert head["ContentLength"] > 0, "encrypted delivered object is empty"


def test_delivery_rerun_is_idempotent():
    """Re-running deliver for the same anchor day overwrites the same fixed keys."""
    if shutil.which("docker") is None:
        pytest.skip("docker not available to re-run the deliver task")

    client = _client()
    prefix = _delivered_prefix()

    def _keys() -> set[str]:
        paginator = client.get_paginator("list_objects_v2")
        keys = set()
        for page in paginator.paginate(Bucket=DELIVERED_BUCKET, Prefix=f"{FEATURES_TABLE}/"):
            for obj in page.get("Contents", []):
                keys.add(obj["Key"])
        return keys

    before = _keys()
    assert f"{prefix}/manifest.json" in before

    result = _run_task("deliver")
    combined = result.stdout + result.stderr
    if re.search(r"Task failed with exception|new_state=(failed|up_for_retry)", combined, re.I):
        pytest.fail(f"deliver re-run reported failure:\n{combined}")

    assert _keys() == before, "re-delivery changed the set of delivered objects"


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

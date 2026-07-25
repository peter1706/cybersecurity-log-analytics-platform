"""End-to-end smoke test for the daily pipeline (day 0).

Assumes the pipeline has already run (see `make e2e` / scripts/e2e_pipeline.sh),
then verifies each Silver source table, the unified Gold computer_features table,
and the encrypted delivered partition + manifest by reading MinIO directly over
the host-mapped S3 port. Shared MinIO/Delta/catalog access lives in
``_e2e_helpers``; the full 7-day window is covered separately by
``test_full_window`` (marked ``e2e_full``).
"""

import json
import re
import shutil

import pytest

pytest.importorskip("boto3")
pytest.importorskip("pyarrow.parquet")

from _e2e_helpers import (  # noqa: E402
    DELIVERED_BUCKET,
    FEATURES_TABLE,
    GOLD_BUCKET,
    SILVER_BUCKET,
    SILVER_SOURCES,
    WINDOW_DAYS,
    catalog_count,
    client,
    delta_row_count,
    run_task,
    table_row_count,
)

pytestmark = pytest.mark.e2e

ANCHOR_DAY = 0  # scripts/e2e_pipeline.sh runs the pipeline for day 0


def test_governance_lineage_rows_persisted():
    """Each layer-crossing task records lineage (landing + Bronze/Silver/Gold)."""
    assert catalog_count("lineage", "to_layer = 'landing'") > 0
    assert catalog_count("lineage", "to_layer = 'bronze'") > 0
    assert catalog_count("lineage", "to_layer = 'silver'") > 0
    assert catalog_count("lineage", "to_layer = 'gold'") > 0


def test_governance_schema_registry_rows_persisted():
    """The Spark jobs register the written schema for Bronze/Silver/Gold."""
    assert catalog_count("schema_registry", "layer = 'gold'") > 0


def test_governance_delivery_manifest_persisted():
    """The deliver task writes a manifest row for the delivered partition."""
    assert (
        catalog_count(
            "delivery_manifests",
            f"window_days = {WINDOW_DAYS} AND anchor_day = {ANCHOR_DAY}",
        )
        == 1
    )


def test_governance_job_runs_persisted():
    """Airflow callbacks log a job-run row per executed task."""
    assert catalog_count("job_runs", "status = 'success'") > 0


def test_checksum_chain_persisted():
    """Every layer records a content checksum (landing -> Bronze -> Silver -> Gold)."""
    assert catalog_count("checksums", "layer = 'landing'") > 0
    assert catalog_count("checksums", "layer = 'bronze'") > 0
    assert catalog_count("checksums", "layer = 'silver'") > 0
    assert (
        catalog_count(
            "checksums",
            f"layer = 'gold' AND window_days = {WINDOW_DAYS} AND day = {ANCHOR_DAY}",
        )
        == 1
    )


@pytest.mark.parametrize("source", SILVER_SOURCES)
def test_silver_source_has_rows(source):
    rows = table_row_count(client(), SILVER_BUCKET, source)
    assert rows > 0, f"Silver {source} table has no rows"


def test_gold_computer_features_has_rows():
    rows = table_row_count(client(), GOLD_BUCKET, FEATURES_TABLE)
    assert rows > 0, "Gold computer_features table has no rows"


def _delivered_prefix() -> str:
    return f"{FEATURES_TABLE}/window_days={WINDOW_DAYS}/anchor_day={ANCHOR_DAY:02d}"


def test_delivery_produces_encrypted_object_and_manifest():
    """The deliver task must write an encrypted Parquet + a contract-conformant manifest."""
    import feature_contract

    s3 = client()
    prefix = _delivered_prefix()

    manifest = json.loads(
        s3.get_object(Bucket=DELIVERED_BUCKET, Key=f"{prefix}/manifest.json")["Body"].read()
    )
    assert manifest["record_count"] > 0
    assert manifest["window_days"] == WINDOW_DAYS
    assert manifest["anchor_day"] == ANCHOR_DAY
    assert manifest["columns"] == feature_contract.EXPECTED_COLUMNS
    assert manifest["encryption"]["scheme"] == "fernet"

    head = s3.head_object(Bucket=DELIVERED_BUCKET, Key=manifest["data_object"])
    assert head["ContentLength"] > 0, "encrypted delivered object is empty"


def test_delivery_rerun_is_idempotent():
    """Re-running deliver for the same anchor day overwrites the same fixed keys."""
    if shutil.which("docker") is None:
        pytest.skip("docker not available to re-run the deliver task")

    s3 = client()
    prefix = _delivered_prefix()

    def _keys() -> set[str]:
        paginator = s3.get_paginator("list_objects_v2")
        keys = set()
        for page in paginator.paginate(Bucket=DELIVERED_BUCKET, Prefix=f"{FEATURES_TABLE}/"):
            for obj in page.get("Contents", []):
                keys.add(obj["Key"])
        return keys

    before = _keys()
    assert f"{prefix}/manifest.json" in before

    result = run_task("deliver")
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

    s3 = client()
    before = delta_row_count(s3, GOLD_BUCKET, FEATURES_TABLE)
    assert before > 0, f"Gold {FEATURES_TABLE} table is empty before the re-run"

    result = run_task("silver_to_gold")
    combined = result.stdout + result.stderr
    if re.search(r"Task failed with exception|new_state=(failed|up_for_retry)", combined, re.I):
        pytest.fail(f"silver_to_gold re-run reported failure:\n{combined}")

    after = delta_row_count(s3, GOLD_BUCKET, FEATURES_TABLE)
    assert after == before, f"{FEATURES_TABLE} row count changed on re-run: {before} -> {after}"

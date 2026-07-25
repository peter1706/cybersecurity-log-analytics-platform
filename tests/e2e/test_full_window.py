"""End-to-end test for a full rolling-window backfill (days 0..N).

Unlike the day-0 smoke test, this exercises the *multi-day* Silver -> Gold
windowing: it assumes the pipeline was backfilled across a full window (see
`make e2e-full` / scripts/e2e_full_window.sh, which runs scripts/backfill.sh),
then asserts that the final anchor day aggregated a genuine N-day window -- the
one behaviour the single-day smoke test structurally cannot cover.

Marked ``e2e_full`` so it is excluded from both the fast unit suite and the
day-0 ``e2e`` job; it is heavyweight (~tens of minutes locally) and opt-in.
"""

import json
import os

import pytest

pytest.importorskip("boto3")
pytest.importorskip("pyarrow.parquet")
import pyarrow.compute as pc  # noqa: E402
from _e2e_helpers import (  # noqa: E402
    DELIVERED_BUCKET,
    FEATURES_TABLE,
    GOLD_BUCKET,
    SILVER_BUCKET,
    SILVER_SOURCES,
    WINDOW_DAYS,
    catalog_count,
    client,
    delta_column_sum_by_partition,
    read_delta_partition,
    table_row_count,
)

pytestmark = pytest.mark.e2e_full

# The backfill seeds days 0..ANCHOR_DAY; the last day is the only anchor whose
# trailing window is fully populated. Defaults match `make e2e-full` (0..6).
ANCHOR_DAY = int(os.environ.get("E2E_FULL_ANCHOR_DAY", str(WINDOW_DAYS - 1)))
FIRST_ANCHOR_DAY = 0
EXPECTED_DAYS = ANCHOR_DAY + 1  # inclusive 0..ANCHOR_DAY


def test_all_silver_sources_backfilled():
    """Each source has Silver rows after the multi-day backfill."""
    s3 = client()
    for source in SILVER_SOURCES:
        assert table_row_count(s3, SILVER_BUCKET, source) > 0, f"Silver {source} empty"


def test_silver_checksums_recorded_for_every_backfilled_day():
    """A Silver checksum exists per (source, day) across the whole window."""
    # 4 sources x EXPECTED_DAYS distinct partitions (upserts, so no duplicates).
    assert catalog_count("checksums", "layer = 'silver'") >= len(SILVER_SOURCES) * EXPECTED_DAYS


def test_gold_checksum_present_for_final_anchor():
    """The final anchor day's Gold partition recorded its content checksum."""
    assert (
        catalog_count(
            "checksums",
            f"layer = 'gold' AND window_days = {WINDOW_DAYS} AND day = {ANCHOR_DAY}",
        )
        == 1
    )


def test_gold_built_for_every_anchor_day():
    """silver_to_gold ran once per anchor day in the backfilled range."""
    assert catalog_count("lineage", "to_layer = 'gold'") >= EXPECTED_DAYS


def test_delivery_manifest_for_final_anchor():
    """The final anchor day was delivered exactly once (fixed keys, idempotent)."""
    assert (
        catalog_count(
            "delivery_manifests",
            f"window_days = {WINDOW_DAYS} AND anchor_day = {ANCHOR_DAY}",
        )
        == 1
    )


def test_final_anchor_delivered_object_and_manifest():
    """The final anchor day produced an encrypted, contract-conformant delivery."""
    import feature_contract

    s3 = client()
    prefix = f"{FEATURES_TABLE}/window_days={WINDOW_DAYS}/anchor_day={ANCHOR_DAY:02d}"
    manifest = json.loads(
        s3.get_object(Bucket=DELIVERED_BUCKET, Key=f"{prefix}/manifest.json")["Body"].read()
    )
    assert manifest["record_count"] > 0
    assert manifest["window_days"] == WINDOW_DAYS
    assert manifest["anchor_day"] == ANCHOR_DAY
    assert manifest["columns"] == feature_contract.EXPECTED_COLUMNS


def test_final_anchor_reports_all_sources_present():
    """With every source backfilled across the window, source_present_* are all true."""
    s3 = client()
    partition = read_delta_partition(s3, GOLD_BUCKET, FEATURES_TABLE, "anchor_day", ANCHOR_DAY)
    assert partition.num_rows > 0
    for source in SILVER_SOURCES:
        column = partition[f"source_present_{source}"]
        assert pc.all(column).as_py() is True, f"source_present_{source} not all true"


def test_window_aggregates_multiple_days():
    """The multi-day proof: the full-window anchor aggregates strictly more than day 0.

    ``auth_out_event_count`` is a plain per-window event counter, so summed across
    computers the full N-day window (final anchor) must exceed the single-day
    window at anchor day 0. This is exactly what the day-0 smoke test cannot show.
    """
    s3 = client()
    sums = delta_column_sum_by_partition(
        s3, GOLD_BUCKET, FEATURES_TABLE, "auth_out_event_count", "anchor_day"
    )
    assert FIRST_ANCHOR_DAY in sums, f"no Gold partition for anchor day {FIRST_ANCHOR_DAY}"
    assert ANCHOR_DAY in sums, f"no Gold partition for anchor day {ANCHOR_DAY}"
    assert len(sums) >= EXPECTED_DAYS, f"expected >= {EXPECTED_DAYS} anchor partitions, got {sums}"
    assert sums[ANCHOR_DAY] > sums[FIRST_ANCHOR_DAY], (
        f"full-window anchor {ANCHOR_DAY} auth events ({sums[ANCHOR_DAY]}) did not exceed "
        f"day-0 window ({sums[FIRST_ANCHOR_DAY]}) -- window may not be aggregating multiple days"
    )

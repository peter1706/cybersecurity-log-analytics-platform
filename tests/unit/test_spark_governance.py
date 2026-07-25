"""Unit tests for spark-processor governance record building (no Spark session).

The jobs' ``governance_records`` methods are pure: they build the lineage +
schema-registry records stamped on each Medallion write. They are exercised here
with ``spark=None`` since they never touch the session.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from jobs.base import BronzeToSilverJob, ComputerFeaturesJob, LandToBronzeJob  # noqa: E402
from jobs.sources import get_source  # noqa: E402


class TestMedallionGovernanceRecords:
    def test_land_to_bronze_crosses_landing_to_bronze(self):
        job = LandToBronzeJob(None, get_source("auth"), day=4)
        lineage, schema = job.governance_records(10, ["source", "day", "time"])

        assert lineage.source == "auth"
        assert lineage.day == 4
        assert lineage.from_layer == "landing"
        assert lineage.to_layer == "bronze"
        assert lineage.record_count == 10
        assert lineage.schema_version == "v1"
        assert lineage.window_days is None

        assert schema.source == "auth"
        assert schema.layer == "bronze"
        assert schema.schema_version == "v1"
        assert schema.columns == ["source", "day", "time"]

    def test_bronze_to_silver_crosses_bronze_to_silver(self):
        job = BronzeToSilverJob(None, get_source("flows"), day=2)
        lineage, schema = job.governance_records(3, ["computer_id"])

        assert lineage.from_layer == "bronze"
        assert lineage.to_layer == "silver"
        assert schema.layer == "silver"
        assert schema.source == "flows"

    def test_schema_version_follows_env(self, monkeypatch):
        monkeypatch.setenv("SCHEMA_VERSION", "v2")
        job = LandToBronzeJob(None, get_source("dns"), day=0)
        lineage, schema = job.governance_records(1, ["a"])
        assert lineage.schema_version == "v2"
        assert schema.schema_version == "v2"


class TestComputerFeaturesGovernanceRecords:
    def test_silver_to_gold_records_window_and_gold_table(self):
        job = ComputerFeaturesJob(None, day=6)
        lineage, schema = job.governance_records(50, ["computer_id", "anchor_day"], window=7)

        assert lineage.source is None
        assert lineage.day == 6
        assert lineage.from_layer == "silver"
        assert lineage.to_layer == "gold"
        assert lineage.record_count == 50
        assert lineage.window_days == 7

        assert schema.source == "computer_features"
        assert schema.layer == "gold"
        assert schema.columns == ["computer_id", "anchor_day"]

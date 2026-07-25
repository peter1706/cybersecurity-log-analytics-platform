"""Unit tests for spark-processor governance record building (no Spark session).

The jobs' ``governance_records`` methods are pure: they build the lineage +
schema-registry records stamped on each Medallion write. They are exercised here
with ``spark=None`` since they never touch the session.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from jobs.base import BronzeToSilverJob, ComputerFeaturesJob, LandToBronzeJob  # noqa: E402
from jobs.checksums import dataframe_checksum  # noqa: E402
from jobs.sources import get_source  # noqa: E402


class _FakeCatalog:
    """Returns a fixed stored checksum for get_checksum (validate_upstream tests)."""

    def __init__(self, stored):
        self._stored = stored

    def get_checksum(self, layer, day, source=None, window_days=None):
        return self._stored


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


class TestChecksumRecords:
    def test_medallion_checksum_record(self):
        record = LandToBronzeJob(None, get_source("auth"), day=3).checksum_record("abc123", 10)
        assert record.layer == "bronze"
        assert record.source == "auth"
        assert record.day == 3
        assert record.checksum == "abc123"
        assert record.record_count == 10
        assert record.window_days is None

    def test_gold_checksum_record_carries_window(self):
        record = ComputerFeaturesJob(None, day=6).checksum_record("xyz", 50, 7)
        assert record.layer == "gold"
        assert record.source is None
        assert record.day == 6
        assert record.window_days == 7
        assert record.checksum == "xyz"
        assert record.record_count == 50


class TestValidateUpstream:
    """The Bronze -> Silver default path recomputes and compares the row checksum."""

    def _job_and_df(self, spark):
        job = BronzeToSilverJob(spark, get_source("auth"), day=0)
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["computer_id", "v"])
        return job, df

    def test_passes_when_checksum_matches(self, spark):
        job, df = self._job_and_df(spark)
        # Should not raise when the stored checksum equals the recomputed one.
        job.validate_upstream(_FakeCatalog(dataframe_checksum(df)), df)

    def test_fails_on_mismatch(self, spark):
        job, df = self._job_and_df(spark)
        with pytest.raises(ValueError, match="checksum mismatch"):
            job.validate_upstream(_FakeCatalog("deadbeef"), df)

    def test_fails_when_upstream_checksum_missing(self, spark):
        job, df = self._job_and_df(spark)
        with pytest.raises(ValueError, match="no bronze checksum"):
            job.validate_upstream(_FakeCatalog(None), df)

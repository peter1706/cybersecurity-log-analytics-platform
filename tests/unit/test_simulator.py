"""Unit tests for the lanl-simulator: pure helpers and landing-lineage writes."""

from __future__ import annotations

import gzip

import pytest

pytest.importorskip("boto3")

import simulate  # noqa: E402


class TestPureHelpers:
    def test_day_of_uses_one_based_seconds(self):
        assert simulate.LandingSimulator.day_of("1") == 0
        assert simulate.LandingSimulator.day_of("86400") == 0
        assert simulate.LandingSimulator.day_of("86401") == 1

    def test_object_key_is_zero_padded(self):
        assert simulate.LandingSimulator.object_key("auth", 6) == "auth/day=06/auth-006.csv.gz"


class TestLandingLineage:
    def test_builds_a_landing_lineage_record(self):
        record = simulate.landing_lineage("auth", 3, 120, "v1")
        assert record.source == "auth"
        assert record.day == 3
        assert record.from_layer is None
        assert record.to_layer == "landing"
        assert record.record_count == 120
        assert record.schema_version == "v1"


class TestLandingChecksum:
    def test_hashes_the_uploaded_bytes(self):
        import hashlib

        blob = b"some gzip bytes"
        record = simulate.landing_checksum("auth", 2, blob, 5)
        assert record.layer == "landing"
        assert record.source == "auth"
        assert record.day == 2
        assert record.record_count == 5
        assert record.algorithm == "sha256"
        assert record.checksum == hashlib.sha256(blob).hexdigest()


class _FakeCatalog:
    """Context-manager stand-in for CatalogClient capturing catalog writes."""

    def __init__(self):
        self.lineage = []
        self.checksums = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def record_lineage(self, record):
        self.lineage.append(record)

    def record_checksum(self, record):
        self.checksums.append(record)


class _FakeS3:
    def __init__(self):
        self.puts = []

    def list_buckets(self):
        return {"Buckets": [{"Name": "landing"}]}

    def create_bucket(self, Bucket):  # noqa: N803 - boto3 kwarg name
        pass

    def put_object(self, Bucket, Key, Body):  # noqa: N803 - boto3 kwarg names
        self.puts.append((Bucket, Key, len(Body)))


def _simulator(tmp_path, catalog):
    sim = simulate.LandingSimulator(
        endpoint="http://minio:9000",
        access_key="user",
        secret_key="secret",
        bucket="landing",
        subset_dir=str(tmp_path),
        schema_version="v1",
        catalog_factory=lambda: catalog,
    )
    sim._client = _FakeS3()
    return sim


def _write_subset(tmp_path, source, lines):
    with gzip.open(tmp_path / f"{source}.txt.gz", "wt", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")


class TestRunRecordsLineage:
    def test_single_day_uploads_and_records_row_count(self, tmp_path):
        # Two day-0 rows (time <= 86400) -> one landing object with record_count 2.
        _write_subset(
            tmp_path,
            "auth",
            [
                "1,U1@D,U2@D,C1,C2,Ntlm,Logon,LogOn,Success",
                "2,U1@D,U3@D,C1,C3,Ntlm,Logon,LogOn,Success",
            ],
        )
        catalog = _FakeCatalog()
        sim = _simulator(tmp_path, catalog)

        keys = sim.run("auth", 0)

        assert keys == ["auth/day=00/auth-000.csv.gz"]
        assert len(sim._client.puts) == 1
        assert len(catalog.lineage) == 1
        record = catalog.lineage[0]
        assert record.to_layer == "landing"
        assert record.source == "auth"
        assert record.day == 0
        assert record.record_count == 2
        # A landing checksum is recorded alongside the lineage row.
        assert len(catalog.checksums) == 1
        assert catalog.checksums[0].layer == "landing"
        assert catalog.checksums[0].day == 0
        assert len(catalog.checksums[0].checksum) == 64  # sha256 hex

    def test_multi_day_range_records_one_row_per_uploaded_day(self, tmp_path):
        _write_subset(
            tmp_path,
            "auth",
            [
                "1,U1@D,U2@D,C1,C2,Ntlm,Logon,LogOn,Success",
                "86401,U1@D,U2@D,C1,C2,Ntlm,Logon,LogOn,Success",
                "86402,U1@D,U2@D,C1,C2,Ntlm,Logon,LogOn,Success",
            ],
        )
        catalog = _FakeCatalog()
        sim = _simulator(tmp_path, catalog)

        keys = sim.run("auth", 0, day_end=1)

        assert len(keys) == 2
        days = sorted(r.day for r in catalog.lineage)
        assert days == [0, 1]
        counts = {r.day: r.record_count for r in catalog.lineage}
        assert counts == {0: 1, 1: 2}

"""Unit tests for the pure delivery helpers (no MinIO/Delta/pyarrow required)."""

from __future__ import annotations

import hashlib
import json

import pytest

import bundle

# A sample opaque master-key string; bundle only hashes it for the manifest key id.
SAMPLE_KEY = "sample-delivery-encryption-key-value=="


class TestChecksum:
    def test_sha256_matches_hashlib(self):
        data = b"some parquet bytes"
        assert bundle.sha256_hex(data) == hashlib.sha256(data).hexdigest()

    def test_key_id_is_stable_and_not_the_key(self):
        kid = bundle.key_id(SAMPLE_KEY)
        assert kid == bundle.key_id(SAMPLE_KEY)  # stable
        assert len(kid) == 12
        assert SAMPLE_KEY not in kid  # never leaks the key material


class TestObjectKeys:
    def test_keys_are_fixed_and_zero_padded(self):
        # Fixed keys per partition are what make re-delivery idempotent.
        prefix = "computer_features/window_days=7/anchor_day=06"
        assert bundle.data_key(6, 7) == f"{prefix}/features.parquet.enc"
        assert bundle.manifest_key(6, 7) == f"{prefix}/manifest.json"


class TestManifest:
    def test_contains_required_fields_and_checksum(self):
        plaintext = b"rows"
        manifest = bundle.build_manifest(
            schema_version="v1",
            window_days=7,
            anchor_day=6,
            record_count=42,
            plaintext=plaintext,
            key=SAMPLE_KEY,
        )
        # dataset/schema version, record count, timestamp, checksum.
        assert manifest["dataset"] == "computer_features"
        assert manifest["schema_version"] == "v1"
        assert manifest["window_days"] == 7
        assert manifest["anchor_day"] == 6
        assert manifest["record_count"] == 42
        # checksum is over the *pre-encryption* plaintext Parquet bytes.
        assert manifest["checksum_sha256"] == bundle.sha256_hex(plaintext)
        assert manifest["encryption"] == {
            "scheme": bundle.ENCRYPTION_SCHEME,
            "key_id": bundle.key_id(SAMPLE_KEY),
        }
        assert manifest["encryption"]["scheme"] == "parquet-modular-aes-gcm-v1"
        assert manifest["data_object"] == bundle.data_key(6, 7)
        assert "created_at" in manifest
        # columns default to the binding schema.
        assert manifest["columns"] == bundle.DELIVERED_COLUMNS
        assert "volume_summary" not in manifest

    def test_volume_summary_is_embedded_when_provided(self):
        volume = bundle.build_volume_summary(
            anchor_day=2,
            window_days=3,
            checksum_rows=[
                ("landing", "auth", 0, None, 10),
                ("bronze", "auth", 0, None, 9),
                ("silver", "auth", 0, None, 8),
                ("gold", None, 2, 3, 5),
            ],
            delivered_record_count=5,
        )
        assert volume["event_days"] == [0, 1, 2]
        assert volume["landing"][0]["record_count"] == 10
        assert volume["gold"] == {"anchor_day": 2, "window_days": 3, "record_count": 5}
        assert volume["delivered"] == {"record_count": 5}

        manifest = bundle.build_manifest(
            schema_version="v1",
            window_days=3,
            anchor_day=2,
            record_count=5,
            plaintext=b"x",
            key=SAMPLE_KEY,
            volume_summary=volume,
        )
        assert manifest["volume_summary"] == volume

    def test_manifest_bytes_roundtrip(self):
        manifest = bundle.build_manifest(
            schema_version="v1",
            window_days=7,
            anchor_day=0,
            record_count=1,
            plaintext=b"x",
            key=SAMPLE_KEY,
        )
        assert json.loads(bundle.manifest_bytes(manifest)) == manifest


class TestManifestRecord:
    def test_maps_manifest_to_governance_record(self):
        manifest = bundle.build_manifest(
            schema_version="v1",
            window_days=7,
            anchor_day=6,
            record_count=42,
            plaintext=b"rows",
            key=SAMPLE_KEY,
        )
        record = bundle.manifest_record(manifest)
        assert record.dataset == "computer_features"
        assert record.dataset_version == manifest["dataset_version"]
        assert record.schema_version == "v1"
        assert record.window_days == 7
        assert record.anchor_day == 6
        assert record.record_count == 42
        assert record.checksum_sha256 == bundle.sha256_hex(b"rows")
        assert record.encryption_scheme == bundle.ENCRYPTION_SCHEME
        assert record.encryption_key_id == bundle.key_id(SAMPLE_KEY)
        assert record.data_object == bundle.data_key(6, 7)


def test_delivered_columns_match_producer_contract():
    """Delivery's schema must equal the producer's binding Gold schema."""
    features = pytest.importorskip("jobs.transforms.features")
    assert bundle.DELIVERED_COLUMNS == features.COMPUTER_FEATURE_COLUMNS

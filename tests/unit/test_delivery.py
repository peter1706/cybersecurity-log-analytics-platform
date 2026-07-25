"""Unit tests for the pure delivery helpers (no MinIO/Delta required)."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("cryptography")

import bundle  # noqa: E402
from cryptography.fernet import Fernet  # noqa: E402


class TestChecksum:
    def test_sha256_matches_hashlib(self):
        import hashlib

        data = b"some parquet bytes"
        assert bundle.sha256_hex(data) == hashlib.sha256(data).hexdigest()

    def test_key_id_is_stable_and_not_the_key(self):
        key = Fernet.generate_key()
        kid = bundle.key_id(key)
        assert kid == bundle.key_id(key)  # stable
        assert len(kid) == 12
        assert key.decode() not in kid  # never leaks the key material


class TestFernetRoundtrip:
    def test_encrypt_then_decrypt_returns_plaintext(self):
        key = Fernet.generate_key()
        plaintext = b"\x00\x01columnar-parquet\xff"
        token = bundle.encrypt(plaintext, key)
        assert token != plaintext
        assert bundle.decrypt(token, key) == plaintext

    def test_wrong_key_cannot_decrypt(self):
        from cryptography.fernet import InvalidToken

        token = bundle.encrypt(b"secret", Fernet.generate_key())
        with pytest.raises(InvalidToken):
            bundle.decrypt(token, Fernet.generate_key())


class TestObjectKeys:
    def test_keys_are_fixed_and_zero_padded(self):
        # Fixed keys per partition are what make re-delivery idempotent.
        prefix = "computer_features/window_days=7/anchor_day=06"
        assert bundle.data_key(6, 7) == f"{prefix}/features.parquet.enc"
        assert bundle.manifest_key(6, 7) == f"{prefix}/manifest.json"


class TestManifest:
    def test_contains_required_fields_and_checksum(self):
        key = Fernet.generate_key()
        plaintext = b"rows"
        manifest = bundle.build_manifest(
            schema_version="v1",
            window_days=7,
            anchor_day=6,
            record_count=42,
            plaintext=plaintext,
            key=key,
        )
        # dataset/schema version, record count, timestamp, checksum.
        assert manifest["dataset"] == "computer_features"
        assert manifest["schema_version"] == "v1"
        assert manifest["window_days"] == 7
        assert manifest["anchor_day"] == 6
        assert manifest["record_count"] == 42
        assert manifest["checksum_sha256"] == bundle.sha256_hex(plaintext)
        assert manifest["encryption"] == {"scheme": "fernet", "key_id": bundle.key_id(key)}
        assert manifest["data_object"] == bundle.data_key(6, 7)
        assert "created_at" in manifest
        # columns default to the binding schema.
        assert manifest["columns"] == bundle.DELIVERED_COLUMNS

    def test_manifest_bytes_roundtrip(self):
        manifest = bundle.build_manifest(
            schema_version="v1",
            window_days=7,
            anchor_day=0,
            record_count=1,
            plaintext=b"x",
            key=Fernet.generate_key(),
        )
        assert json.loads(bundle.manifest_bytes(manifest)) == manifest


def test_delivered_columns_match_producer_contract():
    """Delivery's schema must equal the producer's binding Gold schema."""
    features = pytest.importorskip("jobs.transforms.features")
    assert bundle.DELIVERED_COLUMNS == features.COMPUTER_FEATURE_COLUMNS

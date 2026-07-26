"""Unit tests for the shared Parquet Modular Encryption helpers.

Exercises the delivered-artifact encryption end to end without MinIO/Delta: a
write/read roundtrip, that a plaintext reader cannot read the encrypted file, and
that AES-GCM authentication rejects tampering and the wrong key (the consumer's
integrity guarantee).
"""

from __future__ import annotations

import pytest

pa = pytest.importorskip("pyarrow")
pytest.importorskip("pyarrow.parquet.encryption")
pytest.importorskip("cryptography")

import pyarrow.parquet as pq  # noqa: E402

from catalog.parquet_encryption import (  # noqa: E402
    read_encrypted_parquet,
    write_encrypted_parquet,
)

KEY = "unit-test-delivery-key=="


def _sample_table() -> pa.Table:
    return pa.table(
        {
            "computer_id": ["C1", "C2", "C3"],
            "anchor_day": [6, 6, 6],
            "window_days": [7, 7, 7],
            "auth_out_event_count": [10, 0, 5],
            "auth_out_failure_rate": [0.5, None, 0.0],
        }
    )


class TestRoundtrip:
    def test_write_then_read_returns_same_table(self):
        table = _sample_table()
        encrypted = write_encrypted_parquet(table, KEY)
        assert isinstance(encrypted, bytes)
        restored = read_encrypted_parquet(encrypted, KEY)
        assert restored.equals(table)

    def test_bytes_and_str_keys_are_interchangeable(self):
        table = _sample_table()
        encrypted = write_encrypted_parquet(table, KEY.encode("utf-8"))
        assert read_encrypted_parquet(encrypted, KEY).equals(table)


class TestConfidentiality:
    def test_plaintext_reader_cannot_read_encrypted_file(self):
        encrypted = write_encrypted_parquet(_sample_table(), KEY)
        with pytest.raises(Exception):  # noqa: B017 - pyarrow raises OSError here
            pq.read_table(pa.BufferReader(encrypted))


class TestIntegrity:
    def test_wrong_key_is_rejected(self):
        encrypted = write_encrypted_parquet(_sample_table(), KEY)
        with pytest.raises(Exception):  # noqa: B017
            read_encrypted_parquet(encrypted, "a-different-key")

    def test_tampered_ciphertext_is_rejected(self):
        encrypted = bytearray(write_encrypted_parquet(_sample_table(), KEY))
        encrypted[len(encrypted) // 2] ^= 0x01  # flip one byte
        with pytest.raises(Exception):  # noqa: B017
            read_encrypted_parquet(bytes(encrypted), KEY)

"""Parquet Modular Encryption for the delivered feature artifact.

Encryption at rest for the delivered ``computer_features`` Parquet uses Parquet
Modular Encryption (PME, AES-GCM): the footer and every column are encrypted
*inside* the Parquet file, so the artifact is a self-describing encrypted Parquet
rather than an opaque blob. AES-GCM is authenticated, so any tampering with the
delivered object is detected on read (decryption fails) -- that is the consumer's
runtime integrity check.

PyArrow exposes PME only through its KMS-backed high-level API
(:class:`~pyarrow.parquet.encryption.CryptoFactory` +
:class:`~pyarrow.parquet.encryption.KmsClient`). The local stack has no external
KMS, so :class:`LocalKmsClient` is a minimal in-process key-management service:
PyArrow generates a random per-file data encryption key (DEK), hands it to
:meth:`LocalKmsClient.wrap_key` to be envelope-encrypted under the master key, and
stores the wrapped DEK in the Parquet footer; the reader unwraps it symmetrically.
The master key is the ``delivery_encryption_key`` container secret (any opaque
string -- a 256-bit AES key is derived from it via HKDF-SHA256), shared by the
producer (``delivery``) and the consumer (``ml-mock``).

This module is shared (via the ``catalog`` package baked into both service images)
so the wrap/unwrap scheme cannot drift between the two sides of the interface. Its
runtime dependencies (``pyarrow``, ``cryptography``) are provided by the consuming
service images -- ``catalog``'s own requirements stay driver-only, and importing
``catalog`` never imports this module.
"""

from __future__ import annotations

import base64
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

# Internal master-key label used inside the Parquet footer key metadata. It maps a
# key to the master-key material in the KMS connection config; it is not a secret
# and never leaves the file. The user-facing key *identifier* recorded in the
# manifest is the truncated digest from ``bundle.key_id`` instead, and the
# manifest's scheme label is ``bundle.ENCRYPTION_SCHEME``.
MASTER_KEY_ID = "delivery_encryption_key"

_NONCE_BYTES = 12


def _as_str(key: bytes | str) -> str:
    """Normalize key material to ``str`` for the KMS connection config."""
    return key.decode("utf-8") if isinstance(key, bytes) else key


class LocalKmsClient(pe.KmsClient):
    """In-process KMS: envelope-encrypts per-file DEKs under a master key.

    ``config.custom_kms_conf`` maps :data:`MASTER_KEY_ID` to the opaque master-key
    string; a 256-bit AES key is derived from it with HKDF-SHA256 so any secret
    format (e.g. a Fernet-style key) works. DEK wrapping/unwrapping is AES-GCM, so a
    tampered wrapped key fails authentication on unwrap.
    """

    def __init__(self, config: pe.KmsConnectionConfig) -> None:
        pe.KmsClient.__init__(self)
        self._master_keys = config.custom_kms_conf

    def _master_key(self, master_key_identifier: str) -> bytes:
        secret = self._master_keys[master_key_identifier]
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b"catalog.parquet_encryption master key v1",
        )
        return hkdf.derive(secret.encode("utf-8"))

    def wrap_key(self, key_bytes: bytes, master_key_identifier: str) -> str:
        nonce = os.urandom(_NONCE_BYTES)
        ciphertext = AESGCM(self._master_key(master_key_identifier)).encrypt(
            nonce, key_bytes, None
        )
        return base64.b64encode(nonce + ciphertext).decode("ascii")

    def unwrap_key(self, wrapped_key: str, master_key_identifier: str) -> bytes:
        raw = base64.b64decode(wrapped_key)
        nonce, ciphertext = raw[:_NONCE_BYTES], raw[_NONCE_BYTES:]
        return AESGCM(self._master_key(master_key_identifier)).decrypt(
            nonce, ciphertext, None
        )


def _kms_factory(config: pe.KmsConnectionConfig) -> LocalKmsClient:
    return LocalKmsClient(config)


def _kms_connection(key: bytes | str) -> pe.KmsConnectionConfig:
    return pe.KmsConnectionConfig(custom_kms_conf={MASTER_KEY_ID: _as_str(key)})


def write_encrypted_parquet(table: pa.Table, key: bytes | str) -> bytes:
    """Write ``table`` to an in-memory Parquet file with PME; return its bytes.

    Every column and the footer are encrypted under the master key derived from
    ``key`` (all columns keyed to :data:`MASTER_KEY_ID`, i.e. uniform encryption).
    """
    crypto_factory = pe.CryptoFactory(_kms_factory)
    encryption_config = pe.EncryptionConfiguration(
        footer_key=MASTER_KEY_ID,
        column_keys={MASTER_KEY_ID: list(table.column_names)},
    )
    file_encryption_properties = crypto_factory.file_encryption_properties(
        _kms_connection(key), encryption_config
    )
    sink = pa.BufferOutputStream()
    with pq.ParquetWriter(
        sink, table.schema, encryption_properties=file_encryption_properties
    ) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def read_encrypted_parquet(data: bytes, key: bytes | str) -> pa.Table:
    """Decrypt and read a PME Parquet produced by :func:`write_encrypted_parquet`.

    Raises if ``data`` was tampered with or ``key`` is wrong: AES-GCM
    authentication fails, which is the consumer's integrity guarantee.
    """
    crypto_factory = pe.CryptoFactory(_kms_factory)
    file_decryption_properties = crypto_factory.file_decryption_properties(
        _kms_connection(key), pe.DecryptionConfiguration()
    )
    parquet_file = pq.ParquetFile(
        pa.BufferReader(data), decryption_properties=file_decryption_properties
    )
    return parquet_file.read()

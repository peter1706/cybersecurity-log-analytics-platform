"""Pure delivery bundle helpers: canonical schema, checksums, manifest.

Kept free of heavy dependencies (no ``deltalake``/``pyarrow``/``boto3``) so the
schema/checksum/manifest logic is fast to unit-test. The Delta read, the actual
Parquet Modular Encryption (``catalog.parquet_encryption``), and object I/O live
in ``deliver.py``.

Delivered-at-rest protection is Parquet Modular Encryption (AES-GCM): the footer
and every column of the delivered Parquet are encrypted in place. This module only
owns the manifest metadata that *describes* that scheme (:data:`ENCRYPTION_SCHEME`,
the key id); the encryption itself is done in :mod:`catalog.parquet_encryption`.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from catalog import DeliveryManifest

# Manifest label for the delivered-artifact encryption scheme. AES-GCM Parquet
# Modular Encryption; recorded in the manifest and the governance catalog so the
# consumer/catalog know which scheme protected the artifact.
ENCRYPTION_SCHEME = "parquet-modular-aes-gcm-v1"

# Delivered schema, which acts as a contract between the producer and consumer.
# This is the producer side of the data-science interface contract; the consumer
# (ml-mock) keeps its own copy and a unit test asserts both equal the
# spark-processor's ``COMPUTER_FEATURE_COLUMNS`` so the three never drift.
DELIVERED_COLUMNS = [
    # identification and window
    "computer_id",
    "anchor_day",
    "window_days",
    # auth
    "auth_out_event_count",
    "auth_in_event_count",
    "auth_out_distinct_targets",
    "auth_in_distinct_sources",
    "auth_out_distinct_users",
    "auth_in_distinct_users",
    "auth_out_distinct_human_users",
    "auth_in_distinct_human_users",
    "auth_out_failed_count",
    "auth_in_failed_count",
    "auth_out_failure_rate",
    "auth_in_failure_rate",
    # proc
    "proc_start_count",
    "proc_distinct_process_names",
    "proc_distinct_users",
    "proc_distinct_human_users",
    # flows
    "flows_out_count_distinct",
    "flows_in_count_distinct",
    "flows_out_distinct_targets",
    "flows_in_distinct_sources",
    "flows_out_bytes_sum_distinct",
    "flows_in_bytes_sum_distinct",
    "flows_out_packets_sum_distinct",
    "flows_in_packets_sum_distinct",
    "flows_out_anonymized_port_count_distinct",
    "flows_in_anonymized_port_count_distinct",
    # dns
    "dns_lookup_count",
    "dns_distinct_resolved_hosts",
    # quality/metadata
    "source_present_auth",
    "source_present_proc",
    "source_present_flows",
    "source_present_dns",
]

DATASET = "computer_features"


def sha256_hex(data: bytes) -> str:
    """Return the hex SHA-256 digest of ``data`` (the delivery checksum)."""
    return hashlib.sha256(data).hexdigest()


def key_id(key: bytes | str) -> str:
    """Return a short, non-secret identifier for an encryption key.

    A truncated digest of the key material, so a manifest can name *which* key
    was used without ever exposing the key.
    """
    raw = key.encode() if isinstance(key, str) else key
    return hashlib.sha256(raw).hexdigest()[:12]


def delivered_prefix(anchor_day: int, window_days: int) -> str:
    """Object-key prefix for one delivered partition (fixed -> idempotent)."""
    return f"{DATASET}/window_days={window_days}/anchor_day={anchor_day:02d}"


def data_key(anchor_day: int, window_days: int) -> str:
    """Object key of the encrypted Parquet file for a partition."""
    return f"{delivered_prefix(anchor_day, window_days)}/features.parquet.enc"


def manifest_key(anchor_day: int, window_days: int) -> str:
    """Object key of the manifest JSON for a partition."""
    return f"{delivered_prefix(anchor_day, window_days)}/manifest.json"


def build_manifest(
    *,
    schema_version: str,
    window_days: int,
    anchor_day: int,
    record_count: int,
    plaintext: bytes,
    key: bytes | str,
    columns: list[str] | None = None,
    created_at: str | None = None,
    volume_summary: dict | None = None,
) -> dict:
    """Build the delivery manifest.

    Contains at least dataset/schema version, record count, timestamp, and the
    SHA-256 checksum of the pre-encryption (plaintext) Parquet -- the Gold ->
    delivered link in the audit chain, recorded to the governance catalog -- plus
    the columns for schema conformance and the encryption scheme + key id. The
    consumer's runtime integrity guarantee is the authenticated (AES-GCM)
    decryption itself, so it does not re-derive this checksum from the ciphertext.

    When provided, ``volume_summary`` is a catalog-derived snapshot of layer
    record counts for the window (landing through delivered) so the consumer
    dashboard can show aggregation context without catalog access.
    """
    manifest = {
        "dataset": DATASET,
        "dataset_version": f"{DATASET}-w{window_days}-d{anchor_day:02d}",
        "schema_version": schema_version,
        "window_days": window_days,
        "anchor_day": anchor_day,
        "record_count": record_count,
        "columns": list(columns if columns is not None else DELIVERED_COLUMNS),
        "created_at": created_at or datetime.now(UTC).isoformat(),
        "checksum_sha256": sha256_hex(plaintext),
        "encryption": {"scheme": ENCRYPTION_SCHEME, "key_id": key_id(key)},
        "data_object": data_key(anchor_day, window_days),
    }
    if volume_summary is not None:
        manifest["volume_summary"] = volume_summary
    return manifest


def build_volume_summary(
    *,
    anchor_day: int,
    window_days: int,
    checksum_rows: list[tuple[str, str | None, int, int | None, int | None]],
    delivered_record_count: int,
) -> dict:
    """Build the per-partition aggregation-volume snapshot for a delivery manifest.

    Args:
        anchor_day: Gold/delivered as-of day.
        window_days: Rolling window length used for this partition.
        checksum_rows: Catalog checksum rows as
            ``(layer, source, day, window_days, record_count)``.
        delivered_record_count: Feature-row count written to the delivered store.

    Returns:
        A JSON-serializable volume summary keyed by layer.
    """
    event_days = list(range(max(0, anchor_day - window_days + 1), anchor_day + 1))
    by_layer: dict[str, list[dict]] = {"landing": [], "bronze": [], "silver": []}
    gold_count: int | None = None

    for layer, source, day, row_window, record_count in checksum_rows:
        count = 0 if record_count is None else int(record_count)
        if layer in by_layer:
            entry: dict = {"source": source, "day": day, "record_count": count}
            by_layer[layer].append(entry)
        elif layer == "gold" and day == anchor_day and row_window == window_days:
            gold_count = count

    return {
        "event_days": event_days,
        "landing": by_layer["landing"],
        "bronze": by_layer["bronze"],
        "silver": by_layer["silver"],
        "gold": {
            "anchor_day": anchor_day,
            "window_days": window_days,
            "record_count": gold_count if gold_count is not None else delivered_record_count,
        },
        "delivered": {"record_count": delivered_record_count},
    }


def manifest_bytes(manifest: dict) -> bytes:
    """Serialize a manifest to stable, pretty JSON bytes."""
    return json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")


def manifest_record(manifest: dict) -> DeliveryManifest:
    """Map a delivery manifest dict to its governance-catalog record (pure)."""
    encryption = manifest.get("encryption", {})
    return DeliveryManifest(
        dataset=manifest["dataset"],
        dataset_version=manifest["dataset_version"],
        schema_version=manifest["schema_version"],
        window_days=manifest["window_days"],
        anchor_day=manifest["anchor_day"],
        record_count=manifest["record_count"],
        checksum_sha256=manifest["checksum_sha256"],
        encryption_scheme=encryption.get("scheme"),
        encryption_key_id=encryption.get("key_id"),
        data_object=manifest.get("data_object"),
    )

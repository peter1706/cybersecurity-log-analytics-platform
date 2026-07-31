"""Governance-catalog write/read client.

Two layers, kept separate so the SQL is unit-testable without a live Postgres:

* Pure ``build_*`` functions turn a record into an ``(sql, params)`` pair. They
  have no side effects and are covered by fast unit tests.
* :class:`CatalogClient` opens a psycopg2 connection (imported lazily, so the
  builders and config import cleanly without the driver installed) and executes
  those statements, committing per write.

Writes are idempotent: re-running an anchor/event day upserts the same logical
row (checksums keyed by ``(layer, source, day, window_days)``, manifests by
``(window_days, anchor_day)``) so backfills never duplicate governance records.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .config import CatalogConfig

# --- Record types -----------------------------------------------------------


@dataclass(frozen=True)
class JobRun:
    dag_id: str
    task_id: str
    run_id: str
    job: str
    status: str
    source: str | None = None
    day: int | None = None
    try_number: int = 1
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    duration_ms: int | None = None


@dataclass(frozen=True)
class Lineage:
    day: int
    to_layer: str
    record_count: int
    schema_version: str
    source: str | None = None
    from_layer: str | None = None
    window_days: int | None = None


@dataclass(frozen=True)
class Checksum:
    layer: str
    day: int
    checksum: str
    source: str | None = None
    window_days: int | None = None
    algorithm: str = "sha256"
    record_count: int | None = None


@dataclass(frozen=True)
class SchemaRegistration:
    source: str
    layer: str
    schema_version: str
    columns: list[str] | Mapping[str, Any]


@dataclass(frozen=True)
class DeliveryManifest:
    dataset: str
    dataset_version: str
    schema_version: str
    window_days: int
    anchor_day: int
    record_count: int
    checksum_sha256: str
    encryption_scheme: str | None = None
    encryption_key_id: str | None = None
    data_object: str | None = None


# --- Pure SQL builders ------------------------------------------------------

Statement = tuple[str, tuple[Any, ...]]


def build_insert_job_run(record: JobRun) -> Statement:
    """Build the upsert statement for one ``job_runs`` telemetry row."""
    sql = (
        "INSERT INTO job_runs "
        "(dag_id, task_id, run_id, job, source, day, status, try_number, "
        "error, started_at, finished_at, duration_ms) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (dag_id, task_id, run_id, try_number) DO UPDATE SET "
        "status = EXCLUDED.status, error = EXCLUDED.error, "
        "finished_at = EXCLUDED.finished_at, duration_ms = EXCLUDED.duration_ms"
    )
    params = (
        record.dag_id,
        record.task_id,
        record.run_id,
        record.job,
        record.source,
        record.day,
        record.status,
        record.try_number,
        record.error,
        record.started_at,
        record.finished_at,
        record.duration_ms,
    )
    return sql, params


def build_insert_lineage(record: Lineage) -> Statement:
    """Build the insert statement for one data-lineage record."""
    sql = (
        "INSERT INTO lineage "
        "(source, day, from_layer, to_layer, record_count, schema_version, window_days) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )
    params = (
        record.source,
        record.day,
        record.from_layer,
        record.to_layer,
        record.record_count,
        record.schema_version,
        record.window_days,
    )
    return sql, params


def build_upsert_checksum(record: Checksum) -> Statement:
    """Build the upsert statement for one layer/source/day checksum record."""
    sql = (
        "INSERT INTO checksums "
        "(layer, source, day, window_days, algorithm, checksum, record_count) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (layer, COALESCE(source, ''), day, COALESCE(window_days, -1)) "
        "DO UPDATE SET checksum = EXCLUDED.checksum, algorithm = EXCLUDED.algorithm, "
        "record_count = EXCLUDED.record_count, created_at = now()"
    )
    params = (
        record.layer,
        record.source,
        record.day,
        record.window_days,
        record.algorithm,
        record.checksum,
        record.record_count,
    )
    return sql, params


def build_select_checksum(
    layer: str,
    day: int,
    source: str | None = None,
    window_days: int | None = None,
) -> Statement:
    """Build the lookup statement for a previously recorded checksum.

    Args:
        layer: Medallion layer the checksum was recorded against.
        day: Day index the checksum covers.
        source: Event source, or ``None`` for a layer-wide checksum.
        window_days: Rolling-window length, or ``None`` for a non-windowed checksum.

    Returns:
        The ``(sql, params)`` statement selecting ``checksum`` and ``record_count``.
    """
    sql = (
        "SELECT checksum, record_count FROM checksums "
        "WHERE layer = %s AND COALESCE(source, '') = COALESCE(%s, '') "
        "AND day = %s AND COALESCE(window_days, -1) = COALESCE(%s, -1)"
    )
    return sql, (layer, source, day, window_days)


def build_upsert_schema(record: SchemaRegistration) -> Statement:
    """Build the upsert statement for one source/layer schema registration."""
    sql = (
        "INSERT INTO schema_registry (source, layer, schema_version, columns) "
        "VALUES (%s, %s, %s, %s::jsonb) "
        "ON CONFLICT (source, layer, schema_version) "
        "DO UPDATE SET columns = EXCLUDED.columns, created_at = now()"
    )
    params = (
        record.source,
        record.layer,
        record.schema_version,
        json.dumps(record.columns),
    )
    return sql, params


def build_upsert_delivery_manifest(record: DeliveryManifest) -> Statement:
    """Build the upsert statement for one delivered-partition manifest record."""
    sql = (
        "INSERT INTO delivery_manifests "
        "(dataset, dataset_version, schema_version, window_days, anchor_day, "
        "record_count, checksum_sha256, encryption_scheme, encryption_key_id, data_object) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (window_days, anchor_day) DO UPDATE SET "
        "dataset_version = EXCLUDED.dataset_version, "
        "schema_version = EXCLUDED.schema_version, "
        "record_count = EXCLUDED.record_count, "
        "checksum_sha256 = EXCLUDED.checksum_sha256, "
        "encryption_scheme = EXCLUDED.encryption_scheme, "
        "encryption_key_id = EXCLUDED.encryption_key_id, "
        "data_object = EXCLUDED.data_object, created_at = now()"
    )
    params = (
        record.dataset,
        record.dataset_version,
        record.schema_version,
        record.window_days,
        record.anchor_day,
        record.record_count,
        record.checksum_sha256,
        record.encryption_scheme,
        record.encryption_key_id,
        record.data_object,
    )
    return sql, params


# --- Executor ---------------------------------------------------------------


class CatalogClient:
    """Executes governance writes against the catalog, committing per statement.

    Usable as a context manager so the connection is always closed::

        with CatalogClient.connect(CatalogConfig.from_env()) as catalog:
            catalog.record_lineage(Lineage(...))
    """

    def __init__(self, connection: Any):
        self._conn = connection

    @classmethod
    def connect(cls, config: CatalogConfig | None = None) -> CatalogClient:
        """Open a connection using ``config`` (or one resolved from the env)."""
        import psycopg2  # lazy: keep builders/config importable without the driver

        config = config or CatalogConfig.from_env()
        return cls(psycopg2.connect(config.dsn()))

    def __enter__(self) -> CatalogClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    def _execute(self, statement: Statement) -> None:
        sql, params = statement
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
        self._conn.commit()

    def record_job_run(self, record: JobRun) -> None:
        self._execute(build_insert_job_run(record))

    def record_lineage(self, record: Lineage) -> None:
        self._execute(build_insert_lineage(record))

    def record_checksum(self, record: Checksum) -> None:
        self._execute(build_upsert_checksum(record))

    def register_schema(self, record: SchemaRegistration) -> None:
        self._execute(build_upsert_schema(record))

    def record_delivery_manifest(self, record: DeliveryManifest) -> None:
        self._execute(build_upsert_delivery_manifest(record))

    def get_checksum(
        self,
        layer: str,
        day: int,
        source: str | None = None,
        window_days: int | None = None,
    ) -> str | None:
        """Return the stored checksum for a partition, or ``None`` if absent."""
        sql, params = build_select_checksum(layer, day, source, window_days)
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        return None if row is None else row[0]

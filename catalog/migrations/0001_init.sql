-- Governance catalog schema.
--
-- Plain-PostgreSQL governance store: data lineage, a versioned schema registry,
-- job-run logs, the SHA-256 checksum audit chain (each layer validates the
-- upstream checksum before reading and records its own), and delivery manifests.
--
-- Every statement is idempotent (CREATE ... IF NOT EXISTS) so the migration can
-- be re-applied on every `docker compose up` without failing or duplicating.

-- --------------------------------------------------------------------------
-- job_runs: one row per orchestrated task run (writer: airflow).
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_runs (
    id           BIGSERIAL PRIMARY KEY,
    dag_id       TEXT        NOT NULL,
    task_id      TEXT        NOT NULL,
    run_id       TEXT        NOT NULL,
    job          TEXT        NOT NULL,   -- logical job, e.g. land_to_bronze / deliver
    source       TEXT,                   -- NULL for cross-source jobs (gold/deliver)
    day          INTEGER,                -- anchor/event day the run targets
    status       TEXT        NOT NULL,   -- running / success / failed / up_for_retry
    try_number   INTEGER     NOT NULL DEFAULT 1,
    error        TEXT,                   -- failure message (NULL on success)
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    duration_ms  BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dag_id, task_id, run_id, try_number)
);

CREATE INDEX IF NOT EXISTS job_runs_run_id_idx ON job_runs (run_id);

-- --------------------------------------------------------------------------
-- lineage: one row per layer transition (writers: lanl-simulator, spark-processor,
-- delivery). from_layer is NULL for the raw landing (there is no upstream layer).
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lineage (
    id             BIGSERIAL PRIMARY KEY,
    source         TEXT,                 -- NULL for cross-source gold/delivered
    day            INTEGER     NOT NULL,
    from_layer     TEXT,                 -- NULL for raw landed
    to_layer       TEXT        NOT NULL, -- landing / bronze / silver / gold / delivered
    record_count   BIGINT      NOT NULL,
    schema_version TEXT        NOT NULL,
    window_days    INTEGER,              -- gold / delivered only (rolling window)
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS lineage_day_layer_idx ON lineage (to_layer, day);

-- --------------------------------------------------------------------------
-- checksums: the SHA-256 audit chain. One row per write; a re-run of the same
-- (layer, source, day, window) overwrites its row so backfills stay idempotent.
-- Downstream stages look up the upstream row and fail if it is missing/mismatched.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS checksums (
    id           BIGSERIAL PRIMARY KEY,
    layer        TEXT        NOT NULL,   -- landing / bronze / silver / gold / delivered
    source       TEXT,                   -- NULL for cross-source gold/delivered
    day          INTEGER     NOT NULL,
    window_days  INTEGER,                -- gold / delivered only
    algorithm    TEXT        NOT NULL DEFAULT 'sha256',
    checksum     TEXT        NOT NULL,
    record_count BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency key: COALESCE the nullable dimensions so a re-run upserts the same
-- logical partition rather than appending a duplicate checksum row.
CREATE UNIQUE INDEX IF NOT EXISTS checksums_partition_uidx
    ON checksums (layer, COALESCE(source, ''), day, COALESCE(window_days, -1));

-- --------------------------------------------------------------------------
-- schema_registry: the versioned schema of each source per layer (writer:
-- spark-processor). columns holds the ordered column list / types as JSON.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_registry (
    id             BIGSERIAL PRIMARY KEY,
    source         TEXT        NOT NULL,
    layer          TEXT        NOT NULL,
    schema_version TEXT        NOT NULL,
    columns        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, layer, schema_version)
);

-- --------------------------------------------------------------------------
-- delivery_manifests: governance copy of each delivered manifest (writer:
-- delivery). Mirrors the manifest.json shipped in the delivered bucket; the fixed
-- (window_days, anchor_day) key makes re-delivery overwrite rather than duplicate.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS delivery_manifests (
    id                BIGSERIAL PRIMARY KEY,
    dataset           TEXT        NOT NULL,
    dataset_version   TEXT        NOT NULL,
    schema_version    TEXT        NOT NULL,
    window_days       INTEGER     NOT NULL,
    anchor_day        INTEGER     NOT NULL,
    record_count      BIGINT      NOT NULL,
    checksum_sha256   TEXT        NOT NULL,
    encryption_scheme TEXT,
    encryption_key_id TEXT,
    data_object       TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (window_days, anchor_day)
);

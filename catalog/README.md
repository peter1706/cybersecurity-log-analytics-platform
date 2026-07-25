# Governance catalog

Plain-PostgreSQL governance store for the platform's **metadata plane** —
data lineage, a versioned schema registry, job-run logs, the SHA-256 checksum
audit chain, and delivery manifests. This README is the operational reference;
the per-table writer/event mapping is summarized below.

## Layout

| Path | Purpose |
|---|---|
| `migrations/*.sql` | Idempotent schema DDL, applied in filename order |
| `config.py` | `CatalogConfig.from_env()` — connection settings from `CATALOG_DB_*` |
| `client.py` | `CatalogClient` + record types + pure `build_*` SQL builders |

The repo root is on the pytest `pythonpath`, so unit tests import the package
directly (`from catalog import CatalogClient, Lineage`). Services that record
governance metadata layer this package (and `requirements.txt`) into their image;
that wiring plus the per-task write calls are added alongside the write paths.

## Schema (`migrations/0001_init.sql`)

| Table | Writer | Records |
|---|---|---|
| `job_runs` | airflow | dag/task/run ids, job, status, retries, error, duration |
| `lineage` | simulator, spark-processor, delivery | one row per layer transition (counts, schema version) |
| `checksums` | simulator, spark-processor, delivery | SHA-256 per write; upstream lookup for the audit chain |
| `schema_registry` | spark-processor | versioned per-source column schema |
| `delivery_manifests` | delivery | governance copy of each delivered manifest |

All migrations use `CREATE ... IF NOT EXISTS`, so the `catalog-migrate` one-shot
in `docker-compose.yml` can re-apply them on every `docker compose up` without
error. Add new schema as a new numbered file (`0002_*.sql`, …) rather than
editing an applied migration.

## Connection config

Non-sensitive settings are env vars (see [`.env.example`](../.env.example)):
`CATALOG_DB_HOST` (default `postgres-catalog`), `CATALOG_DB_PORT` (default
`5432`), `CATALOG_DB_NAME`, `CATALOG_DB_USER`. The password is a container secret
read from `/run/secrets/postgres_catalog_password` (via `read_secret`), with a
`CATALOG_DB_PASSWORD` env-var fallback for local tooling and tests.

`read_secret(name, *, env=..., default=..., env_mapping=...)` (also in this
package) is the shared credential loader every service uses: it reads
`/run/secrets/<name>` and falls back to an env var when no secret file is
mounted.

## Idempotency

`checksums` are keyed by `(layer, source, day, window_days)` and
`delivery_manifests` by `(window_days, anchor_day)`, so re-running a day upserts
the same logical row instead of duplicating governance records.

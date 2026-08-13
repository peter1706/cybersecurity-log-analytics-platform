# spark-processor

One Spark service running three Airflow-triggered jobs over Delta Lake on
`s3a://`:

- **(a) landing → Bronze**: parse/validate landed objects, write partitioned Bronze.
- **(b) Bronze → Silver**: clean, type, conform single events.
- **(c) Silver → Gold**: per-computer features over the rolling window — one row
  per computer per **anchor day**. Each run aggregates the anchor day (`--day`)
  plus the preceding `ROLLING_WINDOW_DAYS - 1` days (default 7) and writes them to
  the anchor day's Gold partition. Re-running an anchor day overwrites just that
  partition (dynamic partition overwrite), so it is idempotent — no double counting.

Job (c) is a single cross-source job:

- `silver_to_gold` (no `--source`) → the unified **`computer_features`** table, one
  row per `(computer_id, anchor_day, window_days)` joining all four sources per the
  data-science contract (`docs/requirements/REQUIREMENTS_DATA_SCIENCE_TEAM.md`).
  Per-source features are pure builders (`*_computer_features`) stitched together in
  `transforms/features.py`; counters/sums default to `0`, undefined ratios stay
  `NULL`, and `source_present_*` flags record per-source window availability
  (approximated from partition existence). Partitioned by
  `(window_days, anchor_day)`.

## Notes

- Every task (a)/(b)/(c) re-hashes its upstream input and compares it against the
  checksum recorded at write time (`validate_upstream`), and records its own
  SHA-256 content checksum, lineage row, and (Bronze/Silver) schema-registry row
  to `postgres-catalog` before returning — the checksum chain the governance
  catalog is built on.
- Each stage's transform output is cached (it is read up to three times: the
  row-count guard, the write, and the content checksum) and unpersisted in a
  `finally` block; `land_to_bronze` skips input caching since its checksum is
  over the raw bytes, read once.
- Runs on the data-plane network (`pipeline-net`) using the root MinIO credential
  and the governance catalog, both read from mounted secrets.

## Usage

```bash
# single job (one source, one day) -- what the Airflow DAG launches
python run_job.py land_to_bronze   --source auth --day 0
python run_job.py bronze_to_silver --source auth --day 0
python run_job.py silver_to_gold   --day 0   # cross-source Gold (no --source)

# batched: process a day range and/or all sources in ONE SparkSession, so the
# JVM/Ivy cold start is paid once instead of per (source, day) -- what
# scripts/backfill.sh uses for a multi-day backfill
python run_job.py land_to_bronze   --all-sources --day 0 --day-end 6
python run_job.py bronze_to_silver --all-sources --day 0 --day-end 6
python run_job.py silver_to_gold                 --day 0 --day-end 6
```

Runs in local Spark mode by default (`SPARK_MASTER=local[*]`); the first run
downloads the Delta and `hadoop-aws` jars via Ivy (needs network access) unless
already warmed by the image build (see `Dockerfile`).

## Configuration

Non-sensitive config is env vars; credentials are container secrets mounted at
`/run/secrets/<name>` (loaded via `read_secret`, with an env-var fallback for
local runs and tests).

| Var / secret | Kind | Purpose |
|-----|-----|---------|
| `MINIO_ENDPOINT` | env | MinIO endpoint URL |
| `minio_root_user`, `minio_root_password` | secret | MinIO access keys |
| `LANDING_BUCKET`, `BRONZE_BUCKET`, `SILVER_BUCKET`, `GOLD_BUCKET` | env | per-layer bucket names (default to the layer name) |
| `SCHEMA_VERSION` | env | schema version stamped into lineage/schema-registry rows |
| `ROLLING_WINDOW_DAYS` | env | default rolling-window length for `silver_to_gold` (default `7`) |
| `SPARK_MASTER` | env | Spark master URL (default `local[*]`) |
| `SPARK_DRIVER_MEMORY` | env | driver JVM heap, injected via `PYSPARK_SUBMIT_ARGS` since local mode fixes it at launch |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | env | shuffle partition count (default 200 is wasteful for this single-node bounded dataset) |
| `SPARK_EXECUTOR_MEMORY`, `SPARK_EXECUTOR_CORES`, `SPARK_EXECUTOR_INSTANCES` | env | executor sizing; inert under `local[*]` (driver == executor) but let the same image scale out on a real cluster via env only |
| `SPARK_LOG_LEVEL` | env | Spark log level (default `WARN`) |
| `CATALOG_DB_HOST`, `CATALOG_DB_PORT`, `CATALOG_DB_NAME`, `CATALOG_DB_USER` | env + secret | governance catalog connection (password is the `postgres_catalog_password` secret) |

## Testing

Pure transforms, feature builders, and the assembler are unit-tested against
tiny in-memory Spark DataFrames (no Docker) — see
`tests/unit/test_source_transforms.py`, `tests/unit/test_feature_transforms.py`,
`tests/unit/test_checksums.py`, and `tests/unit/test_spark_governance.py`
(lineage/schema-registry/checksum record builders). End-to-end behaviour
(idempotent re-runs, window correctness, checksum-row persistence) is covered by
`tests/e2e/test_pipeline_smoke.py` and `tests/e2e/test_full_window.py` (`make e2e`
/ `make e2e-full`).

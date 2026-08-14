# lanl-simulator

External producer. Replays the LANL source subset (`data/subset/*.txt.gz`) as
**daily raw batches**, writing one object per source per day to the MinIO
`landing` bucket, and records the landing checksum.

Replays a single day (`--day N`) or, with `--day-end M`, an inclusive range
`[N, M]` in one pass — one landing object per day — to seed the history a
Silver → Gold rolling-window backfill needs.

Object key: `landing/<source>/day=<DD>/<source>-<DDD>.csv.gz`. No typing or
schema is applied — that happens at Bronze; the raw bytes are uploaded verbatim
(still gzip-compressed, source-native format) so the landing checkpoint stays
faithful to what a real producer would drop.

## Notes

- The upload is deterministic and idempotent: fixed object key, fixed gzip
  mtime, so re-running a day overwrites the same object rather than creating a
  duplicate.
- Records a landing lineage row and a SHA-256 checksum (over the exact uploaded
  bytes) to `postgres-catalog` for every `(source, day)` — the first link in the
  checksum chain that `land_to_bronze` verifies before parsing.
- Runs on the data-plane network (`pipeline-net`) using the root MinIO
  credential and the governance catalog, both read from mounted secrets.

## Usage

```bash
# single day, single source -- what the Airflow DAG launches
python simulate.py --source auth --day 0

# multi-day range, single source -- seeds a rolling-window backfill in one pass
python simulate.py --source auth --day 0 --day-end 6
```

`scripts/backfill.sh` runs this once per source over a day range (one landing
object per day, per source).

## Configuration

Non-sensitive config is env vars; credentials are container secrets mounted at
`/run/secrets/<name>` (loaded via `read_secret`, with an env-var fallback for
local runs and tests).

| Var / secret | Kind | Purpose |
|-----|-----|---------|
| `MINIO_ENDPOINT` | env | MinIO endpoint URL |
| `minio_root_user`, `minio_root_password` | secret | MinIO access keys |
| `LANDING_BUCKET` | env | target landing bucket (default `landing`) |
| `SCHEMA_VERSION` | env | schema version stamped into the landing lineage row |
| `SUBSET_DIR` | env | directory holding `<source>.txt.gz` subset files (default `/data/subset`, bind-mounted from the host's `data/subset/`) |
| `CATALOG_DB_HOST`, `CATALOG_DB_PORT`, `CATALOG_DB_NAME`, `CATALOG_DB_USER` | env + secret | governance catalog connection (password is the `postgres_catalog_password` secret) |

## Testing

The day/day-range row-selection logic and the landing lineage/checksum record
builders are unit-tested against the checked-in subset fixtures (no Docker) —
see `tests/unit/test_simulator.py`. End-to-end landing → Bronze behaviour is
covered by `tests/e2e/test_pipeline_smoke.py` (`make e2e`).

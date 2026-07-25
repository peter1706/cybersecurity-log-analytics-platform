# delivery

Hands one Gold `computer_features` partition to the ML consumer. Launched once per
anchor day by the `daily_pipeline` DAG (`deliver` task), after `silver_to_gold`.

For anchor day `--day` and window `--window-days` (default `ROLLING_WINDOW_DAYS`) it:

1. reads the Gold Delta partition `(window_days, anchor_day)` with the `deltalake`
   (delta-rs) reader — partition-correct via `_delta_log`, no double counting;
2. selects the binding contract columns (`bundle.DELIVERED_COLUMNS`) and writes
   them as columnar Parquet;
3. Fernet-encrypts the Parquet at rest and uploads it plus a delivery manifest to
   the `delivered` bucket under
   `computer_features/window_days=<w>/anchor_day=<dd>/`.

Fixed object keys make re-delivery idempotent.

## Notes

- Encryption is a Fernet whole-file wrapper over the columnar Parquet.
- The manifest is written into the `delivered` bucket alongside the data.
- Runs on the data-plane network using the root MinIO credential (read from a
  mounted secret).

## Configuration

Non-sensitive config is env vars; credentials are container secrets mounted at
`/run/secrets/<name>` (loaded via `read_secret`, with an env-var fallback for
local runs).

| Var / secret | Kind | Purpose |
|-----|-----|---------|
| `MINIO_ENDPOINT` | env | MinIO endpoint URL |
| `minio_root_user`, `minio_root_password` | secret | MinIO access keys |
| `delivery_encryption_key` | secret | Fernet key for encrypting delivered Parquet |
| `GOLD_BUCKET` | env | source Gold bucket (default `gold`) |
| `DELIVERED_BUCKET` | env | target delivered bucket (default `delivered`) |
| `SCHEMA_VERSION` | env | schema version stamped into the manifest |
| `ROLLING_WINDOW_DAYS` | env | default `--window-days` |
| `CATALOG_DB_*` | env + secret | governance catalog (password is `postgres_catalog_password`) |

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
- Runs on `platform-net` using the root MinIO credential.

## Configuration (env)

| Var | Purpose |
|-----|---------|
| `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | MinIO access |
| `GOLD_BUCKET` | source Gold bucket (default `gold`) |
| `DELIVERED_BUCKET` | target delivered bucket (default `delivered`) |
| `DELIVERY_ENCRYPTION_KEY` | Fernet key |
| `SCHEMA_VERSION` | schema version stamped into the manifest |
| `ROLLING_WINDOW_DAYS` | default `--window-days` |

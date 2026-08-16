# delivery service

Hands one Gold `computer_features` partition to the ML-mock consumer of the platform. This is launched once per anchor day by the `daily_pipeline` DAG (`deliver` task), after the `silver_to_gold` task.

For anchor day `--day` and window `--window-days` (default `ROLLING_WINDOW_DAYS`) it:

1. Reads the Gold Delta partition `(window_days, anchor_day)` with the `deltalake` (delta-rs) reader — partition-correct via `_delta_log`;
2. Selects the binding contract columns (`bundle.DELIVERED_COLUMNS`) and writes only them as columnar Parquet with Parquet Modular Encryption using Advanced Encription Standard in Galois/Counter Mode (AES-GCM);
3. Uploads the encrypted Parquet plus a delivery manifest to the `delivered` bucket under `computer_features/window_days=<w>/anchor_day=<dd>/`.

## Notes

- Encryption at rest is Parquet Modular Encryption (AES-GCM, footer + all columns) via `catalog.parquet_encryption`; the master key is the `delivery_encryption_key` secret. AES-GCM is authenticated, so any manipulation is detected on the consumer's read.
- The manifest records the scheme + key id and the SHA-256 of the *pre-encryption* Parquet as a Gold to delivered audit-chain checksum. This is written into the `delivered` bucket alongside the data and to the governance catalog.

## Configuration

Non-sensitive config is within `.env` variables; credentials are container secrets mounted at `/run/secrets/<name>` (loaded via `read_secret`, with an env-var fallback for local runs).

| Var / secret | Kind | Purpose |
|-----|-----|---------|
| `MINIO_ENDPOINT` | env | MinIO endpoint URL |
| `minio_root_user`, `minio_root_password` | secret | MinIO access keys |
| `delivery_encryption_key` | secret | master key for Parquet Modular Encryption of delivered Parquet |
| `GOLD_BUCKET` | env | source Gold bucket (default `gold`) |
| `DELIVERED_BUCKET` | env | target delivered bucket (default `delivered`) |
| `SCHEMA_VERSION` | env | schema version stamped into the manifest |
| `ROLLING_WINDOW_DAYS` | env | default `--window-days` |
| `CATALOG_DB_*` | env + secret | governance catalog (password is `postgres_catalog_password`) |

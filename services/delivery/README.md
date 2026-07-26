# delivery

Hands one Gold `computer_features` partition to the ML consumer. Launched once per
anchor day by the `daily_pipeline` DAG (`deliver` task), after `silver_to_gold`.

For anchor day `--day` and window `--window-days` (default `ROLLING_WINDOW_DAYS`) it:

1. reads the Gold Delta partition `(window_days, anchor_day)` with the `deltalake`
   (delta-rs) reader — partition-correct via `_delta_log`, no double counting;
2. selects the binding contract columns (`bundle.DELIVERED_COLUMNS`) and writes
   them as columnar Parquet with Parquet Modular Encryption (AES-GCM);
3. uploads the encrypted Parquet plus a delivery manifest to the `delivered`
   bucket under `computer_features/window_days=<w>/anchor_day=<dd>/`.

Fixed object keys make re-delivery idempotent.

## Notes

- Encryption at rest is Parquet Modular Encryption (AES-GCM, footer + all
  columns) via `catalog.parquet_encryption`; the master key is the
  `delivery_encryption_key` secret. AES-GCM is authenticated, so tampering is
  detected on the consumer's read.
- The manifest records the scheme + key id and the SHA-256 of the *pre-encryption*
  Parquet as the Gold → delivered audit-chain checksum; it is written into the
  `delivered` bucket alongside the data and to the governance catalog.
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
| `delivery_encryption_key` | secret | master key for Parquet Modular Encryption of delivered Parquet |
| `GOLD_BUCKET` | env | source Gold bucket (default `gold`) |
| `DELIVERED_BUCKET` | env | target delivered bucket (default `delivered`) |
| `SCHEMA_VERSION` | env | schema version stamped into the manifest |
| `ROLLING_WINDOW_DAYS` | env | default `--window-days` |
| `CATALOG_DB_*` | env + secret | governance catalog (password is `postgres_catalog_password`) |

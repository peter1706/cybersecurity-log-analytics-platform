# ml-mock

Mock consumer of the `delivered` store, with two run modes from one image:

- **`ml_consume` DAG task** (default entrypoint, `consume.py`): after `deliver`,
  fetches the manifest + encrypted Parquet for an anchor day and decrypts the
  Parquet Modular Encryption (AES-GCM) — the authenticated decryption is the
  integrity check, so a tampered/corrupted artifact or wrong key fails the task —
  then validates the schema exactly against the consumer's feature contract and
  **rejects** any deviation, checks the record count, then logs a simulated
  retrain.
- **Dashboard** (`dashboard.py`, always-on Streamlit on host port 8501, configurable
  via `ML_DASHBOARD_PORT`): lists deliveries, shows manifest metadata, and decrypts
  a selected partition for preview. Reads **only** the `delivered` bucket.

`feature_contract.py` holds the consumer's own copy of the binding schema
(`EXPECTED_COLUMNS`); a unit test asserts it matches the producer's
`COMPUTER_FEATURE_COLUMNS` and delivery's `DELIVERED_COLUMNS`.

## Notes

- Least privilege: runs on the isolated consumer network with a MinIO service
  account scoped to the `delivered` bucket only (never the layer buckets). Its
  credentials come from mounted secrets, and it never talks to the governance
  catalog DB.
- Reads the manifest from the `delivered` bucket alongside the encrypted data.

## Configuration

Non-sensitive config is env vars; credentials are container secrets mounted at
`/run/secrets/<name>` (loaded via `read_secret`, with an env-var fallback for
local runs).

| Var / secret | Kind | Purpose |
|-----|-----|---------|
| `MINIO_ENDPOINT` | env | MinIO endpoint URL |
| `minio_ml_consumer_key`, `minio_ml_consumer_secret` | secret | `delivered`-scoped MinIO service account |
| `delivery_encryption_key` | secret | master key for decrypting Parquet Modular Encryption |
| `DELIVERED_BUCKET` | env | delivered bucket (default `delivered`) |
| `ROLLING_WINDOW_DAYS` | env | default `--window-days` |
| `ML_DASHBOARD_PORT` | env | host port for the dashboard (default `8501`) |

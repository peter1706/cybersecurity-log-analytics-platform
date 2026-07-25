# ml-mock

Mock consumer of the `delivered` store, with two run modes from one image:

- **`ml_consume` DAG task** (default entrypoint, `consume.py`): after `deliver`,
  fetches the manifest + encrypted Parquet for an anchor day, decrypts it, verifies
  the SHA-256 against the manifest, validates the schema exactly against the
  consumer's feature contract and **rejects** any deviation, checks the record
  count, then logs a simulated retrain.
- **Dashboard** (`dashboard.py`, always-on Streamlit on host port 8501, configurable
  via `ML_DASHBOARD_PORT`): lists deliveries, shows manifest metadata, and decrypts
  a selected partition for preview. Reads **only** the `delivered` bucket.

`feature_contract.py` holds the consumer's own copy of the binding schema
(`EXPECTED_COLUMNS`); a unit test asserts it matches the producer's
`COMPUTER_FEATURE_COLUMNS` and delivery's `DELIVERED_COLUMNS`.

## Notes

- Uses the shared root MinIO credential and reads only the `delivered` bucket.
- Reads the manifest from the `delivered` bucket alongside the encrypted data.

## Configuration (env)

| Var | Purpose |
|-----|---------|
| `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | MinIO access |
| `DELIVERED_BUCKET` | delivered bucket (default `delivered`) |
| `DELIVERY_ENCRYPTION_KEY` | Fernet key |
| `ROLLING_WINDOW_DAYS` | default `--window-days` |
| `ML_DASHBOARD_PORT` | host port for the dashboard (default `8501`) |

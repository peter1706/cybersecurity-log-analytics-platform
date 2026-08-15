# ml-mock service

Mock consumer of the `delivered` store. One image but with two entrypoints.

`feature_contract.py` holds the consumer's binding schema (`EXPECTED_COLUMNS`). A test asserts it matches the producer's `COMPUTER_FEATURE_COLUMNS` and delivery's `DELIVERED_COLUMNS`.

## Run modes

| | **`ml_consume`** (default) | **Dashboard** |
|---|---|---|
| Entrypoint | `consume.py` | `dashboard.py` (Streamlit, host port `ML_DASHBOARD_PORT`, default `8501`) |
| Role | DAG task after `deliver` | Always-on, login-gated UI over `delivered` |
| Flow | Fetch manifest + encrypted Parquet for an anchor day → decrypt (Parquet Modular Encryption / AES-GCM; corruption or wrong key fails the task) → exact schema check against the feature contract (any deviation rejects) → record-count check → log a simulated retrain | Pick a recent delivery in the left rail → cockpit from delivered feature columns (one screen) |

### Dashboard layout

- **Risk KPIs** (full width): needs attention, highest-risk computer, high-risk count, top unusual theme
- **Delivery context**: schema status, per-source presence (one expandable "Data sources" group), anchor day, window length, raw and aggregated record counts (schema/coverage detail dialogs)
- **Watchlist**: ranked unusual computers with filters/search; fixed height, internal scroll; click a row to explain it
- **Explainability**: per-computer drivers for the selected row

Risk buckets are percentile-based within the delivery (top 1% High, next 4% Medium). Advanced actions can load a specific `(window_days, anchor_day)` or trigger Airflow `feature_reprocessing` (Bronze→Silver→Gold→deliver→consume; never simulate). Raw feature rows and technical column names are not shown.

## Security & access

- MinIO service account is scoped to the `delivered` bucket only (never layer buckets). Credentials from mounted secrets; the dashboard never talks to the governance catalog DB.
- Dashboard joins `airflow-net` only to call the Airflow REST API (JWT via the Airflow admin principal) to trigger/poll `feature_reprocessing`.
- Login: secrets `ml_dashboard_username` / `ml_dashboard_password` (from `scripts/init_secrets.sh`; defaults `dashboard` / `dashboard`).

## Anomaly heuristic

In-dashboard robust *z-score* for visualization only — not the data-science model, and not written back to delivered storage. Features are `log1p`-scaled before the median/MAD z-score, so heavy-tailed volume counts do not dominate watchlist reasons. High/Medium labels are percentile ranks within the open delivery.

## Modules

| Module | Concern |
|--------|---------|
| `metrics.py` | Aggregations over the delivered frame |
| `charts.py` | Altair specs |
| `theme.py` | Stylesheet + card markup |
| `feature_labels.py` | Plain-language names + percentile risk |
| `watchlist.py` | Triage KPIs, ranked unusual-computer rows, driver rows |
| `partition_view.py`, `schema_view.py`, `volume_view.py` | Selection, schema-diff, volume helpers |

These stay free of Streamlit calls so they remain unit-testable.

`streamlit_config.toml` is baked in as `/app/.streamlit/config.toml` (dark theme + cyan accent). Required: on Streamlit's light base theme, labels/tables render dark-on-dark against the custom stylesheet. Keep colours in sync with `theme.py`.

## Configuration

Non-sensitive config is `.env` variables; credentials are container secrets at `/run/secrets/<name>` (`read_secret`, with env-var fallback for local runs).

| Var / secret | Kind | Purpose |
|-----|-----|---------|
| `MINIO_ENDPOINT` | env | MinIO endpoint URL |
| `minio_ml_consumer_key`, `minio_ml_consumer_secret` | secret | `delivered`-scoped MinIO service account |
| `delivery_encryption_key` | secret | Master key for decrypting Parquet Modular Encryption |
| `ml_dashboard_username`, `ml_dashboard_password` | secret | Dashboard login |
| `airflow_admin_password` | secret | Airflow JWT for reprocess triggers (with `AIRFLOW_ADMIN_USER`) |
| `AIRFLOW_API_URL` | env | Airflow API base URL (default `http://airflow-api-server:8080`) |
| `AIRFLOW_ADMIN_USER` | env | Airflow username for API auth |
| `DELIVERED_BUCKET` | env | Delivered bucket (default `delivered`) |
| `ROLLING_WINDOW_DAYS` | env | Default `--window-days` / UI default |
| `ML_DASHBOARD_PORT` | env | Host port for the dashboard (default `8501`) |

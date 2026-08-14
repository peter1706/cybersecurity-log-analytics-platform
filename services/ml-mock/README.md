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
  via `ML_DASHBOARD_PORT`): login-gated consumer UI over the `delivered` bucket.
  Pick a recent delivery from the left rail and read a triage-first cockpit built
  from the delivered feature columns, laid out to fit one screen: a full-width row
  of risk KPIs (needs attention, highest-risk computer, high-risk count, top unusual
  theme) above a row of delivery context, a ranked watchlist of unusual computers
  with filters/search (click a row to explain it), and an explainability panel of
  per-computer drivers for the selected row. The watchlist has a fixed height and
  scrolls internally so a long list cannot stretch the page. Risk buckets are
  percentile-based within the delivery (top 1% High, next 4% Medium). Delivery
  information covers schema status, per-source presence (collapsed into one
  expandable "Data sources" group), anchor day, window length, raw data records, and
  aggregated record counts (with schema/coverage detail dialogs).
  Advanced actions load a specific `(window_days, anchor_day)` or trigger the
  Airflow `feature_reprocessing` DAG (Bronze→Silver→Gold→deliver→consume; never
  simulate). Raw feature rows and technical column names are deliberately not
  shown.

`feature_contract.py` holds the consumer's own copy of the binding schema
(`EXPECTED_COLUMNS`); a unit test asserts it matches the producer's
`COMPUTER_FEATURE_COLUMNS` and delivery's `DELIVERED_COLUMNS`.

## Notes

- Least privilege for data: MinIO service account scoped to the `delivered`
  bucket only (never the layer buckets). Credentials come from mounted secrets;
  the dashboard never talks to the governance catalog DB.
- The dashboard also joins `airflow-net` solely to call the Airflow REST API
  (JWT via the Airflow admin principal) to trigger/poll `feature_reprocessing`.
- Default login credentials are container secrets `ml_dashboard_username` /
  `ml_dashboard_password` (created by `scripts/init_secrets.sh`, defaults
  `dashboard` / `dashboard`).
- Anomaly figures are an in-dashboard robust z-score heuristic for visualization
  only — not the data-science model, and not written back to delivered storage.
  Features are ``log1p``-scaled before the median/MAD z-score so heavy-tailed
  volume counts (e.g. outgoing sign-ins) do not drown every other watchlist
  reason. High/Medium labels are percentile ranks within the open delivery, not
  absolute production thresholds.
- Dashboard modules split by concern: `metrics.py` (aggregations over the
  delivered frame), `charts.py` (Altair specs), `theme.py` (stylesheet + card
  markup), `feature_labels.py` (plain-language names + percentile risk),
  `watchlist.py` (triage KPIs, ranked unusual-computer rows and driver rows),
  `partition_view.py`, `schema_view.py` and `volume_view.py` (selection,
  schema-diff and volume helpers). Keeping them free of Streamlit calls is what
  makes them unit testable.
- `streamlit_config.toml` is baked into the image as `/app/.streamlit/config.toml`
  and pins the dark theme + cyan accent. It is not optional styling: on the light
  base theme Streamlit's own labels and tables render dark-on-dark against the
  custom stylesheet. Keep its colours in sync with `theme.py`.

## Configuration

Non-sensitive config is env vars; credentials are container secrets mounted at
`/run/secrets/<name>` (loaded via `read_secret`, with an env-var fallback for
local runs).

| Var / secret | Kind | Purpose |
|-----|-----|---------|
| `MINIO_ENDPOINT` | env | MinIO endpoint URL |
| `minio_ml_consumer_key`, `minio_ml_consumer_secret` | secret | `delivered`-scoped MinIO service account |
| `delivery_encryption_key` | secret | master key for decrypting Parquet Modular Encryption |
| `ml_dashboard_username`, `ml_dashboard_password` | secret | dashboard login |
| `airflow_admin_password` | secret | Airflow JWT for reprocess triggers (with `AIRFLOW_ADMIN_USER`) |
| `AIRFLOW_API_URL` | env | Airflow API base URL (default `http://airflow-api-server:8080`) |
| `AIRFLOW_ADMIN_USER` | env | Airflow username for API auth |
| `DELIVERED_BUCKET` | env | delivered bucket (default `delivered`) |
| `ROLLING_WINDOW_DAYS` | env | default `--window-days` / UI default |
| `ML_DASHBOARD_PORT` | env | host port for the dashboard (default `8501`) |

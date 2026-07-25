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
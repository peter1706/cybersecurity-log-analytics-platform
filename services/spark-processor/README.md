# spark-processor

One Spark service running three Airflow-triggered jobs over Delta Lake on
`s3a://`:

- **(a) landing → Bronze**: parse/validate landed objects, write partitioned Bronze.
- **(b) Bronze → Silver**: clean, type, conform single events.
- **(c) Silver → Gold**: per-computer features over the rolling window.
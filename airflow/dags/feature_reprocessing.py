"""feature_reprocessing DAG.

Rebuilds Bronze→Silver for the event days in a rolling window (landing must
already exist via simulate / Makefile), then regenerates Gold, delivery, and
ml_consume for the selected ``(window_days, anchor_day)``.

Airflow carries no data; each stage runs as a short-lived task container.
DAG construction lives in feature_reprocessing_dag.py.
"""

from feature_reprocessing_dag import build_feature_reprocessing

dag = build_feature_reprocessing()

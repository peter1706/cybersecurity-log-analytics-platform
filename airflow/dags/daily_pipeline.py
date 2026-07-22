"""daily_pipeline DAG.

Orchestrates the following steps for one data day:

1. simulate_day
2. land_to_bronze
3. bronze_to_silver
4. silver_to_gold

Airflow carries no data; it launches each stage as a short-lived task container
via the DockerOperator. DAG construction lives in pipeline_dag.py; environment
key names live in pipeline_config.yaml (values from the process environment).
"""

from pipeline_dag import build_daily_pipeline

dag = build_daily_pipeline()

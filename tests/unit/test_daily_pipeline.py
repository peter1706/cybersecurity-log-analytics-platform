"""Structural unit tests for the daily_pipeline DAG factory."""

from __future__ import annotations

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.docker")

from pipeline_dag import build_daily_pipeline  # noqa: E402
from pipeline_settings import PipelineSettings  # noqa: E402


@pytest.fixture
def settings() -> PipelineSettings:
    """Fixed PipelineSettings for DAG construction without reading os.environ."""
    return PipelineSettings(
        img_simulator="clap-lanl-simulator:test",
        img_spark="clap-spark-processor:test",
        network_name="platform-net",
        host_project_dir="/repo",
        task_environment={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ROOT_USER": "user",
            "MINIO_ROOT_PASSWORD": "secret",
            "LANDING_BUCKET": "landing",
            "BRONZE_BUCKET": "bronze",
            "SILVER_BUCKET": "silver",
            "GOLD_BUCKET": "gold",
        },
    )


def test_daily_pipeline_task_order_and_deps(settings):
    """Assert the DAG graph matches expected defaults and dependencies.

    Checks that build_daily_pipeline produces dag_id daily_pipeline, is
    unscheduled (manual trigger only), has catchup disabled, defaults params.day
    to 0, registers exactly the four tasks
    simulate_day -> land_to_bronze -> bronze_to_silver -> silver_to_gold, and
    wires them in that linear order with no extra edges.
    """
    dag = build_daily_pipeline(settings)

    assert dag.dag_id == "daily_pipeline"
    # Airflow 3: unscheduled Dags expose timetable.summary "None" (no cron/interval).
    schedule = getattr(dag, "schedule_interval", None)
    timetable = getattr(dag, "timetable", None)
    assert schedule is None or (timetable is not None and str(timetable.summary) == "None")
    assert dag.catchup is False
    assert dag.params["day"] == 0

    expected = [
        "simulate_day",
        "land_to_bronze",
        "bronze_to_silver",
        "silver_to_gold",
    ]
    assert list(dag.task_dict) == expected

    assert dag.task_dict["simulate_day"].downstream_task_ids == {"land_to_bronze"}
    assert dag.task_dict["land_to_bronze"].downstream_task_ids == {"bronze_to_silver"}
    assert dag.task_dict["bronze_to_silver"].downstream_task_ids == {"silver_to_gold"}
    assert dag.task_dict["silver_to_gold"].downstream_task_ids == set()


def test_daily_pipeline_operator_wiring(settings):
    """Assert DockerOperator fields are taken from the injected settings.

    For simulate_day: image, auth/day command, platform network, MinIO env
    forwarding, and the read-only bind of host data/subset into /data/subset.
    For land_to_bronze: spark image, job command, and the full task_environment
    dict passed through to the container.
    """
    dag = build_daily_pipeline(settings)

    simulate = dag.task_dict["simulate_day"]
    assert simulate.image == "clap-lanl-simulator:test"
    assert simulate.command == ["--source", "auth", "--day", "{{ params.day }}"]
    assert simulate.network_mode == "platform-net"
    assert simulate.environment["MINIO_ENDPOINT"] == "http://minio:9000"
    assert simulate.mounts[0]["Source"] == "/repo/data/subset"
    assert simulate.mounts[0]["Target"] == "/data/subset"

    spark = dag.task_dict["land_to_bronze"]
    assert spark.image == "clap-spark-processor:test"
    assert spark.command == ["land_to_bronze", "--source", "auth", "--day", "{{ params.day }}"]
    assert spark.environment == settings.task_environment

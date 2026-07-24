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
    """Assert the DAG graph matches expected defaults and per-source dependencies.

    Checks that build_daily_pipeline produces dag_id daily_pipeline, is
    unscheduled (manual trigger only), has catchup disabled, defaults params.day
    to 0, runs an independent simulate -> land_to_bronze -> bronze_to_silver chain
    per source inside a group-namespaced TaskGroup, and adds a silver_to_gold step
    for auth only.
    """
    dag = build_daily_pipeline(settings)

    assert dag.dag_id == "daily_pipeline"
    # Airflow 3: unscheduled Dags expose timetable.summary "None" (no cron/interval).
    schedule = getattr(dag, "schedule_interval", None)
    timetable = getattr(dag, "timetable", None)
    assert schedule is None or (timetable is not None and str(timetable.summary) == "None")
    assert dag.catchup is False
    assert dag.params["day"] == 0

    expected = set()
    for source in ("auth", "proc", "flows", "dns"):
        expected |= {
            f"{source}.simulate",
            f"{source}.land_to_bronze",
            f"{source}.bronze_to_silver",
        }
    expected.add("auth.silver_to_gold")
    assert set(dag.task_dict) == expected

    for source in ("auth", "proc", "flows", "dns"):
        assert dag.task_dict[f"{source}.simulate"].downstream_task_ids == {
            f"{source}.land_to_bronze"
        }
        assert dag.task_dict[f"{source}.land_to_bronze"].downstream_task_ids == {
            f"{source}.bronze_to_silver"
        }

    # auth alone continues into Gold; the other sources stop at Silver.
    assert dag.task_dict["auth.bronze_to_silver"].downstream_task_ids == {"auth.silver_to_gold"}
    assert dag.task_dict["auth.silver_to_gold"].downstream_task_ids == set()
    for source in ("proc", "flows", "dns"):
        assert dag.task_dict[f"{source}.bronze_to_silver"].downstream_task_ids == set()


def test_daily_pipeline_operator_wiring(settings):
    """Assert DockerOperator fields are taken from the injected settings.

    For auth.simulate: image, auth/day command, platform network, MinIO env
    forwarding, and the read-only bind of host data/subset into /data/subset.
    For auth.land_to_bronze: spark image, job command, and the full
    task_environment dict passed through to the container.
    """
    dag = build_daily_pipeline(settings)

    simulate = dag.task_dict["auth.simulate"]
    assert simulate.image == "clap-lanl-simulator:test"
    assert simulate.command == ["--source", "auth", "--day", "{{ params.day }}"]
    assert simulate.network_mode == "platform-net"
    assert simulate.environment["MINIO_ENDPOINT"] == "http://minio:9000"
    assert simulate.mounts[0]["Source"] == "/repo/data/subset"
    assert simulate.mounts[0]["Target"] == "/data/subset"

    spark = dag.task_dict["auth.land_to_bronze"]
    assert spark.image == "clap-spark-processor:test"
    assert spark.command == ["land_to_bronze", "--source", "auth", "--day", "{{ params.day }}"]
    assert spark.environment == settings.task_environment

    flows_silver = dag.task_dict["flows.bronze_to_silver"]
    assert flows_silver.command == [
        "bronze_to_silver",
        "--source",
        "flows",
        "--day",
        "{{ params.day }}",
    ]

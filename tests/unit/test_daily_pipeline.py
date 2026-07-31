"""Structural unit tests for the daily_pipeline DAG factory."""

from __future__ import annotations

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.docker")

import datetime as dt  # noqa: E402
import json  # noqa: E402
from datetime import timedelta  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from pipeline_dag import build_daily_pipeline, build_failure_alert, build_job_run  # noqa: E402
from pipeline_settings import PipelineSettings  # noqa: E402


@pytest.fixture
def settings() -> PipelineSettings:
    """Fixed PipelineSettings for DAG construction without reading os.environ."""
    return PipelineSettings(
        img_simulator="clap-lanl-simulator:test",
        img_spark="clap-spark-processor:test",
        img_delivery="clap-delivery:test",
        img_ml_mock="clap-ml-mock:test",
        network_name="clap-pipeline-net",
        ml_network_name="clap-ml-net",
        host_project_dir="/repo",
        # Non-sensitive config only; credentials arrive as mounted /run/secrets.
        task_environment={
            "MINIO_ENDPOINT": "http://minio:9000",
            "LANDING_BUCKET": "landing",
            "BRONZE_BUCKET": "bronze",
            "SILVER_BUCKET": "silver",
            "GOLD_BUCKET": "gold",
            "DELIVERED_BUCKET": "delivered",
            "SCHEMA_VERSION": "v1",
            "SPARK_DRIVER_MEMORY": "3g",
        },
    )


def test_daily_pipeline_task_order_and_deps(settings):
    """Assert the DAG graph matches expected defaults and per-source dependencies.

    Checks that build_daily_pipeline produces dag_id daily_pipeline, is
    unscheduled (manual trigger only), has catchup disabled, defaults params.day
    to 0, runs an independent simulate -> land_to_bronze -> bronze_to_silver chain
    per source inside a group-namespaced TaskGroup, and adds a single cross-source
    silver_to_gold step fed by every source's Silver.
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
    expected.update({"silver_to_gold", "deliver", "ml_consume"})
    assert set(dag.task_dict) == expected

    for source in ("auth", "proc", "flows", "dns"):
        assert dag.task_dict[f"{source}.simulate"].downstream_task_ids == {
            f"{source}.land_to_bronze"
        }
        assert dag.task_dict[f"{source}.land_to_bronze"].downstream_task_ids == {
            f"{source}.bronze_to_silver"
        }

    # Every source's Silver feeds the single cross-source computer_features Gold job,
    # which then flows Gold -> deliver -> ml_consume.
    for source in ("auth", "proc", "flows", "dns"):
        assert dag.task_dict[f"{source}.bronze_to_silver"].downstream_task_ids == {"silver_to_gold"}
    assert dag.task_dict["silver_to_gold"].downstream_task_ids == {"deliver"}
    assert dag.task_dict["deliver"].downstream_task_ids == {"ml_consume"}
    assert dag.task_dict["ml_consume"].downstream_task_ids == set()


def test_simulate_task_wiring(settings):
    """Assert auth.simulate's image, command, network, env, and mounts.

    Covers the platform network, MinIO env forwarding (without leaking
    credentials), and the read-only bind of host data/subset into /data/subset.
    """
    dag = build_daily_pipeline(settings)

    simulate = dag.task_dict["auth.simulate"]
    assert simulate.image == "clap-lanl-simulator:test"
    assert simulate.command == ["--source", "auth", "--day", "{{ params.day }}"]
    assert simulate.network_mode == "clap-pipeline-net"
    assert simulate.environment["MINIO_ENDPOINT"] == "http://minio:9000"
    # No credentials leak into the environment; they arrive as mounted secrets.
    assert "MINIO_ROOT_PASSWORD" not in simulate.environment
    # Mounts: the whole secrets dir (read-only) plus the data/subset bind.
    mount_sources = {m["Source"]: m for m in simulate.mounts}
    assert mount_sources["/repo/secrets"]["Target"] == "/run/secrets"
    assert mount_sources["/repo/secrets"]["ReadOnly"] is True
    assert mount_sources["/repo/data/subset"]["Target"] == "/data/subset"


def test_spark_source_task_wiring(settings):
    """Assert the per-source Spark task command/env wiring (land_to_bronze, bronze_to_silver)."""
    dag = build_daily_pipeline(settings)

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


def test_silver_to_gold_task_wiring(settings):
    """Assert the single cross-source Gold job's command takes no --source."""
    dag = build_daily_pipeline(settings)

    features = dag.task_dict["silver_to_gold"]
    assert features.image == "clap-spark-processor:test"
    assert features.command == ["silver_to_gold", "--day", "{{ params.day }}"]
    assert features.environment == settings.task_environment


def test_deliver_task_wiring(settings):
    """Assert the deliver task's image, command, and environment."""
    dag = build_daily_pipeline(settings)

    deliver = dag.task_dict["deliver"]
    assert deliver.image == "clap-delivery:test"
    assert deliver.command == ["--day", "{{ params.day }}"]
    assert deliver.environment == settings.task_environment


def test_ml_consume_task_wiring(settings):
    """Assert ml_consume's command/env and its network/secret isolation.

    The consumer is isolated to the ML network and only ever mounts its scoped
    secrets -- never the whole secrets dir (which would expose the root keys).
    """
    dag = build_daily_pipeline(settings)

    ml_consume = dag.task_dict["ml_consume"]
    assert ml_consume.image == "clap-ml-mock:test"
    assert ml_consume.command == ["--day", "{{ params.day }}"]
    assert ml_consume.environment == settings.task_environment
    assert ml_consume.network_mode == "clap-ml-net"
    ml_targets = {m["Target"] for m in ml_consume.mounts}
    assert ml_targets == {
        "/run/secrets/minio_ml_consumer_key",
        "/run/secrets/minio_ml_consumer_secret",
        "/run/secrets/delivery_encryption_key",
    }
    assert "/run/secrets" not in ml_targets
    assert all(m["ReadOnly"] for m in ml_consume.mounts)


def test_default_args_retries_and_backoff(settings):
    """Reliability requirement: every task retries >=3 times with capped backoff."""
    dag = build_daily_pipeline(settings)
    # default_args apply to all tasks; spot-check a representative one.
    task = dag.task_dict["silver_to_gold"]
    assert task.retries >= 3
    assert task.retry_exponential_backoff is True
    assert task.retry_delay == timedelta(seconds=30)
    assert task.max_retry_delay == timedelta(minutes=5)


def _callback_context(**overrides) -> dict:
    """A minimal Airflow-callback context for build_job_run."""
    ti = SimpleNamespace(
        dag_id="daily_pipeline",
        task_id="auth.land_to_bronze",
        run_id="manual__2026-01-01",
        try_number=2,
        start_date=dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
        end_date=dt.datetime(2026, 1, 1, 0, 0, 5, tzinfo=dt.UTC),
        duration=5.0,
    )
    context = {
        "task_instance": ti,
        "dag_run": SimpleNamespace(run_id="manual__2026-01-01"),
        "params": {"day": 6},
        "exception": None,
    }
    context.update(overrides)
    return context


class TestBuildJobRun:
    def test_success_splits_source_and_job_and_carries_timing(self):
        record = build_job_run(_callback_context(), "success")
        assert record.dag_id == "daily_pipeline"
        assert record.task_id == "auth.land_to_bronze"
        assert record.source == "auth"
        assert record.job == "land_to_bronze"
        assert record.status == "success"
        assert record.day == 6
        assert record.try_number == 2
        assert record.duration_ms == 5000
        assert record.error is None
        assert record.started_at is not None
        assert record.finished_at is not None

    def test_ungrouped_task_has_no_source(self):
        context = _callback_context()
        context["task_instance"].task_id = "silver_to_gold"
        record = build_job_run(context, "success")
        assert record.source is None
        assert record.job == "silver_to_gold"

    def test_failure_captures_exception_text(self):
        record = build_job_run(_callback_context(exception=RuntimeError("boom")), "failed")
        assert record.status == "failed"
        assert record.error is not None and "boom" in record.error


class TestBuildFailureAlert:
    def test_structured_alert_from_failed_context(self):
        alert = build_failure_alert(_callback_context(exception=RuntimeError("boom")))
        assert alert["alert"] == "task_failure"
        assert alert["dag_id"] == "daily_pipeline"
        assert alert["task_id"] == "auth.land_to_bronze"
        assert alert["source"] == "auth"
        assert alert["job"] == "land_to_bronze"
        assert alert["day"] == 6
        assert alert["try_number"] == 2
        assert alert["error"] is not None and "boom" in alert["error"]
        assert alert["duration_ms"] == 5000
        # Emitted as a single JSON log line, so it must be JSON-serializable.
        assert json.loads(json.dumps(alert, sort_keys=True))["alert"] == "task_failure"

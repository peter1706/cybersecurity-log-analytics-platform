"""Unit tests for feature_reprocessing pure helpers and DAG structure."""

from __future__ import annotations

from pathlib import Path

import pytest

from reprocessing_logic import (
    format_missing_landing,
    missing_landing_partitions,
    window_event_days,
)

DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.mark.parametrize(
    ("anchor", "window", "expected"),
    [
        (6, 7, list(range(0, 7))),
        (3, 2, [2, 3]),
        (0, 7, [0]),
    ],
)
def test_window_event_days(anchor, window, expected):
    assert window_event_days(anchor, window) == expected


def test_window_event_days_rejects_invalid():
    with pytest.raises(ValueError, match="window_days"):
        window_event_days(1, 0)
    with pytest.raises(ValueError, match="anchor_day"):
        window_event_days(-1, 7)


def test_missing_landing_partitions_lists_gaps():
    present = {("auth", 0), ("proc", 0)}

    def has_landing(source: str, day: int) -> bool:
        return (source, day) in present

    missing = missing_landing_partitions(
        [0],
        sources=("auth", "proc", "dns"),
        has_landing=has_landing,
    )
    assert missing == [("dns", 0)]
    assert "dns/day=00" in format_missing_landing(missing)


def test_dag_entrypoints_pass_safe_mode_discovery():
    """Airflow's safe mode only parses files containing both 'dag' and 'airflow'.

    A DAG module that never spells "airflow" is silently skipped — the DAG then
    404s on the REST API instead of failing loudly as an import error.
    """
    ignored = {
        line.strip()
        for line in (DAGS_DIR / ".airflowignore").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }
    entrypoints = [p for p in DAGS_DIR.glob("*.py") if p.name not in ignored]
    assert entrypoints, "no DAG entrypoints found"
    for path in entrypoints:
        text = path.read_text().lower()
        assert "dag" in text and "airflow" in text, f"{path.name} is invisible to safe mode"


pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.docker")

from feature_reprocessing_dag import build_feature_reprocessing  # noqa: E402
from pipeline_settings import PipelineSettings  # noqa: E402


@pytest.fixture
def settings() -> PipelineSettings:
    return PipelineSettings(
        img_simulator="clap-lanl-simulator:test",
        img_spark="clap-spark-processor:test",
        img_delivery="clap-delivery:test",
        img_ml_mock="clap-ml-mock:test",
        network_name="clap-pipeline-net",
        ml_network_name="clap-ml-net",
        host_project_dir="/repo",
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


def test_feature_reprocessing_task_graph(settings):
    dag = build_feature_reprocessing(settings)
    assert dag.dag_id == "feature_reprocessing"
    assert "window_days" in dag.params
    assert set(dag.task_dict) == {
        "preflight_landing",
        "land_to_bronze",
        "bronze_to_silver",
        "silver_to_gold",
        "deliver",
        "ml_consume",
    }
    assert not any("simulate" in task_id for task_id in dag.task_dict)
    assert dag.task_dict["preflight_landing"].downstream_task_ids == {"land_to_bronze"}
    assert dag.task_dict["land_to_bronze"].downstream_task_ids == {"bronze_to_silver"}
    assert dag.task_dict["bronze_to_silver"].downstream_task_ids == {"silver_to_gold"}
    assert dag.task_dict["silver_to_gold"].downstream_task_ids == {"deliver"}
    assert dag.task_dict["deliver"].downstream_task_ids == {"ml_consume"}

    gold = dag.task_dict["silver_to_gold"]
    assert "--window-days" in gold.command
    assert "--all-sources" in dag.task_dict["land_to_bronze"].command

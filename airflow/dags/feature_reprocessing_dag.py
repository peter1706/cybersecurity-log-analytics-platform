"""DAG factory for feature_reprocessing (no module-level side effects).

Rebuilds Bronze→Silver for the rolling window's event days (landing must already
exist), then regenerates Gold→deliver→consume for ``(window_days, anchor_day)``.
Never runs the lanl-simulator.
"""

from __future__ import annotations

import os

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from catalog import CatalogClient
from pipeline_dag import (
    DEFAULT_ARGS,
    ComputerFeaturesOperator,
    DeliveryOperator,
    MlConsumeOperator,
    SparkJobOperator,
)
from pipeline_settings import PipelineSettings, load_pipeline_settings
from reprocessing_logic import (
    format_missing_landing,
    missing_landing_partitions,
    window_event_days,
)

# XCom templates for downstream DockerOperator commands.
_PREFLIGHT = "preflight_landing"
_START = "{{ ti.xcom_pull(task_ids='preflight_landing')['start_day'] }}"
_END = "{{ ti.xcom_pull(task_ids='preflight_landing')['end_day'] }}"
_DAY = "{{ ti.xcom_pull(task_ids='preflight_landing')['day'] }}"
_WINDOW = "{{ ti.xcom_pull(task_ids='preflight_landing')['window_days'] }}"


def run_preflight_landing(**context: object) -> dict[str, int]:
    """Fail fast when landing is missing for any day in the requested window.

    Reads ``day`` / ``window_days`` from the DAG-run ``conf`` (API trigger) with
    a fallback to DAG ``params`` (manual UI trigger).

    Returns:
        Dict with ``day``, ``window_days``, ``start_day``, and ``end_day`` for
        downstream task templates.
    """
    dag_run = context.get("dag_run")
    conf: dict = {}
    if dag_run is not None:
        raw_conf = getattr(dag_run, "conf", None) or {}
        if isinstance(raw_conf, dict):
            conf = raw_conf
    params = context.get("params") or {}
    if not isinstance(params, dict):
        params = {}

    day = int(conf.get("day", params.get("day", 0)))
    window_days = int(conf.get("window_days", params.get("window_days", 7)))
    event_days = window_event_days(day, window_days)

    with CatalogClient.connect() as catalog:

        def has_landing(source: str, event_day: int) -> bool:
            return catalog.get_checksum("landing", event_day, source) is not None

        missing = missing_landing_partitions(event_days, has_landing=has_landing)

    if missing:
        raise ValueError(format_missing_landing(missing))

    return {
        "day": day,
        "window_days": window_days,
        "start_day": event_days[0],
        "end_day": event_days[-1],
    }


def build_feature_reprocessing(settings: PipelineSettings | None = None) -> DAG:
    """Assemble and return the feature_reprocessing DAG."""
    settings = settings or load_pipeline_settings()
    default_window = int(os.environ.get("ROLLING_WINDOW_DAYS", "7"))

    with DAG(
        dag_id="feature_reprocessing",
        description=(
            "Rebuild Bronze→Silver for a window (landing must exist), then "
            "Gold→deliver→consume for (window_days, anchor_day)"
        ),
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        # Trigger-only DAG (schedule=None): active on creation so the dashboard's
        # rebuild works without a manual unpause.
        is_paused_upon_creation=False,
        default_args=DEFAULT_ARGS,
        params={"day": 0, "window_days": default_window},
        tags=["clap", "reprocess"],
    ) as dag:
        preflight = PythonOperator(
            task_id=_PREFLIGHT,
            python_callable=run_preflight_landing,
        )
        land_to_bronze = SparkJobOperator(
            settings=settings,
            job="land_to_bronze",
            all_sources=True,
            day=_START,
            day_end=_END,
        )
        bronze_to_silver = SparkJobOperator(
            settings=settings,
            job="bronze_to_silver",
            all_sources=True,
            day=_START,
            day_end=_END,
        )
        silver_to_gold = ComputerFeaturesOperator(
            settings=settings,
            day=_DAY,
            window_days=_WINDOW,
        )
        deliver = DeliveryOperator(
            settings=settings,
            day=_DAY,
            window_days=_WINDOW,
        )
        ml_consume = MlConsumeOperator(
            settings=settings,
            day=_DAY,
            window_days=_WINDOW,
        )

        (preflight >> land_to_bronze >> bronze_to_silver >> silver_to_gold >> deliver >> ml_consume)

    return dag

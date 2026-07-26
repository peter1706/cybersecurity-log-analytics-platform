"""DAG factory for daily_pipeline (no module-level side effects).

Imported by daily_pipeline.py for Airflow discovery and by unit tests with
injected PipelineSettings.
"""

import json
import logging
from datetime import timedelta

import pendulum
from docker.types import Mount
from pipeline_settings import PipelineSettings, load_pipeline_settings

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, TaskGroup
from catalog import CatalogClient, JobRun

SOURCES = ("auth", "proc", "flows", "dns")
DAY_TEMPLATE = "{{ params.day }}"

# Structured failure alerts are emitted to the logs (no SMTP/e-mail), keeping the
# stack fully offline-capable.
_LOG = logging.getLogger("clap.pipeline")

# Credentials the ML consumer is allowed to hold. It runs on the isolated
# consumer network with a `delivered`-scoped MinIO service account, so only these
# secret files are mounted -- never the MinIO root keys or the catalog password.
ML_CONSUMER_SECRETS = (
    "minio_ml_consumer_key",
    "minio_ml_consumer_secret",
    "delivery_encryption_key",
)


def _secret_mount(host_project_dir: str, name: str) -> Mount:
    """Read-only bind of one host secret file into ``/run/secrets/<name>``."""
    return Mount(
        source=f"{host_project_dir}/secrets/{name}",
        target=f"/run/secrets/{name}",
        type="bind",
        read_only=True,
    )


def _all_secrets_mount(host_project_dir: str) -> Mount:
    """Read-only bind of the whole host secrets dir into ``/run/secrets``."""
    return Mount(
        source=f"{host_project_dir}/secrets",
        target="/run/secrets",
        type="bind",
        read_only=True,
    )


def _iso(value) -> str | None:
    """Return an ISO-8601 string for a datetime, or ``None``."""
    return value.isoformat() if value is not None else None


def build_job_run(context: dict, status: str) -> JobRun:
    """Build a ``job_runs`` record from an Airflow callback context (pure).

    ``task_id`` is group-namespaced (e.g. ``auth.land_to_bronze``); the leading
    group (when present) is the source and the trailing segment is the logical job.
    """
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    params = context.get("params") or {}
    exception = context.get("exception")

    task_id = getattr(ti, "task_id", "") or ""
    parts = task_id.split(".")
    source = parts[0] if len(parts) > 1 else None
    job = parts[-1] if parts else task_id

    duration = getattr(ti, "duration", None)
    day = params.get("day")
    return JobRun(
        dag_id=getattr(ti, "dag_id", "") or "",
        task_id=task_id,
        run_id=getattr(ti, "run_id", None) or getattr(dag_run, "run_id", "") or "",
        job=job,
        status=status,
        source=source,
        day=int(day) if day is not None else None,
        try_number=getattr(ti, "try_number", 1) or 1,
        error=str(exception) if exception else None,
        started_at=_iso(getattr(ti, "start_date", None)),
        finished_at=_iso(getattr(ti, "end_date", None)),
        duration_ms=int(duration * 1000) if duration is not None else None,
    )


def _record_job_run(context: dict, status: str) -> None:
    """Best-effort ``job_runs`` telemetry write from a task callback.

    Unlike the in-container data-lineage writes (which fail the task), a job-run
    row is orchestration telemetry: a callback cannot fail an already-finished
    task, so a catalog blip is logged rather than raised.
    """
    try:
        with CatalogClient.connect() as catalog:
            catalog.record_job_run(build_job_run(context, status))
    except Exception as exc:  # noqa: BLE001 - telemetry must not mask task outcome
        print(f"job_run governance write failed ({status}): {exc}", flush=True)


def build_failure_alert(context: dict) -> dict:
    """Structured failure-alert payload for the logs (pure).

    Reuses :func:`build_job_run` so the alert and the ``job_runs`` telemetry can't
    diverge. Emitted as a single JSON log line by :func:`_on_failure_callback`
    once retries are exhausted -- a log alert rather than SMTP e-mail keeps the
    system offline-capable.
    """
    run = build_job_run(context, "failed")
    return {
        "alert": "task_failure",
        "dag_id": run.dag_id,
        "task_id": run.task_id,
        "run_id": run.run_id,
        "job": run.job,
        "source": run.source,
        "day": run.day,
        "try_number": run.try_number,
        "error": run.error,
        "duration_ms": run.duration_ms,
    }


def _on_success_callback(context: dict) -> None:
    _record_job_run(context, "success")


def _on_failure_callback(context: dict) -> None:
    _LOG.error(
        "pipeline task failure alert: %s",
        json.dumps(build_failure_alert(context), sort_keys=True),
    )
    _record_job_run(context, "failed")


# >=3 retries with exponential backoff (capped) satisfies the reliability
# requirement; the failure callback emits the structured log alert once retries
# are exhausted. Backoff grows 30s -> 60s -> 120s ... up to max_retry_delay.
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_success_callback": _on_success_callback,
    "on_failure_callback": _on_failure_callback,
}


class _PlatformDockerOperator(DockerOperator):
    """DockerOperator preloaded with the platform's shared Docker settings.

    Credentials are delivered as read-only ``/run/secrets`` bind mounts (never
    env vars). By default every task gets the whole host ``secrets/`` dir; pass
    ``secret_mounts`` to restrict a task (the ML consumer only mounts its scoped
    subset). Any task-specific ``mounts`` (e.g. the simulator's data subset) are
    appended to the secret mounts rather than replacing them.
    """

    def __init__(
        self,
        *,
        settings: PipelineSettings,
        secret_mounts: list[Mount] | None = None,
        **kwargs,
    ):
        if secret_mounts is None:
            secret_mounts = [_all_secrets_mount(settings.host_project_dir)]
        extra_mounts = kwargs.pop("mounts", [])
        defaults = {
            "docker_url": "unix:///var/run/docker.sock",
            "network_mode": settings.network_name,
            "auto_remove": "success",
            "mount_tmp_dir": False,
            "tty": False,
            "environment": settings.task_environment,
            "mounts": [*secret_mounts, *extra_mounts],
        }
        super().__init__(**{**defaults, **kwargs})


class SimulatorOperator(_PlatformDockerOperator):
    """Runs the lanl-simulator for one source per day, mounting the source subset."""

    def __init__(
        self,
        *,
        settings: PipelineSettings,
        source: str,
        day: str = DAY_TEMPLATE,
        **kwargs,
    ):
        super().__init__(
            settings=settings,
            task_id="simulate",
            image=settings.img_simulator,
            command=["--source", source, "--day", day],
            mounts=[
                Mount(
                    source=f"{settings.host_project_dir}/data/subset",
                    target="/data/subset",
                    type="bind",
                    read_only=True,
                )
            ],
            **kwargs,
        )


class SparkJobOperator(_PlatformDockerOperator):
    """Runs one spark-processor Medallion job for a source per day."""

    def __init__(
        self,
        *,
        settings: PipelineSettings,
        job: str,
        source: str,
        day: str = DAY_TEMPLATE,
        **kwargs,
    ):
        super().__init__(
            settings=settings,
            task_id=job,
            image=settings.img_spark,
            command=[job, "--source", source, "--day", day],
            **kwargs,
        )


class ComputerFeaturesOperator(_PlatformDockerOperator):
    """Runs the cross-source Silver -> Gold job that builds ``computer_features``.

    This is the single Gold step. It depends on every
    source's Silver being ready.
    """

    def __init__(self, *, settings: PipelineSettings, day: str = DAY_TEMPLATE, **kwargs):
        super().__init__(
            settings=settings,
            task_id="silver_to_gold",
            image=settings.img_spark,
            command=["silver_to_gold", "--day", day],
            **kwargs,
        )


class DeliveryOperator(_PlatformDockerOperator):
    """Delivers the anchor day's Gold partition to the ``delivered`` bucket."""

    def __init__(self, *, settings: PipelineSettings, day: str = DAY_TEMPLATE, **kwargs):
        super().__init__(
            settings=settings,
            task_id="deliver",
            image=settings.img_delivery,
            command=["--day", day],
            **kwargs,
        )


class MlConsumeOperator(_PlatformDockerOperator):
    """Runs the ml-mock consumer to verify the delivered partition and mock-retrain.

    Isolated to the consumer network and given only the `delivered`-scoped MinIO
    service account + the decryption key -- never the MinIO root keys or the
    catalog password (defense in depth alongside the network boundary).
    """

    def __init__(self, *, settings: PipelineSettings, day: str = DAY_TEMPLATE, **kwargs):
        scoped = [_secret_mount(settings.host_project_dir, name) for name in ML_CONSUMER_SECRETS]
        super().__init__(
            settings=settings,
            task_id="ml_consume",
            image=settings.img_ml_mock,
            command=["--day", day],
            network_mode=settings.ml_network_name,
            secret_mounts=scoped,
            **kwargs,
        )


def build_daily_pipeline(settings: PipelineSettings | None = None) -> DAG:
    """Assemble and return the daily_pipeline DAG.

    Each source runs an independent landing -> Bronze -> Silver chain inside its
    own TaskGroup (task ids namespaced by the group, e.g. ``auth.bronze_to_silver``).
    A single cross-source ``silver_to_gold`` step then joins every source's Silver
    into the unified ``computer_features`` Gold table, which is handed off via
    ``deliver`` (encrypt + manifest) and verified by ``ml_consume``.
    """
    settings = settings or load_pipeline_settings()

    with DAG(
        dag_id="daily_pipeline",
        description="Daily pipeline execution from landing -> Bronze -> Silver -> Gold",
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        default_args=DEFAULT_ARGS,
        params={"day": 0},
        tags=["clap"],
    ) as dag:
        silver_tasks = []
        for source in SOURCES:
            with TaskGroup(group_id=source):
                simulate = SimulatorOperator(settings=settings, source=source)
                land_to_bronze = SparkJobOperator(
                    settings=settings, job="land_to_bronze", source=source
                )
                bronze_to_silver = SparkJobOperator(
                    settings=settings, job="bronze_to_silver", source=source
                )
                simulate >> land_to_bronze >> bronze_to_silver
            silver_tasks.append(bronze_to_silver)

        # The unified per-computer feature table joins all sources, so this single
        # Gold step waits for every source's Silver before running once per anchor day.
        silver_to_gold = ComputerFeaturesOperator(settings=settings)
        for silver_task in silver_tasks:
            silver_task >> silver_to_gold

        # Gold -> encrypted delivery -> consumer verification + mock retrain.
        deliver = DeliveryOperator(settings=settings)
        ml_consume = MlConsumeOperator(settings=settings)
        silver_to_gold >> deliver >> ml_consume

    return dag

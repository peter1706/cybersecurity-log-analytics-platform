"""DAG factory for daily_pipeline (no module-level side effects).

Imported by daily_pipeline.py for Airflow discovery and by unit tests with
injected PipelineSettings.
"""

from datetime import timedelta

import pendulum
from docker.types import Mount
from pipeline_settings import PipelineSettings, load_pipeline_settings

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, TaskGroup

SOURCES = ("auth", "proc", "flows", "dns")
DAY_TEMPLATE = "{{ params.day }}"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


class _PlatformDockerOperator(DockerOperator):
    """DockerOperator preloaded with the platform's shared Docker settings."""

    def __init__(self, *, settings: PipelineSettings, **kwargs):
        defaults = {
            "docker_url": "unix:///var/run/docker.sock",
            "network_mode": settings.network_name,
            "auto_remove": "success",
            "mount_tmp_dir": False,
            "tty": False,
            "environment": settings.task_environment,
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
    """Runs the ml-mock consumer to verify the delivered partition and mock-retrain."""

    def __init__(self, *, settings: PipelineSettings, day: str = DAY_TEMPLATE, **kwargs):
        super().__init__(
            settings=settings,
            task_id="ml_consume",
            image=settings.img_ml_mock,
            command=["--day", day],
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

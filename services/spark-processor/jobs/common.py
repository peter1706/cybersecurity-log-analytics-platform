"""Shared Spark/Delta/S3A session setup and path helpers."""

import os

from pyspark.sql import SparkSession

from catalog import read_secret

HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"


def build_spark(app_name: str) -> SparkSession:
    """Return a SparkSession configured for Delta Lake on MinIO over s3a."""
    # Lazy import: keeps this module (and the pure helpers/job record builders
    # that import it) importable without delta-spark installed.
    from delta import configure_spark_with_delta_pip

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    # MinIO keys are container secrets (env fallback for local tooling/tests).
    access = read_secret("minio_root_user", env="MINIO_ROOT_USER")
    secret = read_secret("minio_root_password", env="MINIO_ROOT_PASSWORD")

    # Single-node dataset: the 200-partition shuffle default just creates tiny
    # tasks and scheduling overhead. Keep it configurable (SPARK_SQL_SHUFFLE_
    # PARTITIONS) and only override when set, so tests/other callers keep theirs.
    shuffle_partitions = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS")

    # Executor sizing knobs (memory/cores/count). Inert under the default
    # ``local[*]`` master (driver and executor are the same JVM), but they let the
    # same image scale out on a real cluster by only changing env vars -- the
    # config-only horizontal-scale headroom the design calls for. Applied only when
    # set, so unit tests and local runs keep Spark's defaults.
    executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY")
    executor_cores = os.environ.get("SPARK_EXECUTOR_CORES")
    executor_instances = os.environ.get("SPARK_EXECUTOR_INSTANCES")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    if shuffle_partitions:
        builder = builder.config("spark.sql.shuffle.partitions", shuffle_partitions)
    if executor_memory:
        builder = builder.config("spark.executor.memory", executor_memory)
    if executor_cores:
        builder = builder.config("spark.executor.cores", executor_cores)
    if executor_instances:
        builder = builder.config("spark.executor.instances", executor_instances)

    builder = configure_spark_with_delta_pip(builder, extra_packages=[HADOOP_AWS])
    # In local mode the driver JVM heap is fixed at launch, so spark.driver.memory
    # set on the builder is ignored -- it must be a launcher arg. Inject it into
    # PYSPARK_SUBMIT_ARGS (which getOrCreate reads to start the gateway JVM),
    # preserving whatever configure_spark_with_delta_pip already put there.
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY")
    if driver_memory:
        submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        if "--driver-memory" not in submit_args:
            os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {driver_memory} {submit_args}"

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def bucket(layer: str) -> str:
    """Return the bucket name for a layer (env override or the layer name)."""
    return os.environ.get(f"{layer.upper()}_BUCKET", layer)


def schema_version() -> str:
    """Schema version stamped on governance records (env ``SCHEMA_VERSION``)."""
    return os.environ.get("SCHEMA_VERSION", "v1")


def rolling_window_days() -> int:
    """Length of the Silver -> Gold rolling window in days.

    Read from ``ROLLING_WINDOW_DAYS`` (default 7). Each Gold run aggregates the
    anchor day plus the preceding ``N - 1`` days into one anchor-day partition.
    """
    return int(os.environ.get("ROLLING_WINDOW_DAYS", "7"))


def landing_prefix(source: str, day: int) -> str:
    """s3a path to the landed objects for a source/day."""
    return f"s3a://{bucket('landing')}/{source}/day={day:02d}/"


def table_path(layer: str, table: str) -> str:
    """s3a path to a Delta table in a layer bucket."""
    return f"s3a://{bucket(layer)}/{table}"

"""Shared Spark/Delta/S3A session setup and path helpers."""

import os

from pyspark.sql import SparkSession

HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"


def build_spark(app_name: str) -> SparkSession:
    """Return a SparkSession configured for Delta Lake on MinIO over s3a."""
    # Lazy import: keeps this module (and the pure helpers/job record builders
    # that import it) importable without delta-spark installed.
    from delta import configure_spark_with_delta_pip

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access = os.environ["MINIO_ROOT_USER"]
    secret = os.environ["MINIO_ROOT_PASSWORD"]

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
    spark = configure_spark_with_delta_pip(builder, extra_packages=[HADOOP_AWS]).getOrCreate()
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

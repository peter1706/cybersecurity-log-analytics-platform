"""Pure DataFrame transforms for the auth pipeline.

These functions take and return Spark DataFrames and contain **no I/O** (no
Delta, no S3), so they can be unit-tested against a plain local SparkSession
without MinIO or Delta. The I/O wrappers live in the ``*_to_*`` job modules.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import AUTH_COLUMNS, SECONDS_PER_DAY


def _day_from_time(time_col: "F.Column") -> "F.Column":
    """Derive the LANL day index from a seconds timestamp: (time - 1) // 86400."""
    return F.floor((time_col.cast("long") - F.lit(1)) / F.lit(SECONDS_PER_DAY)).cast("int")


def raw_to_bronze(df_raw: DataFrame, source: str) -> DataFrame:
    """Assign column names to a header-less auth CSV and add source/day partitions.

    Bronze keeps values verbatim (as strings).
    """
    if source != "auth":
        raise ValueError(f"Currently supported source is 'auth', got {source!r}")
    renamed = df_raw
    for i, name in enumerate(AUTH_COLUMNS):
        renamed = renamed.withColumnRenamed(f"_c{i}", name)
    renamed = renamed.select(*AUTH_COLUMNS)
    return renamed.withColumn("source", F.lit(source)).withColumn(
        "day", _day_from_time(F.col("time"))
    )


def _split_user(col: "F.Column", part: int) -> "F.Column":
    """Return the user (part=0) or domain (part=1) of a ``user@domain`` token."""
    return F.split(col, "@").getItem(part)


def auth_bronze_to_silver(df_bronze: DataFrame) -> DataFrame:
    """Clean, type, and conform auth Bronze rows into Silver.

    - casts ``time`` to long
    - splits ``user@domain`` into name + domain
    - flags machine accounts (trailing ``$``)
    - normalizes success/failure to a nullable boolean (unknown -> null)
    - de-duplicates identical events
    """
    typed = (
        df_bronze.withColumn("time", F.col("time").cast("long"))
        .withColumn("src_user_name", _split_user(F.col("src_user"), 0))
        .withColumn("src_domain", _split_user(F.col("src_user"), 1))
        .withColumn("dst_user_name", _split_user(F.col("dst_user"), 0))
        .withColumn("dst_domain", _split_user(F.col("dst_user"), 1))
        .withColumn("src_user_is_machine", F.col("src_user_name").endswith("$"))
        .withColumn("dst_user_is_machine", F.col("dst_user_name").endswith("$"))
        .withColumn(
            "auth_success",
            F.when(F.lower(F.col("success")) == "success", F.lit(True))
            .when(F.lower(F.col("success")) == "fail", F.lit(False))
            .otherwise(F.lit(None).cast("boolean")),
        )
    )
    keep = [
        "time",
        "day",
        "source",
        "src_user_name",
        "src_domain",
        "dst_user_name",
        "dst_domain",
        "src_comp",
        "dst_comp",
        "auth_type",
        "logon_type",
        "auth_orientation",
        "src_user_is_machine",
        "dst_user_is_machine",
        "auth_success",
    ]
    return typed.select(*keep).dropDuplicates()


def auth_silver_to_gold(df_silver: DataFrame) -> DataFrame:
    """Aggregate Silver auth events into per-computer daily features.

    The "computer" is the destination computer (the host being authenticated
    to) -- the entity whose behavior the downstream anomaly model scores.
    """
    grouped = df_silver.groupBy("day", F.col("dst_comp").alias("computer")).agg(
        F.count(F.lit(1)).alias("auth_count"),
        F.sum(F.when(F.col("auth_success") == F.lit(True), 1).otherwise(0)).alias(
            "auth_success_count"
        ),
        F.sum(F.when(F.col("auth_success") == F.lit(False), 1).otherwise(0)).alias(
            "auth_failure_count"
        ),
        F.countDistinct("src_comp").alias("distinct_src_comp"),
        F.countDistinct(F.when(~F.col("src_user_is_machine"), F.col("src_user_name"))).alias(
            "distinct_human_users"
        ),
    )
    return grouped.withColumn(
        "auth_failure_rate",
        F.when(
            F.col("auth_count") > 0,
            F.col("auth_failure_count") / F.col("auth_count"),
        ).otherwise(F.lit(0.0)),
    )

"""Auth (authentication event) transforms."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ._common import _split_user


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

    - groups by ``day`` and destination computer (the host being authenticated
      to) -- the entity whose behavior the downstream anomaly model scores
    - counts total, successful, and failed authentications
    - counts distinct source computers and distinct human (non-machine) users
    - derives the failure rate (0.0 when there are no events)
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

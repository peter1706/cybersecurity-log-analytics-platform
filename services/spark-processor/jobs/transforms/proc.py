"""Proc (process start/stop event) transforms."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ._common import _split_user


def proc_bronze_to_silver(df_bronze: DataFrame) -> DataFrame:
    """Clean, type, and conform proc Bronze rows into Silver.

    - casts ``time`` to long
    - splits ``user@domain`` into name + domain
    - flags machine accounts (trailing ``$``)
    - de-duplicates identical events
    """
    typed = (
        df_bronze.withColumn("time", F.col("time").cast("long"))
        .withColumn("user_name", _split_user(F.col("user"), 0))
        .withColumn("domain", _split_user(F.col("user"), 1))
        .withColumn("user_is_machine", _split_user(F.col("user"), 0).endswith("$"))
    )
    keep = [
        "time",
        "day",
        "source",
        "user_name",
        "domain",
        "user_is_machine",
        "computer",
        "process_name",
        "event_type",
    ]
    return typed.select(*keep).dropDuplicates()


def proc_computer_features(df_silver: DataFrame) -> DataFrame:
    """Per-computer process features for the unified feature table, keyed by ``computer_id``.

    The entity is the computer the process ran on. ``proc_start_count`` counts
    only start events (``event_type`` == ``start``); distinct users are split into
    all users and human (non-machine) users. Output columns: ``computer_id`` plus
    the four ``proc_*`` features.
    """
    return df_silver.groupBy(F.col("computer").alias("computer_id")).agg(
        F.sum(F.when(F.lower(F.col("event_type")) == "start", 1).otherwise(0)).alias(
            "proc_start_count"
        ),
        F.countDistinct("process_name").alias("proc_distinct_process_names"),
        F.countDistinct("user_name").alias("proc_distinct_users"),
        F.countDistinct(F.when(~F.col("user_is_machine"), F.col("user_name"))).alias(
            "proc_distinct_human_users"
        ),
    )

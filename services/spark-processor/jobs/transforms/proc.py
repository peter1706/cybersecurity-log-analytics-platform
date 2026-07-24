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

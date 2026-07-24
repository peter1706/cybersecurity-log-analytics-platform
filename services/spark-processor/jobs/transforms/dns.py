"""DNS (name-resolution lookup) transforms."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def dns_bronze_to_silver(df_bronze: DataFrame) -> DataFrame:
    """Clean, type, and conform DNS Bronze rows into Silver.

    - casts ``time`` to long
    - de-duplicates identical lookups
    """
    typed = df_bronze.withColumn("time", F.col("time").cast("long"))
    keep = [
        "time",
        "day",
        "source",
        "src_comp",
        "resolved_comp",
    ]
    return typed.select(*keep).dropDuplicates()


def dns_computer_features(df_silver: DataFrame) -> DataFrame:
    """Per-computer DNS features for the unified feature table, keyed by ``computer_id``.

    The entity is the computer issuing the lookup (``src_comp``); the resolved host
    is treated as an attribute, not an active computer. Output columns:
    ``computer_id`` plus the two ``dns_*`` features.
    """
    return df_silver.groupBy(F.col("src_comp").alias("computer_id")).agg(
        F.count(F.lit(1)).alias("dns_lookup_count"),
        F.countDistinct("resolved_comp").alias("dns_distinct_resolved_hosts"),
    )

"""Flows (network flow record) transforms."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def flows_bronze_to_silver(df_bronze: DataFrame) -> DataFrame:
    """Clean, type, and conform flow Bronze rows into Silver.

    Flow records frequently arrive as exact duplicates, so identical rows are
    de-duplicated here before any downstream counting. Numeric measures are typed
    to long; computers, ports, and protocol stay as opaque tokens (strings).
    """
    typed = (
        df_bronze.withColumn("time", F.col("time").cast("long"))
        .withColumn("duration", F.col("duration").cast("long"))
        .withColumn("packet_count", F.col("packet_count").cast("long"))
        .withColumn("byte_count", F.col("byte_count").cast("long"))
    )
    keep = [
        "time",
        "day",
        "source",
        "duration",
        "src_comp",
        "src_port",
        "dst_comp",
        "dst_port",
        "protocol",
        "packet_count",
        "byte_count",
    ]
    return typed.select(*keep).dropDuplicates()

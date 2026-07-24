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

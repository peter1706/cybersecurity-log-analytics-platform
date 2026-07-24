"""Shared helpers for the per-source transforms.

Cross-source pieces live here so a source module never imports another. The
transforms are pure ``DataFrame -> DataFrame`` functions with **no I/O** (no
Delta, no S3), so they can be unit-tested against a plain local SparkSession.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..schemas import SECONDS_PER_DAY, SOURCE_COLUMNS


def _day_from_time(time_col: "F.Column") -> "F.Column":
    """Derive the LANL day index from a seconds timestamp: (time - 1) // 86400."""
    return F.floor((time_col.cast("long") - F.lit(1)) / F.lit(SECONDS_PER_DAY)).cast("int")


def _split_user(col: "F.Column", part: int) -> "F.Column":
    """Return the user (part=0) or domain (part=1) of a ``user@domain`` token."""
    return F.split(col, "@").getItem(part)


def raw_to_bronze(df_raw: DataFrame, source: str) -> DataFrame:
    """Assign a source's column names to a header-less CSV and add source/day partitions.

    Bronze keeps values verbatim (as strings). Column layouts are looked up per
    source in ``SOURCE_COLUMNS`` so a new source is a data change, not a code branch.
    """
    columns = SOURCE_COLUMNS.get(source)
    if columns is None:
        raise ValueError(f"unknown source {source!r}; known: {sorted(SOURCE_COLUMNS)}")
    renamed = df_raw
    for i, name in enumerate(columns):
        renamed = renamed.withColumnRenamed(f"_c{i}", name)
    renamed = renamed.select(*columns)
    return renamed.withColumn("source", F.lit(source)).withColumn(
        "day", _day_from_time(F.col("time"))
    )

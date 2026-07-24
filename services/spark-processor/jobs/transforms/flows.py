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


def _is_anonymized_port(port_col: "F.Column") -> "F.Column":
    """True when a port is an anonymized token rather than a well-known number.

    LANL replaces non-standard ports with anonymized tokens (e.g. ``N1``), while
    well-known ports stay numeric (e.g. ``443``). A port counts as anonymized when
    its value is not purely numeric.
    """
    return ~port_col.rlike(r"^[0-9]+$")


def flows_computer_features(df_silver: DataFrame) -> DataFrame:
    """Per-computer network-flow features for the unified feature table, keyed by ``computer_id``.

    Flows are directional: the source computer plays the *outbound* role and the
    destination the *inbound* role. Silver already de-duplicated identical flows,
    so every count/sum here is on the deduplicated basis. Anonymized-port counts
    use the role-relevant port (outbound -> ``src_port``, inbound -> ``dst_port``).

    Counts/sums may be NULL for a computer seen in only one role; the assembler
    coalesces those to 0. Output columns: ``computer_id`` plus the ten
    ``flows_{out,in}_*`` features.
    """
    outbound = df_silver.groupBy(F.col("src_comp").alias("computer_id")).agg(
        F.count(F.lit(1)).alias("flows_out_count_distinct"),
        F.countDistinct("dst_comp").alias("flows_out_distinct_targets"),
        F.sum("byte_count").alias("flows_out_bytes_sum_distinct"),
        F.sum("packet_count").alias("flows_out_packets_sum_distinct"),
        F.sum(F.when(_is_anonymized_port(F.col("src_port")), 1).otherwise(0)).alias(
            "flows_out_anonymized_port_count_distinct"
        ),
    )
    inbound = df_silver.groupBy(F.col("dst_comp").alias("computer_id")).agg(
        F.count(F.lit(1)).alias("flows_in_count_distinct"),
        F.countDistinct("src_comp").alias("flows_in_distinct_sources"),
        F.sum("byte_count").alias("flows_in_bytes_sum_distinct"),
        F.sum("packet_count").alias("flows_in_packets_sum_distinct"),
        F.sum(F.when(_is_anonymized_port(F.col("dst_port")), 1).otherwise(0)).alias(
            "flows_in_anonymized_port_count_distinct"
        ),
    )
    return outbound.join(inbound, on="computer_id", how="fullouter")

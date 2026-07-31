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


def _flows_role_features(
    df_silver: DataFrame,
    *,
    own_comp: str,
    other_comp: str,
    port_col: str,
    prefix: str,
    other_count_label: str,
) -> DataFrame:
    """Per-computer flow aggregates for one directional role (outbound or inbound).

    ``flows_computer_features`` calls this once per role with the source/destination
    columns and role-relevant port swapped, so the two aggregations can't drift apart.
    """
    return df_silver.groupBy(F.col(own_comp).alias("computer_id")).agg(
        F.count(F.lit(1)).alias(f"{prefix}_count_distinct"),
        F.countDistinct(other_comp).alias(f"{prefix}_distinct_{other_count_label}"),
        F.sum("byte_count").alias(f"{prefix}_bytes_sum_distinct"),
        F.sum("packet_count").alias(f"{prefix}_packets_sum_distinct"),
        F.sum(F.when(_is_anonymized_port(F.col(port_col)), 1).otherwise(0)).alias(
            f"{prefix}_anonymized_port_count_distinct"
        ),
    )


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
    outbound = _flows_role_features(
        df_silver,
        own_comp="src_comp",
        other_comp="dst_comp",
        port_col="src_port",
        prefix="flows_out",
        other_count_label="targets",
    )
    inbound = _flows_role_features(
        df_silver,
        own_comp="dst_comp",
        other_comp="src_comp",
        port_col="dst_port",
        prefix="flows_in",
        other_count_label="sources",
    )
    return outbound.join(inbound, on="computer_id", how="fullouter")

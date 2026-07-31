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


def _auth_role_features(
    df_silver: DataFrame,
    *,
    own_comp: str,
    other_comp: str,
    user_name_col: str,
    user_is_machine_col: str,
    prefix: str,
    other_count_label: str,
) -> DataFrame:
    """Per-computer auth aggregates for one directional role (outbound or inbound).

    ``auth_computer_features`` calls this once per role with the source/destination
    columns swapped, so the outbound and inbound aggregations can't drift apart.
    """
    known_count_col = f"_{prefix}_known_count"
    return (
        df_silver.groupBy(F.col(own_comp).alias("computer_id"))
        .agg(
            F.count(F.lit(1)).alias(f"{prefix}_event_count"),
            F.countDistinct(other_comp).alias(f"{prefix}_distinct_{other_count_label}"),
            F.countDistinct(user_name_col).alias(f"{prefix}_distinct_users"),
            F.countDistinct(F.when(~F.col(user_is_machine_col), F.col(user_name_col))).alias(
                f"{prefix}_distinct_human_users"
            ),
            F.sum(F.when(F.col("auth_success") == F.lit(False), 1).otherwise(0)).alias(
                f"{prefix}_failed_count"
            ),
            F.sum(F.when(F.col("auth_success").isNotNull(), 1).otherwise(0)).alias(known_count_col),
        )
        .withColumn(
            f"{prefix}_failure_rate",
            F.when(
                F.col(known_count_col) > 0,
                F.col(f"{prefix}_failed_count") / F.col(known_count_col),
            ),
        )
        .drop(known_count_col)
    )


def auth_computer_features(df_silver: DataFrame) -> DataFrame:
    """Per-computer auth features for the unified feature table, keyed by ``computer_id``.

    Auth events are directional: the source computer plays the *outbound* role and
    the destination computer the *inbound* role, so a computer accrues separate
    ``auth_out_*`` and ``auth_in_*`` features. Failure rates are taken only over
    events with a **known** outcome (``auth_success`` not null) and are left NULL
    when there are none (undefined) -- distinct from a 0.0 rate.

    Counts/distincts may be NULL here for a computer seen in only one role; the
    cross-source assembler coalesces those to 0. Output columns:
    ``computer_id`` plus the twelve ``auth_{out,in}_*`` features.
    """
    outbound = _auth_role_features(
        df_silver,
        own_comp="src_comp",
        other_comp="dst_comp",
        user_name_col="src_user_name",
        user_is_machine_col="src_user_is_machine",
        prefix="auth_out",
        other_count_label="targets",
    )
    inbound = _auth_role_features(
        df_silver,
        own_comp="dst_comp",
        other_comp="src_comp",
        user_name_col="dst_user_name",
        user_is_machine_col="dst_user_is_machine",
        prefix="auth_in",
        other_count_label="sources",
    )
    return outbound.join(inbound, on="computer_id", how="fullouter")

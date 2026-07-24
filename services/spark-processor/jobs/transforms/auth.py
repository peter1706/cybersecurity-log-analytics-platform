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


def auth_computer_features(df_silver: DataFrame) -> DataFrame:
    """Per-computer auth features for the unified feature table, keyed by ``computer_id``.

    Auth events are directional: the source computer plays the *outbound* role and
    the destination computer the *inbound* role, so a computer accrues separate
    ``auth_out_*`` and ``auth_in_*`` features. Failure rates are taken only over
    events with a **known** outcome (``auth_success`` not null) and are left NULL
    when there are none (undefined, per ML-NULL-2) -- distinct from a 0.0 rate.

    Counts/distincts may be NULL here for a computer seen in only one role; the
    cross-source assembler coalesces those to 0. Output columns:
    ``computer_id`` plus the twelve ``auth_{out,in}_*`` features.
    """
    outbound = (
        df_silver.groupBy(F.col("src_comp").alias("computer_id"))
        .agg(
            F.count(F.lit(1)).alias("auth_out_event_count"),
            F.countDistinct("dst_comp").alias("auth_out_distinct_targets"),
            F.countDistinct("src_user_name").alias("auth_out_distinct_users"),
            F.countDistinct(F.when(~F.col("src_user_is_machine"), F.col("src_user_name"))).alias(
                "auth_out_distinct_human_users"
            ),
            F.sum(F.when(F.col("auth_success") == F.lit(False), 1).otherwise(0)).alias(
                "auth_out_failed_count"
            ),
            F.sum(F.when(F.col("auth_success").isNotNull(), 1).otherwise(0)).alias(
                "_auth_out_known_count"
            ),
        )
        .withColumn(
            "auth_out_failure_rate",
            F.when(
                F.col("_auth_out_known_count") > 0,
                F.col("auth_out_failed_count") / F.col("_auth_out_known_count"),
            ),
        )
        .drop("_auth_out_known_count")
    )
    inbound = (
        df_silver.groupBy(F.col("dst_comp").alias("computer_id"))
        .agg(
            F.count(F.lit(1)).alias("auth_in_event_count"),
            F.countDistinct("src_comp").alias("auth_in_distinct_sources"),
            F.countDistinct("dst_user_name").alias("auth_in_distinct_users"),
            F.countDistinct(F.when(~F.col("dst_user_is_machine"), F.col("dst_user_name"))).alias(
                "auth_in_distinct_human_users"
            ),
            F.sum(F.when(F.col("auth_success") == F.lit(False), 1).otherwise(0)).alias(
                "auth_in_failed_count"
            ),
            F.sum(F.when(F.col("auth_success").isNotNull(), 1).otherwise(0)).alias(
                "_auth_in_known_count"
            ),
        )
        .withColumn(
            "auth_in_failure_rate",
            F.when(
                F.col("_auth_in_known_count") > 0,
                F.col("auth_in_failed_count") / F.col("_auth_in_known_count"),
            ),
        )
        .drop("_auth_in_known_count")
    )
    return outbound.join(inbound, on="computer_id", how="fullouter")

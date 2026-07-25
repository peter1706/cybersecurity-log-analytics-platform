"""Cross-source assembly of the unified per-computer Gold feature table.

Source-agnostic: it stitches the per-source ``*_computer_features`` builders into
one row per computer for one anchor day / window length, matching the data-science
feature contract. Column names, order, and null semantics are binding, so they
live here as constants.
"""

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Per-source feature columns, in contract order.
AUTH_FEATURE_COLUMNS = [
    "auth_out_event_count",
    "auth_in_event_count",
    "auth_out_distinct_targets",
    "auth_in_distinct_sources",
    "auth_out_distinct_users",
    "auth_in_distinct_users",
    "auth_out_distinct_human_users",
    "auth_in_distinct_human_users",
    "auth_out_failed_count",
    "auth_in_failed_count",
    "auth_out_failure_rate",
    "auth_in_failure_rate",
]
PROC_FEATURE_COLUMNS = [
    "proc_start_count",
    "proc_distinct_process_names",
    "proc_distinct_users",
    "proc_distinct_human_users",
]
FLOWS_FEATURE_COLUMNS = [
    "flows_out_count_distinct",
    "flows_in_count_distinct",
    "flows_out_distinct_targets",
    "flows_in_distinct_sources",
    "flows_out_bytes_sum_distinct",
    "flows_in_bytes_sum_distinct",
    "flows_out_packets_sum_distinct",
    "flows_in_packets_sum_distinct",
    "flows_out_anonymized_port_count_distinct",
    "flows_in_anonymized_port_count_distinct",
]
DNS_FEATURE_COLUMNS = [
    "dns_lookup_count",
    "dns_distinct_resolved_hosts",
]

# Ratios stay NULL when undefined; every other feature is a counter/sum coalesced
# to 0 when a computer is absent from that source.
RATE_COLUMNS = ["auth_out_failure_rate", "auth_in_failure_rate"]

ID_WINDOW_COLUMNS = ["computer_id", "anchor_day", "window_days"]
SOURCE_PRESENT_COLUMNS = [
    "source_present_auth",
    "source_present_proc",
    "source_present_flows",
    "source_present_dns",
]

_FEATURE_COLUMNS = (
    AUTH_FEATURE_COLUMNS + PROC_FEATURE_COLUMNS + FLOWS_FEATURE_COLUMNS + DNS_FEATURE_COLUMNS
)
COUNT_SUM_COLUMNS = [c for c in _FEATURE_COLUMNS if c not in RATE_COLUMNS]

# The binding output schema, in order.
COMPUTER_FEATURE_COLUMNS = ID_WINDOW_COLUMNS + _FEATURE_COLUMNS + SOURCE_PRESENT_COLUMNS


def assemble_computer_features(
    *,
    auth: DataFrame,
    proc: DataFrame,
    flows: DataFrame,
    dns: DataFrame,
    anchor_day: int,
    window_days: int,
    source_present: Mapping[str, bool],
) -> DataFrame:
    """Join the four per-source feature frames into the unified per-computer table.

    Every computer active in the window in any source (any role) gets exactly one
    row; computers absent from a source get 0 for that source's counters/sums while
    undefined ratios stay NULL. The row is stamped with ``anchor_day`` and
    ``window_days`` (part of the primary key) and the four ``source_present_*``
    availability flags.
    """
    joined = (
        auth.join(proc, on="computer_id", how="fullouter")
        .join(flows, on="computer_id", how="fullouter")
        .join(dns, on="computer_id", how="fullouter")
    )
    filled = joined.fillna(0, subset=COUNT_SUM_COLUMNS)
    stamped = filled.withColumn("anchor_day", F.lit(anchor_day)).withColumn(
        "window_days", F.lit(window_days)
    )
    for source in ("auth", "proc", "flows", "dns"):
        stamped = stamped.withColumn(
            f"source_present_{source}", F.lit(bool(source_present[source]))
        )
    return stamped.select(*COMPUTER_FEATURE_COLUMNS)

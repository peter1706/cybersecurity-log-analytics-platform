"""The data-science feature contract, from the *consumer's* point of view.

ml-mock independently encodes the binding schema it expects and rejects any
delivery that deviates. Keeping this list here — rather than importing it from the
producer — is deliberate: the consumer owns its own contract. A unit test asserts
this list equals the producer's ``COMPUTER_FEATURE_COLUMNS`` and delivery's
``DELIVERED_COLUMNS`` so the three copies never drift apart.
"""

from __future__ import annotations

EXPECTED_COLUMNS = [
    # identification and window
    "computer_id",
    "anchor_day",
    "window_days",
    # auth
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
    # proc
    "proc_start_count",
    "proc_distinct_process_names",
    "proc_distinct_users",
    "proc_distinct_human_users",
    # flows
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
    # dns
    "dns_lookup_count",
    "dns_distinct_resolved_hosts",
    # quality/metadata
    "source_present_auth",
    "source_present_proc",
    "source_present_flows",
    "source_present_dns",
]


class SchemaContractError(ValueError):
    """Raised when a delivery's schema does not match the contract exactly."""


def validate_schema(actual_columns: list[str]) -> None:
    """Reject a delivery whose columns deviate from the contract.

    The match is exact: same names, same order. Reporting missing/extra/reordered
    separately makes a rejected delivery diagnosable.
    """
    if actual_columns == EXPECTED_COLUMNS:
        return

    expected_set, actual_set = set(EXPECTED_COLUMNS), set(actual_columns)
    missing = [c for c in EXPECTED_COLUMNS if c not in actual_set]
    extra = [c for c in actual_columns if c not in expected_set]
    details = []
    if missing:
        details.append(f"missing={missing}")
    if extra:
        details.append(f"unexpected={extra}")
    if not details:  # same set, different order
        details.append("columns present but in the wrong order")
    raise SchemaContractError("delivered schema does not match contract: " + "; ".join(details))

"""Plain-language labels for the consumer-facing dashboard.

Maps technical feature-contract column names to short phrases a non-expert
reader can understand. Used only for presentation — never for schema validation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd

import anomaly

FEATURE_LABELS: dict[str, str] = {
    "computer_id": "Computer",
    "anchor_day": "As-of day",
    "window_days": "Days covered",
    "auth_out_event_count": "Outgoing sign-ins",
    "auth_in_event_count": "Incoming sign-ins",
    "auth_out_distinct_targets": "Different systems contacted for sign-in",
    "auth_in_distinct_sources": "Different systems signing in here",
    "auth_out_distinct_users": "Different accounts signing out",
    "auth_in_distinct_users": "Different accounts signing in",
    "auth_out_distinct_human_users": "Different people signing out",
    "auth_in_distinct_human_users": "Different people signing in",
    "auth_out_failed_count": "Failed outgoing sign-ins",
    "auth_in_failed_count": "Failed incoming sign-ins",
    "auth_out_failure_rate": "Outgoing sign-in failure rate",
    "auth_in_failure_rate": "Incoming sign-in failure rate",
    "proc_start_count": "Programs started",
    "proc_distinct_process_names": "Different programs used",
    "proc_distinct_users": "Different accounts running programs",
    "proc_distinct_human_users": "Different people running programs",
    "flows_out_count_distinct": "Outgoing network connections",
    "flows_in_count_distinct": "Incoming network connections",
    "flows_out_distinct_targets": "Different destinations contacted",
    "flows_in_distinct_sources": "Different sources connecting in",
    "flows_out_bytes_sum_distinct": "Data sent",
    "flows_in_bytes_sum_distinct": "Data received",
    "flows_out_packets_sum_distinct": "Network packets sent",
    "flows_in_packets_sum_distinct": "Network packets received",
    "flows_out_anonymized_port_count_distinct": "Different outgoing network ports",
    "flows_in_anonymized_port_count_distinct": "Different incoming network ports",
    "dns_lookup_count": "Website lookups",
    "dns_distinct_resolved_hosts": "Different websites looked up",
    "source_present_auth": "Authentication events",
    "source_present_proc": "Process starts",
    "source_present_flows": "Network traffic",
    "source_present_dns": "DNS lookups",
    anomaly.SCORE_COLUMN: "Unusual activity score",
}

SOURCE_LABELS: dict[str, str] = {
    "auth": "Authentication events",
    "proc": "Process starts",
    "flows": "Network traffic",
    "dns": "DNS lookups",
}

# Percentile risk buckets within one delivery (not absolute z cut-offs).
HIGH_RISK_FRACTION = 0.01
ATTENTION_FRACTION = 0.05


def label_for(column: str) -> str:
    """Return a plain-language label, falling back to a softened column name."""
    if column in FEATURE_LABELS:
        return FEATURE_LABELS[column]
    return column.replace("_", " ").strip()


def assign_risk_levels(scores: pd.Series) -> pd.Series:
    """Label High / Medium / Normal by rank within this delivery.

    High = top 1% (at least one when n >= 1); Medium = remainder of top 5%.
    Ties use stable ``rank(method="first", ascending=False)``.
    """
    if scores.empty:
        return pd.Series(dtype="object")
    values = pd.to_numeric(scores, errors="coerce").fillna(0.0)
    n = len(values)
    high_n = max(1, int(n * HIGH_RISK_FRACTION))
    attention_n = max(high_n, int(n * ATTENTION_FRACTION))
    medium_n = max(0, attention_n - high_n)
    ranks = values.rank(method="first", ascending=False)
    out = pd.Series("Normal", index=values.index, dtype="object")
    out = out.mask(ranks <= high_n, "High")
    out = out.mask((ranks > high_n) & (ranks <= high_n + medium_n), "Medium")
    return out


def needs_attention(scored: pd.DataFrame) -> int:
    """Count computers in the High or Medium percentile buckets."""
    if scored.empty or anomaly.SCORE_COLUMN not in scored.columns:
        return 0
    risk = assign_risk_levels(scored[anomaly.SCORE_COLUMN])
    return int(risk.isin(("High", "Medium")).sum())


def data_quality_status(manifest: Mapping, expected_columns: Sequence[str]) -> str:
    """Return consumer-facing data-quality status for a delivery."""
    if manifest.get("columns") == list(expected_columns):
        return "✓ Ready"
    return "✗ Problem"


def source_health_flags(
    df: pd.DataFrame,
    sources: Sequence[str] = ("auth", "proc", "flows", "dns"),
) -> list[tuple[str, bool]]:
    """Return ``(friendly name, delivered)`` pairs for each expected source."""
    flags: list[tuple[str, bool]] = []
    for source in sources:
        column = f"source_present_{source}"
        ok = column in df.columns and len(df) > 0 and bool(df[column].all())
        flags.append((SOURCE_LABELS.get(source, source), ok))
    return flags


def source_health_lines(
    df: pd.DataFrame,
    sources: Sequence[str] = ("auth", "proc", "flows", "dns"),
) -> list[str]:
    """Return ✓/✗ lines with friendly source names."""
    return [f"{'✓' if ok else '✗'} {name}" for name, ok in source_health_flags(df, sources)]


def delivery_choice_label(manifest: Mapping) -> str:
    """Build a compact label identifying one delivery.

    Includes the as-of day because several deliveries can share a creation
    date and window length, which would otherwise render identically.
    """
    window = int(manifest["window_days"])
    anchor = int(manifest["anchor_day"])
    records = int(manifest["record_count"])
    created = str(manifest.get("created_at") or "")
    date_part = f"{created[:10]} · " if len(created) >= 10 else ""
    return f"{date_part}day {anchor} · {window}-day view · {records:,} computers"


def standout_sentences(
    result: anomaly.AnomalyResult,
    *,
    n: int = 5,
) -> list[str]:
    """Build plain-language sentences for the top unusual computers.

    Each sentence uses feature labels, never raw column names. Only computers
    in the High/Medium percentile buckets are included.
    """
    if result.scored.empty:
        return []
    risk = assign_risk_levels(result.scored[anomaly.SCORE_COLUMN])
    attention_ids = set(result.scored.loc[risk.isin(("High", "Medium")), "computer_id"].astype(str))
    top = anomaly.top_computers(result.scored, n=n)
    if top.empty:
        return []

    sentences: list[str] = []
    for _, row in top.iterrows():
        computer = str(row["computer_id"])
        if computer not in attention_ids:
            continue
        level = str(risk.loc[result.scored["computer_id"].astype(str) == computer].iloc[0])
        contrib = result.contributions[result.contributions["computer_id"] == computer]
        if contrib.empty:
            reason = "activity that looks unusual compared with the rest of the group"
        else:
            top_feature = str(contrib.sort_values("rank").iloc[0]["feature"])
            reason = label_for(top_feature).lower()
        sentences.append(f"**{computer}** — {level} risk: unusually high {reason}.")
    return sentences

"""Aggregations over a delivered ``computer_features`` partition.

Every metric here is derived directly from binding feature-contract columns so
the dashboard never shows a number that is unlinked from delivered data.
Missing columns contribute nothing rather than raising, because a partition may
legitimately lack a source.
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

SIGN_IN_COLUMNS = ("auth_out_event_count", "auth_in_event_count")
SIGN_IN_FAILED_COLUMNS = ("auth_out_failed_count", "auth_in_failed_count")
PROGRAM_COLUMNS = ("proc_start_count",)
CONNECTION_COLUMNS = ("flows_out_count_distinct", "flows_in_count_distinct")
BYTES_COLUMNS = ("flows_out_bytes_sum_distinct", "flows_in_bytes_sum_distinct")
WEB_COLUMNS = ("dns_lookup_count",)


def column_sum(df: pd.DataFrame, columns: Sequence[str]) -> int:
    """Sum the given columns across all rows, ignoring absent columns."""
    total = 0
    for column in columns:
        if column in df.columns:
            total += int(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())
    return total


def row_total(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    """Row-wise sum of the given columns, ignoring absent columns."""
    present = [c for c in columns if c in df.columns]
    if not present:
        return pd.Series([0] * len(df), index=df.index, dtype="int64")
    frame = df[present].apply(pd.to_numeric, errors="coerce").fillna(0)
    return frame.sum(axis=1)


def activity_totals(df: pd.DataFrame) -> dict[str, int]:
    """Return headline activity counters for one delivered partition."""
    return {
        "computers": int(len(df)),
        "sign_ins": column_sum(df, SIGN_IN_COLUMNS),
        "failed_sign_ins": column_sum(df, SIGN_IN_FAILED_COLUMNS),
        "program_starts": column_sum(df, PROGRAM_COLUMNS),
        "network_connections": column_sum(df, CONNECTION_COLUMNS),
        "network_bytes": column_sum(df, BYTES_COLUMNS),
        "web_lookups": column_sum(df, WEB_COLUMNS),
    }


def sign_in_success_rate(df: pd.DataFrame) -> float | None:
    """Share of sign-ins that succeeded, or ``None`` when there were none."""
    total = column_sum(df, SIGN_IN_COLUMNS)
    if total <= 0:
        return None
    failed = column_sum(df, SIGN_IN_FAILED_COLUMNS)
    return max(0.0, min(1.0, 1.0 - failed / total))


def activity_mix(df: pd.DataFrame) -> list[dict[str, object]]:
    """Comparable totals for the four delivered activity families."""
    totals = activity_totals(df)
    return [
        {"activity": "Sign-ins", "events": totals["sign_ins"]},
        {"activity": "Programs", "events": totals["program_starts"]},
        {"activity": "Network", "events": totals["network_connections"]},
        {"activity": "Web lookups", "events": totals["web_lookups"]},
    ]


def top_computers_by(
    df: pd.DataFrame,
    columns: Sequence[str],
    *,
    n: int = 8,
    value_name: str = "value",
) -> pd.DataFrame:
    """Rank computers by the combined total of ``columns`` (descending)."""
    if df.empty or "computer_id" not in df.columns:
        return pd.DataFrame(columns=["computer_id", value_name])
    ranked = pd.DataFrame(
        {
            "computer_id": df["computer_id"].astype(str),
            value_name: row_total(df, columns),
        }
    )
    ranked = ranked[ranked[value_name] > 0]
    if ranked.empty:
        return pd.DataFrame(columns=["computer_id", value_name])
    return ranked.nlargest(n, value_name).reset_index(drop=True)


def format_count(value: float | int | None) -> str:
    """Abbreviate a count for a key-number card (1234567 -> ``1.23M``)."""
    if value is None:
        return "—"
    number = float(value)
    for limit, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(number) >= limit:
            return f"{number / limit:.2f}{suffix}"
    return f"{int(number):,}"


def format_bytes(value: float | int | None) -> str:
    """Abbreviate a byte total (778000000 -> ``742 MB``)."""
    if value is None:
        return "—"
    number = float(value)
    for limit, suffix in (
        (1024**4, "TB"),
        (1024**3, "GB"),
        (1024**2, "MB"),
        (1024, "KB"),
    ):
        if abs(number) >= limit:
            return f"{number / limit:,.0f} {suffix}"
    return f"{int(number):,} B"


def format_percent(value: float | None) -> str:
    """Render a 0..1 ratio as a percentage, or ``—`` when undefined."""
    if value is None:
        return "—"
    return f"{value * 100:.1f}%"

"""Heuristic unusual-computer scoring for the delivered-features dashboard.

Scores are a local mock for visualization only — not the data-science team's
anomaly model, and they are never written back to the delivered store.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

KEY_COLUMNS = ("computer_id", "anchor_day", "window_days")
SCORE_COLUMN = "anomaly_score"


@dataclass(frozen=True, slots=True)
class AnomalyResult:
    """Scored feature frame plus per-computer top contributing features."""

    scored: pd.DataFrame
    contributions: pd.DataFrame


def _feature_columns(df: pd.DataFrame) -> list[str]:
    """Return numeric behavior columns (exclude keys and source_present flags)."""
    skip = set(KEY_COLUMNS) | {c for c in df.columns if c.startswith("source_present_")}
    cols: list[str] = []
    for name in df.columns:
        if name in skip or name == SCORE_COLUMN:
            continue
        if pd.api.types.is_numeric_dtype(df[name]):
            cols.append(name)
    return cols


def score_computers(df: pd.DataFrame, *, top_features: int = 5) -> AnomalyResult:
    """Attach a robust z-score anomaly score and top feature contributions.

    For each numeric feature, compute a median/MAD robust z-score across
    computers in the partition. The composite ``anomaly_score`` is the mean of
    absolute z-scores. Contributions list the highest-|z| features per computer.

    Args:
        df: Delivered computer_features rows for one partition.
        top_features: How many contributing features to keep per computer.

    Returns:
        An :class:`AnomalyResult` with ``scored`` (original columns + score) and
        ``contributions`` (``computer_id``, ``feature``, ``abs_z``, ``rank``).

    Raises:
        ValueError: If ``computer_id`` is missing or there are no numeric features.
    """
    if "computer_id" not in df.columns:
        raise ValueError("score_computers requires a computer_id column")
    if df.empty:
        empty_contrib = pd.DataFrame(columns=["computer_id", "feature", "abs_z", "rank"])
        return AnomalyResult(scored=df.copy(), contributions=empty_contrib)

    feature_cols = _feature_columns(df)
    if not feature_cols:
        raise ValueError("score_computers found no numeric feature columns")

    z_parts: dict[str, pd.Series] = {}
    for col in feature_cols:
        series = pd.to_numeric(df[col], errors="coerce")
        median = float(series.median(skipna=True))
        mad = float((series - median).abs().median(skipna=True))
        scale = 1.4826 * mad if mad > 0 else float(series.std(skipna=True) or 1.0)
        if scale == 0:
            scale = 1.0
        z_parts[col] = (series - median) / scale

    z_frame = pd.DataFrame(z_parts, index=df.index)
    scored = df.copy()
    scored[SCORE_COLUMN] = z_frame.abs().mean(axis=1).fillna(0.0)

    long = (
        z_frame.abs()
        .assign(computer_id=df["computer_id"].values)
        .melt(id_vars="computer_id", var_name="feature", value_name="abs_z")
    )
    long["rank"] = long.groupby("computer_id")["abs_z"].rank(method="first", ascending=False)
    contributions = (
        long.loc[long["rank"] <= top_features]
        .sort_values(["computer_id", "rank"])
        .reset_index(drop=True)
    )
    return AnomalyResult(scored=scored, contributions=contributions)


def top_computers(scored: pd.DataFrame, *, n: int = 15) -> pd.DataFrame:
    """Return the ``n`` highest-scoring computers (descending)."""
    if SCORE_COLUMN not in scored.columns:
        raise ValueError(f"scored frame missing {SCORE_COLUMN}")
    cols = [c for c in ("computer_id", SCORE_COLUMN) if c in scored.columns]
    return scored.nlargest(n, SCORE_COLUMN)[cols].reset_index(drop=True)

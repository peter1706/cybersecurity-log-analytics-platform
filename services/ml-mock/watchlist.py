"""Triage KPIs and ranked unusual-computer watchlist helpers (Streamlit-free)."""

from __future__ import annotations

import pandas as pd

import anomaly
import feature_labels

WATCHLIST_CAP = 50


def default_selected_computer(scored: pd.DataFrame) -> str | None:
    """Return the computer_id with the highest anomaly score, if any."""
    if scored.empty or anomaly.SCORE_COLUMN not in scored.columns:
        return None
    top = scored.loc[scored[anomaly.SCORE_COLUMN].idxmax()]
    return str(top["computer_id"])


def driver_rows(
    contributions: pd.DataFrame,
    computer_id: str,
    *,
    n: int = 5,
) -> list[dict[str, object]]:
    """Top-n contribution rows for one computer with plain-language labels."""
    if contributions.empty:
        return []
    subset = contributions[contributions["computer_id"].astype(str) == str(computer_id)]
    if subset.empty:
        return []
    ordered = subset.sort_values("rank").head(n)
    return [
        {
            "feature": str(row["feature"]),
            "label": feature_labels.label_for(str(row["feature"])),
            "abs_z": float(row["abs_z"]),
        }
        for _, row in ordered.iterrows()
    ]


def _with_risk(scored: pd.DataFrame) -> pd.DataFrame:
    out = scored.copy()
    out["risk"] = feature_labels.assign_risk_levels(out[anomaly.SCORE_COLUMN])
    return out


def _top_theme(
    attention: pd.DataFrame,
    contributions: pd.DataFrame,
) -> tuple[str | None, int]:
    if attention.empty or contributions.empty:
        return None, 0
    ids = set(attention["computer_id"].astype(str))
    rank1 = contributions[
        (contributions["computer_id"].astype(str).isin(ids))
        & (pd.to_numeric(contributions["rank"], errors="coerce") <= 1)
    ]
    if rank1.empty:
        return None, 0
    counts = rank1["feature"].astype(str).value_counts()
    feature = str(counts.index[0])
    return feature_labels.label_for(feature), int(counts.iloc[0])


def triage_kpis(scored: pd.DataFrame, contributions: pd.DataFrame) -> dict:
    """Aggregate triage headline numbers for one scored partition."""
    empty: dict = {
        "computers": 0,
        "attention": 0,
        "high": 0,
        "medium": 0,
        "highest_id": None,
        "highest_score": None,
        "highest_risk": None,
        "theme_label": None,
        "theme_count": 0,
    }
    if scored.empty or anomaly.SCORE_COLUMN not in scored.columns:
        return empty
    framed = _with_risk(scored)
    attention = framed[framed["risk"].isin(("High", "Medium"))]
    high = int((framed["risk"] == "High").sum())
    medium = int((framed["risk"] == "Medium").sum())
    top = framed.loc[framed[anomaly.SCORE_COLUMN].idxmax()]
    theme_label, theme_count = _top_theme(attention, contributions)
    return {
        "computers": len(framed),
        "attention": high + medium,
        "high": high,
        "medium": medium,
        "highest_id": str(top["computer_id"]),
        "highest_score": float(top[anomaly.SCORE_COLUMN]),
        "highest_risk": str(top["risk"]),
        "theme_label": theme_label,
        "theme_count": theme_count,
    }


def _filter_attention(
    scored: pd.DataFrame,
    *,
    risk_filter: str,
    search: str,
) -> pd.DataFrame:
    framed = _with_risk(scored)
    framed = framed[framed["risk"].isin(("High", "Medium"))]
    if risk_filter == "high":
        framed = framed[framed["risk"] == "High"]
    elif risk_filter == "medium":
        framed = framed[framed["risk"] == "Medium"]
    needle = search.strip().lower()
    if needle:
        framed = framed[
            framed["computer_id"]
            .astype(str)
            .str.lower()
            .str.contains(needle, regex=False)
        ]
    return framed.sort_values(anomaly.SCORE_COLUMN, ascending=False)


def watchlist_filtered_total(
    scored: pd.DataFrame,
    *,
    risk_filter: str = "all",
    search: str = "",
) -> int:
    """Count attention rows after filter/search (before the display cap)."""
    if scored.empty or anomaly.SCORE_COLUMN not in scored.columns:
        return 0
    return len(_filter_attention(scored, risk_filter=risk_filter, search=search))


def watchlist_rows(
    scored: pd.DataFrame,
    contributions: pd.DataFrame,
    *,
    risk_filter: str = "all",
    search: str = "",
    cap: int = WATCHLIST_CAP,
) -> list[dict]:
    """Build ranked watchlist rows (filter/search first, then cap)."""
    if scored.empty or anomaly.SCORE_COLUMN not in scored.columns:
        return []
    framed = _filter_attention(scored, risk_filter=risk_filter, search=search)
    reasons: dict[str, str] = {}
    if not contributions.empty:
        rank1 = contributions[
            pd.to_numeric(contributions["rank"], errors="coerce") <= 1
        ].copy()
        rank1 = rank1.sort_values("rank").drop_duplicates("computer_id", keep="first")
        for _, row in rank1.iterrows():
            reasons[str(row["computer_id"])] = feature_labels.label_for(str(row["feature"]))
    rows: list[dict] = []
    for offset, (_, row) in enumerate(framed.head(cap).iterrows(), start=1):
        cid = str(row["computer_id"])
        rows.append(
            {
                "rank": offset,
                "computer_id": cid,
                "reason": reasons.get(cid, "Unusual activity"),
                "risk": str(row["risk"]),
                "score": float(row[anomaly.SCORE_COLUMN]),
            }
        )
    return rows


def watchlist_caption(
    filtered_total: int,
    shown: int,
    *,
    cap: int = WATCHLIST_CAP,
) -> str | None:
    """Return a cap caption when the filtered set exceeds ``cap``."""
    if filtered_total > cap:
        return f"Showing top {shown} of {filtered_total}"
    return None


def _attention_rank1(scored: pd.DataFrame, contributions: pd.DataFrame) -> pd.DataFrame:
    """Rank-1 contribution rows restricted to attention computers."""
    if scored.empty or contributions.empty or anomaly.SCORE_COLUMN not in scored.columns:
        return pd.DataFrame(columns=["computer_id", "feature"])
    framed = _with_risk(scored)
    ids = set(framed.loc[framed["risk"].isin(("High", "Medium")), "computer_id"].astype(str))
    rank1 = contributions[
        (contributions["computer_id"].astype(str).isin(ids))
        & (pd.to_numeric(contributions["rank"], errors="coerce") <= 1)
    ]
    return rank1.drop_duplicates("computer_id", keep="first")


def driver_frequency(
    scored: pd.DataFrame,
    contributions: pd.DataFrame,
    *,
    n: int = 5,
) -> list[dict]:
    """Most common #1 drivers among attention computers, as label/count rows."""
    rank1 = _attention_rank1(scored, contributions)
    if rank1.empty:
        return []
    counts = rank1["feature"].astype(str).value_counts().head(n)
    return [
        {"label": feature_labels.label_for(str(feature)), "count": int(count)}
        for feature, count in counts.items()
    ]


def _source_of(feature: str) -> str | None:
    for source in ("auth", "proc", "flows", "dns"):
        if feature.startswith(f"{source}_"):
            return source
    return None


def attention_by_family(scored: pd.DataFrame, contributions: pd.DataFrame) -> list[dict]:
    """Count attention computers per activity family of their #1 driver."""
    rank1 = _attention_rank1(scored, contributions)
    if rank1.empty:
        return []
    sources = rank1["feature"].astype(str).map(_source_of).dropna()
    if sources.empty:
        return []
    counts = sources.value_counts()
    return [
        {"label": feature_labels.SOURCE_LABELS.get(str(src), str(src)), "count": int(count)}
        for src, count in counts.items()
    ]

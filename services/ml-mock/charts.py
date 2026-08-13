"""Altair chart builders for the consumer dashboard.

Pure ``DataFrame -> alt.Chart`` functions styled for the dark glass theme, so
the chart definitions can be built and asserted on without Streamlit.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

import theme

_AXIS = {
    "labelColor": theme.TEXT_COLOR,
    "titleColor": theme.MUTED_COLOR,
    "gridColor": theme.GRID_COLOR,
    "domainColor": theme.GRID_COLOR,
    "tickColor": theme.GRID_COLOR,
    "labelFontSize": 11,
}


def _styled(chart: alt.Chart) -> alt.Chart:
    """Apply the shared dark-surface styling to a chart."""
    return (
        chart.configure_view(strokeWidth=0)
        .configure_axis(**_AXIS)
        .configure_legend(labelColor=theme.TEXT_COLOR, titleColor=theme.MUTED_COLOR)
    )


def success_donut(rate: float, *, height: int = 190) -> alt.LayerChart:
    """Donut showing the successful share of sign-ins with the rate in the hole."""
    data = pd.DataFrame(
        {
            "outcome": ["Successful", "Failed"],
            "share": [rate, max(0.0, 1.0 - rate)],
            "order": [0, 1],
        }
    )
    arc = (
        alt.Chart(data)
        .mark_arc(innerRadius=58, outerRadius=80, cornerRadius=3)
        .encode(
            theta=alt.Theta("share:Q", stack=True),
            order=alt.Order("order:Q"),
            color=alt.Color(
                "outcome:N",
                scale=alt.Scale(
                    domain=["Successful", "Failed"],
                    range=[theme.ACCENTS["green"], "#33405c"],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("outcome:N", title="Outcome"),
                alt.Tooltip("share:Q", title="Share", format=".1%"),
            ],
        )
    )
    centre = (
        alt.Chart(pd.DataFrame({"text": [f"{rate * 100:.1f}%"]}))
        .mark_text(size=26, fontWeight="bold", color="#f2f6ff")
        .encode(text="text:N")
    )
    layered = alt.layer(arc, centre).properties(height=height)
    return layered.configure_view(strokeWidth=0)


def activity_mix_bars(rows: list[dict[str, object]], *, height: int = 190) -> alt.Chart:
    """Horizontal bars comparing the four delivered activity families."""
    data = pd.DataFrame(rows)
    chart = (
        alt.Chart(data)
        .mark_bar(cornerRadiusEnd=5, height=20)
        .encode(
            x=alt.X("events:Q", title=None, axis=alt.Axis(format="~s", grid=True)),
            y=alt.Y("activity:N", title=None, sort="-x"),
            color=alt.Color(
                "activity:N",
                scale=alt.Scale(range=list(theme.CHART_PALETTE)),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("activity:N", title="Activity"),
                alt.Tooltip("events:Q", title="Total", format=","),
            ],
        )
        .properties(height=height)
    )
    return _styled(chart)


def direction_split(*, sent: int, received: int, height: int = 96) -> alt.Chart:
    """Stacked bar splitting network bytes into sent and received."""
    data = pd.DataFrame(
        {
            "direction": ["Sent", "Received"],
            "bytes": [sent, received],
        }
    )
    chart = (
        alt.Chart(data)
        .mark_bar(cornerRadiusEnd=5, height=18)
        .encode(
            x=alt.X("bytes:Q", title=None, axis=alt.Axis(format="~s", grid=True)),
            y=alt.Y("direction:N", title=None, sort=["Sent", "Received"]),
            color=alt.Color(
                "direction:N",
                scale=alt.Scale(
                    domain=["Sent", "Received"],
                    range=[theme.ACCENTS["violet"], theme.ACCENTS["cyan"]],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("direction:N", title="Direction"),
                alt.Tooltip("bytes:Q", title="Bytes", format=","),
            ],
        )
        .properties(height=height)
    )
    return _styled(chart)


def ranked_bars(
    df: pd.DataFrame,
    *,
    value_column: str,
    value_title: str,
    accent: str = "amber",
    value_format: str = ",",
    height: int = 230,
) -> alt.Chart:
    """Ranked horizontal bars of computers by a combined delivered total."""
    colour = theme.ACCENTS.get(accent, theme.ACCENTS["cyan"])
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=5, color=colour, height=16)
        .encode(
            x=alt.X(f"{value_column}:Q", title=None, axis=alt.Axis(format="~s", grid=True)),
            y=alt.Y("computer_id:N", title=None, sort="-x"),
            tooltip=[
                alt.Tooltip("computer_id:N", title="Computer"),
                alt.Tooltip(f"{value_column}:Q", title=value_title, format=value_format),
            ],
        )
        .properties(height=height)
    )
    return _styled(chart)


def count_bars(
    rows: list[dict[str, object]],
    *,
    value_title: str,
    accent: str = "violet",
    height: int = 190,
) -> alt.Chart:
    """Horizontal bars of labelled counts (pattern band)."""
    data = pd.DataFrame(rows)
    chart = (
        alt.Chart(data)
        .mark_bar(cornerRadiusEnd=5, color=theme.ACCENTS[accent], height=16)
        .encode(
            x=alt.X("count:Q", title=value_title, axis=alt.Axis(grid=True, format="d")),
            y=alt.Y("label:N", title=None, sort="-x"),
            tooltip=[
                alt.Tooltip("label:N", title="Driver"),
                alt.Tooltip("count:Q", title=value_title, format="d"),
            ],
        )
        .properties(height=height)
    )
    return _styled(chart)


def driver_bars(rows: list[dict[str, object]], *, height: int = 220) -> alt.Chart:
    """Horizontal bars of absolute robust-z drivers (plain-language labels)."""
    data = pd.DataFrame(rows)
    chart = (
        alt.Chart(data)
        .mark_bar(cornerRadiusEnd=5, height=18)
        .encode(
            x=alt.X("abs_z:Q", title="How unusual", axis=alt.Axis(grid=True)),
            y=alt.Y("label:N", title=None, sort="-x"),
            color=alt.Color(
                "abs_z:Q",
                scale=alt.Scale(
                    range=[theme.ACCENTS["cyan"], theme.ACCENTS["amber"], theme.ACCENTS["rose"]]
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("label:N", title="Driver"),
                alt.Tooltip("abs_z:Q", title="|z|", format=".2f"),
            ],
        )
        .properties(height=height)
    )
    return _styled(chart)

"""Glass-mosaic visual theme for the consumer dashboard.

Holds the stylesheet and the small HTML builders for key-number cards. Kept
separate from ``dashboard.py`` so the markup can be unit tested without a
Streamlit runtime.
"""

from __future__ import annotations

from html import escape

ACCENTS: dict[str, str] = {
    "cyan": "#38e0d0",
    "amber": "#f7b955",
    "violet": "#a78bfa",
    "green": "#4ade80",
    "rose": "#fb7185",
}

CHART_PALETTE = ("#38e0d0", "#f7b955", "#a78bfa", "#4ade80")
GRID_COLOR = "#26314a"
TEXT_COLOR = "#c8d3e8"
MUTED_COLOR = "#7c8aa5"

STYLESHEET = """
<style>
.stApp {
  background:
    radial-gradient(1100px 620px at 12% -10%, rgba(56, 224, 208, 0.16), transparent 60%),
    radial-gradient(900px 560px at 88% 0%, rgba(167, 139, 250, 0.16), transparent 62%),
    linear-gradient(170deg, #0b1120 0%, #0e1526 55%, #0a0f1c 100%);
  color: #e6ecf7;
}
section.main > div { padding-top: 1.2rem; }

.cockpit-header { margin-bottom: 0.4rem; }
.cockpit-title {
  font-size: 2.05rem; font-weight: 700; letter-spacing: -0.02em;
  background: linear-gradient(92deg, #ffffff 15%, #8ee9df 70%, #a78bfa 100%);
  -webkit-background-clip: text; background-clip: text; color: transparent;
}
.cockpit-sub { color: #8394b0; font-size: 0.9rem; margin-top: 0.1rem; }

.zone-label {
  text-transform: uppercase; letter-spacing: 0.18em; font-size: 0.7rem;
  color: #7c8aa5; font-weight: 600; margin: 1.35rem 0 0.55rem 0;
  display: flex; align-items: center; gap: 0.7rem;
}
.zone-label::after {
  content: ""; flex: 1; height: 1px;
  background: linear-gradient(90deg, rgba(124, 138, 165, 0.35), transparent);
}

.glass-card {
  position: relative; overflow: hidden;
  border-radius: 18px; padding: 1.05rem 1.15rem 1.1rem 1.15rem;
  background: linear-gradient(155deg, rgba(30, 41, 66, 0.78), rgba(17, 24, 42, 0.62));
  border: 1px solid rgba(148, 172, 214, 0.14);
  box-shadow: 0 18px 40px -26px rgba(0, 0, 0, 0.9);
  backdrop-filter: blur(9px);
  height: 100%;
}
.glass-card::before {
  content: ""; position: absolute; inset: 0 0 auto 0; height: 2px;
  background: linear-gradient(90deg, var(--accent), transparent 78%);
}
.glass-card .card-label {
  text-transform: uppercase; letter-spacing: 0.13em; font-size: 0.66rem;
  color: #8394b0; font-weight: 600;
}
.glass-card .card-value {
  font-size: 2.1rem; font-weight: 700; line-height: 1.15; margin-top: 0.3rem;
  color: #f2f6ff; font-variant-numeric: tabular-nums;
}
.glass-card .card-note { font-size: 0.79rem; color: var(--accent); margin-top: 0.28rem; }

.health-row {
  display: flex; align-items: center; justify-content: space-between;
  padding: 0.34rem 0; font-size: 0.87rem; color: #cdd8ec;
  border-bottom: 1px dashed rgba(148, 172, 214, 0.12);
}
.health-row:last-child { border-bottom: none; }
.health-ok { color: #4ade80; font-weight: 600; }
.health-bad { color: #fb7185; font-weight: 600; }
.health-value { color: #f2f6ff; font-weight: 600; font-variant-numeric: tabular-nums; }

/* Style only the innermost bordered container holding a chart-card marker, so
   the surrounding column wrappers keep their plain layout. */
div[data-testid="stVerticalBlockBorderWrapper"]:has(.chart-card):not(
  :has(div[data-testid="stVerticalBlockBorderWrapper"] .chart-card)
) {
  position: relative; overflow: hidden;
  border-radius: 18px;
  background: linear-gradient(155deg, rgba(30, 41, 66, 0.78), rgba(17, 24, 42, 0.62));
  border: 1px solid rgba(148, 172, 214, 0.14);
  box-shadow: 0 18px 40px -26px rgba(0, 0, 0, 0.9);
  backdrop-filter: blur(9px);
  padding: 0.85rem 1rem 0.6rem 1rem;
  height: 100%;
}
div[data-testid="stVerticalBlockBorderWrapper"]:has(.chart-card):not(
  :has(div[data-testid="stVerticalBlockBorderWrapper"] .chart-card)
)::before {
  content: ""; position: absolute; inset: 0 0 auto 0; height: 2px;
  background: linear-gradient(90deg, rgba(56, 224, 208, 0.85), transparent 78%);
}
/* Let cards in one zone row share a common height. */
div[data-testid="column"]:has(.chart-card),
div[data-testid="column"]:has(.chart-card) > div { height: 100%; }
.chart-card {
  text-transform: uppercase; letter-spacing: 0.13em; font-size: 0.66rem;
  color: #8394b0; font-weight: 600; margin-bottom: 0.15rem;
}

.risk-head { display: flex; align-items: center; gap: 0.6rem; margin: 0.1rem 0 0.35rem 0; }
.risk-title { font-size: 1.05rem; font-weight: 700; color: #e6ecf7; }
.risk-pill {
  font-size: 0.68rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
  color: var(--accent); border: 1px solid color-mix(in srgb, var(--accent) 45%, transparent);
  background: color-mix(in srgb, var(--accent) 16%, transparent);
  border-radius: 999px; padding: 0.12rem 0.6rem;
}

/* The watchlist is a scrolling stack of glass rows. Each row's Streamlit
   button is stretched invisibly over its card, so a click anywhere on the card
   selects that computer without a separate visible control. */
.st-key-watchlist_cards {
  border-radius: 16px;
  border: 1px solid rgba(148, 172, 214, 0.12);
  background: rgba(12, 18, 33, 0.42);
  padding: 0.5rem 0.6rem;
}
/* Row spacing comes from each card's own margin: the overlay button sits out of
   flow, so a block gap would also open dead space inside every row. */
.st-key-watchlist_cards [data-testid="stVerticalBlock"] { gap: 0; }
[class*="st-key-wl_row_"] { position: relative; }
[class*="st-key-wl_row_"] [data-testid="stElementContainer"] { margin: 0 !important; }
/* The card is purely decorative and must never swallow the click; the button's
   own element container is stretched over the whole row on top of it. */
[class*="st-key-wl_row_"] [data-testid="stMarkdown"],
[class*="st-key-wl_row_"] .wl-row { pointer-events: none; }
[class*="st-key-wl_row_"] [data-testid="stElementContainer"]:has([data-testid="stButton"]) {
  position: absolute; inset: 0; z-index: 3;
}
[class*="st-key-wl_row_"] [data-testid="stButton"],
[class*="st-key-wl_row_"] [data-testid="stButton"] button {
  width: 100%; height: 100%; min-height: 0;
  pointer-events: auto;
}
[class*="st-key-wl_row_"] [data-testid="stButton"] button {
  opacity: 0; border: none; background: transparent; cursor: pointer;
}
.wl-row {
  border-radius: 14px;
  margin-bottom: 0.45rem;
  padding: 0.55rem 0.75rem 0.6rem 0.75rem;
  background: linear-gradient(150deg, rgba(30, 41, 66, 0.72), rgba(17, 24, 42, 0.55));
  border: 1px solid rgba(148, 172, 214, 0.13);
  border-left: 3px solid var(--accent);
  transition: background 0.15s ease, border-color 0.15s ease;
}
[class*="st-key-wl_row_"]:hover .wl-row {
  background: linear-gradient(150deg, rgba(38, 52, 82, 0.85), rgba(20, 28, 48, 0.7));
  border-color: rgba(56, 224, 208, 0.3);
}
.wl-row-selected {
  border-color: rgba(56, 224, 208, 0.5);
  background: linear-gradient(150deg, rgba(41, 57, 88, 0.9), rgba(21, 30, 51, 0.72));
  box-shadow: inset 0 0 0 1px rgba(56, 224, 208, 0.22), 0 14px 30px -24px rgba(0, 0, 0, 0.9);
}
.wl-head { display: flex; align-items: baseline; gap: 0.5rem; }
.wl-rank {
  font-size: 0.68rem; font-weight: 700; color: #7c8aa5;
  font-variant-numeric: tabular-nums;
}
.wl-id {
  font-size: 0.95rem; font-weight: 700; color: #38e0d0;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.wl-risk {
  margin-left: auto;
  font-size: 0.62rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
  color: var(--accent);
  border: 1px solid color-mix(in srgb, var(--accent) 45%, transparent);
  background: color-mix(in srgb, var(--accent) 16%, transparent);
  border-radius: 999px; padding: 0.08rem 0.45rem;
  white-space: nowrap;
}
.wl-score {
  font-size: 0.9rem; font-weight: 700; color: #f2f6ff;
  font-variant-numeric: tabular-nums; text-align: right; min-width: 2.2rem;
}
.wl-reason {
  font-size: 0.78rem; color: #9fb2d4; margin-top: 0.15rem;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.wl-bar {
  margin-top: 0.4rem; height: 4px; border-radius: 999px;
  background: rgba(148, 172, 214, 0.14); overflow: hidden;
}
.wl-bar > span {
  display: block; height: 100%; border-radius: 999px;
  background: linear-gradient(
    90deg, color-mix(in srgb, var(--accent) 45%, transparent), var(--accent)
  );
}

div[data-testid="stRadio"] label p { font-size: 0.86rem; }
div[data-testid="stExpander"] details {
  border-radius: 14px; border: 1px solid rgba(148, 172, 214, 0.14);
  background: rgba(19, 27, 46, 0.6);
}

/* Delivery information is a Streamlit container (schema/records rows are
   interactive). */
.st-key-delivery_health {
  position: relative; overflow: hidden;
  border-radius: 18px;
  background: linear-gradient(155deg, rgba(30, 41, 66, 0.78), rgba(17, 24, 42, 0.62));
  border: 1px solid rgba(148, 172, 214, 0.14);
  box-shadow: 0 18px 40px -26px rgba(0, 0, 0, 0.9);
  backdrop-filter: blur(9px);
  padding: 1.05rem 0 0.85rem 0;
}
/* Streamlit stamps pixel widths on its blocks from the container width it
   measured, which ignores CSS padding added here -- so the card's inset comes
   from its direct children, and inner blocks are released back to 100%. */
.st-key-delivery_health > [data-testid="stElementContainer"],
.st-key-delivery_health > [data-testid="stHorizontalBlock"] {
  padding-left: 1.15rem; padding-right: 1.15rem;
}
.st-key-delivery_health [data-testid="stMarkdown"],
.st-key-delivery_health [data-testid="stButton"] { width: 100% !important; }
.st-key-delivery_health::before {
  content: ""; position: absolute; inset: 0 0 auto 0; height: 2px;
  background: linear-gradient(90deg, #4ade80, transparent 78%);
}
.st-key-delivery_health .health-heading {
  text-transform: uppercase; letter-spacing: 0.13em; font-size: 0.66rem;
  color: #8394b0; font-weight: 600; margin-bottom: 0.45rem;
}
.st-key-delivery_health .health-row {
  display: flex; align-items: center; justify-content: space-between;
  padding: 0.34rem 0; font-size: 0.87rem; color: #cdd8ec;
  border-bottom: 1px dashed rgba(148, 172, 214, 0.12);
}
.st-key-delivery_health .health-value {
  color: #f2f6ff; font-weight: 600; font-variant-numeric: tabular-nums;
}
.st-key-delivery_health [data-testid="stHorizontalBlock"] {
  align-items: center !important;
  padding-top: 0.15rem; padding-bottom: 0.35rem;
  border-bottom: 1px dashed rgba(148, 172, 214, 0.12);
  margin-bottom: 0.1rem;
  gap: 0.35rem;
}
.st-key-delivery_health [data-testid="stHorizontalBlock"]:last-of-type {
  border-bottom: none;
  margin-bottom: 0;
}
/* Keep label / status / Details → on one baseline: Streamlit columns stack
   markdown and buttons with different default margins, and the pill button is
   shorter than a text line, so it floats above the label without this.
   Both column test ids are targeted because Streamlit renamed `column` to
   `stColumn`; matching only the old name silently stops centring the row. */
.st-key-delivery_health [data-testid="stHorizontalBlock"] > [data-testid="stColumn"],
.st-key-delivery_health [data-testid="stHorizontalBlock"] > [data-testid="column"] {
  align-self: stretch;
  min-height: 2rem;
}
.st-key-delivery_health [data-testid="stHorizontalBlock"] [data-testid="stColumn"] > div,
.st-key-delivery_health [data-testid="stHorizontalBlock"] [data-testid="column"] > div,
.st-key-delivery_health [data-testid="stHorizontalBlock"] [data-testid="stVerticalBlock"] {
  display: flex;
  flex-direction: column;
  justify-content: center;
  height: 100%;
  width: 100%;
}
.st-key-delivery_health [data-testid="stHorizontalBlock"] [data-testid="stElementContainer"],
.st-key-delivery_health [data-testid="stHorizontalBlock"] [data-testid="stMarkdown"],
.st-key-delivery_health [data-testid="stHorizontalBlock"] [data-testid="stButton"],
.st-key-schema_details_button,
.st-key-coverage_details_button {
  margin: 0 !important; padding-top: 0 !important; padding-bottom: 0 !important;
}
.st-key-delivery_health [data-testid="stHorizontalBlock"] p {
  margin: 0; font-size: 0.87rem; color: #cdd8ec; line-height: 1.3;
}
/* Each row must stay on one line: a mid-word wrap ("Pas / sed") makes the row
   two lines tall and drags the Details → control off the shared baseline. */
.st-key-delivery_health [data-testid="stHorizontalBlock"] p,
.st-key-delivery_health .health-row span,
.st-key-delivery_health .health-ok,
.st-key-delivery_health .health-bad,
.st-key-delivery_health .health-value {
  white-space: nowrap;
}
.st-key-delivery_health .health-row span:first-child {
  min-width: 0; overflow: hidden; text-overflow: ellipsis;
}

/* The per-source rows collapse into one "Data sources" group so the card stays
   compact, and expand in place to show each source as before. */
.st-key-delivery_health .health-group > summary {
  cursor: pointer;
  list-style: none;
}
.st-key-delivery_health .health-group > summary::-webkit-details-marker { display: none; }
.st-key-delivery_health .health-group > summary::marker { content: ""; }
.st-key-delivery_health .health-group > summary:hover { color: #e6ecf7; }
.st-key-delivery_health .health-chevron {
  display: inline-block; width: 0.7rem; margin-right: 0.25rem;
  color: #7c8aa5; font-weight: 700;
  transition: transform 0.15s ease;
}
.st-key-delivery_health .health-group[open] .health-chevron { transform: rotate(90deg); }
.st-key-delivery_health .health-group-body {
  margin-left: 0.7rem;
  padding-left: 0.6rem;
  border-left: 1px solid rgba(148, 172, 214, 0.14);
}

/* Compact "Details →" controls on Schema check and Records rows. */
.st-key-schema_details_button button,
.st-key-coverage_details_button button {
  border-radius: 999px;
  border: 1px solid rgba(148, 172, 214, 0.18);
  background: rgba(15, 21, 38, 0.45);
  color: #9fb2d4;
  font-size: 0.7rem; font-weight: 600;
  letter-spacing: 0.02em;
  min-height: 1.55rem;
  height: 1.55rem;
  padding: 0 0.45rem;
  white-space: nowrap;
  line-height: 1;
}
.st-key-schema_details_button button p,
.st-key-coverage_details_button button p {
  white-space: nowrap; margin: 0; line-height: 1;
}
.st-key-schema_details_button button:hover,
.st-key-coverage_details_button button:hover {
  color: #38e0d0;
  border-color: rgba(56, 224, 208, 0.4);
  background: rgba(17, 24, 42, 0.7);
}
/* Closing a dialog restores focus to the opener. Match the resting style so
   the cyan outline does not linger after the modal is closed. */
.st-key-schema_details_button button:focus,
.st-key-schema_details_button button:focus:not(:active),
.st-key-schema_details_button button:focus-visible,
.st-key-coverage_details_button button:focus,
.st-key-coverage_details_button button:focus:not(:active),
.st-key-coverage_details_button button:focus-visible {
  color: #9fb2d4 !important;
  border-color: rgba(148, 172, 214, 0.18) !important;
  background: rgba(15, 21, 38, 0.45) !important;
  box-shadow: none !important;
  outline: none !important;
}
</style>
"""


def key_card(label: str, value: str, note: str = "", accent: str = "cyan") -> str:
    """Return the HTML for one key-number glass card."""
    colour = ACCENTS.get(accent, ACCENTS["cyan"])
    note_html = f'<div class="card-note">{escape(note)}</div>' if note else ""
    return (
        f'<div class="glass-card" style="--accent:{colour}">'
        f'<div class="card-label">{escape(label)}</div>'
        f'<div class="card-value">{escape(value)}</div>'
        f"{note_html}"
        f"</div>"
    )


def health_card(title: str, rows: list[tuple[str, str, str]], accent: str = "green") -> str:
    """Return the HTML for the delivery-health card.

    ``rows`` are ``(label, value, kind)`` where ``kind`` is ``ok``, ``bad`` or
    ``plain`` and selects how the value is coloured.
    """
    colour = ACCENTS.get(accent, ACCENTS["green"])
    return (
        f'<div class="glass-card" style="--accent:{colour}">'
        f'<div class="card-label">{escape(title)}</div>'
        f'<div style="margin-top:0.5rem">{health_rows(rows)}</div>'
        f"</div>"
    )


def health_rows(rows: list[tuple[str, str, str]]) -> str:
    """Return HTML for health status rows without the outer glass card frame."""
    classes = {"ok": "health-ok", "bad": "health-bad"}
    return "".join(
        f'<div class="health-row"><span>{escape(label)}</span>'
        f'<span class="{classes.get(kind, "health-value")}">{escape(value)}</span></div>'
        for label, value, kind in rows
    )


def health_group(
    title: str,
    summary: tuple[str, str],
    rows: list[tuple[str, str, str]],
    *,
    start_open: bool = False,
) -> str:
    """Return HTML for a collapsible group of health rows.

    ``summary`` is the ``(value, kind)`` shown on the always-visible summary
    line; ``rows`` are the ``(label, value, kind)`` rows revealed on expand.
    Uses a native ``<details>`` element so expanding costs no server rerun.
    """
    classes = {"ok": "health-ok", "bad": "health-bad"}
    value, kind = summary
    return (
        f'<details class="health-group"{" open" if start_open else ""}>'
        f'<summary class="health-row health-summary">'
        f'<span><span class="health-chevron">›</span>{escape(title)}</span>'
        f'<span class="{classes.get(kind, "health-value")}">{escape(value)}</span>'
        f"</summary>"
        f'<div class="health-group-body">{health_rows(rows)}</div>'
        f"</details>"
    )


def inline_value(value: str, note: str = "", accent: str = "cyan") -> str:
    """Return a key number without a card frame, for use inside a chart card."""
    colour = ACCENTS.get(accent, ACCENTS["cyan"])
    note_html = (
        f'<div class="card-note" style="--accent:{colour}">{escape(note)}</div>' if note else ""
    )
    return f'<div><div class="card-value">{escape(value)}</div>{note_html}</div>'


def card_note(text: str, accent: str = "cyan") -> str:
    """Return a small accented note line for use inside a chart card."""
    colour = ACCENTS.get(accent, ACCENTS["cyan"])
    return f'<div class="card-note" style="--accent:{colour}">{escape(text)}</div>'


RISK_ACCENTS: dict[str, str] = {"High": "rose", "Medium": "amber", "Low": "green"}


def risk_pill(title: str, risk: str) -> str:
    """Return a heading with a colour-coded risk pill next to it."""
    colour = ACCENTS.get(RISK_ACCENTS.get(risk, "cyan"), ACCENTS["cyan"])
    return (
        f'<div class="risk-head"><span class="risk-title">{escape(title)}</span>'
        f'<span class="risk-pill" style="--accent:{colour}">{escape(risk)} risk</span></div>'
    )


def watchlist_row(
    rank: int,
    computer_id: str,
    reason: str,
    risk: str,
    score: float,
    *,
    fill: float,
    selected: bool = False,
) -> str:
    """Return the HTML for one watchlist row card.

    ``fill`` is the score as a 0–1 share of the highest score in the list and
    drives the inline bar; ``selected`` highlights the currently inspected row.
    """
    colour = ACCENTS.get(RISK_ACCENTS.get(risk, "cyan"), ACCENTS["cyan"])
    width = max(0.0, min(1.0, fill)) * 100
    classes = "wl-row wl-row-selected" if selected else "wl-row"
    return (
        f'<div class="{classes}" style="--accent:{colour}">'
        f'<div class="wl-head">'
        f'<span class="wl-rank">{rank:02d}</span>'
        f'<span class="wl-id">{escape(computer_id)}</span>'
        f'<span class="wl-risk">{escape(risk)}</span>'
        f'<span class="wl-score">{score:.1f}</span>'
        f"</div>"
        f'<div class="wl-reason">{escape(reason)}</div>'
        f'<div class="wl-bar"><span style="width:{width:.1f}%"></span></div>'
        f"</div>"
    )


def zone_label(text: str) -> str:
    """Return the HTML for a zone heading rule."""
    return f'<div class="zone-label">{escape(text)}</div>'


def chart_label(text: str) -> str:
    """Return the HTML for a chart-card caption."""
    return f'<div class="chart-card">{escape(text)}</div>'

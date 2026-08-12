"""Pure helpers for rendering delivery ``volume_summary`` snapshots."""

from __future__ import annotations


def _layer_row_total(volume: dict, layer: str) -> int | None:
    """Sum catalog record counts for one layer in a volume_summary snapshot."""
    if layer in ("landing", "bronze", "silver"):
        rows = volume.get(layer) or []
        if not rows:
            return None
        return sum(int(row.get("record_count") or 0) for row in rows)
    payload = volume.get(layer) or {}
    count = payload.get("record_count")
    return None if count is None else int(count)


def phase_volume_rows(volume: dict) -> list[dict[str, object]]:
    """Build a before→after row list for each pipeline phase transition.

    Returns one dict per hop (landing→bronze … gold→delivered) with
    ``phase``, ``rows_before``, ``rows_after``, and ``delta``. Missing layer
    totals become ``None`` so the UI can render an em dash.
    """
    layers = ("landing", "bronze", "silver", "gold", "delivered")
    totals = {name: _layer_row_total(volume, name) for name in layers}
    hops = (
        ("Landing → Bronze", "landing", "bronze"),
        ("Bronze → Silver", "bronze", "silver"),
        ("Silver → Gold", "silver", "gold"),
        ("Gold → Delivered", "gold", "delivered"),
    )
    out: list[dict[str, object]] = []
    for label, before_key, after_key in hops:
        before = totals[before_key]
        after = totals[after_key]
        delta: int | None = None
        if before is not None and after is not None:
            delta = after - before
        out.append(
            {
                "phase": label,
                "rows_before": before,
                "rows_after": after,
                "delta": delta,
            }
        )
    return out


def layer_volume_totals(volume: dict) -> list[dict[str, object]]:
    """List total row counts per medallion/delivery layer."""
    return [
        {"layer": name, "record_count": _layer_row_total(volume, name)}
        for name in ("landing", "bronze", "silver", "gold", "delivered")
    ]

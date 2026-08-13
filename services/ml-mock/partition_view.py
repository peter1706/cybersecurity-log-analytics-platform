"""Pure helpers for delivery partition selection and overview status."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd

DEFAULT_SOURCES = ("auth", "proc", "flows", "dns")


def manifest_key(manifest: Mapping) -> tuple[int, int]:
    """Return ``(window_days, anchor_day)`` for a delivery manifest."""
    return (int(manifest["window_days"]), int(manifest["anchor_day"]))


def sort_manifests_newest_first(manifests: Sequence[Mapping]) -> list[dict]:
    """Sort manifests newest-first by ``created_at``, then window/anchor."""

    def sort_key(manifest: Mapping) -> tuple:
        created = str(manifest.get("created_at") or "")
        return (created, int(manifest["window_days"]), int(manifest["anchor_day"]))

    return sorted((dict(m) for m in manifests), key=sort_key, reverse=True)


def find_manifest(
    manifests: Sequence[Mapping],
    *,
    window_days: int,
    anchor_day: int,
) -> dict | None:
    """Return the manifest for ``(window_days, anchor_day)``, if present."""
    for manifest in manifests:
        if (
            int(manifest["window_days"]) == window_days
            and int(manifest["anchor_day"]) == anchor_day
        ):
            return dict(manifest)
    return None


def resolve_active_manifest(
    manifests: Sequence[Mapping],
    *,
    active_key: tuple[int, int] | None,
    selected_row: Mapping | None,
    force_key: tuple[int, int] | None = None,
) -> dict | None:
    """Pick the active delivery: force → table row → session key → newest.

    Args:
        manifests: Available delivery manifests (any order).
        active_key: Previously selected ``(window_days, anchor_day)``.
        selected_row: Overview-table row with ``window_days`` / ``anchor_day``.
        force_key: Explicit key from Load/Rebuild; preferred when present.

    Returns:
        The chosen manifest dict, or ``None`` when the list is empty.
    """
    ordered = sort_manifests_newest_first(manifests)
    if not ordered:
        return None

    if force_key is not None:
        forced = find_manifest(
            ordered,
            window_days=force_key[0],
            anchor_day=force_key[1],
        )
        if forced is not None:
            return forced

    if selected_row is not None:
        selected = find_manifest(
            ordered,
            window_days=int(selected_row["window_days"]),
            anchor_day=int(selected_row["anchor_day"]),
        )
        if selected is not None:
            return selected

    if active_key is not None:
        active = find_manifest(
            ordered,
            window_days=active_key[0],
            anchor_day=active_key[1],
        )
        if active is not None:
            return active

    return ordered[0]


def overview_schema_status(manifest: Mapping, expected_columns: Sequence[str]) -> str:
    """Return ``✅ Passed`` or ``❌ Failed`` for schema contract match."""
    columns = manifest.get("columns")
    if columns == list(expected_columns):
        return "✅ Passed"
    return "❌ Failed"


def overview_source_lines(
    df: pd.DataFrame,
    sources: Sequence[str] = DEFAULT_SOURCES,
) -> list[str]:
    """Return one ``✅``/``❌`` status line per source for Delivery overview."""
    lines: list[str] = []
    for source in sources:
        column = f"source_present_{source}"
        if column not in df.columns or len(df) == 0:
            ok = False
        else:
            ok = bool(df[column].all())
        mark = "✅" if ok else "❌"
        lines.append(f"{mark} {source}")
    return lines


def overview_dataframe(manifests: Sequence[Mapping]) -> pd.DataFrame:
    """Build the Last deliveries overview table (newest first)."""
    rows = [
        {
            "anchor_day": m["anchor_day"],
            "window_days": m["window_days"],
            "records": m["record_count"],
            "schema": m["schema_version"],
            "created_at": m["created_at"],
            "checksum": str(m["checksum_sha256"])[:12] + "...",
        }
        for m in sort_manifests_newest_first(manifests)
    ]
    return pd.DataFrame(rows)

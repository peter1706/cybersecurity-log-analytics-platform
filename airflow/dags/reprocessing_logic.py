"""Pure helpers for feature_reprocessing (no Airflow imports).

Landing must already exist for every ``(source, day)`` in the rolling window;
simulation is never triggered from this path.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence

SOURCES: tuple[str, ...] = ("auth", "proc", "flows", "dns")


def window_event_days(anchor_day: int, window_days: int) -> list[int]:
    """Return inclusive event-day indices covered by the rolling window.

    Args:
        anchor_day: As-of day for the feature partition (non-negative).
        window_days: Rolling window length (must be >= 1).

    Returns:
        Sorted list of day indices in ``[max(0, anchor - window + 1), anchor]``.

    Raises:
        ValueError: If ``anchor_day`` is negative or ``window_days`` < 1.
    """
    if anchor_day < 0:
        raise ValueError(f"anchor_day must be >= 0, got {anchor_day}")
    if window_days < 1:
        raise ValueError(f"window_days must be >= 1, got {window_days}")
    start = max(0, anchor_day - window_days + 1)
    return list(range(start, anchor_day + 1))


def missing_landing_partitions(
    event_days: Sequence[int],
    *,
    sources: Sequence[str] = SOURCES,
    has_landing: Callable[[str, int], bool],
) -> list[tuple[str, int]]:
    """Return ``(source, day)`` pairs that lack a landing checksum.

    Args:
        event_days: Days that must be present in landing for every source.
        sources: Event sources to require.
        has_landing: Predicate ``(source, day) -> True`` when landing exists.

    Returns:
        Sorted list of missing ``(source, day)`` pairs.
    """
    missing: list[tuple[str, int]] = []
    for day in event_days:
        for source in sources:
            if not has_landing(source, day):
                missing.append((source, day))
    return missing


def format_missing_landing(missing: Iterable[tuple[str, int]]) -> str:
    """Format missing landing pairs for a clear preflight error message."""
    pairs = ", ".join(f"{source}/day={day:02d}" for source, day in missing)
    return "Missing landing partitions (run simulate / Makefile backfill first): " f"{pairs}"

"""Per-source specifications (strategy + registry).

A ``SourceSpec`` bundles everything that varies between LANL sources: its column
layout and its Bronze/Silver/Gold transforms. New sources are added by
registering another spec (Increment 2) rather than by branching inside the jobs.
The transforms themselves stay pure functions in ``transforms.py``.
"""

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from pyspark.sql import DataFrame

from .schemas import AUTH_COLUMNS
from .transforms import auth_bronze_to_silver, auth_silver_to_gold, raw_to_bronze


@dataclass(frozen=True)
class SourceSpec:
    """Describes how one LANL source moves through the Medallion layers."""

    name: str
    columns: list[str]
    to_bronze: Callable[[DataFrame], DataFrame]
    to_silver: Callable[[DataFrame], DataFrame]
    to_gold: Callable[[DataFrame], DataFrame]

    @property
    def gold_table(self) -> str:
        """Name of the Gold feature table for this source."""
        return f"{self.name}_features"


AUTH = SourceSpec(
    name="auth",
    columns=AUTH_COLUMNS,
    to_bronze=partial(raw_to_bronze, source="auth"),
    to_silver=auth_bronze_to_silver,
    to_gold=auth_silver_to_gold,
)

SOURCES: dict[str, SourceSpec] = {AUTH.name: AUTH}


def get_source(name: str) -> SourceSpec:
    """Return the registered SourceSpec for ``name`` or raise ValueError."""
    try:
        return SOURCES[name]
    except KeyError:
        raise ValueError(f"unknown source {name!r}; registered: {sorted(SOURCES)}") from None

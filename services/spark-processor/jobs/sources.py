"""Per-source specifications (strategy + registry).

A ``SourceSpec`` bundles what varies between LANL sources for the landing ->
Bronze -> Silver path: its column layout and its Bronze/Silver transforms. New
sources are added by registering another spec rather than by branching inside the
jobs. The Gold layer is source-agnostic -- all four sources feed the single
unified ``computer_features`` table (see ``jobs.base.ComputerFeaturesJob`` and
``transforms.features``), so it is not part of ``SourceSpec``. The transforms
themselves stay pure functions in the ``transforms`` package (one module per
source).
"""

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from pyspark.sql import DataFrame

from .schemas import AUTH_COLUMNS, DNS_COLUMNS, FLOWS_COLUMNS, PROC_COLUMNS
from .transforms import (
    auth_bronze_to_silver,
    dns_bronze_to_silver,
    flows_bronze_to_silver,
    proc_bronze_to_silver,
    raw_to_bronze,
)


@dataclass(frozen=True)
class SourceSpec:
    """Describes how one LANL source moves through landing -> Bronze -> Silver."""

    name: str
    columns: list[str]
    to_bronze: Callable[[DataFrame], DataFrame]
    to_silver: Callable[[DataFrame], DataFrame]


AUTH = SourceSpec(
    name="auth",
    columns=AUTH_COLUMNS,
    to_bronze=partial(raw_to_bronze, source="auth"),
    to_silver=auth_bronze_to_silver,
)

PROC = SourceSpec(
    name="proc",
    columns=PROC_COLUMNS,
    to_bronze=partial(raw_to_bronze, source="proc"),
    to_silver=proc_bronze_to_silver,
)

FLOWS = SourceSpec(
    name="flows",
    columns=FLOWS_COLUMNS,
    to_bronze=partial(raw_to_bronze, source="flows"),
    to_silver=flows_bronze_to_silver,
)

DNS = SourceSpec(
    name="dns",
    columns=DNS_COLUMNS,
    to_bronze=partial(raw_to_bronze, source="dns"),
    to_silver=dns_bronze_to_silver,
)

SOURCES: dict[str, SourceSpec] = {s.name: s for s in (AUTH, PROC, FLOWS, DNS)}


def get_source(name: str) -> SourceSpec:
    """Return the registered SourceSpec for ``name`` or raise ValueError."""
    try:
        return SOURCES[name]
    except KeyError:
        raise ValueError(f"unknown source {name!r}; registered: {sorted(SOURCES)}") from None

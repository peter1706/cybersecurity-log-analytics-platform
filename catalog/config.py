"""Governance-catalog connection config, resolved from the environment.

Shared by every service that writes governance records (lanl-simulator,
spark-processor, delivery) and by Airflow's job-run callbacks. Non-sensitive
connection settings are plain env vars (documented in ``.env.example``); the
password follows the same env-var pattern as ``AIRFLOW_DB_PASSWORD`` today (a
stand-in to be replaced by a Docker secret later).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_HOST = "postgres-catalog"
DEFAULT_PORT = 5432


@dataclass(frozen=True)
class CatalogConfig:
    """Resolved connection settings for the governance catalog."""

    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> CatalogConfig:
        """Build config from environment variables.

        ``CATALOG_DB_HOST``/``CATALOG_DB_PORT`` default to the compose service
        name and the Postgres port; ``CATALOG_DB_NAME``/``CATALOG_DB_USER``/
        ``CATALOG_DB_PASSWORD`` are required (raising :class:`KeyError` when
        missing) so a task fails fast rather than writing lineage nowhere.
        """
        source = os.environ if env is None else env
        return cls(
            host=source.get("CATALOG_DB_HOST", DEFAULT_HOST),
            port=int(source.get("CATALOG_DB_PORT", str(DEFAULT_PORT))),
            dbname=_require(source, "CATALOG_DB_NAME"),
            user=_require(source, "CATALOG_DB_USER"),
            password=_require(source, "CATALOG_DB_PASSWORD"),
        )

    def dsn(self) -> str:
        """Return a libpq keyword/value connection string."""
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise KeyError(
            f"Required environment variable {key!r} is missing or empty. "
            "Set it in .env (see .env.example) so the governance catalog is reachable."
        )
    return value

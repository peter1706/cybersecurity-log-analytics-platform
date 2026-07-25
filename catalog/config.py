"""Governance-catalog connection config, resolved from the environment.

Shared by every service that writes governance records (lanl-simulator,
spark-processor, delivery) and by Airflow's job-run callbacks. Non-sensitive
connection settings are plain env vars (documented in ``.env.example``); the
password is a container secret read from ``/run/secrets/postgres_catalog_password``
(with an env-var fallback for local tooling and tests).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from .secrets import read_secret

DEFAULT_HOST = "postgres-catalog"
DEFAULT_PORT = 5432
CATALOG_PASSWORD_SECRET = "postgres_catalog_password"


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
        name and the Postgres port; ``CATALOG_DB_NAME``/``CATALOG_DB_USER`` are
        required plain env vars (raising :class:`KeyError` when missing) so a task
        fails fast rather than writing lineage nowhere. The password is a
        container secret (``postgres_catalog_password``) with a
        ``CATALOG_DB_PASSWORD`` env fallback.
        """
        source = os.environ if env is None else env
        return cls(
            host=source.get("CATALOG_DB_HOST", DEFAULT_HOST),
            port=int(source.get("CATALOG_DB_PORT", str(DEFAULT_PORT))),
            dbname=_require(source, "CATALOG_DB_NAME"),
            user=_require(source, "CATALOG_DB_USER"),
            password=read_secret(
                CATALOG_PASSWORD_SECRET,
                env="CATALOG_DB_PASSWORD",
                env_mapping=source,
            ),
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

"""Governance catalog: schema migrations and the shared write/read client.

``postgres-catalog`` is the plain-PostgreSQL governance store (lineage, schema
registry, job-run logs, checksum audit chain, delivery manifests). This package
holds the SQL migrations (``catalog/migrations``) and the small client the
pipeline services and Airflow use to record governance metadata as part of each
task.
"""

from .client import (
    CatalogClient,
    Checksum,
    DeliveryManifest,
    JobRun,
    Lineage,
    SchemaRegistration,
)
from .config import CatalogConfig

__all__ = [
    "CatalogClient",
    "CatalogConfig",
    "Checksum",
    "DeliveryManifest",
    "JobRun",
    "Lineage",
    "SchemaRegistration",
]

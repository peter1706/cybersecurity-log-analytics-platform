"""Per-source pure DataFrame transforms.

One module per LANL source (``auth``, ``proc``, ``flows``, ``dns``), plus
``_common`` for the shared helpers and the source-agnostic ``raw_to_bronze``.
The public transform functions are re-exported here so callers import them from
``jobs.transforms`` regardless of which module they live in.
"""

from ._common import raw_to_bronze
from .auth import auth_bronze_to_silver, auth_computer_features
from .dns import dns_bronze_to_silver, dns_computer_features
from .features import COMPUTER_FEATURE_COLUMNS, assemble_computer_features
from .flows import flows_bronze_to_silver, flows_computer_features
from .proc import proc_bronze_to_silver, proc_computer_features

__all__ = [
    "raw_to_bronze",
    "auth_bronze_to_silver",
    "proc_bronze_to_silver",
    "flows_bronze_to_silver",
    "dns_bronze_to_silver",
    # Unified per-computer Gold feature table.
    "auth_computer_features",
    "proc_computer_features",
    "flows_computer_features",
    "dns_computer_features",
    "assemble_computer_features",
    "COMPUTER_FEATURE_COLUMNS",
]

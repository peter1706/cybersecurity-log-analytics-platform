"""Deterministic content checksum for a Spark DataFrame.

The checksum is over *logical content*, not the physical Delta/Parquet layout, so
the stage that writes a partition and the stage that later reads it back compute
the same value regardless of file count, ordering, or compression. This is what
makes the integrity chain (each stage validates the upstream checksum before
reading, records its own after writing) verifiable across transitions.

Construction (all inside Spark, so writer and reader agree):

1. Each row is rendered to a canonical string: every column cast to string with a
   sentinel for NULL, joined in a fixed (sorted) column order.
2. Each row string gets a 64-bit ``xxhash64``, shifted to be non-negative so
   summing cannot cancel via sign.
3. The per-row hashes are summed as an exact big integer and combined with the
   row count and the sorted column names into a single SHA-256.

Summing is commutative, so row order and partition layout do not affect the
result, while the column set, every cell value, and the row multiplicity do. The
aggregate streams (partial sums merged across partitions), so it stays within a
small heap even for large partitions -- unlike collecting every row hash.

This is content-integrity (corruption/tamper detection between stages), not an
adversarial MAC: an additive combine of 64-bit hashes is not collision-proof
against a crafted swap, which is out of scope for this pipeline.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Separators unlikely to occur in the data, so distinct cell values cannot be
# confused for one another after joining.
_FIELD_SEP = "\x1f"  # unit separator, between columns of a row
_NULL_SENTINEL = "\x00"  # distinguishes NULL from the empty string
# 2**63: shifts a signed 64-bit hash into [0, 2**64) so sums never cancel by sign.
# A Decimal (not a Python int) so the literal is not marshalled as a Java long,
# which would overflow at exactly 2**63.
_SIGN_SHIFT = Decimal(9223372036854775808)


def dataframe_checksum(df: DataFrame) -> str:
    """Return the deterministic SHA-256 (hex) of a DataFrame's content.

    Deterministic and independent of row order / partition layout; sensitive to
    the column set, every value, and the row count.
    """
    columns = sorted(df.columns)
    row_repr = F.concat_ws(
        _FIELD_SEP,
        *[F.coalesce(F.col(c).cast("string"), F.lit(_NULL_SENTINEL)) for c in columns],
    )
    row_hash = F.xxhash64(row_repr).cast("decimal(38,0)") + F.lit(_SIGN_SHIFT)
    row = (
        df.select(row_hash.alias("h"))
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.coalesce(F.sum("h"), F.lit(0)).cast("decimal(38,0)").alias("s"),
        )
        .collect()[0]
    )
    header = ",".join(columns)
    summary = f"{header}|{row['n']}|{row['s']}"
    return hashlib.sha256(summary.encode("utf-8")).hexdigest()

#!/usr/bin/env python3
"""lanl-simulator: replay a LANL source for one day (or a range) into MinIO landing.

The simulator is the external producer. It streams a source subset file
(``data/subset/<source>.txt.gz``, header-less gzipped CSV), keeps the rows whose
derived day index matches the requested day (or falls in the requested
``[--day, --day-end]`` range), and uploads them verbatim (still gzip-compressed,
source-native format) as one object per day to the landing bucket:

    landing/<source>/day=<DD>/<source>-<DDD>.csv.gz

A multi-day range (``--day-end``) is filtered in a single pass and seeds the
history a Silver -> Gold rolling-window backfill needs.

No typing or schema is applied here -- that happens at Bronze. Writing the bytes
as received keeps the raw-landing checkpoint faithful to what a real producer
would drop. The upload is deterministic and idempotent (fixed object key, fixed
gzip mtime), so re-running a day overwrites the same object.
"""

import argparse
import gzip
import hashlib
import io
import os
import sys
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import boto3
from botocore.client import Config

from catalog import CatalogClient, Checksum, Lineage, read_secret

SECONDS_PER_DAY = 86400
SOURCES = ("auth", "proc", "flows", "dns")


def landing_lineage(source: str, day: int, record_count: int, schema_version: str) -> Lineage:
    """Build the raw-landing lineage record for one uploaded source/day (pure)."""
    return Lineage(
        source=source,
        day=day,
        from_layer=None,
        to_layer="landing",
        record_count=record_count,
        schema_version=schema_version,
    )


def landing_checksum(source: str, day: int, blob: bytes, record_count: int) -> Checksum:
    """Build the raw-landing checksum record over the uploaded object bytes (pure).

    The digest is over the exact bytes uploaded (deterministic: fixed gzip
    mtime), so the land_to_bronze job can re-read the object and verify it before
    parsing -- the first link in the integrity chain.
    """
    return Checksum(
        layer="landing",
        source=source,
        day=day,
        checksum=hashlib.sha256(blob).hexdigest(),
        record_count=record_count,
    )


class LandingSimulator:
    """Replays LANL source subsets into the MinIO landing bucket."""

    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        subset_dir: str | Path,
        schema_version: str = "v1",
        catalog_factory: Callable[[], AbstractContextManager[CatalogClient]] | None = None,
    ):
        self.bucket = bucket
        self.subset_dir = Path(subset_dir)
        self.schema_version = schema_version
        # Factory returning a governance-catalog client context manager. Default
        # opens a real connection; tests inject a fake.
        self._catalog_factory = catalog_factory or CatalogClient.connect
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    @classmethod
    def from_env(cls, subset_dir: str | Path) -> "LandingSimulator":
        """Build a simulator from the MinIO endpoint/config and MinIO secrets.

        The endpoint, bucket, and schema version are non-sensitive env vars; the
        MinIO access/secret keys are container secrets (with an env fallback).
        """
        return cls(
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=read_secret("minio_root_user", env="MINIO_ROOT_USER"),
            secret_key=read_secret("minio_root_password", env="MINIO_ROOT_PASSWORD"),
            bucket=os.environ.get("LANDING_BUCKET", "landing"),
            subset_dir=subset_dir,
            schema_version=os.environ.get("SCHEMA_VERSION", "v1"),
        )

    @staticmethod
    def day_of(time_str: str) -> int:
        """Return the day index for a LANL timestamp given in seconds."""
        return (int(time_str) - 1) // SECONDS_PER_DAY

    @staticmethod
    def object_key(source: str, day: int) -> str:
        """Return the landing object key for a source/day."""
        return f"{source}/day={day:02d}/{source}-{day:03d}.csv.gz"

    def source_path(self, source: str) -> Path:
        """Return the local subset path for a source."""
        return self.subset_dir / f"{source}.txt.gz"

    def filter_days(
        self, source: str, start: int, end: int
    ) -> tuple[dict[int, bytes], dict[int, int]]:
        """Return per-day gzip blobs and per-day row counts for the inclusive range.

        Reads the time-sorted source subset once, keeping rows whose derived day
        falls in ``[start, end]`` and grouping them by day. Returns a mapping of
        day -> gzip blob and a parallel mapping of day -> row count (the count is
        recorded as landing lineage); days with no rows are simply absent. Stops
        reading as soon as the stream passes ``end``.
        """
        src_path = self.source_path(source)
        if not src_path.exists():
            raise FileNotFoundError(f"source subset not found: {src_path}")

        lines_by_day: dict[int, list[str]] = {}
        with gzip.open(src_path, "rt", encoding="utf-8", newline="") as fh:
            for line in fh:
                comma = line.find(",")
                if comma <= 0:
                    continue
                d = self.day_of(line[:comma])
                if d > end:
                    break
                if d < start:
                    continue
                lines_by_day.setdefault(d, []).append(line if line.endswith("\n") else line + "\n")

        blobs: dict[int, bytes] = {}
        counts: dict[int, int] = {}
        for d, lines in lines_by_day.items():
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as out:
                for line in lines:
                    out.write(line.encode("utf-8"))
            blobs[d] = buf.getvalue()
            counts[d] = len(lines)
            print(f"  filtered {len(lines):,} rows for day {d}", flush=True)
        return blobs, counts

    def ensure_bucket(self) -> None:
        """Create the landing bucket if it does not already exist."""
        existing = {b["Name"] for b in self._client.list_buckets().get("Buckets", [])}
        if self.bucket not in existing:
            self._client.create_bucket(Bucket=self.bucket)
            print(f"  created bucket {self.bucket}", flush=True)

    def run(self, source: str, day: int, day_end: int | None = None) -> list[str]:
        """Filter a source over one day or an inclusive range and upload each day.

        With ``day_end`` unset (or equal to ``day``) this replays a single day,
        preserving the original one-object-per-run behavior. With ``day_end >
        day`` it replays the inclusive range ``[day, day_end]`` in a single pass,
        uploading one landing object per day. Uploads are idempotent (fixed object
        keys, fixed gzip mtime), so re-running overwrites the same objects.

        A single-day request with no matching rows is an error; in a multi-day
        range, days with no rows are skipped with a notice and only a range that
        yields nothing at all is an error.
        """
        end = day if day_end is None else day_end
        if end < day:
            raise ValueError(f"--day-end ({end}) must be >= --day ({day})")

        blobs, counts = self.filter_days(source, day, end)
        self.ensure_bucket()
        keys: list[str] = []
        # Record raw-landing lineage as part of the same task, after each upload.
        # A catalog failure fails the task (governance is not optional).
        with self._catalog_factory() as catalog:
            for d in range(day, end + 1):
                blob = blobs.get(d)
                if blob is None:
                    if day == end:
                        raise ValueError(
                            f"no rows found for day {d} in {self.source_path(source)}; "
                            "check the subset day range"
                        )
                    print(f"  day {d}: no rows in subset, skipping", flush=True)
                    continue
                key = self.object_key(source, d)
                self._client.put_object(Bucket=self.bucket, Key=key, Body=blob)
                catalog.record_lineage(landing_lineage(source, d, counts[d], self.schema_version))
                catalog.record_checksum(landing_checksum(source, d, blob, counts[d]))
                print(
                    f"lanl-simulator: {source} day={d} -> s3://{self.bucket}/{key} "
                    f"({len(blob):,} bytes, {counts[d]:,} rows)",
                    flush=True,
                )
                keys.append(key)

        if not keys:
            raise ValueError(
                f"no rows found for days {day}..{end} in {self.source_path(source)}; "
                "check the subset day range"
            )
        return keys


def main() -> None:
    """Parse args and run the simulator for one source/day."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, choices=SOURCES)
    parser.add_argument("--day", type=int, required=True)
    parser.add_argument(
        "--day-end",
        type=int,
        default=None,
        help="optional: replay the inclusive day range [--day, --day-end] "
        "(seeds a rolling-window backfill in one pass)",
    )
    parser.add_argument(
        "--subset-dir",
        type=Path,
        default=Path(os.environ.get("SUBSET_DIR", "/data/subset")),
        help="directory holding <source>.txt.gz subset files",
    )
    args = parser.parse_args()

    simulator = LandingSimulator.from_env(args.subset_dir)
    simulator.run(args.source, args.day, args.day_end)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - surface a clear failure to Airflow
        print(f"lanl-simulator FAILED: {exc}", file=sys.stderr)
        raise

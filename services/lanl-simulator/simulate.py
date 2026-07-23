#!/usr/bin/env python3
"""lanl-simulator: replay one LANL source for one day into the MinIO landing zone.

The simulator is the external producer. It streams a source subset file
(``data/subset/<source>.txt.gz``, header-less gzipped CSV), keeps only the rows
whose derived day index matches the requested day, and uploads them verbatim
(still gzip-compressed, source-native format) as a single object to the landing
bucket:

    landing/<source>/day=<DD>/<source>-<DDD>.csv.gz

No typing or schema is applied here -- that happens at Bronze. Writing the bytes
as received keeps the raw-landing checkpoint faithful to what a real producer
would drop. The upload is deterministic and idempotent (fixed object key, fixed
gzip mtime), so re-running a day overwrites the same object.
"""

import argparse
import gzip
import io
import os
import sys

import boto3
from botocore.client import Config

SECONDS_PER_DAY = 86400
SOURCES = ("auth", "proc", "flows", "dns")


class LandingSimulator:
    """Replays LANL source subsets into the MinIO landing bucket."""

    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        subset_dir: str,
    ):
        self.bucket = bucket
        self.subset_dir = subset_dir
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    @classmethod
    def from_env(cls, subset_dir: str) -> "LandingSimulator":
        """Build a simulator from the standard MinIO environment variables."""
        return cls(
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            bucket=os.environ.get("LANDING_BUCKET", "landing"),
            subset_dir=subset_dir,
        )

    @staticmethod
    def day_of(time_str: str) -> int:
        """Return the day index for a LANL timestamp given in seconds."""
        return (int(time_str) - 1) // SECONDS_PER_DAY

    @staticmethod
    def object_key(source: str, day: int) -> str:
        """Return the landing object key for a source/day."""
        return f"{source}/day={day:02d}/{source}-{day:03d}.csv.gz"

    def source_path(self, source: str) -> str:
        """Return the local subset path for a source."""
        return os.path.join(self.subset_dir, f"{source}.txt.gz")

    def filter_day(self, source: str, day: int) -> bytes:
        """Return a gzip-compressed blob of the rows whose day == target day.

        Source files are time-sorted, so we stop as soon as we pass the day.
        """
        src_path = self.source_path(source)
        if not os.path.exists(src_path):
            raise FileNotFoundError(f"source subset not found: {src_path}")

        buf = io.BytesIO()
        kept = 0
        with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as out:
            with gzip.open(src_path, "rt", encoding="utf-8", newline="") as fh:
                for line in fh:
                    comma = line.find(",")
                    if comma <= 0:
                        continue
                    d = self.day_of(line[:comma])
                    if d > day:
                        break
                    if d < day:
                        continue
                    out.write((line if line.endswith("\n") else line + "\n").encode("utf-8"))
                    kept += 1
        if kept == 0:
            raise ValueError(
                f"no rows found for day {day} in {src_path}; check the subset day range"
            )
        print(f"  filtered {kept:,} rows for day {day}", flush=True)
        return buf.getvalue()

    def ensure_bucket(self) -> None:
        """Create the landing bucket if it does not already exist."""
        existing = {b["Name"] for b in self._client.list_buckets().get("Buckets", [])}
        if self.bucket not in existing:
            self._client.create_bucket(Bucket=self.bucket)
            print(f"  created bucket {self.bucket}", flush=True)

    def run(self, source: str, day: int) -> str:
        """Filter one source/day and upload it to the landing bucket."""
        key = self.object_key(source, day)
        print(f"lanl-simulator: {source} day={day} -> s3://{self.bucket}/{key}", flush=True)
        blob = self.filter_day(source, day)
        self.ensure_bucket()
        self._client.put_object(Bucket=self.bucket, Key=key, Body=blob)
        print(f"  uploaded {len(blob):,} bytes", flush=True)
        return key


def main() -> None:
    """Parse args and run the simulator for one source/day."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, choices=SOURCES)
    parser.add_argument("--day", type=int, required=True)
    parser.add_argument(
        "--subset-dir",
        default=os.environ.get("SUBSET_DIR", "/data/subset"),
        help="directory holding <source>.txt.gz subset files",
    )
    args = parser.parse_args()

    simulator = LandingSimulator.from_env(args.subset_dir)
    simulator.run(args.source, args.day)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - surface a clear failure to Airflow
        print(f"lanl-simulator FAILED: {exc}", file=sys.stderr)
        raise

#!/usr/bin/env python3
"""spark-processor entrypoint: dispatch one or more Medallion jobs.

Usage:
    # single job (one source, one day) -- what the Airflow DAG launches
    python run_job.py <land_to_bronze|bronze_to_silver> --source auth --day 0
    python run_job.py silver_to_gold --day 0   # cross-source Gold (no --source)

    # batched: process a day range and/or all sources in ONE SparkSession, so
    # the JVM/Ivy cold start is paid once instead of per (source, day). Used by
    # scripts/backfill.sh for the multi-day backfill.
    python run_job.py land_to_bronze  --all-sources --day 0 --day-end 6
    python run_job.py bronze_to_silver --all-sources --day 0 --day-end 6
    python run_job.py silver_to_gold                --day 0 --day-end 6

Runs in local Spark mode by default (SPARK_MASTER=local[*]); the first run
downloads the Delta and hadoop-aws jars via Ivy (needs network access).
"""

import argparse
import sys

from jobs.base import JOB_CLASSES, ComputerFeaturesJob
from jobs.common import build_spark
from jobs.sources import SOURCES, get_source


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job", choices=sorted([*JOB_CLASSES, ComputerFeaturesJob.name]))
    parser.add_argument(
        "--source",
        default="auth",
        choices=sorted(SOURCES),
        help="single source; ignored by the cross-source silver_to_gold job",
    )
    parser.add_argument(
        "--all-sources",
        action="store_true",
        help="process every source in one session (per-source jobs only)",
    )
    parser.add_argument("--day", type=int, required=True, help="first (or only) day")
    parser.add_argument(
        "--day-end",
        type=int,
        default=None,
        help="if set, process the inclusive day range [--day, --day-end] in one session",
    )
    return parser.parse_args()


def main() -> None:
    """Parse args, build one Spark session, run the selected job(s), then stop it."""
    args = _parse_args()

    day_end = args.day if args.day_end is None else args.day_end
    if day_end < args.day:
        raise ValueError(f"--day-end ({day_end}) must be >= --day ({args.day})")
    days = range(args.day, day_end + 1)
    span = f"day{args.day}" if day_end == args.day else f"day{args.day}-{day_end}"

    if args.job == ComputerFeaturesJob.name:
        # Cross-source Gold: one job per anchor day, ascending so each anchor's
        # rolling window sees the Silver days already present.
        spark = build_spark(f"clap-{args.job}-{span}")
        try:
            for day in days:
                ComputerFeaturesJob(spark, day).run()
        finally:
            spark.stop()
        return

    source_names = sorted(SOURCES) if args.all_sources else [args.source]
    scope = "all" if args.all_sources else args.source
    spark = build_spark(f"clap-{args.job}-{scope}-{span}")
    try:
        for day in days:
            for name in source_names:
                JOB_CLASSES[args.job](spark, get_source(name), day).run()
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"spark-processor FAILED: {exc}", file=sys.stderr)
        raise

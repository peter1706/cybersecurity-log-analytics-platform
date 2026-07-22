#!/usr/bin/env python3
"""spark-processor entrypoint: dispatch one Medallion job.

Usage:
    python run_job.py <land_to_bronze|bronze_to_silver|silver_to_gold> \
        --source auth --day 0

Runs in local Spark mode by default (SPARK_MASTER=local[*]); the first run
downloads the Delta and hadoop-aws jars via Ivy (needs network access).
"""

import argparse
import sys

from jobs.base import JOB_CLASSES
from jobs.common import build_spark
from jobs.sources import SOURCES, get_source


def main() -> None:
    """Parse args, build Spark, run the selected job, and stop the session."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job", choices=sorted(JOB_CLASSES))
    parser.add_argument("--source", default="auth", choices=sorted(SOURCES))
    parser.add_argument("--day", type=int, required=True)
    args = parser.parse_args()

    source = get_source(args.source)
    spark = build_spark(f"clap-{args.job}-{source.name}-day{args.day}")
    try:
        JOB_CLASSES[args.job](spark, source, args.day).run()
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"spark-processor FAILED: {exc}", file=sys.stderr)
        raise

"""Shared pytest fixtures.

Provides a session-scoped local SparkSession for the transform unit tests. No
MinIO or Delta is required -- the pure transforms operate on in-memory
DataFrames.
"""

import logging

import pytest

# py4j logs "Closing down clientserver connection" at INFO from a __del__
# finalizer during interpreter shutdown, after pytest has closed the captured
# streams -- which surfaces as a harmless "I/O operation on closed file"
# logging error. Raising the level above INFO suppresses that shutdown noise.
logging.getLogger("py4j").setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def spark():
    """Return a lightweight local SparkSession for unit tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("clap-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

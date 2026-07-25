"""Unit tests for the deterministic DataFrame content checksum."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from jobs.checksums import dataframe_checksum  # noqa: E402


class TestDeterminism:
    def test_same_content_same_checksum(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
        assert dataframe_checksum(df) == dataframe_checksum(df)

    def test_row_order_does_not_matter(self, spark):
        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
        df2 = spark.createDataFrame([(2, "b"), (1, "a")], ["id", "v"])
        assert dataframe_checksum(df1) == dataframe_checksum(df2)

    def test_column_selection_order_does_not_matter(self, spark):
        df = spark.createDataFrame([(1, "a")], ["id", "v"])
        assert dataframe_checksum(df.select("id", "v")) == dataframe_checksum(df.select("v", "id"))


class TestSensitivity:
    def test_value_change_changes_checksum(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["id", "v"])
        df2 = spark.createDataFrame([(1, "b")], ["id", "v"])
        assert dataframe_checksum(df1) != dataframe_checksum(df2)

    def test_column_name_change_changes_checksum(self, spark):
        df1 = spark.createDataFrame([(1,)], ["a"])
        df2 = spark.createDataFrame([(1,)], ["b"])
        assert dataframe_checksum(df1) != dataframe_checksum(df2)

    def test_null_is_distinct_from_empty_string(self, spark):
        from pyspark.sql import types as T

        schema = T.StructType([T.StructField("v", T.StringType(), True)])
        df_null = spark.createDataFrame([(None,)], schema)
        df_empty = spark.createDataFrame([("",)], schema)
        assert dataframe_checksum(df_null) != dataframe_checksum(df_empty)

    def test_row_count_change_changes_checksum(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["id", "v"])
        df2 = spark.createDataFrame([(1, "a"), (1, "a")], ["id", "v"])
        assert dataframe_checksum(df1) != dataframe_checksum(df2)

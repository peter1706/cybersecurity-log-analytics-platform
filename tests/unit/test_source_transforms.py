"""Unit tests for the proc/flows/dns pure transforms (no MinIO/Delta required)."""

from jobs.schemas import DNS_COLUMNS, FLOWS_COLUMNS, PROC_COLUMNS
from jobs.transforms import (
    dns_bronze_to_silver,
    flows_bronze_to_silver,
    proc_bronze_to_silver,
    raw_to_bronze,
)


def _raw(spark, columns, rows):
    return spark.createDataFrame(rows, [f"_c{i}" for i in range(len(columns))])


class TestProc:
    def _silver(self, spark, rows):
        return proc_bronze_to_silver(raw_to_bronze(_raw(spark, PROC_COLUMNS, rows), "proc"))

    def test_columns_and_day_derivation(self, spark):
        rows = [("86401", "U1@Dom1", "C1", "P1", "Start")]
        bronze = raw_to_bronze(_raw(spark, PROC_COLUMNS, rows), "proc")
        assert {"source", "day"}.issubset(set(bronze.columns))
        row = bronze.collect()[0]
        assert row["source"] == "proc"
        assert row["day"] == 1  # time=86401 -> day 1

    def test_user_split_and_machine_flag(self, spark):
        rows = [
            ("10", "U1@Dom1", "C1", "P1", "Start"),
            ("20", "C2$@Dom1", "C2", "P2", "End"),
        ]
        by_time = {r["time"]: r for r in self._silver(spark, rows).collect()}
        assert by_time[10]["user_name"] == "U1"
        assert by_time[10]["domain"] == "Dom1"
        assert by_time[10]["user_is_machine"] is False
        assert by_time[20]["user_is_machine"] is True
        assert isinstance(by_time[10]["time"], int)

    def test_deduplicates_identical_rows(self, spark):
        row = ("10", "U1@Dom1", "C1", "P1", "Start")
        assert self._silver(spark, [row, row]).count() == 1


class TestFlows:
    def _silver(self, spark, rows):
        return flows_bronze_to_silver(raw_to_bronze(_raw(spark, FLOWS_COLUMNS, rows), "flows"))

    def test_typing(self, spark):
        rows = [("10", "5", "C1", "N1", "C2", "443", "6", "12", "1024")]
        row = self._silver(spark, rows).collect()[0]
        assert isinstance(row["time"], int)
        assert row["duration"] == 5
        assert row["packet_count"] == 12
        assert row["byte_count"] == 1024
        assert row["dst_port"] == "443"

    def test_deduplicates_identical_flows(self, spark):
        row = ("10", "5", "C1", "N1", "C2", "443", "6", "12", "1024")
        assert self._silver(spark, [row, row, row]).count() == 1


class TestDns:
    def _silver(self, spark, rows):
        return dns_bronze_to_silver(raw_to_bronze(_raw(spark, DNS_COLUMNS, rows), "dns"))

    def test_columns_and_typing(self, spark):
        rows = [("10", "C1", "C2")]
        row = self._silver(spark, rows).collect()[0]
        assert isinstance(row["time"], int)
        assert row["src_comp"] == "C1"
        assert row["resolved_comp"] == "C2"
        assert row["source"] == "dns"

    def test_deduplicates_identical_lookups(self, spark):
        row = ("10", "C1", "C2")
        assert self._silver(spark, [row, row]).count() == 1

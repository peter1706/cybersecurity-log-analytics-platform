"""Unit tests for the pure auth transforms (no MinIO/Delta required)."""

import pytest
from jobs.schemas import AUTH_COLUMNS
from jobs.transforms import auth_bronze_to_silver, auth_silver_to_gold, raw_to_bronze

RAW_COLS = [f"_c{i}" for i in range(len(AUTH_COLUMNS))]


def _raw(spark, rows):
    return spark.createDataFrame(rows, RAW_COLS)


class TestRawToBronze:
    def test_columns_and_day_derivation(self, spark):
        rows = [
            ("1", "U1@Dom1", "U2@Dom1", "C1", "C2", "Kerberos", "Network", "LogOn", "Success"),
            ("86401", "U1@Dom1", "U2@Dom1", "C1", "C2", "Kerberos", "Network", "LogOn", "Fail"),
        ]
        bronze = raw_to_bronze(_raw(spark, rows), "auth").orderBy("time")
        assert set(AUTH_COLUMNS).issubset(set(bronze.columns))
        assert {"source", "day"}.issubset(set(bronze.columns))
        collected = bronze.collect()
        assert collected[0]["day"] == 0  # time=1  -> day 0
        assert collected[1]["day"] == 1  # time=86401 -> day 1
        assert collected[0]["source"] == "auth"

    def test_rejects_unsupported_source(self, spark):
        with pytest.raises(ValueError):
            raw_to_bronze(_raw(spark, [("1", "", "", "", "", "", "", "", "")]), "proc")


class TestBronzeToSilver:
    def _silver(self, spark, rows):
        return auth_bronze_to_silver(raw_to_bronze(_raw(spark, rows), "auth"))

    def test_typing_and_flags(self, spark):
        rows = [
            ("10", "U1@Dom1", "U2@Dom1", "C1", "C2", "Kerberos", "Network", "LogOn", "Success"),
            ("20", "C3$@Dom1", "U2@Dom1", "C3", "C2", "NTLM", "Network", "LogOn", "Fail"),
            ("30", "U1@Dom1", "U2@Dom1", "C1", "C2", "Kerberos", "Network", "LogOn", "?"),
        ]
        silver = self._silver(spark, rows)
        by_time = {r["time"]: r for r in silver.collect()}

        assert by_time[10]["auth_success"] is True
        assert by_time[20]["auth_success"] is False
        assert by_time[30]["auth_success"] is None  # unknown -> null
        assert by_time[20]["src_user_is_machine"] is True  # trailing $
        assert by_time[10]["src_user_is_machine"] is False
        assert by_time[10]["src_user_name"] == "U1"
        assert by_time[10]["src_domain"] == "Dom1"
        assert isinstance(by_time[10]["time"], int)

    def test_deduplicates_identical_rows(self, spark):
        row = ("10", "U1@Dom1", "U2@Dom1", "C1", "C2", "Kerberos", "Network", "LogOn", "Success")
        silver = self._silver(spark, [row, row])
        assert silver.count() == 1


class TestSilverToGold:
    def test_per_computer_features(self, spark):
        rows = [
            # three auths targeting C2: 2 success, 1 fail; two distinct src comps/users
            ("10", "U1@Dom1", "U9@Dom1", "C1", "C2", "Kerberos", "Network", "LogOn", "Success"),
            ("20", "U2@Dom1", "U9@Dom1", "C3", "C2", "Kerberos", "Network", "LogOn", "Success"),
            ("30", "C4$@Dom1", "U9@Dom1", "C4", "C2", "NTLM", "Network", "LogOn", "Fail"),
        ]
        silver = auth_bronze_to_silver(raw_to_bronze(_raw(spark, rows), "auth"))
        gold = auth_silver_to_gold(silver)
        c2 = next(r for r in gold.collect() if r["computer"] == "C2")

        assert c2["auth_count"] == 3
        assert c2["auth_success_count"] == 2
        assert c2["auth_failure_count"] == 1
        assert c2["distinct_src_comp"] == 3
        assert c2["distinct_human_users"] == 2  # C4$ excluded as machine
        assert abs(c2["auth_failure_rate"] - (1 / 3)) < 1e-9

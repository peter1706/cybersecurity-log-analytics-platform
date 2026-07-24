"""Unit tests for the unified per-computer Gold feature builders and assembler.

Pure transforms only (no MinIO/Delta): each builder is exercised on tiny Silver
DataFrames, then the assembler is checked for the contract's join, null (0 vs
NULL), column-set, and source_present semantics.
"""

from jobs.schemas import AUTH_COLUMNS, DNS_COLUMNS, FLOWS_COLUMNS, PROC_COLUMNS
from jobs.transforms import (
    COMPUTER_FEATURE_COLUMNS,
    assemble_computer_features,
    auth_bronze_to_silver,
    auth_computer_features,
    dns_bronze_to_silver,
    dns_computer_features,
    flows_bronze_to_silver,
    flows_computer_features,
    proc_bronze_to_silver,
    proc_computer_features,
    raw_to_bronze,
)


def _raw(spark, columns, rows):
    return spark.createDataFrame(rows, [f"_c{i}" for i in range(len(columns))])


def _auth_silver(spark, rows):
    return auth_bronze_to_silver(raw_to_bronze(_raw(spark, AUTH_COLUMNS, rows), "auth"))


def _proc_silver(spark, rows):
    return proc_bronze_to_silver(raw_to_bronze(_raw(spark, PROC_COLUMNS, rows), "proc"))


def _flows_silver(spark, rows):
    return flows_bronze_to_silver(raw_to_bronze(_raw(spark, FLOWS_COLUMNS, rows), "flows"))


def _dns_silver(spark, rows):
    return dns_bronze_to_silver(raw_to_bronze(_raw(spark, DNS_COLUMNS, rows), "dns"))


def _by_id(df):
    """Collect a per-computer feature frame into a {computer_id: Row} dict."""
    return {row["computer_id"]: row for row in df.collect()}


class TestAuthComputerFeatures:
    def test_out_and_in_roles(self, spark):
        rows = [
            # C1 authenticates outbound to C2 (success) and C3 (fail, known)
            ("10", "U1@D", "U9@D", "C1", "C2", "Kerberos", "Network", "LogOn", "Success"),
            ("20", "U1@D", "U8@D", "C1", "C3", "Kerberos", "Network", "LogOn", "Fail"),
            # machine account C9$ authenticates inbound to C2
            ("30", "C9$@D", "U7@D", "C4", "C2", "NTLM", "Network", "LogOn", "Success"),
        ]
        feats = _by_id(auth_computer_features(_auth_silver(spark, rows)))

        c1 = feats["C1"]
        assert c1["auth_out_event_count"] == 2
        assert c1["auth_out_distinct_targets"] == 2  # C2, C3
        assert c1["auth_out_distinct_users"] == 1  # U1
        assert c1["auth_out_distinct_human_users"] == 1
        assert c1["auth_out_failed_count"] == 1
        assert abs(c1["auth_out_failure_rate"] - 0.5) < 1e-9  # 1 fail / 2 known
        assert c1["auth_in_event_count"] is None  # C1 never a destination (assembler -> 0)

        c2 = feats["C2"]
        assert c2["auth_in_event_count"] == 2  # from C1 and C4
        assert c2["auth_in_distinct_sources"] == 2

    def test_failure_rate_null_when_no_known_outcome(self, spark):
        rows = [
            ("10", "U1@D", "U9@D", "C1", "C2", "Kerberos", "Network", "LogOn", "?"),
        ]
        c1 = _by_id(auth_computer_features(_auth_silver(spark, rows)))["C1"]
        assert c1["auth_out_event_count"] == 1
        assert c1["auth_out_failed_count"] == 0
        assert c1["auth_out_failure_rate"] is None  # undefined, not 0.0

    def test_aggregates_across_the_whole_window(self, spark):
        """Events spread across days collapse to one row per computer (window correctness).

        Builders aggregate whatever window the job hands them regardless of the
        event ``day`` -- days 4 (t=345601), 5 (t=432001), 6 (t=518401) all fold into
        one C1 outbound row.
        """
        rows = [
            ("345601", "U1@D", "U9@D", "C1", "C2", "Kerberos", "Network", "LogOn", "Success"),
            ("432001", "U2@D", "U8@D", "C1", "C3", "Kerberos", "Network", "LogOn", "Fail"),
            ("518401", "U1@D", "U7@D", "C1", "C2", "Kerberos", "Network", "LogOn", "Success"),
        ]
        silver = _auth_silver(spark, rows)
        assert {r["day"] for r in silver.collect()} == {4, 5, 6}  # spans the window

        feats = _by_id(auth_computer_features(silver))
        assert len([c for c in feats if c == "C1"]) == 1  # one row per computer
        c1 = feats["C1"]
        assert c1["auth_out_event_count"] == 3
        assert c1["auth_out_distinct_targets"] == 2  # C2, C3
        assert c1["auth_out_failed_count"] == 1


class TestProcComputerFeatures:
    def test_counts_and_human_users(self, spark):
        rows = [
            ("10", "U1@D", "C1", "P1", "Start"),
            ("20", "U1@D", "C1", "P2", "End"),
            ("30", "C2$@D", "C1", "P1", "Start"),
        ]
        c1 = next(r for r in proc_computer_features(_proc_silver(spark, rows)).collect())
        assert c1["computer_id"] == "C1"
        assert c1["proc_start_count"] == 2  # two Start events
        assert c1["proc_distinct_process_names"] == 2  # P1, P2
        assert c1["proc_distinct_users"] == 2  # U1, C2$
        assert c1["proc_distinct_human_users"] == 1  # C2$ excluded


class TestFlowsComputerFeatures:
    def test_roles_sums_and_anonymized_ports(self, spark):
        rows = [
            # C1 -> C2 on anonymized src port N1, numeric dst port 443
            ("10", "5", "C1", "N1", "C2", "443", "6", "12", "1000"),
            # C1 -> C3 on numeric src port 1024, anonymized dst port N2
            ("20", "5", "C1", "1024", "C3", "N2", "6", "8", "500"),
        ]
        feats = _by_id(flows_computer_features(_flows_silver(spark, rows)))

        c1 = feats["C1"]
        assert c1["flows_out_count_distinct"] == 2
        assert c1["flows_out_distinct_targets"] == 2  # C2, C3
        assert c1["flows_out_bytes_sum_distinct"] == 1500
        assert c1["flows_out_packets_sum_distinct"] == 20
        assert c1["flows_out_anonymized_port_count_distinct"] == 1  # only N1 (src side)

        c2 = feats["C2"]
        assert c2["flows_in_count_distinct"] == 1
        assert c2["flows_in_anonymized_port_count_distinct"] == 0  # dst port 443 numeric
        c3 = feats["C3"]
        assert c3["flows_in_anonymized_port_count_distinct"] == 1  # dst port N2


class TestDnsComputerFeatures:
    def test_lookup_counts(self, spark):
        rows = [
            ("10", "C1", "C2"),
            ("20", "C1", "C3"),
            ("30", "C1", "C2"),  # duplicate resolved host
        ]
        c1 = next(r for r in dns_computer_features(_dns_silver(spark, rows)).collect())
        assert c1["computer_id"] == "C1"
        assert c1["dns_lookup_count"] == 3
        assert c1["dns_distinct_resolved_hosts"] == 2  # C2, C3


class TestAssembleComputerFeatures:
    def _assemble(self, spark, *, anchor_day=6, window_days=7, present=None):
        auth = auth_computer_features(
            _auth_silver(
                spark,
                [("10", "U1@D", "U9@D", "C1", "C2", "Kerberos", "Network", "LogOn", "Success")],
            )
        )
        proc = proc_computer_features(_proc_silver(spark, [("20", "U1@D", "C1", "P1", "Start")]))
        flows = flows_computer_features(
            _flows_silver(spark, [("30", "5", "C1", "N1", "C2", "443", "6", "12", "1000")])
        )
        dns = dns_computer_features(_dns_silver(spark, [("40", "C5", "C6")]))
        present = present or {"auth": True, "proc": True, "flows": False, "dns": True}
        return assemble_computer_features(
            auth=auth,
            proc=proc,
            flows=flows,
            dns=dns,
            anchor_day=anchor_day,
            window_days=window_days,
            source_present=present,
        )

    def test_exact_output_schema(self, spark):
        gold = self._assemble(spark)
        assert gold.columns == COMPUTER_FEATURE_COLUMNS
        assert len(COMPUTER_FEATURE_COLUMNS) == 3 + 12 + 4 + 10 + 2 + 4  # = 35

    def test_union_of_all_computers_and_null_semantics(self, spark):
        gold = self._assemble(spark, anchor_day=6, window_days=7)
        rows = {r["computer_id"]: r for r in gold.collect()}

        # Every computer active in any source in any role gets a row (ML-ENT-3):
        # C1 (auth/proc/flows src), C2 (auth/flows dst), C5 (dns lookup owner).
        assert {"C1", "C2", "C5"}.issubset(set(rows))
        # DNS resolved host is an attribute, not an active computer -> no row.
        assert "C6" not in rows

        # C5 only issued a DNS lookup -> auth/proc/flows counters coalesced to 0.
        c5 = rows["C5"]
        assert c5["dns_lookup_count"] == 1
        assert c5["auth_out_event_count"] == 0  # ML-NULL-1: absent source -> 0
        assert c5["proc_start_count"] == 0
        assert c5["flows_out_count_distinct"] == 0
        # Undefined ratio stays NULL, not 0.0 (ML-NULL-2).
        assert c5["auth_out_failure_rate"] is None

        # Window key columns are stamped on every row (ML-ENT-2).
        assert c5["anchor_day"] == 6
        assert c5["window_days"] == 7

    def test_source_present_flags(self, spark):
        gold = self._assemble(
            spark, present={"auth": True, "proc": True, "flows": False, "dns": True}
        )
        row = gold.collect()[0]
        assert row["source_present_auth"] is True
        assert row["source_present_flows"] is False

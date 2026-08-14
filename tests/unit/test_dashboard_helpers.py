"""Unit tests for dashboard auth, anomaly scoring, and Airflow client shaping."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

import anomaly
import auth
import charts
import feature_contract
import feature_labels
import metrics
import partition_view
import schema_view
import theme
import volume_view
import watchlist
from airflow_client import AirflowApiError, AirflowAuthError, AirflowClient, AirflowConfig


def test_credentials_match_accepts_exact_pair():
    assert auth.credentials_match(
        "dashboard",
        "dashboard",
        expected_username="dashboard",
        expected_password="dashboard",
    )


def test_credentials_match_rejects_wrong_password():
    assert not auth.credentials_match(
        "dashboard",
        "nope",
        expected_username="dashboard",
        expected_password="dashboard",
    )


def test_credentials_match_requires_configured_secrets():
    with pytest.raises(ValueError, match="not configured"):
        auth.credentials_match("u", "p", expected_username="", expected_password="x")


def test_score_computers_is_deterministic_and_ranks_outlier():
    df = pd.DataFrame(
        {
            "computer_id": ["c1", "c2", "c3", "c4"],
            "anchor_day": [6, 6, 6, 6],
            "window_days": [7, 7, 7, 7],
            "auth_out_event_count": [1, 1, 1, 100],
            "proc_start_count": [2, 2, 2, 2],
            "source_present_auth": [True, True, True, True],
        }
    )
    first = anomaly.score_computers(df)
    second = anomaly.score_computers(df)
    assert (
        first.scored[anomaly.SCORE_COLUMN].tolist() == second.scored[anomaly.SCORE_COLUMN].tolist()
    )
    top = anomaly.top_computers(first.scored, n=1)
    assert top.iloc[0]["computer_id"] == "c4"
    assert anomaly.SCORE_COLUMN not in ["source_present_auth"]


def test_score_computers_requires_computer_id():
    with pytest.raises(ValueError, match="computer_id"):
        anomaly.score_computers(pd.DataFrame({"auth_out_event_count": [1]}))


def test_score_computers_log1p_keeps_sparse_driver_visible():
    """A heavy-tailed auth volume must not always beat a true sparse outlier."""
    n = 40
    df = pd.DataFrame(
        {
            "computer_id": [f"c{i}" for i in range(n)],
            "anchor_day": [6] * n,
            "window_days": [7] * n,
            # Dense moderate auth volume for most hosts; one busy host.
            "auth_out_event_count": [30] * (n - 2) + [80, 30],
            # Sparse feature: almost all zeros, one clear DNS outlier.
            "dns_lookup_count": [0] * (n - 1) + [500],
        }
    )
    result = anomaly.score_computers(df)
    dns_row = result.contributions[
        (result.contributions["computer_id"] == f"c{n - 1}")
        & (result.contributions["rank"] <= 1)
    ]
    assert not dns_row.empty
    assert dns_row.iloc[0]["feature"] == "dns_lookup_count"


def test_airflow_client_fetch_token_and_trigger_body():
    config = AirflowConfig(base_url="http://airflow:8080/", username="admin", password="secret")
    client = AirflowClient(config)

    with patch("airflow_client._request") as mock_request:
        mock_request.return_value = {"access_token": "tok"}
        assert client.fetch_token() == "tok"
        mock_request.assert_called_once_with(
            "POST",
            "http://airflow:8080/auth/token",
            body={"username": "admin", "password": "secret"},
        )

    with patch("airflow_client._request") as mock_request:
        mock_request.side_effect = [
            {"access_token": "tok"},
            {"dag_run_id": "run-1", "state": "queued"},
        ]
        out = client.trigger_dag_run(
            "feature_reprocessing",
            conf={"day": 6, "window_days": 10},
            dag_run_id="run-1",
        )
        assert out["dag_run_id"] == "run-1"
        trigger_call = mock_request.call_args_list[1]
        assert trigger_call.args[0] == "POST"
        assert trigger_call.args[1].endswith("/api/v2/dags/feature_reprocessing/dagRuns")
        assert trigger_call.kwargs["headers"]["Authorization"] == "Bearer tok"
        assert trigger_call.kwargs["body"]["logical_date"] is None
        assert trigger_call.kwargs["body"]["conf"] == {"day": 6, "window_days": 10}
        assert trigger_call.kwargs["body"]["dag_run_id"] == "run-1"


def test_airflow_client_maps_http_errors():
    config = AirflowConfig(base_url="http://airflow:8080", username="a", password="b")
    client = AirflowClient(config)
    with patch("airflow_client._request", side_effect=AirflowApiError("boom")):
        with pytest.raises(AirflowAuthError, match="boom"):
            client.fetch_token()


def test_phase_volume_rows_lists_before_after_each_hop():
    volume = {
        "event_days": [0, 1],
        "landing": [
            {"source": "auth", "day": 0, "record_count": 100},
            {"source": "auth", "day": 1, "record_count": 50},
        ],
        "bronze": [{"source": "auth", "day": 0, "record_count": 140}],
        "silver": [{"source": "auth", "day": 0, "record_count": 130}],
        "gold": {"anchor_day": 1, "window_days": 2, "record_count": 10},
        "delivered": {"record_count": 10},
    }
    rows = volume_view.phase_volume_rows(volume)
    assert [r["phase"] for r in rows] == [
        "Landing → Bronze",
        "Bronze → Silver",
        "Silver → Gold",
        "Gold → Delivered",
    ]
    assert rows[0] == {
        "phase": "Landing → Bronze",
        "rows_before": 150,
        "rows_after": 140,
        "delta": -10,
    }
    assert rows[-1]["rows_before"] == 10
    assert rows[-1]["rows_after"] == 10
    assert rows[-1]["delta"] == 0
    totals = volume_view.layer_volume_totals(volume)
    assert totals[0] == {"layer": "landing", "record_count": 150}
    assert totals[-1] == {"layer": "delivered", "record_count": 10}
    assert volume_view.layer_row_total(volume, "landing") == 150
    assert volume_view.layer_row_total({}, "landing") is None


def _manifest(
    *,
    window_days: int,
    anchor_day: int,
    created_at: str,
    columns: list[str] | None = None,
) -> dict:
    return {
        "window_days": window_days,
        "anchor_day": anchor_day,
        "record_count": 10,
        "schema_version": "v1",
        "created_at": created_at,
        "checksum_sha256": "abc123def4567890",
        "columns": columns or ["computer_id", "anchor_day", "window_days"],
    }


def test_sort_manifests_newest_first_by_created_at():
    older = _manifest(window_days=7, anchor_day=1, created_at="2026-08-01T00:00:00+00:00")
    newer = _manifest(window_days=5, anchor_day=2, created_at="2026-08-07T12:00:00+00:00")
    ordered = partition_view.sort_manifests_newest_first([older, newer])
    assert partition_view.manifest_key(ordered[0]) == (5, 2)


def test_resolve_active_manifest_defaults_to_newest():
    older = _manifest(window_days=7, anchor_day=1, created_at="2026-08-01T00:00:00+00:00")
    newer = _manifest(window_days=5, anchor_day=2, created_at="2026-08-07T12:00:00+00:00")
    chosen = partition_view.resolve_active_manifest(
        [older, newer],
        active_key=None,
        selected_row=None,
    )
    assert chosen is not None
    assert partition_view.manifest_key(chosen) == (5, 2)


def test_resolve_active_manifest_prefers_selected_row_then_session_then_force():
    a = _manifest(window_days=7, anchor_day=1, created_at="2026-08-01T00:00:00+00:00")
    b = _manifest(window_days=5, anchor_day=2, created_at="2026-08-07T12:00:00+00:00")
    c = _manifest(window_days=3, anchor_day=0, created_at="2026-08-06T00:00:00+00:00")

    from_row = partition_view.resolve_active_manifest(
        [a, b, c],
        active_key=(7, 1),
        selected_row={"window_days": 3, "anchor_day": 0},
    )
    assert from_row is not None
    assert partition_view.manifest_key(from_row) == (3, 0)

    from_session = partition_view.resolve_active_manifest(
        [a, b, c],
        active_key=(7, 1),
        selected_row=None,
    )
    assert from_session is not None
    assert partition_view.manifest_key(from_session) == (7, 1)

    forced = partition_view.resolve_active_manifest(
        [a, b, c],
        active_key=(7, 1),
        selected_row={"window_days": 3, "anchor_day": 0},
        force_key=(5, 2),
    )
    assert forced is not None
    assert partition_view.manifest_key(forced) == (5, 2)


def test_resolve_active_manifest_falls_back_when_session_stale():
    newer = _manifest(window_days=5, anchor_day=2, created_at="2026-08-07T12:00:00+00:00")
    chosen = partition_view.resolve_active_manifest(
        [newer],
        active_key=(7, 99),
        selected_row=None,
    )
    assert chosen is not None
    assert partition_view.manifest_key(chosen) == (5, 2)


def test_overview_schema_and_source_status_lines():
    expected = ["computer_id", "anchor_day", "window_days"]
    ok = _manifest(window_days=5, anchor_day=2, created_at="t", columns=expected)
    bad = _manifest(window_days=5, anchor_day=2, created_at="t", columns=["computer_id"])
    assert partition_view.overview_schema_status(ok, expected) == "✅ Passed"
    assert partition_view.overview_schema_status(bad, expected) == "❌ Failed"

    df = pd.DataFrame(
        {
            "source_present_auth": [True, True],
            "source_present_proc": [True, False],
            "source_present_flows": [True, True],
            "source_present_dns": [True, True],
        }
    )
    assert partition_view.overview_source_lines(df) == [
        "✅ auth",
        "❌ proc",
        "✅ flows",
        "✅ dns",
    ]


def test_feature_labels_cover_contract_columns():
    missing = [
        c for c in feature_contract.EXPECTED_COLUMNS if c not in feature_labels.FEATURE_LABELS
    ]
    assert missing == []
    assert "auth_out_failed_count" not in feature_labels.label_for("auth_out_failed_count")
    assert feature_labels.label_for("auth_out_failed_count") == "Failed outgoing sign-ins"


def test_assign_risk_levels_percentiles_on_100():
    scores = pd.Series([float(100 - i) for i in range(100)])
    risk = feature_labels.assign_risk_levels(scores)
    assert int((risk == "High").sum()) == 1
    assert int((risk == "Medium").sum()) == 4
    assert int((risk == "Normal").sum()) == 95
    assert risk.iloc[0] == "High"
    assert set(risk.iloc[1:5]) == {"Medium"}


def test_assign_risk_levels_single_computer_is_high():
    risk = feature_labels.assign_risk_levels(pd.Series([0.1], index=["a"]))
    assert risk.tolist() == ["High"]


def test_assign_risk_levels_stable_on_ties():
    scores = pd.Series([5.0, 5.0, 5.0, 1.0, 0.0])
    risk = feature_labels.assign_risk_levels(scores)
    assert int((risk == "High").sum()) == 1
    assert int((risk == "Medium").sum()) == 0
    assert risk.iloc[0] == "High"


def test_needs_attention_counts_high_and_medium():
    scored = pd.DataFrame(
        {
            "computer_id": [f"c{i}" for i in range(100)],
            anomaly.SCORE_COLUMN: [float(100 - i) for i in range(100)],
        }
    )
    assert feature_labels.needs_attention(scored) == 5


def test_standout_sentences_use_plain_labels_not_column_names():
    df = pd.DataFrame(
        {
            "computer_id": ["c1", "c2", "c3", "c4"],
            "anchor_day": [6, 6, 6, 6],
            "window_days": [7, 7, 7, 7],
            "auth_out_failed_count": [1, 1, 1, 100],
            "proc_start_count": [2, 2, 2, 2],
            "source_present_auth": [True, True, True, True],
        }
    )
    result = anomaly.score_computers(df)
    sentences = feature_labels.standout_sentences(result, n=1)
    assert sentences
    joined = " ".join(sentences).lower()
    assert "auth_out_failed_count" not in joined
    assert "failed outgoing sign-ins" in joined
    assert "c4" in joined


def test_source_health_and_delivery_labels_are_consumer_facing():
    expected = feature_contract.EXPECTED_COLUMNS
    ok = _manifest(window_days=5, anchor_day=2, created_at="2026-08-07T12:00:00Z", columns=expected)
    assert feature_labels.data_quality_status(ok, expected) == "✓ Ready"
    assert "5-day view" in feature_labels.delivery_choice_label(ok)
    assert "2026-08-07" in feature_labels.delivery_choice_label(ok)
    df = pd.DataFrame(
        {
            "source_present_auth": [True, True],
            "source_present_proc": [True, False],
            "source_present_flows": [True, True],
            "source_present_dns": [True, True],
        }
    )
    assert feature_labels.source_health_lines(df) == [
        "✓ Authentication events",
        "✗ Process starts",
        "✓ Network traffic",
        "✓ DNS lookups",
    ]


def _activity_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "computer_id": ["c1", "c2", "c3"],
            "auth_out_event_count": [10, 20, 30],
            "auth_in_event_count": [5, 5, 5],
            "auth_out_failed_count": [1, 0, 6],
            "auth_in_failed_count": [0, 0, 3],
            "proc_start_count": [4, 8, 12],
            "flows_out_count_distinct": [2, 3, 4],
            "flows_in_count_distinct": [1, 1, 1],
            "flows_out_bytes_sum_distinct": [1000, 5000, 200],
            "flows_in_bytes_sum_distinct": [100, 500, 20],
            "dns_lookup_count": [7, 7, 7],
        }
    )


def test_activity_totals_sum_paired_columns():
    totals = metrics.activity_totals(_activity_frame())
    assert totals["computers"] == 3
    assert totals["sign_ins"] == 75
    assert totals["failed_sign_ins"] == 10
    assert totals["program_starts"] == 24
    assert totals["network_connections"] == 12
    assert totals["network_bytes"] == 6820
    assert totals["web_lookups"] == 21


def test_activity_totals_tolerate_missing_columns():
    totals = metrics.activity_totals(pd.DataFrame({"computer_id": ["c1"]}))
    assert totals["computers"] == 1
    assert totals["sign_ins"] == 0
    assert totals["network_bytes"] == 0


def test_sign_in_success_rate_and_undefined_case():
    rate = metrics.sign_in_success_rate(_activity_frame())
    assert rate is not None
    assert rate == pytest.approx(1 - 10 / 75)
    assert metrics.sign_in_success_rate(pd.DataFrame({"computer_id": ["c1"]})) is None


def test_top_computers_by_ranks_combined_totals():
    ranked = metrics.top_computers_by(
        _activity_frame(), metrics.BYTES_COLUMNS, n=2, value_name="bytes"
    )
    assert list(ranked["computer_id"]) == ["c2", "c1"]
    assert int(ranked.iloc[0]["bytes"]) == 5500


def test_top_computers_by_drops_zero_rows():
    df = pd.DataFrame({"computer_id": ["a", "b"], "auth_out_failed_count": [0, 0]})
    assert metrics.top_computers_by(df, metrics.SIGN_IN_FAILED_COLUMNS).empty


def test_activity_mix_covers_four_families():
    mix = metrics.activity_mix(_activity_frame())
    assert [row["activity"] for row in mix] == [
        "Sign-ins",
        "Programs",
        "Network",
        "Web lookups",
    ]
    assert mix[0]["events"] == 75


def test_number_and_byte_formatting():
    assert metrics.format_count(999) == "999"
    assert metrics.format_count(1234) == "1.23K"
    assert metrics.format_count(2_840_000) == "2.84M"
    assert metrics.format_count(None) == "—"
    assert metrics.format_bytes(512) == "512 B"
    assert metrics.format_bytes(5 * 1024**3) == "5 GB"
    assert metrics.format_percent(0.9876) == "98.8%"
    assert metrics.format_percent(None) == "—"


def test_source_health_flags_pair_names_with_status():
    df = pd.DataFrame(
        {
            "source_present_auth": [True, True],
            "source_present_proc": [True, False],
        }
    )
    assert feature_labels.source_health_flags(df) == [
        ("Authentication events", True),
        ("Process starts", False),
        ("Network traffic", False),
        ("DNS lookups", False),
    ]


def test_key_card_escapes_content_and_applies_accent():
    markup = theme.key_card("Sign-ins", "2.84M", "10 failed", accent="amber")
    assert theme.ACCENTS["amber"] in markup
    assert "2.84M" in markup
    assert "<script>" not in theme.key_card("<script>", "1")


def test_health_card_marks_failures():
    markup = theme.health_card("Delivery health", [("Schema check", "Failed", "bad")])
    assert "health-bad" in markup
    assert "Failed" in markup


def test_health_rows_render_without_card_frame():
    markup = theme.health_rows([("Records", "400", "plain")])
    assert "health-row" in markup
    assert "glass-card" not in markup
    assert "400" in markup


def test_health_group_collapses_rows_behind_a_summary():
    rows = [("Authentication events", "Delivered", "ok"), ("DNS lookups", "Missing", "bad")]
    markup = theme.health_group("Data sources", ("1 of 2 delivered", "bad"), rows)
    assert markup.startswith("<details")
    assert "<summary" in markup
    assert "1 of 2 delivered" in markup
    assert "health-bad" in markup
    # Collapsed by default, and every grouped row is still present.
    assert " open" not in markup
    assert "Authentication events" in markup
    assert "DNS lookups" in markup
    assert " open" in theme.health_group("Data sources", ("2 of 2", "ok"), rows, start_open=True)


def test_watchlist_row_marks_risk_selection_and_bar_share():
    markup = theme.watchlist_row(
        3,
        "C1234",
        "Failed outgoing sign-ins",
        "High",
        8.25,
        fill=0.5,
        selected=True,
    )
    assert theme.ACCENTS["rose"] in markup
    assert "wl-row-selected" in markup
    assert ">03<" in markup
    assert "C1234" in markup
    assert ">8.2<" in markup
    assert "width:50.0%" in markup

    plain = theme.watchlist_row(1, "<script>", "Data sent", "Medium", 2.0, fill=4.0)
    assert "wl-row-selected" not in plain
    assert "<script>" not in plain
    # An out-of-range share is clamped so the bar cannot overflow its track.
    assert "width:100.0%" in plain


def test_schema_diff_reports_missing_unexpected_and_order():
    expected = ["a", "b", "c"]
    assert schema_view.schema_match(expected, expected)
    missing = schema_view.schema_diff(["a", "c"], expected)
    assert missing["ok"] is False
    assert missing["missing"] == ["b"]
    assert missing["unexpected"] == []
    extra = schema_view.schema_diff(["a", "b", "c", "x"], expected)
    assert extra["unexpected"] == ["x"]
    reordered = schema_view.schema_diff(["a", "c", "b"], expected)
    assert reordered["order_mismatch"] is True
    assert reordered["first_order_diff"] == (1, "b", "c")


def test_schema_summary_and_column_rows():
    expected = ["computer_id", "anchor_day"]
    manifest = {
        "schema_version": "v1",
        "dataset": "computer_features",
        "dataset_version": "computer_features-w7-d06",
        "columns": ["computer_id"],
    }
    summary = dict(schema_view.schema_summary_rows(manifest, expected))
    assert summary["Status"] == "Failed"
    assert summary["Missing"] == "anchor_day"
    assert summary["Expected columns"] == "2"
    assert summary["Delivered columns"] == "1"
    rows = schema_view.schema_column_rows(
        ["computer_id", "extra"],
        expected,
        label_for=lambda c: c.upper(),
    )
    by_column = {row["column"]: row for row in rows}
    assert by_column["computer_id"]["status"] == "ok"
    assert by_column["anchor_day"]["status"] == "missing"
    assert by_column["extra"]["status"] == "unexpected"
    assert by_column["extra"]["label"] == "EXTRA"


def test_chart_builders_produce_specs():
    assert charts.success_donut(0.87).to_dict()["layer"]
    assert charts.activity_mix_bars(metrics.activity_mix(_activity_frame())).to_dict()["mark"]
    ranked = metrics.top_computers_by(_activity_frame(), metrics.BYTES_COLUMNS, value_name="bytes")
    spec = charts.ranked_bars(ranked, value_column="bytes", value_title="Bytes").to_dict()
    assert spec["encoding"]["y"]["field"] == "computer_id"


def test_triage_kpis_counts_and_theme():
    scored, contrib = _attention_fixture()
    kpis = watchlist.triage_kpis(scored, contrib)
    assert kpis["computers"] == 100
    assert kpis["attention"] == 5
    assert kpis["high"] == 1
    assert kpis["medium"] == 4
    assert kpis["highest_id"] == "c0"
    assert kpis["highest_risk"] == "High"
    assert kpis["theme_label"] == feature_labels.label_for("flows_out_bytes_sum_distinct")
    assert kpis["theme_count"] == 3


def test_watchlist_rows_cap_filter_search_and_labels():
    scored, contrib = _attention_fixture()
    rows = watchlist.watchlist_rows(scored, contrib, risk_filter="all", cap=3)
    assert len(rows) == 3
    assert rows[0]["computer_id"] == "c0"
    assert rows[0]["rank"] == 1
    assert rows[0]["reason"] == feature_labels.label_for("flows_out_bytes_sum_distinct")
    high_only = watchlist.watchlist_rows(scored, contrib, risk_filter="high", cap=50)
    assert len(high_only) == 1 and high_only[0]["risk"] == "High"
    found = watchlist.watchlist_rows(scored, contrib, search="c3", cap=50)
    assert [r["computer_id"] for r in found] == ["c3"]


def test_watchlist_caption_and_filtered_total():
    scored, _contrib = _attention_fixture()
    assert watchlist.watchlist_caption(80, 50) == "Showing top 50 of 80"
    assert watchlist.watchlist_caption(20, 20) is None
    assert watchlist.watchlist_filtered_total(scored, risk_filter="all") == 5
    assert watchlist.watchlist_filtered_total(scored, risk_filter="high") == 1


def test_risk_pill_colours_by_risk_level():
    high = theme.risk_pill("C1", "High")
    medium = theme.risk_pill("C1", "Medium")
    assert theme.ACCENTS["rose"] in high
    assert theme.ACCENTS["amber"] in medium
    assert "High risk" in high


def test_driver_frequency_and_family_use_attention_only():
    scored, contrib = _attention_fixture()
    freq = watchlist.driver_frequency(scored, contrib, n=5)
    assert freq[0] == {
        "label": feature_labels.label_for("flows_out_bytes_sum_distinct"),
        "count": 3,
    }
    assert sum(row["count"] for row in freq) == 5  # attention computers only
    families = watchlist.attention_by_family(scored, contrib)
    by_label = {row["label"]: row["count"] for row in families}
    assert by_label[feature_labels.SOURCE_LABELS["flows"]] == 3
    assert by_label[feature_labels.SOURCE_LABELS["auth"]] == 2


def test_count_bars_uses_label_and_count_fields():
    spec = charts.count_bars(
        [{"label": "Data sent", "count": 3}], value_title="Computers"
    ).to_dict()
    assert spec["encoding"]["y"]["field"] == "label"
    assert spec["encoding"]["x"]["field"] == "count"


def test_driver_rows_live_in_watchlist():
    contrib = pd.DataFrame(
        {
            "computer_id": ["c_high", "c_high"],
            "feature": ["auth_out_failed_count", "dns_lookup_count"],
            "abs_z": [4.0, 2.0],
            "rank": [1.0, 2.0],
        }
    )
    rows = watchlist.driver_rows(contrib, "c_high", n=2)
    assert rows[0]["label"] == feature_labels.label_for("auth_out_failed_count")
    assert watchlist.driver_rows(contrib, "missing") == []
    assert watchlist.default_selected_computer(_scored_frame()) == "c_high"
    assert watchlist.default_selected_computer(pd.DataFrame()) is None


def _attention_fixture():
    scored = pd.DataFrame(
        {
            "computer_id": [f"c{i}" for i in range(100)],
            anomaly.SCORE_COLUMN: [float(100 - i) for i in range(100)],
        }
    )
    contrib_rows = []
    for i, feat in enumerate(
        ["flows_out_bytes_sum_distinct"] * 3
        + ["auth_out_failed_count"] * 2
        + ["proc_start_count"] * 95
    ):
        contrib_rows.append(
            {"computer_id": f"c{i}", "feature": feat, "abs_z": 3.0, "rank": 1.0}
        )
    return scored, pd.DataFrame(contrib_rows)


def _scored_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "computer_id": ["c_norm", "c_med", "c_high", "c_norm2"],
            "auth_out_event_count": [10, 50, 200, 12],
            "auth_in_event_count": [5, 10, 80, 6],
            "flows_out_bytes_sum_distinct": [1000, 5000, 90000, 1200],
            "flows_in_bytes_sum_distinct": [100, 500, 10000, 150],
            anomaly.SCORE_COLUMN: [0.2, 1.5, 3.2, 0.3],
        }
    )


def test_driver_bars_use_label_field():
    rows = [
        {
            "label": "Failed outgoing sign-ins",
            "abs_z": 4.0,
            "feature": "auth_out_failed_count",
        },
        {
            "label": "Data sent",
            "abs_z": 2.0,
            "feature": "flows_out_bytes_sum_distinct",
        },
    ]
    spec = charts.driver_bars(rows).to_dict()
    assert spec["encoding"]["y"]["field"] == "label"


def test_delivery_labels_distinguish_same_date_and_size():
    a = _manifest(window_days=7, anchor_day=2, created_at="2026-07-31T12:00:00Z")
    b = _manifest(window_days=7, anchor_day=6, created_at="2026-07-31T12:00:00Z")
    label_a = feature_labels.delivery_choice_label(a)
    label_b = feature_labels.delivery_choice_label(b)
    assert label_a != label_b
    assert "day 2" in label_a
    assert "day 6" in label_b

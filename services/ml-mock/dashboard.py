#!/usr/bin/env python3
"""Always-on Streamlit dashboard over the delivered store (host port 8501).

Authenticated consumer cockpit: pick a recent delivery, review plain-language
status cards and unusual-computer findings, and optionally rebuild a window
via the Airflow API. Feature rows and technical column names are not shown.

Reads *only* the ``delivered`` bucket for data (same least-privilege boundary
as ``ml_consume``). Joins ``airflow-net`` solely to trigger/poll reprocessing.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime

import boto3
import pandas as pd
import streamlit as st
from botocore.client import BaseClient, Config

import anomaly
import auth
import charts
import feature_contract
import feature_labels
import partition_view
import schema_view
import theme
import volume_view
import watchlist
from airflow_client import AirflowApiError, AirflowAuthError, AirflowClient, AirflowConfig
from catalog import read_secret
from catalog.parquet_encryption import read_encrypted_parquet

DATASET = "computer_features"
REPROCESS_DAG = "feature_reprocessing"
logger = logging.getLogger(__name__)


def _s3_client() -> BaseClient:
    """Return a boto3 S3 client using the delivered-scoped MinIO account."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=read_secret("minio_ml_consumer_key", env="MINIO_ML_CONSUMER_KEY"),
        aws_secret_access_key=read_secret(
            "minio_ml_consumer_secret", env="MINIO_ML_CONSUMER_SECRET"
        ),
        config=Config(signature_version="s3v4"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _list_manifests(client: BaseClient, bucket: str) -> list[dict]:
    """List every delivery manifest under ``computer_features/``."""
    paginator = client.get_paginator("list_objects_v2")
    manifests: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{DATASET}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("manifest.json"):
                body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                manifests.append(json.loads(body))
    return manifests


def _find_manifest(
    manifests: list[dict],
    *,
    window_days: int,
    anchor_day: int,
) -> dict | None:
    """Return the manifest for ``(window_days, anchor_day)``, if present."""
    return partition_view.find_manifest(
        manifests,
        window_days=window_days,
        anchor_day=anchor_day,
    )


def _load_partition(client: BaseClient, bucket: str, manifest: dict, key: str) -> pd.DataFrame:
    """Decrypt the delivered Parquet for ``manifest`` into a pandas DataFrame."""
    ciphertext = client.get_object(Bucket=bucket, Key=manifest["data_object"])["Body"].read()
    return read_encrypted_parquet(ciphertext, key).to_pandas()


def _airflow_client() -> AirflowClient:
    """Build an Airflow client from env + the admin password secret."""
    return AirflowClient(
        AirflowConfig(
            base_url=os.environ.get("AIRFLOW_API_URL", "http://airflow-api-server:8080"),
            username=os.environ.get("AIRFLOW_ADMIN_USER", "admin"),
            password=read_secret("airflow_admin_password", env="AIRFLOW_ADMIN_PASSWORD"),
        )
    )


def _render_login() -> bool:
    """Render the login form; return True when the session is authenticated."""
    if st.session_state.get("authenticated"):
        return True

    st.title("Cybersecurity Log Analytics Dashboard")
    st.caption("Sign in to view recent security deliveries and unusual activity.")
    with st.form("login"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in")
    if not submitted:
        return False

    expected_user = read_secret("ml_dashboard_username", env="ML_DASHBOARD_USERNAME", default="")
    expected_pass = read_secret("ml_dashboard_password", env="ML_DASHBOARD_PASSWORD", default="")
    try:
        ok = auth.credentials_match(
            username,
            password,
            expected_username=expected_user,
            expected_password=expected_pass,
        )
    except ValueError as exc:
        st.error(str(exc))
        return False
    if not ok:
        st.error("Invalid username or password.")
        return False
    st.session_state.authenticated = True
    st.rerun()
    return True


def _trigger_reprocess(anchor_day: int, window_days: int) -> str:
    """Trigger ``feature_reprocessing`` and return the dag_run_id."""
    run_id = (
        f"dashboard__w{window_days}_d{anchor_day:02d}_"
        f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    )
    client = _airflow_client()
    token = client.fetch_token()
    response = client.trigger_dag_run(
        REPROCESS_DAG,
        conf={"day": anchor_day, "window_days": window_days},
        dag_run_id=run_id,
        token=token,
    )
    return str(response.get("dag_run_id") or run_id)


def _wait_for_dag_run(
    dag_run_id: str,
    *,
    poll_seconds: float = 5.0,
    timeout: float = 3600.0,
) -> str:
    """Poll a DAG run until it leaves a non-terminal state; return final state."""
    client = _airflow_client()
    token = client.fetch_token()
    deadline = time.monotonic() + timeout
    terminal = {"success", "failed", "skipped"}
    status = st.empty()
    while time.monotonic() < deadline:
        payload = client.get_dag_run(REPROCESS_DAG, dag_run_id, token=token)
        state = str(payload.get("state") or "unknown")
        status.info(f"Rebuild run `{dag_run_id}` → **{state}**")
        if state in terminal:
            return state
        time.sleep(poll_seconds)
    raise TimeoutError(f"Timed out waiting for DAG run {dag_run_id}")


def _render_volume_body(volume: dict) -> None:
    """Render the data-coverage tables (no surrounding container)."""
    st.caption(
        "How much data went into this delivery — from raw logs through to "
        "the final feature file."
    )
    st.write(f"Days included: {volume.get('event_days', [])}")

    st.markdown("**Rows before / after each processing step**")
    st.dataframe(
        pd.DataFrame(volume_view.phase_volume_rows(volume)).rename(
            columns={
                "phase": "Step",
                "rows_before": "Rows before",
                "rows_after": "Rows after",
                "delta": "Change",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("**Totals by stage**")
    st.dataframe(
        pd.DataFrame(volume_view.layer_volume_totals(volume)).rename(
            columns={"layer": "Stage", "record_count": "Rows"}
        ),
        use_container_width=True,
        hide_index=True,
    )

    for layer, title in (
        ("landing", "Raw logs"),
        ("bronze", "Cleaned raw data"),
        ("silver", "Prepared events"),
    ):
        rows = volume.get(layer) or []
        if rows:
            st.markdown(f"**{title} by source / day**")
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


@st.dialog("Data coverage details", width="large")
def _coverage_dialog(volume: dict) -> None:
    """Open the coverage tables in a modal, linked from the Records row."""
    _render_volume_body(volume)


def _render_schema_body(manifest: dict) -> None:
    """Render schema-contract details (no surrounding container)."""
    expected = feature_contract.EXPECTED_COLUMNS
    diff = schema_view.schema_diff(manifest.get("columns"), expected)
    st.caption(
        "Delivered columns must match the feature contract exactly — "
        "same names, same order."
    )

    summary = pd.DataFrame(
        schema_view.schema_summary_rows(manifest, expected),
        columns=["Check", "Value"],
    )
    st.dataframe(summary, use_container_width=True, hide_index=True)

    if diff["ok"]:
        st.success("Schema matches the consumer feature contract.")
    else:
        st.error("Schema does not match the consumer feature contract.")

    column_rows = schema_view.schema_column_rows(
        manifest.get("columns"),
        expected,
        label_for=feature_labels.label_for,
    )
    st.markdown("**Column comparison**")
    st.dataframe(
        pd.DataFrame(column_rows).rename(
            columns={
                "position": "Pos.",
                "column": "Column",
                "label": "Meaning",
                "status": "Status",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


@st.dialog("Schema check details", width="large")
def _schema_dialog(manifest: dict) -> None:
    """Open schema details in a modal, linked from the Schema check row."""
    _render_schema_body(manifest)


def _html(markup: str) -> None:
    """Render a theme HTML fragment."""
    st.markdown(markup, unsafe_allow_html=True)


def _render_health(manifest: dict, df: pd.DataFrame) -> None:
    """Render the consolidated delivery-information card (quality + sources + size)."""
    schema_ok = schema_view.schema_match(
        manifest.get("columns"), feature_contract.EXPECTED_COLUMNS
    )
    source_rows = [
        (name, "Delivered" if ok else "Missing", "ok" if ok else "bad")
        for name, ok in feature_labels.source_health_flags(df)
    ]
    source_rows.append(("Days covered", str(int(manifest["window_days"])), "plain"))
    volume = manifest.get("volume_summary")
    records_label = f"{int(manifest['record_count']):,}"

    with st.container(key="delivery_health"):
        _html('<div class="health-heading">Delivery information</div>')
        label_col, status_col, action_col = st.columns(
            [2.0, 1.0, 1.35], vertical_alignment="center"
        )
        with label_col:
            st.markdown("Schema check")
        with status_col:
            status = "Passed" if schema_ok else "Failed"
            css = "health-ok" if schema_ok else "health-bad"
            _html(f'<span class="{css}">{status}</span>')
        with action_col:
            if st.button(
                "Details →",
                key="schema_details_button",
                use_container_width=True,
                help="Compare delivered columns to the feature contract.",
            ):
                _schema_dialog(manifest)

        _html(theme.health_rows(source_rows))

        rec_label, rec_value, rec_action = st.columns(
            [2.0, 1.0, 1.35], vertical_alignment="center"
        )
        with rec_label:
            st.markdown("Records")
        with rec_value:
            _html(f'<span class="health-value">{records_label}</span>')
        with rec_action:
            if isinstance(volume, dict):
                if st.button(
                    "Details →",
                    key="coverage_details_button",
                    use_container_width=True,
                    help="See how much data went into this delivery, from raw logs to features.",
                ):
                    _coverage_dialog(volume)
            else:
                st.button(
                    "Details →",
                    key="coverage_details_button",
                    use_container_width=True,
                    disabled=True,
                    help="Coverage details are not available for this delivery yet.",
                )

def _render_triage(kpis: dict) -> None:
    """Render the four triage headline cards."""
    highest = kpis["highest_id"] or "—"
    if kpis["highest_id"] is not None and kpis["highest_score"] is not None:
        highest_note = f"{kpis['highest_risk']} · {float(kpis['highest_score']):.1f}"
    else:
        highest_note = "no computers scored"
    theme_label = kpis["theme_label"] or "—"
    theme_note = (
        f"#1 driver for {kpis['theme_count']} of {kpis['attention']}"
        if kpis["theme_label"]
        else "no attention computers"
    )
    cards = (
        (
            "Needs attention",
            f"{kpis['attention']:,}",
            f"top 5% of {kpis['computers']:,} computers",
            "rose",
        ),
        ("Highest risk", str(highest), highest_note, "amber"),
        (
            "High risk",
            f"{kpis['high']:,}",
            f"top 1% · {kpis['medium']:,} at medium",
            "violet",
        ),
        ("Top unusual theme", str(theme_label), theme_note, "cyan"),
    )
    for column, (label, value, note, accent) in zip(st.columns(4), cards, strict=True):
        with column:
            _html(theme.key_card(label, value, note, accent))


RISK_CELL_COLOURS = {
    "High": theme.ACCENTS["rose"],
    "Medium": theme.ACCENTS["amber"],
}


def _risk_cell_style(value: object) -> str:
    colour = RISK_CELL_COLOURS.get(str(value), theme.TEXT_COLOR)
    return f"color: {colour}; font-weight: 700; background-color: {colour}22"


def _render_watchlist_table(rows: list[dict]) -> None:
    """Render the ranked watchlist; clicking a row selects that computer."""
    table = pd.DataFrame(rows).rename(
        columns={
            "rank": "#",
            "computer_id": "Computer",
            "reason": "Why it stands out",
            "risk": "Risk",
            "score": "Score",
        }
    )
    styled = table.style.map(_risk_cell_style, subset=["Risk"]).map(
        lambda _value: f"color: {theme.ACCENTS['cyan']}; font-weight: 700",
        subset=["Computer"],
    )

    event = st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="watchlist_table",
        column_config={
            "#": st.column_config.NumberColumn(width="small"),
            "Score": st.column_config.ProgressColumn(
                "Score",
                format="%.1f",
                min_value=0.0,
                max_value=float(max(r["score"] for r in rows)) or 1.0,
            ),
        },
    )
    # A stale selection index can outlive a filter change, so bound it to the rows shown.
    picked = [index for index in event.selection["rows"] if index < len(rows)]
    if picked:
        st.session_state.selected_computer_id = rows[picked[0]]["computer_id"]
    elif st.session_state.get("selected_computer_id") not in {r["computer_id"] for r in rows}:
        st.session_state.selected_computer_id = rows[0]["computer_id"]


def _render_pattern_zone(result: anomaly.AnomalyResult) -> None:
    """Render what kinds of unusual activity dominate this delivery."""
    left, right = st.columns(2)
    drivers = watchlist.driver_frequency(result.scored, result.contributions)
    families = watchlist.attention_by_family(result.scored, result.contributions)

    with left, st.container(border=True):
        _html(theme.chart_label("Most common drivers"))
        if not drivers:
            st.info("No attention computers in this delivery.")
        else:
            st.altair_chart(
                charts.count_bars(drivers, value_title="Computers", accent="violet"),
                use_container_width=True,
                theme=None,
            )

    with right, st.container(border=True):
        _html(theme.chart_label("Attention by activity family"))
        if not families:
            st.info("No attention computers in this delivery.")
        else:
            st.altair_chart(
                charts.count_bars(families, value_title="Computers", accent="cyan"),
                use_container_width=True,
                theme=None,
            )


def _render_investigate(result: anomaly.AnomalyResult) -> None:
    """Render watchlist + explain panel for unusual computers."""
    scored = result.scored
    default_id = watchlist.default_selected_computer(scored)
    if not st.session_state.get("selected_computer_id"):
        st.session_state.selected_computer_id = default_id

    left, right = st.columns([1.2, 1], gap="medium")
    with left, st.container(border=True):
        _html(theme.chart_label("Watchlist — most unusual"))
        filter_label = st.radio(
            "Risk filter",
            options=["All attention", "High only", "Medium"],
            horizontal=True,
            label_visibility="collapsed",
            key="watchlist_risk_filter",
        )
        risk_filter = {
            "All attention": "all",
            "High only": "high",
            "Medium": "medium",
        }[filter_label]
        search = st.text_input(
            "Find computer",
            value="",
            placeholder="Find computer…",
            label_visibility="collapsed",
            key="watchlist_search",
        )
        rows = watchlist.watchlist_rows(
            scored,
            result.contributions,
            risk_filter=risk_filter,
            search=search,
        )
        filtered_total = watchlist.watchlist_filtered_total(
            scored, risk_filter=risk_filter, search=search
        )
        if rows:
            _render_watchlist_table(rows)
            caption = watchlist.watchlist_caption(filtered_total, len(rows))
            if caption:
                st.caption(caption)
        else:
            st.info("No computers match this filter.")
            st.session_state.selected_computer_id = default_id

    with right, st.container(border=True):
        _html(theme.chart_label("Why this computer stands out"))
        selected = st.session_state.get("selected_computer_id")
        if not selected:
            st.write("Select a computer in the watchlist to see what stands out.")
        else:
            match = scored.loc[
                scored["computer_id"].astype(str) == str(selected),
                anomaly.SCORE_COLUMN,
            ]
            if match.empty:
                st.write("Select a computer in the watchlist to see what stands out.")
            else:
                risk = str(
                    feature_labels.assign_risk_levels(scored[anomaly.SCORE_COLUMN]).loc[
                        match.index[0]
                    ]
                )
                _html(theme.risk_pill(str(selected), risk))
                rows = watchlist.driver_rows(result.contributions, str(selected), n=5)
                if not rows:
                    st.write("Nothing looks unusually high for this computer.")
                else:
                    st.altair_chart(
                        charts.driver_bars(rows),
                        use_container_width=True,
                        theme=None,
                    )
                    top_label = str(rows[0]["label"]).lower()
                    st.markdown(
                        f"Unusually high **{top_label}** compared with the rest of the group."
                    )
        st.caption(
            "Estimated in this view by comparing each computer's delivered "
            "features against the rest of the group — not the production model. "
            "Risk levels are relative to this delivery (top 1% High, next 4% Medium)."
        )


def _render_partition(_manifest: dict, df: pd.DataFrame) -> None:
    """Render triage KPIs and the investigate watchlist for one partition."""
    try:
        result = anomaly.score_computers(df)
    except ValueError as exc:
        st.info(f"Unusual activity could not be estimated for this delivery: {exc}")
        return
    kpis = watchlist.triage_kpis(result.scored, result.contributions)
    _html(theme.zone_label("Triage"))
    _render_triage(kpis)
    _html(theme.zone_label("Investigate"))
    _render_investigate(result)
    _html(theme.zone_label("What kinds of unusual activity"))
    _render_pattern_zone(result)


def _render_advanced_actions(manifests: list[dict]) -> None:
    """Collapsed load/rebuild controls for operators."""
    default_window = int(os.environ.get("ROLLING_WINDOW_DAYS", "7"))
    with st.expander("Advanced actions", expanded=False):
        st.caption("For operators: load a specific window or rebuild it via Airflow.")
        controls = st.columns([1, 1, 1, 1])
        window_days = int(
            controls[0].number_input(
                "Days in window",
                min_value=1,
                value=default_window,
                step=1,
            )
        )
        anchor_day = int(controls[1].number_input("As-of day number", min_value=0, value=0, step=1))
        load_clicked = controls[2].button("Load delivery", type="primary")
        rebuild_clicked = controls[3].button("Rebuild via Airflow")

        if rebuild_clicked:
            try:
                run_id = _trigger_reprocess(anchor_day, window_days)
                state = _wait_for_dag_run(run_id)
            except (
                AirflowAuthError,
                AirflowApiError,
                TimeoutError,
                ValueError,
                KeyError,
            ) as exc:
                st.error(f"Rebuild failed: {exc}")
                return
            if state != "success":
                st.error(f"Rebuild finished with status: {state}")
                return
            st.session_state.pending_delivery_key = (window_days, anchor_day)
            st.session_state.flash_success = (
                f"Rebuild finished for the {window_days}-day view as of day {anchor_day}."
            )
            st.rerun()

        if load_clicked:
            candidate = _find_manifest(
                manifests,
                window_days=window_days,
                anchor_day=anchor_day,
            )
            if candidate is None:
                st.warning(
                    f"No delivery found for a {window_days}-day view as of day "
                    f"{anchor_day}. Use Rebuild via Airflow if the raw logs already exist."
                )
            else:
                st.session_state.pending_delivery_key = (window_days, anchor_day)
                st.rerun()


def _delivery_label(option: object, key_to_manifest: dict) -> str:
    """Render a delivery option label, tolerating keys with no manifest."""
    manifest = key_to_manifest.get(option)
    if manifest is None:
        return str(option)
    return feature_labels.delivery_choice_label(manifest)


def main() -> None:
    """Streamlit entrypoint: login gate, then consumer delivery cockpit."""
    st.set_page_config(page_title="Cybersecurity Log Analytics Dashboard", layout="wide")
    _html(theme.STYLESHEET)
    if not _render_login():
        return

    top = st.columns([5, 1])
    with top[0]:
        _html(
            '<div class="cockpit-header">'
            '<div class="cockpit-title">Cybersecurity Log Analytics Dashboard</div>'
            '<div class="cockpit-sub">Unusual computer behaviour in the latest '
            "delivered feature set.</div></div>"
        )
    if top[1].button("Log out"):
        st.session_state.clear()
        st.rerun()

    flash = st.session_state.pop("flash_success", None)
    if flash:
        st.success(flash)

    bucket = os.environ.get("DELIVERED_BUCKET", "delivered")
    key = read_secret("delivery_encryption_key", env="DELIVERY_ENCRYPTION_KEY", default="")
    client = _s3_client()

    try:
        manifests = _list_manifests(client, bucket)
    except (OSError, ValueError, KeyError) as exc:
        logger.exception("Could not list deliveries in '%s'", bucket)
        st.error(f"Could not list deliveries in '{bucket}': {exc}")
        return
    except Exception as exc:  # noqa: BLE001 - boto/botocore error hierarchy is broad
        logger.exception("Could not list deliveries in '%s'", bucket)
        st.error(f"Could not list deliveries in '{bucket}': {exc}")
        return

    if not manifests:
        st.info("No deliveries are available yet.")
        _render_advanced_actions([])
        return

    ordered = partition_view.sort_manifests_newest_first(manifests)
    options = [partition_view.manifest_key(m) for m in ordered]
    key_to_manifest = {partition_view.manifest_key(m): m for m in ordered}

    # A queued selection from Advanced actions is applied here: once the
    # selectbox exists, Streamlit refuses writes to its own session_state entry.
    pending = st.session_state.pop("pending_delivery_key", None)
    if pending is not None and tuple(pending) in key_to_manifest:
        st.session_state.delivery_choice = tuple(pending)
    if st.session_state.get("delivery_choice") not in key_to_manifest:
        st.session_state.delivery_choice = options[0]

    left, right = st.columns([1, 3], gap="large")
    with left:
        _html(theme.zone_label("Deliveries"))
        chosen_key = st.selectbox(
            "Choose a delivery",
            options,
            format_func=lambda option: _delivery_label(option, key_to_manifest),
            key="delivery_choice",
            label_visibility="collapsed",
        )
        selected = key_to_manifest[chosen_key]
        if st.session_state.get("selection_manifest_key") != chosen_key:
            st.session_state.selection_manifest_key = chosen_key
            st.session_state.selected_computer_id = None

    if not key:
        with right:
            st.warning("Encryption key is not configured; this delivery cannot be opened.")
        _render_advanced_actions(manifests)
        return

    try:
        df = _load_partition(client, bucket, selected, key)
    except Exception as exc:  # noqa: BLE001 - decrypt/IO failures must surface
        logger.exception("Could not decrypt/preview partition")
        detail = str(exc).strip() or type(exc).__name__
        if type(exc).__name__ == "InvalidTag":
            detail = (
                "Could not open this delivery because the encryption key "
                "does not match. Ask an operator to rebuild the dashboard "
                "and delivery services from the same shared catalog package."
            )
        with right:
            st.error(f"Could not open this delivery: {detail}")
        _render_advanced_actions(manifests)
        return

    st.session_state.active_manifest = selected
    with left:
        _render_health(selected, df)
    with right:
        _render_partition(selected, df)

    _render_advanced_actions(manifests)


if __name__ == "__main__":
    main()

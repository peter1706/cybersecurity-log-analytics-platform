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
    source_flags = feature_labels.source_health_flags(df)
    source_rows = [
        (name, "Delivered" if ok else "Missing", "ok" if ok else "bad") for name, ok in source_flags
    ]
    delivered = sum(1 for _, ok in source_flags if ok)
    source_summary = (
        f"{delivered} of {len(source_flags)} delivered",
        "ok" if delivered == len(source_flags) else "bad",
    )
    volume = manifest.get("volume_summary")
    delivered_label = f"{int(manifest['record_count']):,}"
    raw_total = volume_view.layer_row_total(volume, "landing") if isinstance(volume, dict) else None
    raw_label = f"{raw_total:,}" if raw_total is not None else "—"

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

        _html(theme.health_group("Data sources", source_summary, source_rows))
        _html(
            theme.health_rows(
                [
                    ("Anchor day", str(int(manifest["anchor_day"])), "plain"),
                    ("Days covered", str(int(manifest["window_days"])), "plain"),
                    ("Raw data records", raw_label, "plain"),
                ]
            )
        )

        rec_label, rec_value, rec_action = st.columns(
            [2.0, 1.0, 1.35], vertical_alignment="center"
        )
        with rec_label:
            st.markdown("Aggregated records")
        with rec_value:
            _html(f'<span class="health-value">{delivered_label}</span>')
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
            "High risk count",
            f"{kpis['high']:,}",
            f"top 1% · {kpis['medium']:,} at medium",
            "violet",
        ),
        ("Top unusual theme", str(theme_label), theme_note, "cyan"),
    )
    for column, (label, value, note, accent) in zip(st.columns(4), cards, strict=True):
        with column:
            _html(theme.key_card(label, value, note, accent))


def _render_watchlist_cards(rows: list[dict], *, height: int = 360) -> None:
    """Render the ranked watchlist as clickable glass cards.

    Each card carries a full-size transparent button, so a click anywhere on the
    card selects that computer. A fixed pixel height keeps the (up to 50-row)
    list from stretching the page: extra rows scroll inside the stack.
    """
    shown_ids = [str(row["computer_id"]) for row in rows]
    # A selection can outlive a filter change, so bound it to the rows shown.
    if st.session_state.get("selected_computer_id") not in shown_ids:
        st.session_state.selected_computer_id = shown_ids[0]
    selected = str(st.session_state.selected_computer_id)
    top_score = max(float(row["score"]) for row in rows) or 1.0

    with st.container(height=height, border=False, key="watchlist_cards"):
        for index, row in enumerate(rows):
            computer_id = str(row["computer_id"])
            with st.container(key=f"wl_row_{index}"):
                _html(
                    theme.watchlist_row(
                        int(row["rank"]),
                        computer_id,
                        str(row["reason"]),
                        str(row["risk"]),
                        float(row["score"]),
                        fill=float(row["score"]) / top_score,
                        selected=computer_id == selected,
                    )
                )
                # Re-run so the picked card's highlight and the detail panel
                # both reflect the new selection in this same interaction.
                if st.button(f"Select {computer_id}", key=f"wl_pick_{index}"):
                    st.session_state.selected_computer_id = computer_id
                    st.rerun()


def _render_watchlist_panel(result: anomaly.AnomalyResult) -> None:
    """Render the ranked watchlist card (filters, search, fixed-height stack).

    Runs before the explain panel so the row-click selection it writes to
    ``selected_computer_id`` is visible when the detail panel reads it.
    """
    scored = result.scored
    default_id = watchlist.default_selected_computer(scored)
    if not st.session_state.get("selected_computer_id"):
        st.session_state.selected_computer_id = default_id

    with st.container(border=True):
        st.caption("Select a computer to inspect it on the right.")
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
            _render_watchlist_cards(rows)
            caption = watchlist.watchlist_caption(filtered_total, len(rows))
            if caption:
                st.caption(caption)
        else:
            st.info("No computers match this filter.")
            st.session_state.selected_computer_id = default_id


def _render_explain_panel(result: anomaly.AnomalyResult) -> None:
    """Render the 'why this computer stands out' detail for the current pick."""
    scored = result.scored
    with st.container(border=True):
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
                        charts.driver_bars(rows, height=200),
                        use_container_width=True,
                        theme=None,
                    )
                    top_label = str(rows[0]["label"]).lower()
                    st.markdown(
                        f"Unusually high **{top_label}** compared with the rest of the group."
                    )
        st.caption(
            "Estimated in this view by comparing each computer's delivered "
            "features against the rest of the group. "
            "Risk levels are relative to this delivery (top 1% High, next 4% Medium)."
        )


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

    # The risk KPIs get their own full-width row (filled once the delivery is
    # scored) rather than sitting beside the taller delivery-information card,
    # which left most of that column empty. The findings row below then splits
    # into delivery context, the watchlist, and the selected computer.
    kpi_zone = st.container()
    body = st.columns([1.5, 2.2, 1.7], gap="large")
    with body[0]:
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
        st.error(f"Could not open this delivery: {detail}")
        _render_advanced_actions(manifests)
        return

    st.session_state.active_manifest = selected
    with body[0]:
        _render_health(selected, df)

    try:
        result = anomaly.score_computers(df)
    except ValueError as exc:
        with kpi_zone:
            _html(theme.zone_label("Computer Risk Statistics"))
            st.info(f"Unusual activity could not be estimated for this delivery: {exc}")
        _render_advanced_actions(manifests)
        return

    kpis = watchlist.triage_kpis(result.scored, result.contributions)
    with kpi_zone:
        _html(theme.zone_label("Computer Risk Statistics"))
        _render_triage(kpis)

    # Watchlist renders before the detail panel so a row click is already in
    # session state when the detail panel reads the selected computer.
    with body[1]:
        _html(theme.zone_label("Investigate Affected Computers"))
        _render_watchlist_panel(result)
    with body[2]:
        _html(theme.zone_label("Why this computer stands out"))
        _render_explain_panel(result)

    _render_advanced_actions(manifests)


if __name__ == "__main__":
    main()

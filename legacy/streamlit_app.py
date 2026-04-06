"""AcquiGuru — Merchant-facing demo copilot (MD-directed + typed tools).

Goal: make the demo *work reliably* and feel agentic.
Non-goal: production-grade safety.

Run:
  streamlit run main.py

Env:
  DATABASE_URL=postgresql://demo:demo@localhost:5433/payments_demo
  OLLAMA_BASE_URL=http://localhost:11434
  OLLAMA_MODEL=mistral
"""

import json
import logging
from pathlib import Path
from typing import Any

import streamlit as st
from sqlalchemy import create_engine, text

import app.merchant_os as merchant_os
from app.merchant_os import (
    build_report_briefs,
    build_report_packs,
    confirm_merchant_action,
    confirm_background_proactive_card_action,
    ensure_background_proactive_refresh,
    get_merchant_os_snapshot,
    preview_merchant_action,
    preview_background_proactive_card_action,
    rows_to_csv,
    scope_snapshot_to_terminal,
    terminal_scope_options,
    update_background_proactive_card_state,
    update_existing_action_details,
    update_existing_action_status,
)
from config import Config
from app.copilot.runtime import run_turn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("acquiguru_demo")

ROOT = Path(__file__).resolve().parents[1]
AGENT_DIR = ROOT / "agent"


@st.cache_resource
def get_engine():
    return create_engine(Config.DATABASE_URL)


@st.cache_data(show_spinner=False, ttl=60)
def load_merchant_snapshot(merchant_id: str) -> dict[str, Any]:
    return get_merchant_os_snapshot(get_engine(), merchant_id.strip(), days=30)


def pick_default_merchant_id() -> str:
    """Pick a MID for demo: first merchant_id in the source table."""
    with get_engine().connect() as conn:
        row = conn.execute(text(f"SELECT merchant_id FROM {Config.QUERY_SOURCE_TABLE} LIMIT 1")).fetchone()
        return str(row[0]) if row and row[0] else ""


def render_tool_trace(turn):
    with st.expander("Tool trace", expanded=False):
        for i, (tc, tr) in enumerate(zip(turn.tool_calls, turn.tool_results), start=1):
            st.markdown(f"**{i}. {tc.name}**")
            st.code(json.dumps(tc.args, ensure_ascii=False, indent=2), language="json")
            if tr.ok:
                st.success("ok")
                st.code(json.dumps(tr.output, ensure_ascii=False, default=str, indent=2)[:6000], language="json")
            else:
                st.error(tr.error or "tool failed")


def format_inr(value: Any) -> str:
    try:
        amount = float(value or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    return f"Rs {amount:,.2f}"


def format_percent(value: Any) -> str:
    try:
        pct = float(value or 0.0)
    except (TypeError, ValueError):
        pct = 0.0
    return f"{pct:.2f}%"


def dict_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def render_dataframe(title: str, rows: list[dict[str, Any]], *, empty_text: str, height: int = 260):
    st.subheader(title)
    if rows:
        st.dataframe(rows, use_container_width=True, height=height)
    else:
        st.info(empty_text)


def render_scope_banner(snapshot: dict[str, Any], *, for_chat: bool = False):
    scope = snapshot.get("scope", {}) if isinstance(snapshot.get("scope"), dict) else {}
    if str(scope.get("level") or "merchant").lower() != "terminal":
        return
    label = str(scope.get("label") or "Terminal scope")
    notes = [str(item) for item in (scope.get("notes") or []) if str(item).strip()]
    if for_chat:
        st.caption(
            f"{label} selected. Supported analytics tools use terminal scope automatically; some payouts and dispute tools remain merchant-wide in this sprint."
        )
        return
    message = f"Active scope: {label}."
    if notes:
        message = f"{message} {' '.join(notes)}"
    st.info(message)


def render_task_cards(*, merchant_id: str, tasks: list[dict[str, Any]], key_prefix: str, empty_text: str):
    if not tasks:
        st.info(empty_text)
        return

    for idx, task in enumerate(tasks):
        if not isinstance(task, dict):
            continue
        state_key = f"{key_prefix}_preview_{idx}"
        result_key = f"{key_prefix}_result_{idx}"
        with st.container(border=True):
            st.markdown(f"**{task.get('title') or 'Untitled task'}**")
            if task.get("description"):
                st.write(task.get("description"))

            meta: list[str] = []
            if task.get("priority"):
                meta.append(f"priority: {task.get('priority')}")
            if task.get("priority_score") is not None:
                meta.append(f"score: {float(task.get('priority_score') or 0.0):.1f}")
            if task.get("confidence") is not None:
                meta.append(f"confidence: {float(task.get('confidence') or 0.0):.2f}")
            if task.get("evidence_ids"):
                meta.append(f"evidence: {', '.join(str(x) for x in task.get('evidence_ids')[:3])}")
            if meta:
                st.caption(" | ".join(meta))

            if st.button("Preview action", key=f"{key_prefix}_button_{idx}"):
                st.session_state[state_key] = preview_merchant_action(
                    get_engine(),
                    merchant_id,
                    action_type=str(task.get("action_type") or "FOLLOW_UP"),
                    payload=dict(task.get("payload") or {}),
                )

            preview = st.session_state.get(state_key)
            if isinstance(preview, dict):
                st.caption("Action preview")
                st.json(preview)
                token = str(preview.get("confirmation_token") or "")
                if token and st.button("Create action", key=f"{key_prefix}_confirm_{idx}"):
                    st.session_state[result_key] = confirm_merchant_action(
                        get_engine(),
                        merchant_id,
                        confirmation_token=token,
                    )
                    load_merchant_snapshot.clear()

            result = st.session_state.get(result_key)
            if isinstance(result, dict):
                if result.get("action_id"):
                    st.success(f"Created action {result.get('action_id')}")
                elif result.get("error"):
                    st.error(str(result.get("error")))
                st.json(result)


def render_existing_action_cards(snapshot: dict[str, Any], *, key_prefix: str, show_editor: bool = False):
    existing_actions = dict_rows(snapshot.get("existing_actions"))
    merchant_id = str(snapshot.get("merchant_id") or "")
    if not existing_actions:
        st.info("No persisted actions are present for this merchant yet.")
        return

    for idx, action in enumerate(existing_actions):
        action_id = action.get("action_id")
        key_suffix = f"{action_id}" if action_id is not None else f"row_{idx}"
        result_key = f"{key_prefix}_result_{key_suffix}"
        status = str(action.get("status") or "UNKNOWN").upper()
        with st.container(border=True):
            st.markdown(f"**{action.get('title') or action.get('category') or 'Action'}**")
            st.caption(f"status: {status} | category: {action.get('category') or 'unknown'}")
            metadata: list[str] = []
            if action.get("source"):
                metadata.append(f"source: {action.get('source')}")
            if action.get("owner"):
                metadata.append(f"owner: {action.get('owner')}")
            if action.get("priority_score") is not None:
                metadata.append(f"score: {float(action.get('priority_score') or 0.0):.1f}")
            if action.get("follow_up_date"):
                metadata.append(f"follow-up: {action.get('follow_up_date')}")
            if action.get("created_at"):
                metadata.append(f"created: {action.get('created_at')}")
            if action.get("updated_at") and action.get("updated_at") != action.get("created_at"):
                metadata.append(f"updated: {action.get('updated_at')}")
            if metadata:
                st.caption(" | ".join(metadata))
            if action.get("description"):
                st.write(str(action.get("description")))
            if action.get("blocked_reason"):
                st.caption(f"blocked: {action.get('blocked_reason')}")
            if action.get("notes"):
                st.caption(f"notes: {action.get('notes')}")
            evidence_ids = [str(item) for item in (action.get("evidence_ids") or []) if str(item).strip()]
            if evidence_ids:
                st.caption(f"evidence: {', '.join(evidence_ids[:5])}")

            if status not in {"IN_PROGRESS", "CLOSED", "RESOLVED", "DONE"} and st.button(
                "Mark in progress",
                key=f"{key_prefix}_progress_{key_suffix}",
            ):
                st.session_state[result_key] = update_existing_action_status(
                    get_engine(),
                    merchant_id,
                    action_id=action_id,
                    status="IN_PROGRESS",
                )
                load_merchant_snapshot.clear()

            if status not in {"CLOSED", "RESOLVED", "DONE"} and st.button(
                "Close action",
                key=f"{key_prefix}_close_{key_suffix}",
            ):
                st.session_state[result_key] = update_existing_action_status(
                    get_engine(),
                    merchant_id,
                    action_id=action_id,
                    status="CLOSED",
                )
                load_merchant_snapshot.clear()

            result = st.session_state.get(result_key)
            if isinstance(result, dict):
                if result.get("updated"):
                    st.success(f"Updated action {result.get('action_id')} to {result.get('status')}")
                elif result.get("error"):
                    st.error(str(result.get("error")))
                st.json(result)

            if show_editor:
                details_result_key = f"{key_prefix}_details_result_{key_suffix}"
                with st.expander("Manage action details", expanded=False):
                    owner_input = st.text_input(
                        "Owner",
                        value=str(action.get("owner") or ""),
                        key=f"{key_prefix}_owner_{key_suffix}",
                    )
                    follow_up_input = st.text_input(
                        "Follow-up date (YYYY-MM-DD)",
                        value=str(action.get("follow_up_date") or ""),
                        key=f"{key_prefix}_followup_{key_suffix}",
                    )
                    blocked_input = st.text_input(
                        "Blocked reason",
                        value=str(action.get("blocked_reason") or ""),
                        key=f"{key_prefix}_blocked_{key_suffix}",
                    )
                    notes_input = st.text_area(
                        "Notes",
                        value=str(action.get("notes") or ""),
                        height=100,
                        key=f"{key_prefix}_notes_{key_suffix}",
                    )
                    if st.button("Save details", key=f"{key_prefix}_details_save_{key_suffix}"):
                        st.session_state[details_result_key] = update_existing_action_details(
                            get_engine(),
                            merchant_id,
                            action_id=action_id,
                            owner=owner_input,
                            notes=notes_input,
                            blocked_reason=blocked_input,
                            follow_up_date=follow_up_input,
                        )
                        load_merchant_snapshot.clear()

                    details_result = st.session_state.get(details_result_key)
                    if isinstance(details_result, dict):
                        if details_result.get("updated"):
                            st.success(f"Saved action details for {details_result.get('action_id')}")
                        elif details_result.get("error"):
                            st.error(str(details_result.get("error")))
                        st.json(details_result)


def render_proactive_cards(cards: list[dict[str, Any]], *, key_prefix: str, empty_text: str, interactive: bool = False):
    if not cards:
        st.info(empty_text)
        return

    for idx, card in enumerate(cards):
        if not isinstance(card, dict):
            continue
        dedupe_key = str(card.get("dedupe_key") or f"{key_prefix}_{idx}")
        result_key = f"{key_prefix}_card_result_{idx}"
        preview_key = f"{key_prefix}_card_preview_{idx}"
        with st.container(border=True):
            title = f"{card.get('icon') or '🔎'} {card.get('title') or 'Untitled signal'}"
            st.markdown(f"**{title}**")
            meta: list[str] = []
            if card.get("type"):
                meta.append(f"type: {card.get('type')}")
            if card.get("card_state"):
                meta.append(f"state: {card.get('card_state')}")
            if card.get("confidence") is not None:
                meta.append(f"confidence: {float(card.get('confidence') or 0.0):.2f}")
            if card.get("impact_rupees") is not None:
                meta.append(f"impact: {format_inr(card.get('impact_rupees'))}")
            if card.get("verification_status"):
                meta.append(str(card.get("verification_status")))
            if meta:
                st.caption(" | ".join(meta))
            if card.get("body"):
                st.write(str(card.get("body")))
            actions = [str(item) for item in (card.get("actions") or []) if str(item).strip()]
            for action in actions[:3]:
                st.write(f"- {action}")
            evidence_ids = [str(item) for item in (card.get("evidence_ids") or []) if str(item).strip()]
            if evidence_ids:
                st.caption(f"evidence: {', '.join(evidence_ids[:4])}")
            if card.get("card_notes"):
                st.caption(f"notes: {card.get('card_notes')}")
            if card.get("converted_action_id"):
                st.caption(f"converted action: {card.get('converted_action_id')}")

            if interactive:
                notes_value = str(card.get("card_notes") or "")
                notes_input = st.text_input(
                    "Card notes",
                    value=notes_value,
                    key=f"{key_prefix}_notes_{idx}",
                )
                action_cols = st.columns(4)
                if action_cols[0].button("Acknowledge", key=f"{key_prefix}_ack_{idx}"):
                    st.session_state[result_key] = update_background_proactive_card_state(
                        get_engine(),
                        st.session_state.merchant_id,
                        dedupe_key=dedupe_key,
                        state="ACKNOWLEDGED",
                        card_notes=notes_input,
                        converted_action_id=card.get("converted_action_id"),
                    )
                    load_merchant_snapshot.clear()
                if action_cols[1].button("Dismiss", key=f"{key_prefix}_dismiss_{idx}"):
                    st.session_state[result_key] = update_background_proactive_card_state(
                        get_engine(),
                        st.session_state.merchant_id,
                        dedupe_key=dedupe_key,
                        state="DISMISSED",
                        card_notes=notes_input,
                        converted_action_id=card.get("converted_action_id"),
                    )
                    load_merchant_snapshot.clear()
                if action_cols[2].button("Preview action", key=f"{key_prefix}_preview_{idx}"):
                    st.session_state[preview_key] = preview_background_proactive_card_action(
                        get_engine(),
                        st.session_state.merchant_id,
                        dedupe_key=dedupe_key,
                    )
                if action_cols[3].button("Save note", key=f"{key_prefix}_save_note_{idx}"):
                    st.session_state[result_key] = update_background_proactive_card_state(
                        get_engine(),
                        st.session_state.merchant_id,
                        dedupe_key=dedupe_key,
                        state=str(card.get("card_state") or "NEW"),
                        card_notes=notes_input,
                        converted_action_id=card.get("converted_action_id"),
                    )
                    load_merchant_snapshot.clear()

                preview = st.session_state.get(preview_key)
                if isinstance(preview, dict):
                    st.caption("Card action preview")
                    st.json(preview)
                    token = str(preview.get("confirmation_token") or "")
                    if token and st.button("Create action from card", key=f"{key_prefix}_confirm_{idx}"):
                        st.session_state[result_key] = confirm_background_proactive_card_action(
                            get_engine(),
                            st.session_state.merchant_id,
                            dedupe_key=dedupe_key,
                            confirmation_token=token,
                        )
                        load_merchant_snapshot.clear()

                result = st.session_state.get(result_key)
                if isinstance(result, dict):
                    if result.get("updated"):
                        st.success(f"Updated card {dedupe_key}")
                    elif result.get("action_id"):
                        st.success(f"Created action {result.get('action_id')} from card")
                    elif result.get("error"):
                        st.error(str(result.get("error")))
                    st.json(result)


def render_home(snapshot: dict[str, Any]):
    merchant = snapshot.get("merchant_profile", {}).get("merchant", {}) or {}
    classification = snapshot.get("classification", {}) or {}
    summary = snapshot.get("summary", {}) or {}
    coverage = snapshot.get("data_coverage", {}) or {}
    window = snapshot.get("window", {}) or {}
    pay_mode_drivers = dict_rows(snapshot.get("failure_drivers", {}).get("payment_mode", {}).get("rows"))
    response_code_drivers = dict_rows(snapshot.get("failure_drivers", {}).get("response_code", {}).get("rows"))
    growth_tasks = dict_rows(snapshot.get("growth_tasks"))
    proactive_cards = dict_rows(snapshot.get("proactive_cards"))
    operations_cards = [card for card in proactive_cards if str(card.get("lane") or "").lower() == "operations"]
    growth_cards = [card for card in proactive_cards if str(card.get("lane") or "").lower() == "growth"]
    cashflow = snapshot.get("cashflow", {}) or {}
    past_expected = cashflow.get("past_expected", {}) if isinstance(cashflow.get("past_expected"), dict) else {}

    st.subheader(merchant.get("merchant_trade_name") or snapshot.get("merchant_id") or "Merchant")
    st.caption(
        f"Window: {window.get('from', '-')}"
        f" to {window.get('to', '-')}"
        f" | Segment: {classification.get('label', 'Unknown')}"
        f" | Coverage: {coverage.get('coverage_label', 'Payments only')}"
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Attempts", f"{int(summary.get('attempts') or 0):,}")
    col2.metric("Success rate", format_percent(summary.get("success_rate_pct")))
    col3.metric("Success GMV", format_inr(summary.get("success_gmv")))
    col4.metric("Failed GMV", format_inr(summary.get("failed_gmv")))

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Terminals", f"{int(summary.get('terminal_count') or 0):,}")
    col6.metric("Open chargebacks", f"{int(summary.get('open_chargebacks') or 0):,}")
    col7.metric("Refunds", f"{int(summary.get('refund_count') or 0):,}")
    col8.metric("Settlements", f"{int(summary.get('settlement_count') or 0):,}")

    left, right = st.columns(2)
    with left:
        st.subheader("Operating fit")
        st.markdown(f"**{classification.get('label', 'Unknown')}**")
        for reason in classification.get("reasons") or []:
            st.write(f"- {reason}")

        st.subheader("Actions due")
        actions: list[str] = []
        if int(past_expected.get("past_expected_count") or 0) > 0:
            actions.append(
                f"{int(past_expected.get('past_expected_count') or 0)} settlement(s) are past expected date for {format_inr(past_expected.get('past_expected_amount'))}."
            )
        if int(summary.get("open_chargebacks") or 0) > 0:
            actions.append(f"{int(summary.get('open_chargebacks') or 0)} chargeback case(s) need review.")
        if pay_mode_drivers:
            top_mode = pay_mode_drivers[0]
            actions.append(
                f"{top_mode.get('driver', 'UNKNOWN')} is the largest failed payment mode with {int(top_mode.get('failed_txns') or 0):,} failed txns."
            )
        if not actions:
            actions.append("No immediate payment ops exceptions detected in the current data window.")
        for item in actions[:3]:
            st.write(f"- {item}")

    with right:
        st.subheader("Coverage")
        st.write(f"- Data coverage: {coverage.get('coverage_label', 'Payments only')}")
        st.write(f"- Business type: {merchant.get('nature_of_business') or 'Unknown'}")
        st.write(f"- City: {merchant.get('business_city') or 'Unknown'}")
        st.write(f"- Risk category: {merchant.get('merchant_risk_category') or 'Unknown'}")
        st.write(f"- Status: {merchant.get('merchant_status') or 'Unknown'}")

    render_dataframe(
        "Payment mode performance",
        dict_rows(snapshot.get("kpi_by_mode")),
        empty_text="No payment mode KPIs were returned for this merchant.",
    )
    render_dataframe(
        "Top response-code failure drivers",
        response_code_drivers,
        empty_text="No verified response-code failure drivers were returned.",
    )
    st.subheader("Action center")
    render_existing_action_cards(snapshot, key_prefix="home_actions")

    st.subheader("Proactive inbox")
    left_cards, right_cards = st.columns(2)
    with left_cards:
        st.markdown("**Operations signals**")
        render_proactive_cards(
            operations_cards,
            key_prefix="home_ops_cards",
            empty_text="No proactive operations signals were generated for this merchant.",
        )
    with right_cards:
        st.markdown("**Growth signals**")
        render_proactive_cards(
            growth_cards,
            key_prefix="home_growth_cards",
            empty_text="No proactive growth signals were generated for this merchant.",
        )

    st.subheader("Growth opportunities")
    render_task_cards(
        merchant_id=str(snapshot.get("merchant_id") or ""),
        tasks=growth_tasks,
        key_prefix="home_growth",
        empty_text="No proactive growth opportunities were generated for this merchant.",
    )


def render_lane_chat(*, lane: str, title: str, caption: str, prompt: str, chat_key: str, snapshot: dict[str, Any] | None = None):
    st.subheader(title)
    st.caption(caption)
    if isinstance(snapshot, dict):
        render_scope_banner(snapshot, for_chat=True)

    for msg in st.session_state[chat_key]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("trace"):
                render_tool_trace(msg["trace"])

    user_input = st.chat_input(prompt, key=f"{lane}_chat_input")
    if not user_input:
        return

    st.session_state[chat_key].append({"role": "user", "content": user_input})

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            turn = run_turn(
                engine=get_engine(),
                agent_dir=AGENT_DIR,
                merchant_id=st.session_state.merchant_id,
                question=user_input,
                forced_lane=lane,
                terminal_id=st.session_state.get("selected_terminal_id") or None,
            )
        st.markdown(turn.answer)
        render_tool_trace(turn)

    st.session_state[chat_key].append(
        {
            "role": "assistant",
            "content": turn.answer,
            "trace": turn,
        }
    )


def render_money(snapshot: dict[str, Any]):
    cashflow = snapshot.get("cashflow", {}) or {}
    amounts = cashflow.get("amounts", {}) if isinstance(cashflow.get("amounts"), dict) else {}
    past_expected = cashflow.get("past_expected", {}) if isinstance(cashflow.get("past_expected"), dict) else {}
    operations_tasks = dict_rows(snapshot.get("operations_tasks"))
    money_tasks = [task for task in operations_tasks if str(task.get("action_type") or "").startswith("SETTLEMENT")]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Pending amount", format_inr(amounts.get("pending_amount")))
    col2.metric("Settled amount", format_inr(amounts.get("settled_amount")))
    col3.metric("Past expected count", f"{int(past_expected.get('past_expected_count') or 0):,}")
    col4.metric("Past expected amount", format_inr(past_expected.get("past_expected_amount")))

    st.subheader("Money tasks")
    render_task_cards(
        merchant_id=str(snapshot.get("merchant_id") or ""),
        tasks=money_tasks,
        key_prefix="money_tasks",
        empty_text="No settlement or payout tasks were generated for the current window.",
    )

    render_dataframe(
        "Recent settlement timing view",
        dict_rows(cashflow.get("recent")),
        empty_text="No recent settlement timing data was returned.",
    )
    render_dataframe(
        "In-window settlements",
        dict_rows(snapshot.get("settlements", {}).get("rows")),
        empty_text="No settlements were returned for the current window.",
    )


def render_disputes(snapshot: dict[str, Any]):
    chargeback_rows = dict_rows(snapshot.get("chargebacks", {}).get("rows"))
    refund_rows = dict_rows(snapshot.get("refunds", {}).get("rows"))
    operations_tasks = dict_rows(snapshot.get("operations_tasks"))
    dispute_tasks = [task for task in operations_tasks if "CHARGEBACK" in str(task.get("action_type") or "")]
    open_chargebacks = 0
    for row in chargeback_rows:
        status = str(row.get("status") or "").upper()
        if status and status not in {"CLOSED", "RESOLVED"}:
            open_chargebacks += 1

    col1, col2, col3 = st.columns(3)
    col1.metric("Open chargebacks", f"{open_chargebacks:,}")
    col2.metric("Total chargebacks", f"{len(chargeback_rows):,}")
    col3.metric("Refunds", f"{len(refund_rows):,}")

    st.subheader("Dispute tasks")
    render_task_cards(
        merchant_id=str(snapshot.get("merchant_id") or ""),
        tasks=dispute_tasks,
        key_prefix="dispute_tasks",
        empty_text="No dispute tasks were generated for the current window.",
    )

    render_dataframe(
        "Chargebacks",
        chargeback_rows,
        empty_text="No chargeback data was returned for the current window.",
    )
    render_dataframe(
        "Refunds",
        refund_rows,
        empty_text="No refund data was returned for the current window.",
    )


def render_terminals(snapshot: dict[str, Any]):
    terminal_rows = dict_rows(snapshot.get("terminals", {}).get("rows"))
    health_rows = dict_rows(snapshot.get("terminal_health", {}).get("rows"))

    col1, col2 = st.columns(2)
    col1.metric("Active terminal rows", f"{len(terminal_rows):,}")
    col2.metric("Health signal rows", f"{len(health_rows):,}")

    render_dataframe(
        "Terminal performance",
        terminal_rows,
        empty_text="No terminal performance data was returned for the current window.",
    )
    render_dataframe(
        "Terminal health summary",
        health_rows,
        empty_text="No terminal health telemetry is connected for this merchant.",
    )


def render_connected_systems(snapshot: dict[str, Any]):
    coverage = snapshot.get("data_coverage", {}) or {}
    classification = snapshot.get("classification", {}) or {}
    integrations = coverage.get("integrations", {}) if isinstance(coverage.get("integrations"), dict) else {}
    data_domains = coverage.get("data_domains", {}) if isinstance(coverage.get("data_domains"), dict) else {}
    operating_signals = snapshot.get("operating_signals", {}) or {}

    st.subheader("Data coverage")
    st.write(f"- Coverage label: {coverage.get('coverage_label', 'Payments only')}")
    st.write(f"- Merchant segment: {classification.get('label', 'Unknown')}")

    integration_rows: list[dict[str, Any]] = []
    for name, payload in integrations.items():
        if not isinstance(payload, dict):
            continue
        integration_rows.append(
            {
                "integration": name,
                "connected": "Yes" if payload.get("connected") else "No",
                "provider": payload.get("provider"),
                "status": payload.get("status"),
                "source_table": payload.get("source_table"),
            }
        )

    render_dataframe(
        "External integrations",
        integration_rows,
        empty_text="No external integrations were detected for this merchant.",
    )

    data_rows: list[dict[str, Any]] = []
    for name, payload in data_domains.items():
        if not isinstance(payload, dict):
            continue
        data_rows.append(
            {
                "domain": name,
                "available": "Yes" if payload.get("available") else "No",
                "source_table": payload.get("source_table"),
                "row_count": payload.get("row_count"),
                "latest_date": payload.get("latest_date"),
            }
        )

    render_dataframe(
        "Internal data coverage",
        data_rows,
        empty_text="No internal acquiring data coverage was detected for this merchant.",
    )

    st.subheader("Operating signals")
    signal_rows = [{"signal": key, "value": value} for key, value in operating_signals.items()]
    if signal_rows:
        st.dataframe(signal_rows, use_container_width=True, height=220)
    else:
        st.info("No extra operating signals were derived from the current merchant dataset.")


def render_reports(snapshot: dict[str, Any]):
    packs = build_report_packs(snapshot)
    briefs_by_id = {str(brief.get("id") or ""): brief for brief in build_report_briefs(snapshot)}
    pack_tabs = st.tabs([str(pack.get("title") or "Report pack") for pack in packs])

    for tab, pack in zip(pack_tabs, packs):
        with tab:
            st.subheader(str(pack.get("title") or "Report pack"))
            brief = briefs_by_id.get(str(pack.get("id") or ""))
            if isinstance(brief, dict):
                st.caption(str(brief.get("subject") or ""))
                st.markdown("**Briefing summary**")
                for line in brief.get("summary_lines") or []:
                    st.write(f"- {line}")
                export_cols = st.columns(2)
                export_cols[0].download_button(
                    "Download email brief",
                    data=brief.get("email_bytes") or b"",
                    file_name=f"{snapshot.get('merchant_id', 'merchant')}_{pack.get('id', 'report')}_brief.txt",
                    mime="text/plain",
                    key=f"download_email_brief_{pack.get('id')}",
                )
                export_cols[1].download_button(
                    "Download print brief",
                    data=brief.get("print_html_bytes") or b"",
                    file_name=f"{snapshot.get('merchant_id', 'merchant')}_{pack.get('id', 'report')}_brief.html",
                    mime="text/html",
                    key=f"download_print_brief_{pack.get('id')}",
                )
                with st.expander("Preview email brief", expanded=False):
                    st.code(str(brief.get("email_text") or ""), language="text")
            st.markdown("**Datasets**")
            for dataset in pack.get("datasets") or []:
                if not isinstance(dataset, dict):
                    continue
                key = str(dataset.get("key") or "dataset")
                title = str(dataset.get("title") or key.replace("_", " ").title())
                rows = dict_rows(dataset.get("rows"))
                st.markdown(f"**{title}**")
                if rows:
                    st.download_button(
                        f"Download {title}",
                        data=rows_to_csv(rows),
                        file_name=f"{snapshot.get('merchant_id', 'merchant')}_{key}.csv",
                        mime="text/csv",
                        key=f"download_{key}",
                    )
                    st.dataframe(rows[:50], use_container_width=True, height=220)
                else:
                    st.info(f"No {title.lower()} available for this pack.")


def render_growth_workspace(snapshot: dict[str, Any]):
    growth_cards = [
        row for row in dict_rows(snapshot.get("proactive_cards")) if str(row.get("lane") or "").lower() == "growth"
    ]
    st.subheader("Growth signals")
    render_proactive_cards(
        growth_cards,
        key_prefix="growth_tab_cards",
        empty_text="No proactive growth signals are available for this merchant.",
        interactive=True,
    )
    st.subheader("Growth queue")
    render_task_cards(
        merchant_id=str(snapshot.get("merchant_id") or ""),
        tasks=dict_rows(snapshot.get("growth_tasks")),
        key_prefix="growth_workspace",
        empty_text="No proactive growth tasks were generated for this merchant.",
    )


st.set_page_config(page_title="AcquiGuru Merchant OS (Pilot)", layout="wide")

st.title("AcquiGuru Merchant OS (Pilot)")
st.caption("Payments transparency, operating workflows, and two isolated agents: Operations and Growth.")

if "merchant_id" not in st.session_state:
    st.session_state.merchant_id = pick_default_merchant_id()

if "operations_chat" not in st.session_state:
    st.session_state.operations_chat = []

if "growth_chat" not in st.session_state:
    st.session_state.growth_chat = []

if "selected_terminal_id" not in st.session_state:
    st.session_state.selected_terminal_id = ""

with st.sidebar:
    st.header("Demo controls")
    st.session_state.merchant_id = st.text_input("merchant_id", st.session_state.merchant_id)
    st.text_input("DB table (read)", Config.QUERY_SOURCE_TABLE, disabled=True)
    st.text_input("LLM", f"ollama:{Config.OLLAMA_MODEL}", disabled=True)

    if st.button("Refresh merchant data"):
        load_merchant_snapshot.clear()

    if st.button("Refresh proactive cards"):
        st.session_state["proactive_refresh_result"] = ensure_background_proactive_refresh(
            get_engine(),
            st.session_state.merchant_id,
            days=30,
            limit=8,
            force=True,
        )
        load_merchant_snapshot.clear()

    if st.button("Clear chat"):
        st.session_state.operations_chat = []
        st.session_state.growth_chat = []

snapshot_error = ""
snapshot: dict[str, Any] = {}
auto_refresh_status: dict[str, Any] = {}
if st.session_state.merchant_id:
    try:
        auto_refresh_status = ensure_background_proactive_refresh(
            get_engine(),
            st.session_state.merchant_id,
            days=30,
            limit=8,
        )
        if auto_refresh_status.get("refreshed"):
            st.session_state["proactive_refresh_result"] = auto_refresh_status
            load_merchant_snapshot.clear()
        snapshot = load_merchant_snapshot(st.session_state.merchant_id)
    except Exception as exc:
        snapshot_error = str(exc)
        logger.warning("Merchant snapshot load failed: %s", exc)

terminal_options: list[str] = []
display_snapshot: dict[str, Any] = snapshot
if snapshot:
    terminal_options = terminal_scope_options(snapshot)
    if st.session_state.selected_terminal_id not in terminal_options:
        st.session_state.selected_terminal_id = ""
    with st.sidebar:
        st.selectbox(
            "Terminal focus",
            options=[""] + terminal_options,
            key="selected_terminal_id",
            format_func=lambda value: "All terminals" if not value else str(value),
        )
    display_snapshot = scope_snapshot_to_terminal(get_engine(), snapshot, st.session_state.selected_terminal_id)

with st.sidebar:
    if auto_refresh_status:
        if auto_refresh_status.get("auto_enabled"):
            st.caption(
                f"Auto proactive refresh: next due {auto_refresh_status.get('next_refresh_at') or 'pending'}"
            )
        else:
            st.caption("Auto proactive refresh is disabled.")
    proactive_refresh_result = st.session_state.get("proactive_refresh_result")
    if isinstance(proactive_refresh_result, dict):
        if proactive_refresh_result.get("refreshed"):
            st.caption(
                f"Proactive cards refreshed: {int(proactive_refresh_result.get('generated_count') or 0)} generated."
            )
        elif proactive_refresh_result.get("reason") == "not_due":
            st.caption("Proactive cards are current; no refresh was needed.")
    if snapshot:
        merchant = snapshot.get("merchant_profile", {}).get("merchant", {}) or {}
        st.divider()
        st.markdown(f"**{merchant.get('merchant_trade_name') or st.session_state.merchant_id}**")
        st.caption(display_snapshot.get("classification", {}).get("label", "Unknown"))
        st.caption(display_snapshot.get("data_coverage", {}).get("coverage_label", "Payments only"))
        if st.session_state.selected_terminal_id:
            st.caption(f"Terminal focus: {st.session_state.selected_terminal_id}")
    elif snapshot_error:
        st.error(snapshot_error)

if snapshot_error:
    st.error(f"Merchant snapshot could not be loaded: {snapshot_error}")

if display_snapshot:
    render_scope_banner(display_snapshot)

tabs = st.tabs(
    [
        "Home",
        "Action Center",
        "Operations Agent",
        "Growth Agent",
        "Money",
        "Disputes",
        "Terminals",
        "Connected Systems",
        "Reports",
    ]
)

with tabs[0]:
    if display_snapshot:
        render_home(display_snapshot)
    else:
        st.info("Merchant snapshot is not available yet.")

with tabs[1]:
    if display_snapshot:
        st.subheader("Action Center")
        cleanup_fn = getattr(merchant_os, "cleanup_legacy_actions", None)
        cleanup_result = st.session_state.get("action_center_cleanup_result")
        if isinstance(cleanup_result, dict):
            if cleanup_result.get("hidden_count"):
                st.success(
                    f"Hidden {int(cleanup_result.get('hidden_count') or 0)} legacy or duplicate action(s)."
                )
            elif cleanup_result.get("error"):
                st.error(str(cleanup_result.get("error")))
            else:
                st.info("No legacy or duplicate actions needed cleanup.")
        if cleanup_fn is None:
            st.warning("Legacy action cleanup is unavailable until the latest app code is loaded.")
        elif st.button("Hide legacy and duplicate items", key="action_center_cleanup"):
            st.session_state["action_center_cleanup_result"] = cleanup_fn(
                get_engine(),
                str(display_snapshot.get("merchant_id") or ""),
            )
            load_merchant_snapshot.clear()
            st.rerun()
        render_existing_action_cards(display_snapshot, key_prefix="action_center", show_editor=True)
    else:
        st.info("Action center is unavailable until a merchant snapshot is loaded.")

with tabs[2]:
    if display_snapshot:
        st.subheader("Operations signals")
        render_proactive_cards(
            [row for row in dict_rows(display_snapshot.get("proactive_cards")) if str(row.get("lane") or "").lower() == "operations"],
            key_prefix="operations_tab_cards",
            empty_text="No proactive operations signals are available for this merchant.",
            interactive=True,
        )
    render_lane_chat(
        lane="operations",
        title="Operations Agent",
        caption="Handles settlements, deductions, reconciliation, refunds, and chargeback workflows.",
        prompt="Ask about payouts, deductions, settlements, chargebacks, or reconciliation…",
        chat_key="operations_chat",
        snapshot=display_snapshot,
    )

with tabs[3]:
    if display_snapshot:
        render_growth_workspace(display_snapshot)
    render_lane_chat(
        lane="growth",
        title="Growth Agent",
        caption="Handles acceptance lift, failure reduction, routing, device opportunities, and revenue nudges.",
        prompt="Ask about success rate, failure drivers, acceptance, terminals, or growth opportunities…",
        chat_key="growth_chat",
        snapshot=display_snapshot,
    )

with tabs[4]:
    if display_snapshot:
        render_money(display_snapshot)
    else:
        st.info("Money view is unavailable until a merchant snapshot is loaded.")

with tabs[5]:
    if display_snapshot:
        render_disputes(display_snapshot)
    else:
        st.info("Disputes view is unavailable until a merchant snapshot is loaded.")

with tabs[6]:
    if display_snapshot:
        render_terminals(display_snapshot)
    else:
        st.info("Terminal view is unavailable until a merchant snapshot is loaded.")

with tabs[7]:
    if display_snapshot:
        render_connected_systems(display_snapshot)
    else:
        st.info("Connected systems view is unavailable until a merchant snapshot is loaded.")

with tabs[8]:
    if display_snapshot:
        render_reports(display_snapshot)
    else:
        st.info("Reports are unavailable until a merchant snapshot is loaded.")

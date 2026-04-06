from __future__ import annotations

import datetime as dt
import json
from typing import Any

from sqlalchemy import text

from app.application.workflows.ops_intake import auto_intake_settlement_ops_cases
from app.application.workflows import reporting as reporting_workflow
from app.data.actions import repository as action_repository
from app.data.evidence import normalize_evidence_ids
from app.intelligence.action_center import ACTIVE_STATUSES as ACTIVE_QUEUE_STATUSES
from app.intelligence.action_center import LOW_SIGNAL_TITLES as LOW_SIGNAL_ACTION_TITLES
from app.intelligence.action_center import create_action
from app.intelligence.insight_cards import generate_insight_cards
from app.intelligence.payout_shortfall_monitor import generate_payout_shortfall_alerts
from app.copilot.toolcalling import default_window_from_max_date
from app.copilot.tools import (
    ToolContext,
    cashflow_snapshot,
    compute_kpis,
    get_merchant_context,
    intelligence_probe,
    list_chargebacks,
    list_refunds,
    list_settlements,
    propose_and_create_merchant_action,
    terminal_health_summary,
    terminal_performance,
    verify_failure_drivers,
)
from app.data.merchant_ops import repository as merchant_ops_repository
from app.data.proactive import repository as proactive_repository
from config import Config


INTEGRATION_TABLE_CANDIDATES: dict[str, tuple[str, ...]] = {
    "erp": ("erp_connections", "merchant_erp_connections", "merchant_integrations"),
    "accounting": ("accounting_connections", "merchant_accounting_connections", "merchant_integrations"),
    "pos": ("pos_connections", "merchant_pos_connections", "merchant_integrations"),
    "api": ("merchant_api_connections", "webhook_endpoints", "api_clients", "merchant_integrations"),
}

BACKGROUND_CARD_LANE_MAP: dict[str, str] = {
    "chargeback_deadline": "operations",
    "settlement_delay": "operations",
    "kyc_expiry": "operations",
    "refund_rate_spike": "operations",
    "high_value_failed_txns": "growth",
    "success_rate_drop": "growth",
    "terminal_anomaly": "growth",
    "upi_callback_delay_spike": "growth",
}


def _table_columns(engine: Any, table: str) -> set[str]:
    return merchant_ops_repository.table_columns(engine, table)


def _table_exists(engine: Any, table: str) -> bool:
    return merchant_ops_repository.table_exists(engine, table)


def _ctx(engine: Any, merchant_id: str) -> ToolContext:
    return ToolContext(engine=engine, merchant_id=merchant_id)


def _safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        return {}


def _summary_row(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return rows[0]
    return {}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _evidence_ids_from_payload(value: Any) -> list[str]:
    return normalize_evidence_ids(value)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _dict_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, dict)]


def _terminal_id_value(row: Any) -> str | None:
    if not isinstance(row, dict):
        return None
    return _clean_optional_text(row.get("terminal_id")) or _clean_optional_text(row.get("tid"))


def _row_references_terminal(row: Any, terminal_id: str) -> bool:
    normalized_tid = _clean_optional_text(terminal_id)
    if normalized_tid is None or not isinstance(row, dict):
        return False

    if _terminal_id_value(row) == normalized_tid:
        return True

    for evidence_id in row.get("evidence_ids") or []:
        text_value = str(evidence_id or "").strip()
        if text_value in {f"terminal:{normalized_tid}", f"tid:{normalized_tid}"}:
            return True

    for payload_key in ("payload", "evidence_payload"):
        payload = row.get(payload_key)
        if isinstance(payload, dict):
            payload_tid = _clean_optional_text(payload.get("terminal_id")) or _clean_optional_text(payload.get("tid"))
            if payload_tid == normalized_tid:
                return True

    return False


def _filter_rows_by_terminal(rows: Any, terminal_id: str) -> list[dict[str, Any]]:
    return [dict(row) for row in (rows or []) if isinstance(row, dict) and _row_references_terminal(row, terminal_id)]


def terminal_scope_options(snapshot: dict[str, Any]) -> list[str]:
    terminal_ids: set[str] = set()
    terminals = snapshot.get("terminals", {}) if isinstance(snapshot.get("terminals"), dict) else {}
    terminal_health = snapshot.get("terminal_health", {}) if isinstance(snapshot.get("terminal_health"), dict) else {}
    for payload in (terminals.get("rows"), terminal_health.get("rows")):
        for row in payload or []:
            terminal_id = _terminal_id_value(row)
            if terminal_id:
                terminal_ids.add(terminal_id)
    return sorted(terminal_ids)


def _action_dedupe_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _normalize_text(row.get("category")),
        _normalize_text(row.get("title")),
        _normalize_text(row.get("description")),
    )


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _normalize_follow_up_date(value: Any) -> str | None:
    text_value = _clean_optional_text(value)
    if text_value is None:
        return None
    try:
        return dt.date.fromisoformat(text_value).isoformat()
    except Exception as exc:
        raise ValueError("follow_up_date must be YYYY-MM-DD") from exc


def _terminal_scope_summary_from_source(
    engine: Any,
    merchant_id: str,
    terminal_id: str,
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    return merchant_ops_repository.terminal_scope_summary_from_source(
        engine,
        merchant_id,
        terminal_id,
        from_date,
        to_date,
        source_table=Config.QUERY_SOURCE_TABLE,
    )


def _terminal_scope_kpis_by_mode(
    engine: Any,
    merchant_id: str,
    terminal_id: str,
    from_date: str,
    to_date: str,
) -> list[dict[str, Any]]:
    return merchant_ops_repository.terminal_scope_kpis_by_mode(
        engine,
        merchant_id,
        terminal_id,
        from_date,
        to_date,
        source_table=Config.QUERY_SOURCE_TABLE,
    )


def _terminal_scope_failure_drivers(
    engine: Any,
    merchant_id: str,
    terminal_id: str,
    from_date: str,
    to_date: str,
    *,
    by: str,
    limit: int = 5,
) -> dict[str, Any]:
    return merchant_ops_repository.terminal_scope_failure_drivers(
        engine,
        merchant_id,
        terminal_id,
        from_date,
        to_date,
        by=by,
        limit=limit,
        source_table=Config.QUERY_SOURCE_TABLE,
    )


def _action_meta_from_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    meta = evidence.get("action_meta")
    return meta if isinstance(meta, dict) else {}


def _background_card_lane(card: dict[str, Any]) -> str:
    explicit_lane = str(card.get("lane") or "").strip().lower()
    if explicit_lane in {"operations", "growth"}:
        return explicit_lane
    card_id = str(card.get("id") or "").strip().lower()
    return BACKGROUND_CARD_LANE_MAP.get(card_id, "growth")


def _background_card_action_spec(card: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    card_id = str(card.get("id") or "").strip().lower()
    lane = str(card.get("lane") or _background_card_lane(card)).strip().lower()
    if card_id.startswith("payout_shortfall_"):
        action_type = "SETTLEMENT_INVESTIGATION"
    elif card_id == "chargeback_deadline":
        action_type = "CHARGEBACK_REVIEW"
    elif card_id == "settlement_delay":
        action_type = "SETTLEMENT_INVESTIGATION"
    elif card_id == "terminal_anomaly":
        action_type = "TERMINAL_REVIEW"
    elif card_id == "kyc_expiry":
        action_type = "KYC_RENEWAL"
    elif lane == "growth":
        action_type = "GROWTH_PLAYBOOK"
    else:
        action_type = "FOLLOW_UP"

    payload = {
        "category": lane or "workflow",
        "title": str(card.get("title") or "Proactive merchant signal").strip(),
        "description": str(card.get("body") or "").strip(),
        "impact_rupees": float(card.get("impact_rupees") or 0.0),
        "confidence": float(card.get("confidence") or 0.0),
        "terminal_id": _clean_optional_text(card.get("terminal_id")),
        "owner": "merchant_ui",
        "evidence": {
            "source": str(card.get("source") or "insight_card_engine"),
            "dedupe_key": str(card.get("dedupe_key") or ""),
            "evidence_ids": normalize_evidence_ids(card.get("evidence_ids")),
            "actions": list(card.get("actions") or []),
        },
    }
    return action_type, payload


def _count_rows(engine: Any, table: str, merchant_id: str) -> int:
    return merchant_ops_repository.count_rows(engine, table, merchant_id)


def _integration_status_from_table(engine: Any, table: str, merchant_id: str, integration_type: str) -> dict[str, Any] | None:
    return merchant_ops_repository.integration_status_from_table(engine, table, merchant_id, integration_type)


def detect_connected_systems(engine: Any, merchant_id: str) -> dict[str, Any]:
    return merchant_ops_repository.detect_connected_systems(
        engine,
        merchant_id,
        integration_table_candidates=INTEGRATION_TABLE_CANDIDATES,
        query_source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )


def _operating_signals(engine: Any, merchant_id: str, from_date: str, to_date: str) -> dict[str, Any]:
    return merchant_ops_repository.operating_signals(
        engine,
        merchant_id,
        from_date,
        to_date,
        query_source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )


def classify_merchant(*, merchant_profile: dict[str, Any], kpi_snapshot: dict[str, Any], data_coverage: dict[str, Any], operating_signals: dict[str, Any]) -> dict[str, Any]:
    merchant = merchant_profile.get("merchant") if isinstance(merchant_profile.get("merchant"), dict) else {}
    annual_turnover = float(merchant.get("annual_turnover") or 0.0)
    attempts = int(kpi_snapshot.get("attempts") or 0)
    distinct_terminals = int(operating_signals.get("distinct_terminals") or 0)
    invoice_coverage = float(operating_signals.get("invoice_reference_coverage_pct") or 0.0)
    systems = data_coverage.get("systems") if isinstance(data_coverage.get("systems"), dict) else {}

    reasons: list[str] = []
    code = "guided"
    label = "Guided Ops"

    if bool(systems.get("erp", {}).get("connected")) or bool(systems.get("accounting", {}).get("connected")):
        code = "enterprise_connected"
        label = "Enterprise / Connected"
        reasons.append("Explicit ERP/accounting integration detected.")
    elif annual_turnover >= 250000000.0 or distinct_terminals >= 5 or attempts >= 15000:
        code = "enterprise"
        label = "Enterprise Control Plane"
        reasons.append("High operating scale detected from turnover, terminal count, or transaction volume.")
    elif distinct_terminals >= 2 or attempts >= 4000 or invoice_coverage >= 60.0:
        code = "control_plane"
        label = "Control Plane Merchant"
        reasons.append("Moderate complexity detected from terminal spread, transaction volume, or billing reference coverage.")
    else:
        reasons.append("Merchant appears best suited to guided payments operations workflows.")

    if bool(systems.get("pos", {}).get("connected")) and "Explicit ERP/accounting integration detected." not in reasons:
        reasons.append("POS connectivity detected.")
    if bool(systems.get("settlements", {}).get("connected")):
        reasons.append("Settlement operations data available.")
    if bool(systems.get("chargebacks", {}).get("row_count")) or bool(systems.get("refunds", {}).get("row_count")):
        reasons.append("Dispute/refund workflows available.")

    return {
        "code": code,
        "label": label,
        "reasons": reasons[:4],
    }


def list_existing_actions(engine: Any, merchant_id: str, *, limit: int = 10) -> list[dict[str, Any]]:
    return action_repository.list_existing_actions(
        engine,
        merchant_id,
        limit=limit,
        low_signal_titles=LOW_SIGNAL_ACTION_TITLES,
    )


def cleanup_legacy_actions(engine: Any, merchant_id: str, *, hide_status: str = "HIDDEN") -> dict[str, Any]:
    return action_repository.cleanup_legacy_actions(
        engine,
        merchant_id,
        hide_status=hide_status,
        low_signal_titles=LOW_SIGNAL_ACTION_TITLES,
        active_queue_statuses=ACTIVE_QUEUE_STATUSES,
    )


def update_existing_action_status(engine: Any, merchant_id: str, *, action_id: Any, status: str) -> dict[str, Any]:
    return action_repository.update_existing_action_status(
        engine,
        merchant_id,
        action_id=action_id,
        status=status,
    )


def update_existing_action_details(
    engine: Any,
    merchant_id: str,
    *,
    action_id: Any,
    owner: Any = None,
    notes: Any = None,
    blocked_reason: Any = None,
    follow_up_date: Any = None,
) -> dict[str, Any]:
    return action_repository.update_existing_action_details(
        engine,
        merchant_id,
        action_id=action_id,
        owner=owner,
        notes=notes,
        blocked_reason=blocked_reason,
        follow_up_date=follow_up_date,
    )


def _ensure_proactive_cards_schema(engine: Any) -> set[str]:
    return proactive_repository.ensure_proactive_cards_schema(engine)


def _ensure_proactive_refresh_schedule_schema(engine: Any) -> set[str]:
    return proactive_repository.ensure_proactive_refresh_schedule_schema(engine)


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_iso_timestamp(value: Any) -> dt.datetime | None:
    text_value = _clean_optional_text(value)
    if text_value is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text_value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def get_background_refresh_status(
    engine: Any,
    merchant_id: str,
    *,
    days: int = 30,
    min_interval_minutes: int | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    interval_minutes = max(1, int(min_interval_minutes or getattr(Config, "PROACTIVE_REFRESH_INTERVAL_MINUTES", 30) or 30))
    current_time = now.astimezone(dt.timezone.utc) if isinstance(now, dt.datetime) and now.tzinfo else (now.replace(tzinfo=dt.timezone.utc) if isinstance(now, dt.datetime) else _utc_now())
    return proactive_repository.get_background_refresh_status(
        engine,
        merchant_id,
        days=days,
        interval_minutes=interval_minutes,
        auto_enabled=bool(getattr(Config, "PROACTIVE_AUTO_REFRESH_ENABLED", True)),
        now=current_time,
    )


def ensure_background_proactive_refresh(
    engine: Any,
    merchant_id: str,
    *,
    days: int = 30,
    limit: int = 8,
    min_interval_minutes: int | None = None,
    force: bool = False,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    interval_minutes = max(1, int(min_interval_minutes or getattr(Config, "PROACTIVE_REFRESH_INTERVAL_MINUTES", 30) or 30))
    current_time = now.astimezone(dt.timezone.utc) if isinstance(now, dt.datetime) and now.tzinfo else (now.replace(tzinfo=dt.timezone.utc) if isinstance(now, dt.datetime) else _utc_now())
    status = get_background_refresh_status(
        engine,
        merchant_id,
        days=days,
        min_interval_minutes=interval_minutes,
        now=current_time,
    )
    if not bool(getattr(Config, "PROACTIVE_AUTO_REFRESH_ENABLED", True)) and not force:
        status["skipped"] = True
        status["reason"] = "auto_refresh_disabled"
        return status
    if not force and not status.get("due"):
        status["skipped"] = True
        status["reason"] = "not_due"
        status["refreshed"] = False
        return status

    result = refresh_background_proactive_cards(engine, merchant_id, days=days, limit=limit)
    next_refresh_at = current_time + dt.timedelta(minutes=interval_minutes)
    window = result.get("window", {}) if isinstance(result.get("window"), dict) else {}
    proactive_repository.upsert_background_refresh_schedule(
        engine,
        merchant_id,
        days=int(days),
        current_time=current_time,
        next_refresh_at=next_refresh_at,
        window_from=str(window.get("from") or ""),
        window_to=str(window.get("to") or ""),
        generated_count=int(result.get("generated_count") or 0),
        inserted_count=int(result.get("inserted_count") or 0),
    )

    refreshed_status = get_background_refresh_status(
        engine,
        merchant_id,
        days=days,
        min_interval_minutes=interval_minutes,
        now=current_time,
    )
    refreshed_status["refreshed"] = True
    refreshed_status["generated_count"] = int(result.get("generated_count") or 0)
    refreshed_status["inserted_count"] = int(result.get("inserted_count") or 0)
    refreshed_status["ops_case_intake"] = dict(result.get("ops_case_intake") or {})
    refreshed_status["reason"] = "forced" if force else "due"
    return refreshed_status


def update_background_proactive_card_state(
    engine: Any,
    merchant_id: str,
    *,
    dedupe_key: str,
    state: str,
    card_notes: Any = None,
    converted_action_id: Any = None,
) -> dict[str, Any]:
    return proactive_repository.update_background_proactive_card_state(
        engine,
        merchant_id,
        dedupe_key=dedupe_key,
        state=state,
        card_notes=card_notes,
        converted_action_id=converted_action_id,
    )


def preview_background_proactive_card_action(engine: Any, merchant_id: str, *, dedupe_key: str) -> dict[str, Any]:
    cards = list_background_proactive_cards(engine, merchant_id, limit=100)
    card = next((item for item in cards if str(item.get("dedupe_key") or "") == str(dedupe_key)), None)
    if not card:
        return {"error": "proactive card not found", "dedupe_key": dedupe_key}
    existing_action_id = _clean_optional_text(card.get("linked_action_id") or card.get("converted_action_id"))
    if existing_action_id:
        return {
            "dedupe_key": dedupe_key,
            "existing_action_id": existing_action_id,
            "status": "already_linked",
            "message": "This proactive signal already has an Action Center item.",
        }
    action_type, payload = _background_card_action_spec(card)
    return preview_merchant_action(engine, merchant_id, action_type=action_type, payload=payload)


def confirm_background_proactive_card_action(
    engine: Any,
    merchant_id: str,
    *,
    dedupe_key: str,
    confirmation_token: str,
) -> dict[str, Any]:
    result = confirm_merchant_action(engine, merchant_id, confirmation_token=confirmation_token)
    if result.get("action_id"):
        update_background_proactive_card_state(
            engine,
            merchant_id,
            dedupe_key=dedupe_key,
            state="CONVERTED",
            converted_action_id=result.get("action_id"),
        )
    return result


def list_background_proactive_cards(engine: Any, merchant_id: str, *, limit: int = 8) -> list[dict[str, Any]]:
    return proactive_repository.list_background_proactive_cards(engine, merchant_id, limit=limit)


def refresh_background_proactive_cards(engine: Any, merchant_id: str, *, days: int = 30, limit: int = 8) -> dict[str, Any]:
    window_from, window_to = default_window_from_max_date(engine, merchant_id, days=days)
    generated = list(generate_insight_cards(engine=engine, merchant_id=merchant_id, window_days=days) or [])
    shortfall_alerts = generate_payout_shortfall_alerts(
        engine,
        merchant_id,
        window_from=window_from,
        window_to=window_to,
        limit=min(int(limit or 8), 3),
    )
    generated.extend([alert.get("card") for alert in shortfall_alerts if isinstance(alert.get("card"), dict)])
    ranked = sorted(
        [card for card in generated if isinstance(card, dict)],
        key=lambda card: (float(card.get("impact_rupees") or 0.0), float(card.get("confidence") or 0.0)),
        reverse=True,
    )[: int(limit)]
    shortfall_by_card_id = {
        str(alert.get("card", {}).get("id") or ""): alert
        for alert in shortfall_alerts
        if isinstance(alert, dict) and isinstance(alert.get("card"), dict)
    }

    result = proactive_repository.persist_background_proactive_cards(
        engine,
        merchant_id,
        window_from=window_from,
        window_to=window_to,
        ranked_cards=ranked,
        shortfall_by_card_id=shortfall_by_card_id,
        lane_resolver=_background_card_lane,
        create_action_fn=lambda mid, action: create_action(engine, mid, action),
    )
    persisted_cards = [
        card
        for card in proactive_repository.list_background_proactive_cards(engine, merchant_id, limit=100)
        if isinstance(card, dict)
        and isinstance(card.get("window"), dict)
        and str(card.get("window", {}).get("from") or "") == window_from
        and str(card.get("window", {}).get("to") or "") == window_to
    ]
    intake = auto_intake_settlement_ops_cases(
        engine,
        merchant_id,
        cards=persisted_cards,
    )
    result["ops_case_intake"] = intake
    return result


def build_operational_tasks(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    merchant_id = str(snapshot.get("merchant_id") or "")
    window = snapshot.get("window", {}) if isinstance(snapshot.get("window"), dict) else {}
    cashflow = snapshot.get("cashflow", {}) if isinstance(snapshot.get("cashflow"), dict) else {}
    past_expected = cashflow.get("past_expected", {}) if isinstance(cashflow.get("past_expected"), dict) else {}
    summary = snapshot.get("summary", {}) if isinstance(snapshot.get("summary"), dict) else {}
    settlements = snapshot.get("settlements", {}) if isinstance(snapshot.get("settlements"), dict) else {}

    past_expected_count = int(past_expected.get("past_expected_count") or 0)
    if past_expected_count > 0:
        tasks.append(
            {
                "lane": "operations",
                "title": "Investigate delayed settlements",
                "description": (
                    f"{past_expected_count} settlement(s) are past expected date for "
                    f"Rs {float(past_expected.get('past_expected_amount') or 0.0):,.2f}."
                ),
                "action_type": "SETTLEMENT_INVESTIGATION",
                "payload": {
                    "merchant_id": merchant_id,
                    "window": window,
                    "past_expected_count": past_expected_count,
                    "past_expected_amount": float(past_expected.get("past_expected_amount") or 0.0),
                },
                "priority": "high",
            }
        )

    open_chargebacks = int(summary.get("open_chargebacks") or 0)
    if open_chargebacks > 0:
        tasks.append(
            {
                "lane": "operations",
                "title": "Review open chargebacks",
                "description": f"{open_chargebacks} chargeback case(s) are still open and need merchant review.",
                "action_type": "CHARGEBACK_REVIEW",
                "payload": {
                    "merchant_id": merchant_id,
                    "window": window,
                    "open_chargebacks": open_chargebacks,
                },
                "priority": "high",
            }
        )

    settlement_rows = settlements.get("rows") if isinstance(settlements.get("rows"), list) else []
    if settlement_rows:
        latest = settlement_rows[0] if isinstance(settlement_rows[0], dict) else {}
        if latest.get("settlement_id"):
            tasks.append(
                {
                    "lane": "operations",
                    "title": "Drill into latest settlement",
                    "description": (
                        f"Latest settlement {latest.get('settlement_id')} is in status "
                        f"{latest.get('status') or 'UNKNOWN'} for Rs {float(latest.get('amount_rupees') or 0.0):,.2f}."
                    ),
                    "action_type": "SETTLEMENT_DETAIL_REVIEW",
                    "payload": {
                        "merchant_id": merchant_id,
                        "settlement_id": str(latest.get("settlement_id")),
                    },
                    "priority": "medium",
                }
            )

    return tasks[:4]


def build_growth_tasks(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    merchant_id = str(snapshot.get("merchant_id") or "")
    window = snapshot.get("window", {}) if isinstance(snapshot.get("window"), dict) else {}
    scope = snapshot.get("scope", {}) if isinstance(snapshot.get("scope"), dict) else {}
    terminal_id = _clean_optional_text(scope.get("terminal_id"))

    for reco in snapshot.get("intelligence", {}).get("recommendations") or []:
        if not isinstance(reco, dict):
            continue
        category = str(reco.get("category") or "").lower()
        if category not in {"growth", "performance"}:
            continue
        title = str(reco.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        tasks.append(
            {
                "lane": "growth",
                "title": title,
                "description": str(reco.get("summary") or "").strip(),
                "action_type": "GROWTH_PLAYBOOK",
                "payload": {
                        "merchant_id": merchant_id,
                        "window": window,
                        "recommendation_title": title,
                        "category": reco.get("category"),
                        "impact_rupees": reco.get("impact_rupees"),
                        "evidence_ids": reco.get("evidence_ids") or [],
                        "terminal_id": terminal_id,
                    },
                    "priority_score": reco.get("priority_score"),
                    "confidence": reco.get("confidence"),
                    "evidence_ids": reco.get("evidence_ids") or [],
                }
        )

    pay_mode_rows = snapshot.get("failure_drivers", {}).get("payment_mode", {}).get("rows") or []
    if pay_mode_rows and isinstance(pay_mode_rows[0], dict):
        top = pay_mode_rows[0]
        driver = str(top.get("driver") or "UNKNOWN")
        title = f"Reduce {driver} acceptance failures"
        if title not in seen_titles:
            tasks.append(
                {
                    "lane": "growth",
                    "title": title,
                    "description": (
                        f"{driver} has {int(top.get('failed_txns') or 0):,} failed txns and "
                        f"Rs {float(top.get('failed_gmv') or 0.0):,.2f} failed GMV in the active window."
                    ),
                    "action_type": "ACCEPTANCE_REVIEW",
                    "payload": {
                        "merchant_id": merchant_id,
                        "window": window,
                        "driver": driver,
                        "failed_txns": int(top.get("failed_txns") or 0),
                        "failed_gmv": float(top.get("failed_gmv") or 0.0),
                        "terminal_id": terminal_id,
                    },
                    "priority": "high",
                }
            )

    return tasks[:4]


def scope_snapshot_to_terminal(engine: Any, snapshot: dict[str, Any], terminal_id: str | None) -> dict[str, Any]:
    normalized_tid = _clean_optional_text(terminal_id)
    base_snapshot = dict(snapshot or {})
    if not normalized_tid:
        base_snapshot["scope"] = {
            "level": "merchant",
            "label": "All terminals",
            "terminal_id": None,
            "notes": [],
        }
        return base_snapshot

    window = base_snapshot.get("window", {}) if isinstance(base_snapshot.get("window"), dict) else {}
    merchant_id = str(base_snapshot.get("merchant_id") or "")
    from_date = str(window.get("from") or "")
    to_date = str(window.get("to") or "")
    scoped = dict(base_snapshot)

    scoped["terminals"] = {
        **(base_snapshot.get("terminals", {}) if isinstance(base_snapshot.get("terminals"), dict) else {}),
        "rows": _filter_rows_by_terminal((base_snapshot.get("terminals", {}) or {}).get("rows"), normalized_tid),
        "evidence": [f"terminal:{normalized_tid}"],
    }
    scoped["terminal_health"] = {
        **(base_snapshot.get("terminal_health", {}) if isinstance(base_snapshot.get("terminal_health"), dict) else {}),
        "rows": _filter_rows_by_terminal((base_snapshot.get("terminal_health", {}) or {}).get("rows"), normalized_tid),
        "evidence": [f"tid:{normalized_tid}"],
    }
    scoped["existing_actions"] = [
        dict(row)
        for row in _dict_rows(base_snapshot.get("existing_actions"))
        if _row_references_terminal(row, normalized_tid)
    ]
    scoped["proactive_cards"] = [
        dict(row)
        for row in _dict_rows(base_snapshot.get("proactive_cards"))
        if _row_references_terminal(row, normalized_tid)
    ]

    summary = dict(base_snapshot.get("summary") or {})
    terminal_summary = _terminal_scope_summary_from_source(engine, merchant_id, normalized_tid, from_date, to_date)
    if terminal_summary:
        summary.update(
            {
                "attempts": int(terminal_summary.get("attempts") or 0),
                "success_txns": int(terminal_summary.get("success_txns") or 0),
                "fail_txns": int(terminal_summary.get("fail_txns") or 0),
                "success_rate_pct": float(terminal_summary.get("success_rate_pct") or 0.0),
                "success_gmv": float(terminal_summary.get("success_gmv") or 0.0),
                "failed_gmv": float(terminal_summary.get("failed_gmv") or 0.0),
                "terminal_count": 1,
            }
        )
        scoped["kpi_snapshot"] = dict(terminal_summary)
    else:
        terminal_rows = scoped["terminals"].get("rows") or []
        terminal_row = terminal_rows[0] if terminal_rows and isinstance(terminal_rows[0], dict) else {}
        attempts = int(terminal_row.get("attempts") or 0)
        success_rate_pct = float(terminal_row.get("success_rate_pct") or 0.0)
        success_txns = int(round(attempts * success_rate_pct / 100.0)) if attempts else 0
        summary.update(
            {
                "attempts": attempts,
                "success_txns": success_txns,
                "fail_txns": max(attempts - success_txns, 0),
                "success_rate_pct": success_rate_pct,
                "success_gmv": float(terminal_row.get("success_gmv") or 0.0),
                "terminal_count": 1,
            }
        )
        scoped["kpi_snapshot"] = {
            "attempts": attempts,
            "success_txns": success_txns,
            "fail_txns": max(attempts - success_txns, 0),
            "success_rate_pct": success_rate_pct,
            "success_gmv": float(terminal_row.get("success_gmv") or 0.0),
            "failed_gmv": float(summary.get("failed_gmv") or 0.0),
        }
    scoped["summary"] = summary

    scoped["kpi_by_mode"] = _terminal_scope_kpis_by_mode(engine, merchant_id, normalized_tid, from_date, to_date)
    scoped["failure_drivers"] = {
        "payment_mode": _terminal_scope_failure_drivers(
            engine,
            merchant_id,
            normalized_tid,
            from_date,
            to_date,
            by="payment_mode",
            limit=5,
        ),
        "response_code": _terminal_scope_failure_drivers(
            engine,
            merchant_id,
            normalized_tid,
            from_date,
            to_date,
            by="response_code",
            limit=5,
        ),
    }
    scoped["scope"] = {
        "level": "terminal",
        "label": f"Terminal {normalized_tid}",
        "terminal_id": normalized_tid,
        "notes": [
            "Transaction metrics, failure drivers, terminal health, proactive cards, and report packs are filtered to the selected terminal.",
            "Settlements, chargebacks, refunds, action-center rows, and agent chat remain merchant-wide unless terminal evidence is explicitly present.",
        ],
    }

    scoped_intelligence = dict(base_snapshot.get("intelligence") or {})
    scoped_intelligence["recommendations"] = [
        dict(row)
        for row in scoped_intelligence.get("recommendations") or []
        if isinstance(row, dict) and _row_references_terminal(row, normalized_tid)
    ]
    scoped["intelligence"] = scoped_intelligence
    scoped["growth_tasks"] = build_growth_tasks(scoped)
    scoped["operations_tasks"] = [
        dict(row)
        for row in _dict_rows(base_snapshot.get("operations_tasks"))
        if _row_references_terminal(row, normalized_tid)
    ]
    return scoped


def preview_merchant_action(engine: Any, merchant_id: str, *, action_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    ctx = _ctx(engine, merchant_id)
    return propose_and_create_merchant_action(
        ctx,
        action_type=action_type,
        payload=payload or {},
    )


def confirm_merchant_action(engine: Any, merchant_id: str, *, confirmation_token: str) -> dict[str, Any]:
    ctx = _ctx(engine, merchant_id)
    return propose_and_create_merchant_action(
        ctx,
        action_type="CONFIRM",
        confirmation_token=confirmation_token,
    )


def get_merchant_os_snapshot(engine: Any, merchant_id: str, *, days: int = 30) -> dict[str, Any]:
    from_date, to_date = default_window_from_max_date(engine, merchant_id, days=days)
    ctx = _ctx(engine, merchant_id)

    merchant_profile = _safe_call(get_merchant_context, ctx) or {}
    overall = _safe_call(compute_kpis, ctx, from_date=from_date, to_date=to_date, group_by="none") or {}
    by_mode = _safe_call(compute_kpis, ctx, from_date=from_date, to_date=to_date, group_by="payment_mode") or {}
    cashflow = _safe_call(cashflow_snapshot, ctx, from_date=from_date, to_date=to_date) or {}
    settlements = _safe_call(list_settlements, ctx, from_date=from_date, to_date=to_date, limit=20) or {}
    chargebacks = _safe_call(list_chargebacks, ctx, status="all", from_date=from_date, to_date=to_date, limit=20) or {}
    refunds = _safe_call(list_refunds, ctx, from_date=from_date, to_date=to_date, limit=20) or {}
    terminals = _safe_call(terminal_performance, ctx, from_date=from_date, to_date=to_date, limit=20) or {}
    terminal_health = _safe_call(terminal_health_summary, ctx, from_date=from_date, to_date=to_date, group_by="tid", limit=20) or {}
    fail_by_mode = _safe_call(verify_failure_drivers, ctx, from_date=from_date, to_date=to_date, by="payment_mode", limit=5) or {}
    fail_by_code = _safe_call(verify_failure_drivers, ctx, from_date=from_date, to_date=to_date, by="response_code", limit=5) or {}
    intelligence = _safe_call(intelligence_probe, ctx, window_days=min(max(int(days or 30), 7), 90), enable_reasoning=False) or {}
    proactive_cards = list_background_proactive_cards(engine, merchant_id, limit=8)
    if not proactive_cards:
        _safe_call(refresh_background_proactive_cards, engine, merchant_id, days=days, limit=8)
        proactive_cards = list_background_proactive_cards(engine, merchant_id, limit=8)

    data_coverage = detect_connected_systems(engine, merchant_id)
    op_signals = _operating_signals(engine, merchant_id, from_date, to_date)
    kpi_snapshot = _summary_row(overall)
    classification = classify_merchant(
        merchant_profile=merchant_profile,
        kpi_snapshot=kpi_snapshot,
        data_coverage=data_coverage,
        operating_signals=op_signals,
    )

    open_chargebacks = 0
    for row in chargebacks.get("rows") or []:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").upper()
        if status and status not in {"CLOSED", "RESOLVED"}:
            open_chargebacks += 1

    summary = {
        "attempts": int(kpi_snapshot.get("attempts") or 0),
        "success_txns": int(kpi_snapshot.get("success_txns") or 0),
        "fail_txns": int(kpi_snapshot.get("fail_txns") or 0),
        "success_rate_pct": float(kpi_snapshot.get("success_rate_pct") or 0.0),
        "success_gmv": float(kpi_snapshot.get("success_gmv") or 0.0),
        "failed_gmv": float(kpi_snapshot.get("failed_gmv") or 0.0),
        "terminal_count": int(op_signals.get("distinct_terminals") or len(terminals.get("rows") or [])),
        "open_chargebacks": open_chargebacks,
        "refund_count": len(refunds.get("rows") or []),
        "settlement_count": len(settlements.get("rows") or []),
    }

    snapshot = {
        "merchant_id": merchant_id,
        "window": {"from": from_date, "to": to_date},
        "merchant_profile": merchant_profile,
        "summary": summary,
        "kpi_snapshot": kpi_snapshot,
        "kpi_by_mode": by_mode.get("rows") or [],
        "cashflow": cashflow,
        "settlements": settlements,
        "chargebacks": chargebacks,
        "refunds": refunds,
        "terminals": terminals,
        "terminal_health": terminal_health,
        "failure_drivers": {
            "payment_mode": fail_by_mode,
            "response_code": fail_by_code,
        },
        "data_coverage": data_coverage,
        "operating_signals": op_signals,
        "classification": classification,
        "intelligence": intelligence,
        "existing_actions": list_existing_actions(engine, merchant_id, limit=10),
        "proactive_cards": proactive_cards,
    }
    snapshot["operations_tasks"] = build_operational_tasks(snapshot)
    snapshot["growth_tasks"] = build_growth_tasks(snapshot)

    return snapshot


def rows_to_csv(rows: list[dict[str, Any]]) -> bytes:
    return reporting_workflow.rows_to_csv(rows)


def _report_window_text(snapshot: dict[str, Any]) -> str:
    window = snapshot.get("window", {}) if isinstance(snapshot.get("window"), dict) else {}
    from_date = str(window.get("from") or "").strip()
    to_date = str(window.get("to") or "").strip()
    if from_date and to_date:
        return f"{from_date} to {to_date}"
    return "current window"


def _report_scope_text(snapshot: dict[str, Any]) -> str:
    scope = snapshot.get("scope", {}) if isinstance(snapshot.get("scope"), dict) else {}
    label = str(scope.get("label") or "All terminals").strip()
    return label or "All terminals"


def _report_merchant_name(snapshot: dict[str, Any]) -> str:
    merchant = snapshot.get("merchant_profile", {}).get("merchant", {}) if isinstance(snapshot.get("merchant_profile"), dict) else {}
    name = str(merchant.get("merchant_trade_name") or snapshot.get("merchant_id") or "Merchant").strip()
    return name or "Merchant"


def _report_dataset_overview(pack: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for dataset in pack.get("datasets") or []:
        if not isinstance(dataset, dict):
            continue
        title = str(dataset.get("title") or dataset.get("key") or "Dataset").strip()
        rows = dataset.get("rows") if isinstance(dataset.get("rows"), list) else []
        lines.append(f"{title}: {len(rows):,} row(s)")
    return lines


def _brief_text_bytes(text_value: str) -> bytes:
    return str(text_value or "").encode("utf-8")


def build_report_briefs(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return reporting_workflow.build_report_briefs(snapshot)


def build_report_packs(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return reporting_workflow.build_report_packs(snapshot)

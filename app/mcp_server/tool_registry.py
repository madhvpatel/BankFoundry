from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, Callable

from config import Config

from app.data.connectors import list_connector_runs_for_case
from app.data.knowledge import repository as knowledge_repository
from app.data.disputes import repository as disputes_repository
from app.data.merchants import repository as merchants_repository
from app.data.ops import repository as ops_repository
from app.data.proactive import repository as proactive_repository
from app.data.settlements import repository as settlements_repository
from app.data.terminals import repository as terminals_repository
from app.data.transactions import repository as transactions_repository
from app.intelligence.quality_checks import run_data_quality_checks
from app.mcp_server.guards import bounded_limit, bounded_window, require_merchant_id
from app.mcp_server.sql_verifier import execute_verified_sql
from app.mcp_server.schemas import (
    CaseScopedInput,
    CaseScopedLimitInput,
    ChargebackDetailInput,
    ChargebackListInput,
    ComplianceGuidanceInput,
    DateWindow,
    FailureBreakdownInput,
    MCPToolDescriptor,
    MerchantDaysInput,
    MerchantScopedInput,
    MerchantWindowInput,
    OpsQueueListInput,
    PaymentModeMixInput,
    PaymentsKnowledgeInput,
    RecentTransactionsInput,
    RefundDetailInput,
    RefundListInput,
    SettlementCaseActionInput,
    SettlementDetailInput,
    SettlementListInput,
    SettlementShortfallInput,
    TerminalFailureBreakdownInput,
    TerminalHealthSummaryInput,
    TerminalProfileInput,
    ToolEnvelope,
    ToolClassification,
    ToolStatus,
    TransactionDetailInput,
    VerifiedSQLInput,
    VerificationStatus,
)
from app.ontology.ops import runbook_for_case_type, sla_policy_for_priority
from app.project_paths import repo_path

ToolHandler = Callable[[Any, dict[str, Any]], ToolEnvelope]
BLOCKED_INTEGRATION_FIXTURES_DIR = repo_path("tests", "fixtures", "bank_foundry")
INCIDENT_CASE_TYPES = {"background_refresh_issue", "incident_response"}


def _window_payload(start_date: date, end_date: date) -> DateWindow:
    return DateWindow(start_date=start_date, end_date=end_date)


def _dedupe_text_values(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text_value = str(value or "").strip()
        if text_value and text_value not in seen:
            seen.add(text_value)
            out.append(text_value)
    return out


def _load_blocked_integration_fixture(name: str) -> dict[str, Any]:
    path = BLOCKED_INTEGRATION_FIXTURES_DIR / name
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _fixture_payload_for_merchant(payload: dict[str, Any], merchant_id: str, key: str) -> dict[str, Any]:
    merchants = payload.get("merchants")
    if isinstance(merchants, dict):
        merchant_payload = merchants.get(merchant_id) or merchants.get("default")
        if isinstance(merchant_payload, dict):
            out = dict(merchant_payload)
            out.setdefault("merchant_id", merchant_id)
            out.setdefault(key, [])
            return out
    if str(payload.get("merchant_id") or "").strip() == merchant_id:
        out = dict(payload)
        out.setdefault(key, [])
        return out
    return {"merchant_id": merchant_id, key: []}


def _case_detail_for_tool(engine: Any, *, merchant_id: str, case_id: str) -> tuple[dict[str, Any] | None, list[str], list[str]]:
    detail = ops_repository.get_case_detail(engine, case_id)
    if detail is None:
        return None, [], ["Ops case was not found for the requested case id."]
    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    actual_merchant_id = str(case_row.get("merchant_id") or "").strip()
    if actual_merchant_id != merchant_id:
        return None, [], ["Ops case does not belong to the requested merchant scope."]
    evidence_ids = [str(item) for item in (case_row.get("evidence_ids") or []) if str(item or "").strip()]
    return detail, evidence_ids, []


def _support_tool_evidence(
    evidence_ids: list[str],
    case_id: str,
    *,
    merchant_id: str,
    source: str = "",
) -> list[str]:
    out = _dedupe_case_evidence(evidence_ids, case_id)
    if "fixture_fallback" in source:
        fixture_id = f"fixture:support_case_history:{merchant_id}"
        if fixture_id not in out:
            out.append(fixture_id)
    return out


def _extract_case_entity_id(
    detail: dict[str, Any],
    *,
    pinned_key: str,
    evidence_prefix: str,
) -> str | None:
    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    source_payload = case_row.get("source_payload") if isinstance(case_row.get("source_payload"), dict) else {}
    memory = detail.get("memory") if isinstance(detail.get("memory"), dict) else {}
    pinned_entities = memory.get("pinned_entities") if isinstance(memory.get("pinned_entities"), dict) else {}
    pinned_value = str(pinned_entities.get(pinned_key) or "").strip()
    if pinned_value:
        return pinned_value

    candidates: list[str] = []
    candidates.extend(str(item) for item in (case_row.get("evidence_ids") or []) if item)
    candidates.extend(str(item) for item in (memory.get("confirmed_evidence_ids") or []) if item)
    candidates.extend(str(item) for item in (source_payload.get("evidence_ids") or source_payload.get("sources") or []) if item)
    for item in candidates:
        if item.lower().startswith(f"{evidence_prefix}:"):
            value = item.split(":", 1)[1].strip()
            if value:
                return value

    explicit = str(source_payload.get(pinned_key) or source_payload.get("source_ref") or case_row.get("source_ref") or "").strip()
    if explicit.lower().startswith(f"{evidence_prefix}:"):
        return explicit.split(":", 1)[1].strip() or None
    return None


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _approval_state_payload(approvals: list[dict[str, Any]]) -> dict[str, Any]:
    if not approvals:
        return {"status": "not_requested"}
    latest = approvals[0]
    return {
        "status": str(latest.get("status") or "PENDING").lower(),
        "approval_id": latest.get("approval_id"),
        "action_type": latest.get("action_type"),
        "requested_at": latest.get("requested_at"),
        "receipt_ref": latest.get("receipt_ref"),
        "connector_status": latest.get("connector_status"),
    }


def _runbook_steps_with_progress(case_type: str, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    runbook = runbook_for_case_type(case_type)
    task_by_step = {
        str((task.get("metadata") or {}).get("step_id") or ""): task
        for task in tasks
        if isinstance(task, dict)
    }
    return [
        {
            "step_id": step.step_id,
            "title": step.title,
            "description": step.description,
            "action_type": step.action_type,
            "status": str((task_by_step.get(step.step_id) or {}).get("status") or "OPEN").upper(),
            "task_id": (task_by_step.get(step.step_id) or {}).get("task_id"),
            "owner": (task_by_step.get(step.step_id) or {}).get("owner"),
        }
        for step in runbook.steps
    ]


def _normalized_case_substrate(detail: dict[str, Any]) -> dict[str, Any]:
    work_item = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    tasks = detail.get("tasks") if isinstance(detail.get("tasks"), list) else []
    approvals = detail.get("approvals") if isinstance(detail.get("approvals"), list) else []
    return {
        "work_item": work_item,
        "tasks": tasks,
        "timeline": detail.get("timeline") if isinstance(detail.get("timeline"), list) else [],
        "approvals": approvals,
        "connector_runs": detail.get("connector_runs") if isinstance(detail.get("connector_runs"), list) else [],
        "memory": detail.get("memory") if isinstance(detail.get("memory"), dict) else {},
        "approval_state": _approval_state_payload(approvals),
        "runbook_steps": _runbook_steps_with_progress(str(work_item.get("case_type") or "manual_ops_review"), tasks),
    }


def _case_lookup_error(
    *,
    tool_name: str,
    merchant_id: str,
    case_id: str,
    evidence_ids: list[str],
    notes: list[str],
) -> ToolEnvelope:
    return ToolEnvelope(
        status=ToolStatus.error,
        verification=VerificationStatus.unverified,
        tool_name=tool_name,
        merchant_id=merchant_id,
        data={"case_id": case_id},
        evidence_ids=evidence_ids,
        notes=notes,
        error_message=notes[0] if notes else "Ops case not found",
    )


def get_merchant_profile(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    payload = merchants_repository.fetch_merchant_context(engine, merchant_id)
    merchant = payload.get("merchant") if isinstance(payload.get("merchant"), dict) else {}
    found = bool(merchant) and any(str(merchant.get(key) or "").strip() for key in ("merchant_trade_name", "nature_of_business", "business_city"))
    evidence_ids = [f"merchant:{merchant_id}"]
    if payload.get("risk_profile"):
        evidence_ids.append(f"merchant_risk:{merchant_id}")
    if payload.get("kyc"):
        evidence_ids.append(f"merchant_kyc:{merchant_id}")
    notes: list[str] = []
    verification = VerificationStatus.verified if found else VerificationStatus.unverified
    if not found:
        notes.append("Merchant profile was not found in the current merchant master tables.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_merchant_profile",
        merchant_id=merchant_id,
        data=payload,
        evidence_ids=evidence_ids,
        notes=notes,
    )


def get_risk_profile(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    payload = merchants_repository.fetch_merchant_context(engine, merchant_id)
    risk = payload.get("risk_profile") if isinstance(payload.get("risk_profile"), dict) else None
    notes: list[str] = []
    verification = VerificationStatus.verified if risk else VerificationStatus.unverified
    if not risk:
        notes.append("Merchant risk profile was not found in the current risk tables.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_risk_profile",
        merchant_id=merchant_id,
        data={"risk_profile": risk},
        evidence_ids=[f"merchant_risk:{merchant_id}"] if risk else [],
        notes=notes,
    )


def get_kyc_status(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    payload = merchants_repository.fetch_merchant_context(engine, merchant_id)
    kyc = payload.get("kyc") if isinstance(payload.get("kyc"), dict) else None
    notes: list[str] = []
    verification = VerificationStatus.verified if kyc else VerificationStatus.unverified
    if not kyc:
        notes.append("Merchant KYC snapshot was not found in the current KYC tables.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_kyc_status",
        merchant_id=merchant_id,
        data={"kyc": kyc},
        evidence_ids=[f"merchant_kyc:{merchant_id}"] if kyc else [],
        notes=notes,
    )


def get_watchlist_hits(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    fixture = _load_blocked_integration_fixture("watchlist_hits.json")
    payload = _fixture_payload_for_merchant(fixture, merchant_id, "hits")
    hits = [dict(item) for item in (payload.get("hits") or []) if isinstance(item, dict)]
    notes = ["Watchlist hits are currently fixture-backed until the live screening connector is available."]
    if not fixture:
        notes.append("The seeded watchlist fixture could not be loaded.")
    elif not hits:
        notes.append("No watchlist hits were found in the seeded fixture for this merchant.")
    evidence_ids = _dedupe_text_values(
        [f"fixture:watchlist_hits:{merchant_id}"]
        + [f"watchlist:{item.get('source_ref')}" for item in hits if item.get("source_ref")]
    )
    return ToolEnvelope(
        status=ToolStatus.ok if fixture else ToolStatus.error,
        verification=VerificationStatus.unverified,
        tool_name="get_watchlist_hits",
        merchant_id=merchant_id,
        data={
            "integration_mode": "fixture_backed",
            "has_hits": bool(hits),
            "hit_count": len(hits),
            "hits": hits,
        },
        evidence_ids=evidence_ids,
        notes=notes,
        error_message=None if fixture else "watchlist fixture was not available",
    )


def get_screening_results(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    fixture = _load_blocked_integration_fixture("screening_results.json")
    payload = _fixture_payload_for_merchant(fixture, merchant_id, "results")
    results = [dict(item) for item in (payload.get("results") or []) if isinstance(item, dict)]
    overall_status = str(payload.get("overall_status") or "").strip() or ("needs_review" if results else "clear")
    notes = ["Screening results are fixture-backed and must not be treated as a live clearance decision."]
    if not fixture:
        notes.append("The seeded screening-results fixture could not be loaded.")
    elif not results:
        notes.append("No screening results were found in the seeded fixture for this merchant.")
    evidence_ids = _dedupe_text_values(
        [f"fixture:screening_results:{merchant_id}"]
        + [f"screening:{item.get('source_ref')}" for item in results if item.get("source_ref")]
    )
    return ToolEnvelope(
        status=ToolStatus.ok if fixture else ToolStatus.error,
        verification=VerificationStatus.unverified,
        tool_name="get_screening_results",
        merchant_id=merchant_id,
        data={
            "integration_mode": "fixture_backed",
            "overall_status": overall_status,
            "results_count": len(results),
            "results": results,
        },
        evidence_ids=evidence_ids,
        notes=notes,
        error_message=None if fixture else "screening-results fixture was not available",
    )


def get_aml_case_context(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_aml_case_context",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    substrate = _normalized_case_substrate(detail)
    work_item = substrate.get("work_item") if isinstance(substrate.get("work_item"), dict) else {}
    timeline = substrate.get("timeline") if isinstance(substrate.get("timeline"), list) else []
    tasks = substrate.get("tasks") if isinstance(substrate.get("tasks"), list) else []
    memory = substrate.get("memory") if isinstance(substrate.get("memory"), dict) else {}
    approval_state = substrate.get("approval_state") if isinstance(substrate.get("approval_state"), dict) else {}
    source_payload = work_item.get("source_payload") if isinstance(work_item.get("source_payload"), dict) else {}
    latest_events = timeline[-3:]
    open_tasks = [task for task in tasks if isinstance(task, dict) and str(task.get("status") or "").upper() != "DONE"]
    aml_evidence = [
        str(item)
        for item in (work_item.get("evidence_ids") or [])
        if str(item or "").strip().lower().startswith(("watchlist:", "screening:", "aml:", "case:"))
    ]
    notes = list(preflight_notes)
    case_type = str(work_item.get("case_type") or "").strip().lower()
    if case_type not in {"aml_investigation", "aml_review", "screening_review", "watchlist_review"}:
        notes.append("The case is not explicitly typed as an AML investigation, so AML context is using the current case envelope.")
    if not aml_evidence:
        notes.append("No AML-specific evidence ids are pinned on the case yet.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_aml_case_context",
        merchant_id=merchant_id,
        data={
            "case_id": validated.case_id,
            "lane": work_item.get("lane"),
            "case_type": case_type or None,
            "status": work_item.get("status"),
            "priority": work_item.get("priority"),
            "summary": work_item.get("summary"),
            "source_ref": work_item.get("source_ref"),
            "source_payload": source_payload,
            "approval_state": approval_state,
            "pinned_entities": memory.get("pinned_entities") if isinstance(memory.get("pinned_entities"), dict) else {},
            "aml_evidence_ids": aml_evidence,
            "recent_timeline": latest_events,
            "open_task_count": len(open_tasks),
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids + aml_evidence, validated.case_id),
        notes=notes,
    )


def get_velocity_anomalies(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantWindowInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    payload = transactions_repository.detect_velocity_anomalies(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    notes = ["Velocity analysis is derived from current transaction rows in the bounded merchant window."]
    if payload.get("summary"):
        notes.append(str(payload.get("summary")))
    if payload.get("error"):
        notes.append("Velocity anomaly analysis could not be fully verified from the transaction source table.")
    verification = VerificationStatus.verified if payload.get("verified") else VerificationStatus.unverified
    return ToolEnvelope(
        status=ToolStatus.ok if not payload.get("error") else ToolStatus.error,
        verification=verification,
        tool_name="get_velocity_anomalies",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "summary": payload.get("summary"),
            "anomalies": payload.get("anomalies") or [],
            "window_metrics": payload.get("window_metrics") or {},
            "daily_breakdown": payload.get("daily_breakdown") or [],
            "top_payment_modes": payload.get("top_payment_modes") or [],
            "top_hours": payload.get("top_hours") or [],
        },
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=str(payload.get("error") or "") or None,
    )


def get_dispute_risk_signals(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantWindowInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    payload = disputes_repository.dispute_risk_signals(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
    )
    notes = ["Dispute risk signals are derived from merchant-scoped chargeback and refund tables in the selected window."]
    if payload.get("summary"):
        notes.append(str(payload.get("summary")))
    if payload.get("error"):
        notes.append("Dispute risk analysis could not be fully verified from the current dispute tables.")
    verification = VerificationStatus.verified if payload.get("verified") else VerificationStatus.unverified
    return ToolEnvelope(
        status=ToolStatus.ok if not payload.get("error") else ToolStatus.error,
        verification=verification,
        tool_name="get_dispute_risk_signals",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "summary": payload.get("summary"),
            "signals": payload.get("signals") or [],
            "metrics": payload.get("metrics") or {},
            "latest_chargebacks": payload.get("latest_chargebacks") or [],
            "latest_refunds": payload.get("latest_refunds") or [],
        },
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=str(payload.get("error") or "") or None,
    )


def retrieve_compliance_guidance(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = ComplianceGuidanceInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    fixture = _load_blocked_integration_fixture("compliance_guidance.json")
    topic = str(validated.topic or "merchant_screening").strip() or "merchant_screening"
    payload = fixture
    topics = fixture.get("topics")
    if isinstance(topics, dict):
        selected = topics.get(topic) or topics.get("merchant_screening")
        if not selected and topics:
            selected = next((value for value in topics.values() if isinstance(value, dict)), {})
        payload = dict(selected) if isinstance(selected, dict) else {}
    guidance = [str(item).strip() for item in (payload.get("guidance") or []) if str(item or "").strip()]
    selected_topic = str(payload.get("topic") or topic).strip() or topic
    notes = ["Compliance guidance is currently fixture-backed and should be treated as advisory context only."]
    if not fixture:
        notes.append("The seeded compliance-guidance fixture could not be loaded.")
    elif not guidance:
        notes.append("No compliance guidance entries were available for the requested topic.")
    return ToolEnvelope(
        status=ToolStatus.ok if fixture else ToolStatus.error,
        verification=VerificationStatus.unverified,
        tool_name="retrieve_compliance_guidance",
        merchant_id=merchant_id,
        data={
            "integration_mode": "fixture_backed",
            "topic": selected_topic,
            "guidance": guidance,
        },
        evidence_ids=[f"fixture:compliance_guidance:{selected_topic}"],
        notes=notes,
        error_message=None if fixture else "compliance-guidance fixture was not available",
    )


def get_background_refresh_health(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantDaysInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    days = max(1, min(int(validated.days or 30), 90))
    interval_minutes = max(1, int(getattr(Config, "PROACTIVE_REFRESH_INTERVAL_MINUTES", 30) or 30))
    payload = proactive_repository.get_background_refresh_status(
        engine,
        merchant_id,
        days=days,
        interval_minutes=interval_minutes,
        auto_enabled=bool(getattr(Config, "PROACTIVE_AUTO_REFRESH_ENABLED", True)),
    )
    cards = proactive_repository.list_background_proactive_cards(engine, merchant_id, limit=100)
    state_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    for card in cards:
        state = str(card.get("card_state") or "NEW").upper()
        lane = str(card.get("lane") or "unknown").lower()
        state_counts[state] = state_counts.get(state, 0) + 1
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
    notes: list[str] = []
    verification = VerificationStatus.verified
    if payload.get("due"):
        notes.append("Background proactive refresh is currently due or overdue.")
    if not payload.get("auto_enabled"):
        notes.append("Background proactive refresh is disabled by configuration.")
    if not cards:
        notes.append("No proactive cards are currently stored for this merchant.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_background_refresh_health",
        merchant_id=merchant_id,
        data={
            "refresh_status": payload,
            "stored_card_count": len(cards),
            "state_counts": state_counts,
            "lane_counts": lane_counts,
        },
        evidence_ids=[f"background_refresh:{merchant_id}:{days}d"],
        notes=notes,
    )


def get_window_kpis(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantWindowInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    payload = transactions_repository.compute_kpis(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        group_by="none",
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    row = ((payload.get("rows") or [{}])[0] if isinstance(payload, dict) else {}) or {}
    attempts = int(row.get("attempts") or 0)
    success_txns = int(row.get("success_txns") or 0)
    fail_txns = int(row.get("fail_txns") or 0)
    success_gmv = float(row.get("success_gmv") or 0.0)
    failed_gmv = float(row.get("failed_gmv") or 0.0)
    metrics = {
        "attempts": attempts,
        "success_txns": success_txns,
        "fail_txns": fail_txns,
        "success_rate_pct": float(row.get("success_rate_pct") or 0.0),
        "success_gmv": success_gmv,
        "failed_gmv": failed_gmv,
        "avg_attempt_value": round((success_gmv + failed_gmv) / attempts, 2) if attempts else 0.0,
        "avg_success_ticket": round(success_gmv / success_txns, 2) if success_txns else 0.0,
    }
    notes: list[str] = []
    if success_txns:
        notes.append("avg_success_ticket is computed over successful transactions only.")
    verification = VerificationStatus.verified
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_window_kpis",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={"kpis": metrics},
        evidence_ids=[str(item) for item in payload.get("evidence", [])] if isinstance(payload, dict) else [],
        notes=notes,
    )


def get_failure_breakdown(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = FailureBreakdownInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    limit = bounded_limit(validated.limit, minimum=1, maximum=20)
    payload = transactions_repository.verify_failure_drivers(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        by=validated.dimension,
        limit=limit,
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    error = str(payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    notes: list[str] = []
    verification = VerificationStatus.verified
    if error:
        verification = VerificationStatus.unverified
        notes.append("Failure breakdown query did not complete cleanly.")
    elif not rows:
        notes.append("No failed transactions were found in the requested window.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_failure_breakdown",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "dimension": validated.dimension,
            "breakdown": rows,
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])] if isinstance(payload, dict) else [],
        notes=notes,
        error_message=error or None,
    )


def get_payment_mode_mix(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = PaymentModeMixInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    limit = bounded_limit(validated.limit, minimum=1, maximum=20)
    terminal_id = str(validated.terminal_id or "").strip() or None
    payload = transactions_repository.get_payment_mode_mix(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        limit=limit,
        terminal_id=terminal_id,
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    error = str(payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    notes: list[str] = []
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    if error:
        notes.append("Payment-mode mix could not be computed from the current transaction schema.")
    elif not rows:
        notes.append("No payment-mode rows were found in the requested window.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_payment_mode_mix",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={"rows": rows, "summary": payload.get("summary") or {}, "terminal_id": terminal_id},
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_recent_transactions(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = RecentTransactionsInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=90)
    limit = bounded_limit(validated.limit, minimum=1, maximum=100)
    terminal_id = str(validated.terminal_id or "").strip() or None
    payload = transactions_repository.list_transactions(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        status=validated.status,
        payment_mode=str(validated.payment_mode or "ALL").strip() or "ALL",
        limit=limit,
        terminal_id=terminal_id,
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    error = str(payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    notes: list[str] = []
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    if error:
        notes.append("Recent transaction listing could not be read from the current transaction schema.")
    elif not rows:
        notes.append("No transactions matched the requested scope.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_recent_transactions",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "rows": rows,
            "status": validated.status,
            "payment_mode": str(validated.payment_mode or "ALL").strip() or "ALL",
            "terminal_id": terminal_id,
        },
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_transaction_detail(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = TransactionDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    tx_id = str(validated.tx_id or "").strip()
    if not tx_id:
        raise ValueError("tx_id is required")
    payload = transactions_repository.get_transaction_detail(
        engine,
        merchant_id=merchant_id,
        tx_id=tx_id,
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    row = payload.get("row") if isinstance(payload.get("row"), dict) else None
    error = str(payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    notes: list[str] = []
    verification = VerificationStatus.verified if row and not error else VerificationStatus.unverified
    if error:
        notes.append("Transaction detail could not be read from the current transaction schema.")
    elif row is None:
        notes.append("Transaction detail was not found for the requested merchant and transaction id.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_transaction_detail",
        merchant_id=merchant_id,
        data={"transaction": row},
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_terminal_profile(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = TerminalProfileInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    terminal_id = str(validated.terminal_id or "").strip()
    if not terminal_id:
        raise ValueError("terminal_id is required")
    payload = terminals_repository.get_terminal_profile(
        engine,
        merchant_id=merchant_id,
        terminal_id=terminal_id,
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    terminal = payload.get("terminal") if isinstance(payload.get("terminal"), dict) else {}
    latest_health = payload.get("latest_health") if isinstance(payload.get("latest_health"), dict) else None
    tx_summary = payload.get("tx_summary") if isinstance(payload.get("tx_summary"), dict) else {}
    error = str(payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    has_evidence = bool(latest_health or any(value is not None for value in terminal.values()) or tx_summary)
    notes: list[str] = []
    verification = VerificationStatus.verified if has_evidence and not error else VerificationStatus.unverified
    if error:
        notes.append("Terminal profile could not be assembled from the current terminal sources.")
    elif not has_evidence:
        notes.append("Terminal profile was not found for the requested merchant and terminal.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_terminal_profile",
        merchant_id=merchant_id,
        data={"terminal": terminal, "latest_health": latest_health, "tx_summary": tx_summary},
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_terminal_health_summary(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = TerminalHealthSummaryInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=90)
    limit = bounded_limit(validated.limit, minimum=1, maximum=100)
    terminal_id = str(validated.terminal_id or "").strip() or None
    payload = terminals_repository.terminal_health_summary(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        group_by=validated.group_by,
        limit=limit,
        terminal_id=terminal_id,
    )
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    error = str(payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    notes: list[str] = []
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    if error:
        notes.append("Terminal health summary could not be read from the current schema.")
    elif not rows:
        notes.append("No terminal health snapshots matched the requested scope.")
    evidence_ids = [str(item) for item in payload.get("evidence", []) if str(item or "").strip()]
    if terminal_id:
        evidence_ids.append(f"terminal:{terminal_id}")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_terminal_health_summary",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={"rows": rows, "group_by": validated.group_by, "terminal_id": terminal_id},
        evidence_ids=_dedupe_text_values(evidence_ids),
        notes=notes,
        error_message=error or None,
    )


def get_terminal_failure_breakdown(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = TerminalFailureBreakdownInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    terminal_id = str(validated.terminal_id or "").strip()
    if not terminal_id:
        raise ValueError("terminal_id is required")
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    limit = bounded_limit(validated.limit, minimum=1, maximum=20)
    payload = transactions_repository.verify_failure_drivers(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        by=validated.dimension,
        limit=limit,
        terminal_id=terminal_id,
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    error = str(payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    notes: list[str] = []
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    if error:
        notes.append("Terminal-specific failure breakdown did not complete cleanly.")
    elif not rows:
        notes.append("No terminal-linked failures were found in the requested window.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_terminal_failure_breakdown",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={"terminal_id": terminal_id, "dimension": validated.dimension, "breakdown": rows},
        evidence_ids=_dedupe_text_values(
            [str(item) for item in payload.get("evidence", []) if str(item or "").strip()] + [f"terminal:{terminal_id}"]
        ),
        notes=notes,
        error_message=error or None,
    )


def retrieve_payments_knowledge(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = PaymentsKnowledgeInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    payload = knowledge_repository.retrieve_payments_knowledge(
        query=validated.query,
        top_k=bounded_limit(validated.top_k, minimum=1, maximum=10),
    )
    results = payload.get("results") if isinstance(payload.get("results"), list) else []
    error = str(payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    notes: list[str] = []
    verification = VerificationStatus.verified if results and not error else VerificationStatus.unverified
    if error:
        notes.append("Payments knowledge lookup could not be completed.")
    elif not results:
        notes.append("No payments-knowledge snippets matched the requested query.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="retrieve_payments_knowledge",
        merchant_id=merchant_id,
        data={"query": validated.query, "results": results, "result_count": len(results)},
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_chargeback_summary(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantWindowInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    payload = disputes_repository.chargeback_summary(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
    )
    error = str(payload.get("error") or "").strip()
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Chargeback summary could not be fully read from the current schema.")
    elif not int(payload.get("chargebacks_count") or 0):
        notes.append("No chargebacks were found in the requested window.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_chargeback_summary",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "chargebacks_count": int(payload.get("chargebacks_count") or 0),
            "open_chargebacks_count": int(payload.get("open_chargebacks_count") or 0),
            "overdue_chargebacks_count": int(payload.get("overdue_chargebacks_count") or 0),
            "due_soon_chargebacks_count": int(payload.get("due_soon_chargebacks_count") or 0),
            "chargebacks_amount": float(payload.get("chargebacks_amount") or 0.0),
            "stage_distribution": payload.get("stage_distribution") or [],
            "top_reason": payload.get("top_reason"),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def list_chargebacks(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = ChargebackListInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    limit = bounded_limit(validated.limit, minimum=1, maximum=100)
    payload = disputes_repository.list_chargebacks(
        engine,
        merchant_id=merchant_id,
        status=validated.status,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        limit=limit,
    )
    error = str(payload.get("error") or "").strip()
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Chargeback list could not be fully read from the current schema.")
    elif not payload.get("rows"):
        notes.append("No chargebacks matched the requested scope.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="list_chargebacks",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={"status": validated.status, "rows": payload.get("rows") or []},
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_chargeback_detail(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = ChargebackDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    chargeback_id = str(validated.chargeback_id or "").strip()
    if not chargeback_id:
        raise ValueError("chargeback_id is required")
    payload = disputes_repository.get_chargeback_detail(
        engine,
        merchant_id=merchant_id,
        chargeback_id=chargeback_id,
    )
    error = str(payload.get("error") or "").strip()
    row = payload.get("row") if isinstance(payload.get("row"), dict) else None
    verification = VerificationStatus.verified if row and not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Chargeback detail could not be fully read from the current schema.")
    elif row is None:
        notes.append("Chargeback detail was not found for the requested merchant and chargeback id.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_chargeback_detail",
        merchant_id=merchant_id,
        data={"chargeback": row},
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_refund_summary(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantWindowInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    summary = disputes_repository.refund_summary(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
    )
    payload = disputes_repository.list_refunds(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        limit=25,
    )
    error = str(payload.get("error") or "").strip()
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Refund summary could not be fully read from the current schema.")
    elif not int(summary.get("refunds_count") or 0):
        notes.append("No refunds were found in the requested window.")
    latest_refund = (payload.get("rows") or [None])[0]
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_refund_summary",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "refunds_count": int(summary.get("refunds_count") or 0),
            "refunds_amount": float(summary.get("refunds_amount") or 0.0),
            "latest_refund": latest_refund,
        },
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def list_refunds(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = RefundListInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    limit = bounded_limit(validated.limit, minimum=1, maximum=100)
    payload = disputes_repository.list_refunds(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        limit=limit,
    )
    error = str(payload.get("error") or "").strip()
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Refund list could not be fully read from the current schema.")
    elif not payload.get("rows"):
        notes.append("No refunds matched the requested scope.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="list_refunds",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={"rows": payload.get("rows") or []},
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_refund_detail(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = RefundDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    refund_id = str(validated.refund_id or "").strip()
    if not refund_id:
        raise ValueError("refund_id is required")
    payload = disputes_repository.get_refund_detail(
        engine,
        merchant_id=merchant_id,
        refund_id=refund_id,
    )
    error = str(payload.get("error") or "").strip()
    row = payload.get("row") if isinstance(payload.get("row"), dict) else None
    verification = VerificationStatus.verified if row and not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Refund detail could not be fully read from the current schema.")
    elif row is None:
        notes.append("Refund detail was not found for the requested merchant and refund id.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_refund_detail",
        merchant_id=merchant_id,
        data={"refund": row},
        evidence_ids=[str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        notes=notes,
        error_message=error or None,
    )


def get_support_case_history(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="get_support_case_history",
            merchant_id=merchant_id,
            data={"case_id": validated.case_id},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    payload = ops_repository.get_support_case_history_context(
        engine,
        merchant_id=merchant_id,
        case_id=validated.case_id,
    )
    source = str(payload.get("source") or "")
    recent_cases = payload.get("recent_cases") if isinstance(payload.get("recent_cases"), list) else []
    notes = list(preflight_notes)
    verification = VerificationStatus.verified
    if "fixture_fallback" in source:
        verification = VerificationStatus.unverified
        notes.append("Support history is using seeded fixture data because related local support cases were not found.")
    elif not recent_cases:
        verification = VerificationStatus.unverified
        notes.append("No related support cases were found for this merchant.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_support_case_history",
        merchant_id=merchant_id,
        data=payload,
        evidence_ids=_support_tool_evidence(evidence_ids, validated.case_id, merchant_id=merchant_id, source=source),
        notes=notes,
    )


def get_contact_and_escalation_context(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="get_contact_and_escalation_context",
            merchant_id=merchant_id,
            data={"case_id": validated.case_id},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    payload = ops_repository.get_contact_and_escalation_context(
        engine,
        merchant_id=merchant_id,
        case_id=validated.case_id,
    )
    source = str(payload.get("source") or "")
    contacts = payload.get("contacts") if isinstance(payload.get("contacts"), list) else []
    escalations = payload.get("escalations") if isinstance(payload.get("escalations"), list) else []
    notes = list(preflight_notes)
    verification = VerificationStatus.verified
    if "fixture_fallback" in source:
        verification = VerificationStatus.unverified
        notes.append("Contact or escalation context is partially using seeded fixture data.")
    elif not contacts and not escalations:
        verification = VerificationStatus.unverified
        notes.append("No contact or escalation context is currently recorded on the local case.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_contact_and_escalation_context",
        merchant_id=merchant_id,
        data=payload,
        evidence_ids=_support_tool_evidence(evidence_ids, validated.case_id, merchant_id=merchant_id, source=source),
        notes=notes,
    )


def get_customer_service_context(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="get_customer_service_context",
            merchant_id=merchant_id,
            data={"case_id": validated.case_id},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    payload = ops_repository.get_customer_service_context(
        engine,
        merchant_id=merchant_id,
        case_id=validated.case_id,
    )
    source = str(payload.get("source") or "")
    notes = list(preflight_notes)
    verification = VerificationStatus.verified
    has_context = bool(payload.get("preferred_channel") or payload.get("recent_support_cases") or payload.get("contacts") or payload.get("escalations"))
    if "fixture_fallback" in source:
        verification = VerificationStatus.unverified
        notes.append("Customer-service context is partially using seeded fixture data.")
    elif not has_context:
        verification = VerificationStatus.unverified
        notes.append("Customer-service context is incomplete for this case.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_customer_service_context",
        merchant_id=merchant_id,
        data=payload,
        evidence_ids=_support_tool_evidence(evidence_ids, validated.case_id, merchant_id=merchant_id, source=source),
        notes=notes,
    )


def get_case_detail(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_case_detail",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    data = _normalized_case_substrate(detail)
    notes = list(preflight_notes)
    if not data["connector_runs"]:
        notes.append("No connector runs have been recorded for this case yet.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_case_detail",
        merchant_id=merchant_id,
        data=data,
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=notes,
    )


def get_case_timeline(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_case_timeline",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    approvals = detail.get("approvals") if isinstance(detail.get("approvals"), list) else []
    timeline = detail.get("timeline") if isinstance(detail.get("timeline"), list) else []
    latest_event = timeline[-1] if timeline else None
    summary_lines: list[str] = []
    if latest_event:
        summary_lines.append(
            f"Latest case event is {str(latest_event.get('event_type') or 'unknown').replace('_', ' ')}"
            f" at {latest_event.get('created_at')}."
        )
    if len(timeline) > 1:
        summary_lines.append(
            f"Timeline contains {len(timeline)} event(s) from {timeline[0].get('created_at')} to {timeline[-1].get('created_at')}."
        )
    approval_state = _approval_state_payload(approvals)
    if str(approval_state.get("status") or "not_requested").lower() != "not_requested":
        summary_lines.append(
            f"Latest approval state is {str(approval_state.get('status') or '').upper()}."
        )
    notes = list(preflight_notes)
    if not timeline:
        notes.append("No timeline events have been recorded for this case yet.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_case_timeline",
        merchant_id=merchant_id,
        data={
            "work_item": case_row,
            "timeline": timeline,
            "event_count": len(timeline),
            "latest_event": latest_event,
            "approval_state": approval_state,
            "summary_lines": summary_lines,
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=notes,
    )


def get_case_tasks(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_case_tasks",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    tasks = detail.get("tasks") if isinstance(detail.get("tasks"), list) else []
    open_tasks = [task for task in tasks if str(task.get("status") or "").upper() != "DONE"]
    task_summary = {
        "task_count": int(case_row.get("task_count") or 0),
        "open_task_count": int(case_row.get("open_task_count") or 0),
        "done_task_count": int(case_row.get("done_task_count") or 0),
        "overdue_task_count": int(case_row.get("overdue_task_count") or 0),
    }
    notes = list(preflight_notes)
    if not tasks:
        notes.append("No tasks have been recorded for this case yet.")
    elif task_summary["overdue_task_count"] > 0:
        notes.append("This case has overdue tasks that need operator follow-up.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_case_tasks",
        merchant_id=merchant_id,
        data={
            "work_item": case_row,
            "tasks": tasks,
            "task_summary": task_summary,
            "next_open_task": open_tasks[0] if open_tasks else None,
            "runbook_steps": _runbook_steps_with_progress(str(case_row.get("case_type") or "manual_ops_review"), tasks),
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=notes,
    )


def get_case_memory(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_case_memory",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    memory = ops_repository.get_case_memory(engine, validated.case_id)
    notes = list(preflight_notes)
    if not memory.get("created_at"):
        notes.append("No persisted case memory has been stored yet; this is the empty default memory shape.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_case_memory",
        merchant_id=merchant_id,
        data={"work_item": case_row, "memory": memory},
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=notes,
    )


def get_sla_snapshot(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_sla_snapshot",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    priority = str(case_row.get("priority") or "medium").strip().lower()
    policy = sla_policy_for_priority(priority)
    now = datetime.now(timezone.utc)
    due_at = _parse_iso_datetime(case_row.get("due_at"))
    hours_to_due = round((due_at - now).total_seconds() / 3600, 1) if due_at else None
    hours_past_due = round(max((now - due_at).total_seconds() / 3600, 0.0), 1) if due_at else 0.0
    notes = list(preflight_notes)
    if case_row.get("sla_breached"):
        notes.append("Case SLA is currently breached.")
    elif case_row.get("sla_warning"):
        notes.append("Case is currently inside the SLA warning window.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_sla_snapshot",
        merchant_id=merchant_id,
        data={
            "work_item": case_row,
            "sla": {
                "priority": priority,
                "status": case_row.get("status"),
                "opened_at": case_row.get("opened_at"),
                "due_at": case_row.get("due_at"),
                "age_hours": case_row.get("age_hours"),
                "target_hours": policy.target_hours,
                "warning_hours": policy.warning_hours,
                "hours_to_due": hours_to_due,
                "hours_past_due": hours_past_due,
                "sla_warning": bool(case_row.get("sla_warning")),
                "sla_breached": bool(case_row.get("sla_breached")),
                "attention_level": case_row.get("attention_level"),
                "waiting_on": case_row.get("waiting_on"),
                "approval_pending": bool(case_row.get("approval_pending")),
                "connector_attention": bool(case_row.get("connector_attention")),
                "open_task_count": int(case_row.get("open_task_count") or 0),
                "overdue_task_count": int(case_row.get("overdue_task_count") or 0),
            },
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=notes,
    )


def list_ops_queue(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = OpsQueueListInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    limit = bounded_limit(validated.limit, minimum=1, maximum=100)
    lane = str(validated.lane or "").strip().lower() or None
    status_value = str(validated.status or "ACTIVE").strip().upper() or "ACTIVE"
    status = None if status_value == "ALL" else status_value
    owner = str(validated.owner or "").strip() or None

    listing = ops_repository.list_cases(
        engine,
        merchant_id=merchant_id,
        lane=lane,
        status=status,
        owner=owner,
        limit=limit,
    )
    approvals = ops_repository.list_approvals(
        engine,
        merchant_id=merchant_id,
        lane=lane,
        status="PENDING",
        limit=min(limit, 25),
    )
    evidence_ids = [
        f"ops_queue:{merchant_id}:{lane or 'all'}:{(status or 'ALL').lower()}:{limit}",
    ]
    evidence_ids.extend(
        f"case:{str(item.get('case_id') or '').strip()}"
        for item in (listing.get("cases") or [])
        if str(item.get("case_id") or "").strip()
    )
    notes: list[str] = []
    if not (listing.get("cases") or []) and not approvals:
        notes.append("No queue items matched the requested merchant scope and filters.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="list_ops_queue",
        merchant_id=merchant_id,
        data={
            "lane": lane,
            "status": status or "ALL",
            "owner": owner,
            "limit": limit,
            "cases": listing.get("cases") or [],
            "queue_summary": listing.get("queue_summary") or {},
            "approvals": approvals,
        },
        evidence_ids=list(dict.fromkeys(evidence_ids)),
        notes=notes,
    )


def list_connector_runs(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="list_connector_runs",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    runs = list_connector_runs_for_case(engine, validated.case_id)
    latest_run = runs[0] if runs else None
    status_counts: dict[str, int] = {}
    for run in runs:
        status = str(run.get("status") or "UNKNOWN").upper()
        status_counts[status] = status_counts.get(status, 0) + 1
    connector_attention = bool(latest_run and str(latest_run.get("status") or "").upper() in {"FAILED", "SKIPPED"})
    notes = list(preflight_notes)
    if not runs:
        notes.append("No connector runs have been recorded for this case yet.")
    elif connector_attention:
        notes.append("Latest connector run needs operator follow-up.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="list_connector_runs",
        merchant_id=merchant_id,
        data={
            "work_item": case_row,
            "runs": runs,
            "run_count": len(runs),
            "latest_run": latest_run,
            "status_counts": status_counts,
            "connector_attention": connector_attention,
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=notes,
    )


def summarize_case_timeline(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="summarize_case_timeline",
            merchant_id=merchant_id,
            data={"case_id": validated.case_id},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    timeline = detail.get("timeline") if isinstance(detail.get("timeline"), list) else []
    tasks = detail.get("tasks") if isinstance(detail.get("tasks"), list) else []
    approvals = detail.get("approvals") if isinstance(detail.get("approvals"), list) else []
    latest_events = timeline[-3:]
    open_tasks = [task for task in tasks if str(task.get("status") or "").upper() != "DONE"]
    latest_approval = approvals[0] if approvals else None
    lines: list[str] = []
    if latest_events:
        latest_event = latest_events[-1]
        lines.append(
            f"Latest case event is {str(latest_event.get('event_type') or 'unknown').replace('_', ' ')}"
            f" at {latest_event.get('created_at')}."
        )
    if open_tasks:
        first_task = open_tasks[0]
        lines.append(
            f"{len(open_tasks)} task(s) remain open; next task is {first_task.get('title')}."
        )
    if latest_approval:
        lines.append(
            f"Latest approval is {str(latest_approval.get('status') or 'UNKNOWN').upper()} for {latest_approval.get('action_type')}."
        )
    if not lines:
        lines.append("No timeline or task activity has been recorded on this case yet.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="summarize_case_timeline",
        merchant_id=merchant_id,
        data={
            "case_id": validated.case_id,
            "summary_lines": lines,
            "latest_events": latest_events,
            "open_task_count": len(open_tasks),
            "latest_approval_status": str((latest_approval or {}).get("status") or "").upper() or None,
            "case_status": case_row.get("status"),
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=preflight_notes,
    )


def _dedupe_case_evidence(evidence_ids: list[str], case_id: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in [f"case:{case_id}"] + list(evidence_ids):
        text_value = str(item or "").strip()
        if text_value and text_value not in seen:
            seen.add(text_value)
            out.append(text_value)
    return out


def _settlement_action_wrapper(
    engine: Any,
    *,
    tool_name: str,
    arguments: dict[str, Any],
    allowed_case_types: set[str],
    default_action_type: str,
) -> ToolEnvelope:
    validated = SettlementCaseActionInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(
        engine,
        merchant_id=merchant_id,
        case_id=validated.case_id,
    )
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name=tool_name,
            merchant_id=merchant_id,
            data={"status": "blocked", "reason": preflight_notes[0] if preflight_notes else "Ops case not found"},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    approvals = detail.get("approvals") if isinstance(detail.get("approvals"), list) else []
    approval_state = _approval_state_payload(approvals)
    case_type = str(case_row.get("case_type") or "").strip().lower()
    settlement_id = (
        str(validated.settlement_id or "").strip()
        or _extract_case_entity_id(detail, pinned_key="settlement_id", evidence_prefix="settlement")
        or None
    )
    merged_evidence = _dedupe_case_evidence(
        evidence_ids
        + [str(item) for item in validated.evidence_ids if str(item or "").strip()]
        + ([f"settlement:{settlement_id}"] if settlement_id else []),
        validated.case_id,
    )
    notes = list(preflight_notes)
    notes.append("No external write was executed. This tool only prepares an approval-gated settlement action wrapper.")

    if str(case_row.get("status") or "").upper() in {"RESOLVED", "CLOSED"}:
        return ToolEnvelope(
            status=ToolStatus.ok,
            verification=VerificationStatus.not_applicable,
            tool_name=tool_name,
            merchant_id=merchant_id,
            data={"status": "blocked", "reason": "Case is already resolved."},
            evidence_ids=merged_evidence,
            notes=notes,
        )

    if case_type not in allowed_case_types:
        return ToolEnvelope(
            status=ToolStatus.ok,
            verification=VerificationStatus.not_applicable,
            tool_name=tool_name,
            merchant_id=merchant_id,
            data={
                "status": "blocked",
                "reason": "This settlement action does not apply to the current case type.",
                "case_type": case_type or None,
            },
            evidence_ids=merged_evidence,
            notes=notes,
        )

    action_type = default_action_type
    if default_action_type == "SETTLEMENT_ESCALATION" and case_type in {"processed_unsettled_payout", "delayed_payout_exception"}:
        action_type = "PAYOUT_DELAY_INTERVENTION"

    recommended_action = str(
        validated.recommended_action
        or case_row.get("summary")
        or "Review the case for settlement follow-through."
    ).strip()
    title = str(case_row.get("title") or validated.case_id or "Settlement case").strip()
    payload_summary = str(
        validated.payload_summary
        or (f"{title} for settlement {settlement_id}" if settlement_id else title)
    ).strip()
    latest_connector_status = str(approval_state.get("connector_status") or "").strip().upper() or None
    normalized_approval_status = str(approval_state.get("status") or "not_requested").strip().lower() or "not_requested"

    wrapper_status = "ready"
    dispatch_readiness = "approval_required"
    reason: str | None = None
    if normalized_approval_status == "pending":
        wrapper_status = "blocked"
        dispatch_readiness = "pending_approval"
        reason = "A settlement approval is already pending for this case."
    elif normalized_approval_status == "approved":
        if latest_connector_status in {"SUCCESS", "QUEUED"}:
            dispatch_readiness = "dispatch_already_attempted"
            reason = "The latest approved action has already moved into connector handling."
        else:
            dispatch_readiness = "approved_for_dispatch"
    elif normalized_approval_status == "rejected":
        reason = "The last approval was rejected, so the next approval should include updated supporting evidence."

    verification = VerificationStatus.verified
    if not settlement_id:
        verification = VerificationStatus.unverified
        wrapper_status = "blocked"
        dispatch_readiness = "missing_settlement_context"
        reason = "Pin a settlement id on the case before preparing a settlement action."
        notes.append("Settlement context is incomplete for this action wrapper.")

    payload = {
        "case_id": validated.case_id,
        "merchant_id": merchant_id,
        "lane": case_row.get("lane"),
        "case_type": case_type,
        "settlement_id": settlement_id,
        "evidence_ids": merged_evidence,
        "recommended_action": recommended_action,
        "case_title": title,
    }
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name=tool_name,
        merchant_id=merchant_id,
        data={
            "status": wrapper_status,
            "reason": reason,
            "approval_required": True,
            "approval_state": normalized_approval_status,
            "dispatch_readiness": dispatch_readiness,
            "action_type": action_type,
            "payload_summary": payload_summary,
            "payload": payload,
            "downstream_target": "settlement_ops_core",
            "idempotency_expectation": "Connector idempotency is derived from approval_id + case_id + action_type after approval.",
            "approval_context": {
                "approval_id": approval_state.get("approval_id"),
                "receipt_ref": approval_state.get("receipt_ref"),
                "connector_status": latest_connector_status,
            },
        },
        evidence_ids=merged_evidence,
        notes=notes,
    )


def _load_blocked_integration_fixture(name: str) -> dict[str, Any]:
    path = BLOCKED_INTEGRATION_FIXTURES_DIR / name
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _case_has_blocked_monitoring_hint(detail: dict[str, Any]) -> bool:
    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    source_payload = case_row.get("source_payload") if isinstance(case_row.get("source_payload"), dict) else {}
    evidence_ids = [str(item or "").strip().lower() for item in (case_row.get("evidence_ids") or []) if str(item or "").strip()]
    if any(item.startswith("alert:") for item in evidence_ids):
        return True
    source = str(case_row.get("source") or "").strip().lower()
    if source in {"monitoring", "observability", "alert"}:
        return True
    text_hints = " ".join(
        str(item or "").strip().lower()
        for item in (
            case_row.get("source_ref"),
            source_payload.get("service"),
            source_payload.get("summary"),
            source_payload.get("body"),
            case_row.get("title"),
            case_row.get("summary"),
        )
        if str(item or "").strip()
    )
    return any(token in text_hints for token in ("monitoring", "alert", "outage", "pager"))


def _refresh_status_for_case(engine: Any, merchant_id: str) -> dict[str, Any]:
    interval_minutes = max(1, int(getattr(Config, "PROACTIVE_REFRESH_INTERVAL_MINUTES", 30) or 30))
    return proactive_repository.get_background_refresh_status(
        engine,
        merchant_id,
        days=30,
        interval_minutes=interval_minutes,
        auto_enabled=bool(getattr(Config, "PROACTIVE_AUTO_REFRESH_ENABLED", True)),
    )


def _connector_status_counts(runs: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for run in runs:
        status = str(run.get("status") or "UNKNOWN").upper()
        counts[status] = counts.get(status, 0) + 1
    return counts


def _latest_run_timestamp(run: dict[str, Any]) -> str | None:
    for key in ("completed_at", "updated_at", "created_at", "dispatched_at"):
        value = str(run.get(key) or "").strip()
        if value:
            return value
    return None


def _build_internal_monitoring_alerts(
    *,
    case_row: dict[str, Any],
    refresh_status: dict[str, Any],
    connector_runs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    case_id = str(case_row.get("case_id") or "").strip()
    merchant_id = str(case_row.get("merchant_id") or "").strip()
    case_type = str(case_row.get("case_type") or "").strip().lower()
    if case_type in INCIDENT_CASE_TYPES and bool(refresh_status.get("due")):
        alerts.append(
            {
                "alert_id": f"internal:background_refresh:{merchant_id}:30d",
                "service": "background_refresh",
                "severity": "high",
                "status": "open",
                "opened_at": refresh_status.get("next_refresh_at") or refresh_status.get("last_refresh_at"),
                "summary": "Background refresh is currently due or overdue.",
                "source": "internal_state",
                "evidence_id": f"background_refresh:{merchant_id}:30d",
            }
        )

    latest_run = connector_runs[0] if connector_runs else None
    latest_status = str((latest_run or {}).get("status") or "").upper()
    if latest_run and latest_status in {"FAILED", "SKIPPED"}:
        connector_name = str(latest_run.get("connector_name") or "settlement_ops_core").strip()
        error_message = str(latest_run.get("error_message") or "").strip()
        summary = f"Latest connector run ended in {latest_status}."
        if error_message:
            summary += f" {error_message}"
        alerts.append(
            {
                "alert_id": f"internal:connector:{case_id}:{latest_run.get('run_id') or latest_status.lower()}",
                "service": connector_name,
                "severity": "high" if latest_status == "FAILED" else "medium",
                "status": "open",
                "opened_at": _latest_run_timestamp(latest_run),
                "summary": summary,
                "source": "internal_state",
                "evidence_id": f"connector_run:{latest_run.get('run_id')}",
            }
        )
    return alerts


def _build_job_failures(
    *,
    case_row: dict[str, Any],
    refresh_status: dict[str, Any],
    connector_runs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    case_id = str(case_row.get("case_id") or "").strip()
    merchant_id = str(case_row.get("merchant_id") or "").strip()
    case_type = str(case_row.get("case_type") or "").strip().lower()
    latest_run = connector_runs[0] if connector_runs else None
    latest_status = str((latest_run or {}).get("status") or "").upper()
    if latest_run and latest_status in {"FAILED", "SKIPPED"}:
        error_message = str(latest_run.get("error_message") or "").strip()
        failures.append(
            {
                "job_id": f"connector:{case_id}:{latest_run.get('run_id') or latest_status.lower()}",
                "job_name": "connector_dispatch",
                "status": latest_status,
                "occurred_at": _latest_run_timestamp(latest_run),
                "summary": (
                    f"Connector dispatch for {latest_run.get('connector_name') or 'settlement_ops_core'} "
                    f"ended in {latest_status}."
                ),
                "error_message": error_message or None,
                "source": "internal_state",
                "evidence_id": f"connector_run:{latest_run.get('run_id')}",
            }
        )
    if case_type in INCIDENT_CASE_TYPES and bool(refresh_status.get("due")):
        failures.append(
            {
                "job_id": f"background_refresh:{merchant_id}:30d",
                "job_name": "background_refresh",
                "status": "OVERDUE",
                "occurred_at": refresh_status.get("next_refresh_at") or refresh_status.get("last_refresh_at"),
                "summary": "Background refresh is due or overdue and needs operator review.",
                "error_message": None,
                "source": "internal_state",
                "evidence_id": f"background_refresh:{merchant_id}:30d",
            }
        )
    return failures


def _operational_case_state(engine: Any, detail: dict[str, Any]) -> dict[str, Any]:
    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    tasks = detail.get("tasks") if isinstance(detail.get("tasks"), list) else []
    approvals = detail.get("approvals") if isinstance(detail.get("approvals"), list) else []
    timeline = detail.get("timeline") if isinstance(detail.get("timeline"), list) else []
    connector_runs = detail.get("connector_runs") if isinstance(detail.get("connector_runs"), list) else []
    merchant_id = str(case_row.get("merchant_id") or "").strip()
    refresh_status = _refresh_status_for_case(engine, merchant_id) if merchant_id else {}
    internal_alerts = _build_internal_monitoring_alerts(
        case_row=case_row,
        refresh_status=refresh_status,
        connector_runs=connector_runs,
    )
    job_failures = _build_job_failures(
        case_row=case_row,
        refresh_status=refresh_status,
        connector_runs=connector_runs,
    )
    return {
        "case_row": case_row,
        "tasks": tasks,
        "approvals": approvals,
        "timeline": timeline,
        "connector_runs": connector_runs,
        "latest_connector_run": connector_runs[0] if connector_runs else None,
        "connector_status_counts": _connector_status_counts(connector_runs),
        "refresh_status": refresh_status,
        "open_tasks": [task for task in tasks if str(task.get("status") or "").upper() != "DONE"],
        "latest_approval": approvals[0] if approvals else None,
        "latest_event": timeline[-1] if timeline else None,
        "blocked_monitoring_hint": _case_has_blocked_monitoring_hint(detail),
        "internal_alerts": internal_alerts,
        "job_failures": job_failures,
    }


def _collect_event_evidence(items: list[dict[str, Any]]) -> list[str]:
    evidence_ids: list[str] = []
    for item in items:
        evidence_id = str(item.get("evidence_id") or "").strip()
        if evidence_id:
            evidence_ids.append(evidence_id)
    return evidence_ids


def get_policy_rule_explanation(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="get_policy_rule_explanation",
            merchant_id=merchant_id,
            data={"case_id": validated.case_id},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    case_type = str(case_row.get("case_type") or "manual_ops_review").strip().lower()
    priority = str(case_row.get("priority") or "medium").strip().lower()
    approval_state = str(case_row.get("approval_state") or "not_requested").strip().lower()
    runbook = runbook_for_case_type(case_type)
    sla_policy = sla_policy_for_priority(priority)
    explanation_lines = [
        f"Case type {case_type} uses runbook {runbook.code} with {len(runbook.steps)} step(s).",
        f"Priority {priority} has an SLA target of {sla_policy.target_hours} hour(s) and warning threshold at {sla_policy.warning_hours} hour(s).",
        f"Current approval state is {approval_state}.",
    ]
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_policy_rule_explanation",
        merchant_id=merchant_id,
        data={
            "case_id": validated.case_id,
            "case_type": case_type,
            "runbook_code": runbook.code,
            "runbook_title": runbook.title,
            "runbook_steps": [
                {
                    "step_id": step.step_id,
                    "title": step.title,
                    "description": step.description,
                    "action_type": step.action_type,
                }
                for step in runbook.steps
            ],
            "priority": priority,
            "sla_policy": {
                "target_hours": sla_policy.target_hours,
                "warning_hours": sla_policy.warning_hours,
            },
            "approval_state": approval_state,
            "explanation_lines": explanation_lines,
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=preflight_notes,
    )


def get_connector_health(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="get_connector_health",
            merchant_id=merchant_id,
            data={"case_id": validated.case_id},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    runs = list_connector_runs_for_case(engine, validated.case_id)
    latest_run = runs[0] if runs else None
    status_counts: dict[str, int] = {}
    for run in runs:
        status = str(run.get("status") or "UNKNOWN").upper()
        status_counts[status] = status_counts.get(status, 0) + 1
    connector_attention = bool(latest_run and str(latest_run.get("status") or "").upper() in {"FAILED", "SKIPPED"})
    notes = list(preflight_notes)
    if not runs:
        notes.append("No connector runs have been recorded for this case yet.")
    elif connector_attention:
        notes.append("Latest connector run needs operator follow-up.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_connector_health",
        merchant_id=merchant_id,
        data={
            "case_id": validated.case_id,
            "run_count": len(runs),
            "latest_run": latest_run,
            "status_counts": status_counts,
            "connector_attention": connector_attention,
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=notes,
    )


def get_api_health(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_api_health",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    state = _operational_case_state(engine, detail)
    latest_run = state.get("latest_connector_run") if isinstance(state.get("latest_connector_run"), dict) else None
    run_count = len(state.get("connector_runs") or [])
    notes = list(preflight_notes)
    evidence_extra: list[str] = []
    verification = VerificationStatus.verified
    source = "internal_state"
    connector_name = str((latest_run or {}).get("connector_name") or "settlement_ops_core").strip()
    latest_probe = latest_run
    status_counts = state.get("connector_status_counts") if isinstance(state.get("connector_status_counts"), dict) else {}

    if latest_run:
        latest_status = str(latest_run.get("status") or "UNKNOWN").upper()
        if latest_status == "SUCCESS":
            health_status = "healthy"
        elif latest_status == "QUEUED":
            health_status = "pending"
        elif latest_status == "SKIPPED":
            health_status = "blocked"
        elif latest_status == "FAILED":
            health_status = "degraded"
        else:
            health_status = "unknown"
        if latest_run.get("run_id"):
            evidence_extra.append(f"connector_run:{latest_run.get('run_id')}")
        if health_status in {"degraded", "blocked"}:
            notes.append("Current API health is derived from the latest connector execution state.")
    else:
        health_status = "unknown"
        blocked_hint = bool(state.get("blocked_monitoring_hint"))
        internal_alerts = state.get("internal_alerts") if isinstance(state.get("internal_alerts"), list) else []
        if blocked_hint and not internal_alerts:
            fixture = _load_blocked_integration_fixture("connector_health.json")
            fixture_probe = fixture.get("latest_run") if isinstance(fixture.get("latest_run"), dict) else {}
            connector_name = str(fixture.get("connector_name") or connector_name).strip()
            health_status = str(fixture.get("status") or "unknown").strip().lower() or "unknown"
            latest_probe = fixture_probe or None
            run_count = int(fixture.get("run_count") or 0)
            last_run_status = str(
                fixture_probe.get("status") or fixture.get("last_run_status") or ""
            ).strip().upper()
            status_counts = {last_run_status: 1} if last_run_status else {}
            source = "fixture"
            verification = VerificationStatus.unverified
            evidence_extra.append(f"fixture:connector_health:{connector_name or 'settlement_ops_core'}")
            notes.append("External API monitoring is blocked; returning seeded connector health fixture.")
        else:
            notes.append("No connector execution telemetry is recorded for this case in current internal state.")

    attention_required = health_status in {"degraded", "blocked"}
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_api_health",
        merchant_id=merchant_id,
        data={
            "case_id": validated.case_id,
            "service_name": connector_name or "settlement_ops_core",
            "status": health_status,
            "source": source,
            "run_count": run_count,
            "status_counts": status_counts,
            "latest_probe": latest_probe,
            "attention_required": attention_required,
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids + evidence_extra, validated.case_id),
        notes=notes,
    )


def get_monitoring_alerts(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedLimitInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_monitoring_alerts",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    limit = bounded_limit(validated.limit, minimum=1, maximum=25)
    state = _operational_case_state(engine, detail)
    alerts = [dict(item) for item in (state.get("internal_alerts") or []) if isinstance(item, dict)]
    notes = list(preflight_notes)
    source = "internal_state"
    verification = VerificationStatus.verified
    evidence_extra = _collect_event_evidence(alerts)

    if not alerts and bool(state.get("blocked_monitoring_hint")):
        fixture = _load_blocked_integration_fixture("monitoring_alerts.json")
        fixture_alerts = [dict(item) for item in (fixture.get("alerts") or []) if isinstance(item, dict)]
        fixture_service = str(fixture.get("service") or "monitoring").strip() or "monitoring"
        alerts = [
            {
                **item,
                "service": str(item.get("service") or fixture_service).strip() or fixture_service,
                "source": "fixture",
                "evidence_id": f"fixture:monitoring_alert:{item.get('alert_id') or idx + 1}",
            }
            for idx, item in enumerate(fixture_alerts)
        ]
        source = "fixture"
        verification = VerificationStatus.unverified if alerts else VerificationStatus.verified
        if alerts:
            evidence_extra = _collect_event_evidence(alerts)
            notes.append("External monitoring is blocked; returning seeded alert fixture.")

    alerts = alerts[:limit]
    if not alerts:
        notes.append("No active monitoring alerts were found for this case in current internal state.")

    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_monitoring_alerts",
        merchant_id=merchant_id,
        data={
            "case_id": validated.case_id,
            "alerts": alerts,
            "alert_count": len(alerts),
            "source": source,
            "blocked_monitoring": bool(state.get("blocked_monitoring_hint")),
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids + evidence_extra, validated.case_id),
        notes=notes,
    )


def get_incident_context(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_incident_context",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    state = _operational_case_state(engine, detail)
    case_row = state.get("case_row") if isinstance(state.get("case_row"), dict) else {}
    refresh_status = state.get("refresh_status") if isinstance(state.get("refresh_status"), dict) else {}
    latest_run = state.get("latest_connector_run") if isinstance(state.get("latest_connector_run"), dict) else None
    latest_event = state.get("latest_event") if isinstance(state.get("latest_event"), dict) else None
    latest_approval = state.get("latest_approval") if isinstance(state.get("latest_approval"), dict) else None
    internal_alerts = [dict(item) for item in (state.get("internal_alerts") or []) if isinstance(item, dict)]
    job_failures = [dict(item) for item in (state.get("job_failures") or []) if isinstance(item, dict)]

    summary_lines: list[str] = []
    if refresh_status:
        summary_lines.append(
            f"Background refresh is {str(refresh_status.get('status') or 'IDLE').upper()} "
            + (
                f"with next refresh at {refresh_status.get('next_refresh_at')}."
                if refresh_status.get("next_refresh_at")
                else "with no next refresh time recorded."
            )
        )
    if latest_run:
        summary_lines.append(
            f"Latest connector run is {str(latest_run.get('status') or 'UNKNOWN').upper()}"
            + (
                f" with HTTP {latest_run.get('http_status_code')}."
                if latest_run.get("http_status_code") is not None
                else "."
            )
        )
    if internal_alerts:
        summary_lines.append(f"{len(internal_alerts)} internal monitoring alert(s) currently need attention.")
    if job_failures:
        summary_lines.append(f"{len(job_failures)} internal job failure(s) currently need operator follow-up.")
    if latest_event:
        summary_lines.append(
            f"Latest case event is {str(latest_event.get('event_type') or 'unknown').replace('_', ' ')} at {latest_event.get('created_at')}."
        )
    if not summary_lines:
        summary_lines.append("No incident-specific internal state signals are currently recorded for this case.")

    notes = list(preflight_notes)
    if bool(state.get("blocked_monitoring_hint")):
        notes.append("External monitoring is blocked; this incident context is limited to internal state only.")

    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_incident_context",
        merchant_id=merchant_id,
        data={
            "case_id": validated.case_id,
            "case_status": case_row.get("status"),
            "case_type": case_row.get("case_type"),
            "priority": case_row.get("priority"),
            "blocked_reason": case_row.get("blocked_reason"),
            "refresh_status": refresh_status,
            "connector_summary": {
                "run_count": len(state.get("connector_runs") or []),
                "latest_run": latest_run,
                "status_counts": state.get("connector_status_counts") or {},
            },
            "latest_event": latest_event,
            "latest_approval_status": str((latest_approval or {}).get("status") or "").upper() or None,
            "open_task_count": len(state.get("open_tasks") or []),
            "internal_alert_count": len(internal_alerts),
            "job_failure_count": len(job_failures),
            "blocked_monitoring": bool(state.get("blocked_monitoring_hint")),
            "summary_lines": summary_lines,
        },
        evidence_ids=_dedupe_case_evidence(
            evidence_ids + [f"background_refresh:{merchant_id}:30d"] + _collect_event_evidence(internal_alerts) + _collect_event_evidence(job_failures),
            validated.case_id,
        ),
        notes=notes,
    )


def get_job_failures(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedLimitInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return _case_lookup_error(
            tool_name="get_job_failures",
            merchant_id=merchant_id,
            case_id=validated.case_id,
            evidence_ids=evidence_ids,
            notes=preflight_notes,
        )

    limit = bounded_limit(validated.limit, minimum=1, maximum=25)
    state = _operational_case_state(engine, detail)
    failures = [dict(item) for item in (state.get("job_failures") or []) if isinstance(item, dict)][:limit]
    notes = list(preflight_notes)
    if not failures:
        notes.append("No failed or overdue internal jobs are currently recorded for this case.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_job_failures",
        merchant_id=merchant_id,
        data={
            "case_id": validated.case_id,
            "job_failures": failures,
            "job_count": len(failures),
            "attention_required": bool(failures),
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids + _collect_event_evidence(failures), validated.case_id),
        notes=notes,
    )


def get_data_quality_checks(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantWindowInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    table_name = str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features") or "transaction_features").strip() or "transaction_features"
    try:
        report = run_data_quality_checks(
            engine,
            merchant_id,
            start_date,
            end_date,
            table=table_name,
        )
    except Exception:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="get_data_quality_checks",
            merchant_id=merchant_id,
            window=_window_payload(start_date, end_date),
            data={"passed": False, "issues": [], "metrics": {}},
            evidence_ids=[],
            notes=["Transaction data quality checks could not be completed for the requested window."],
            error_message="Transaction data quality checks could not be completed for the requested window.",
        )

    notes: list[str] = []
    metrics = report.get("metrics") if isinstance(report.get("metrics"), dict) else {}
    if not bool(report.get("passed")):
        notes.append("Transaction data quality issues were detected in the requested window.")
    if not int(metrics.get("total_rows") or 0):
        notes.append("No transaction rows were found in the requested window.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="get_data_quality_checks",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "passed": bool(report.get("passed")),
            "issues": [str(item) for item in (report.get("issues") or []) if str(item or "").strip()],
            "metrics": metrics,
        },
        evidence_ids=[f"data_quality:{merchant_id}:{start_date.isoformat()}:{end_date.isoformat()}"],
        notes=notes,
    )


def draft_case_note(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="draft_case_note",
            merchant_id=merchant_id,
            data={"status": "blocked", "reason": preflight_notes[0] if preflight_notes else "Ops case not found"},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    memory = detail.get("memory") if isinstance(detail.get("memory"), dict) else {}
    latest_summary = memory.get("latest_summary") if isinstance(memory.get("latest_summary"), dict) else {}
    timeline = detail.get("timeline") if isinstance(detail.get("timeline"), list) else []
    tasks = detail.get("tasks") if isinstance(detail.get("tasks"), list) else []
    latest_event = timeline[-1] if timeline else {}
    open_tasks = [task for task in tasks if str(task.get("status") or "").upper() != "DONE"]

    lines = [
        f"Case note for {case_row.get('case_id')}: {case_row.get('title') or 'Ops case'}",
    ]
    executive_summary = str(latest_summary.get("executive_summary") or case_row.get("summary") or "").strip()
    if executive_summary:
        lines.append(executive_summary)
    if latest_event:
        lines.append(
            f"Latest activity: {str(latest_event.get('event_type') or 'update').replace('_', ' ')} at {latest_event.get('created_at')}."
        )
    if open_tasks:
        lines.append(f"Open tasks remaining: {len(open_tasks)}. Next task: {open_tasks[0].get('title')}.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="draft_case_note",
        merchant_id=merchant_id,
        data={"status": "ready", "body": "\n".join(lines)},
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=preflight_notes,
    )


def draft_approval_request(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="draft_approval_request",
            merchant_id=merchant_id,
            data={"status": "blocked", "reason": preflight_notes[0] if preflight_notes else "Ops case not found"},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    approvals = detail.get("approvals") if isinstance(detail.get("approvals"), list) else []
    if str(case_row.get("status") or "").upper() in {"RESOLVED", "CLOSED"}:
        return ToolEnvelope(
            status=ToolStatus.ok,
            verification=VerificationStatus.not_applicable,
            tool_name="draft_approval_request",
            merchant_id=merchant_id,
            data={"status": "blocked", "reason": "Case is already resolved."},
            evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
            notes=preflight_notes,
        )
    if approvals and str(approvals[0].get("status") or "").upper() == "PENDING":
        return ToolEnvelope(
            status=ToolStatus.ok,
            verification=VerificationStatus.not_applicable,
            tool_name="draft_approval_request",
            merchant_id=merchant_id,
            data={"status": "blocked", "reason": "This case already has a pending approval."},
            evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
            notes=preflight_notes,
        )

    case_type = str(case_row.get("case_type") or "").strip().lower()
    if case_type == "chargeback_review":
        action_type = "CHARGEBACK_REVIEW"
    elif case_type == "refund_exception":
        action_type = "REFUND_FOLLOW_UP"
    else:
        action_type = "FOLLOW_UP"

    payload_summary = f"{case_row.get('title') or 'Ops case'} approval draft"
    payload = {
        "case_id": validated.case_id,
        "merchant_id": merchant_id,
        "lane": case_row.get("lane"),
        "case_type": case_type,
        "evidence_ids": evidence_ids,
        "recommended_action": str(case_row.get("summary") or "Review the case for follow-through.").strip(),
    }
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="draft_approval_request",
        merchant_id=merchant_id,
        data={
            "status": "ready",
            "action_type": action_type,
            "payload_summary": payload_summary,
            "payload": payload,
        },
        evidence_ids=_dedupe_case_evidence(evidence_ids, validated.case_id),
        notes=preflight_notes,
    )


def draft_merchant_update(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = CaseScopedInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    detail, evidence_ids, preflight_notes = _case_detail_for_tool(engine, merchant_id=merchant_id, case_id=validated.case_id)
    if detail is None:
        return ToolEnvelope(
            status=ToolStatus.error,
            verification=VerificationStatus.unverified,
            tool_name="draft_merchant_update",
            merchant_id=merchant_id,
            data={"status": "blocked", "reason": preflight_notes[0] if preflight_notes else "Ops case not found"},
            evidence_ids=evidence_ids,
            notes=preflight_notes,
            error_message=preflight_notes[0] if preflight_notes else "Ops case not found",
        )

    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    tasks = detail.get("tasks") if isinstance(detail.get("tasks"), list) else []
    open_tasks = [task for task in tasks if str(task.get("status") or "").upper() != "DONE"]
    customer_context = ops_repository.get_customer_service_context(
        engine,
        merchant_id=merchant_id,
        case_id=validated.case_id,
    )
    source = str(customer_context.get("source") or "")
    ticket_reference = str(
        customer_context.get("ticket_reference")
        or (case_row.get("source_payload") or {}).get("ticket_id")
        or case_row.get("source_ref")
        or ""
    ).strip()
    preferred_channel = str(customer_context.get("preferred_channel") or "").strip() or None
    open_escalation_count = int(customer_context.get("open_escalation_count") or 0)
    chargeback_id = _extract_case_entity_id(detail, pinned_key="chargeback_id", evidence_prefix="chargeback") or ""
    refund_id = _extract_case_entity_id(detail, pinned_key="refund_id", evidence_prefix="refund") or ""

    chargeback_row: dict[str, Any] = {}
    refund_row: dict[str, Any] = {}
    if chargeback_id:
        chargeback_payload = disputes_repository.get_chargeback_detail(
            engine,
            merchant_id=merchant_id,
            chargeback_id=chargeback_id,
        )
        chargeback_row = chargeback_payload.get("row") if isinstance(chargeback_payload.get("row"), dict) else {}
    if refund_id:
        refund_payload = disputes_repository.get_refund_detail(
            engine,
            merchant_id=merchant_id,
            refund_id=refund_id,
        )
        refund_row = refund_payload.get("row") if isinstance(refund_payload.get("row"), dict) else {}

    case_status = str(case_row.get("status") or "OPEN").replace("_", " ").lower()
    title = str(case_row.get("title") or "support request").strip()
    if chargeback_id:
        subject = f"Update on chargeback {chargeback_id}"
    elif refund_id:
        subject = f"Update on refund {refund_id}"
    elif ticket_reference:
        subject = f"Update on support case {ticket_reference}"
    else:
        subject = f"Update on {title}"

    intro_target = (
        f"support case {ticket_reference}"
        if ticket_reference
        else (f"chargeback {chargeback_id}" if chargeback_id else (f"refund {refund_id}" if refund_id else title))
    )
    intro = f"Hello,\n\nWe are writing with an update on {intro_target}."

    if chargeback_row:
        status_text = str(chargeback_row.get("status") or "UNDER REVIEW").replace("_", " ").lower()
        due_by = str(chargeback_row.get("due_by") or "").strip()
        status_line = f"We are currently reviewing chargeback {chargeback_id}, which is {status_text}."
        if due_by:
            status_line = f"We are currently reviewing chargeback {chargeback_id}, which is {status_text} and due for response by {due_by}."
    elif refund_row:
        status_text = str(refund_row.get("status") or "UNDER REVIEW").replace("_", " ").lower()
        status_line = f"We are currently reviewing refund {refund_id}, which is {status_text}."
    else:
        status_line = f"Your case is currently {case_status} while our team reviews the latest details."

    if open_escalation_count > 0:
        next_step_line = "The case is already with the relevant team for follow-up, and we will share the next update once that review is complete."
    elif chargeback_id:
        next_step_line = "Our next step is to complete the dispute review and confirm the response package."
    elif refund_id:
        next_step_line = "Our next step is to confirm the latest refund state with the payments team."
    elif open_tasks:
        next_step_line = f"Our next step is {str(open_tasks[0].get('title') or 'the current review step').lower()}."
    else:
        next_step_line = "Our team is reviewing the latest case details and will share the next update after that review."

    closing = "We will send another update as soon as there is a confirmed change to share."
    body = f"{intro}\n\n{status_line} {next_step_line}\n\n{closing}"

    notes = list(preflight_notes)
    verification = VerificationStatus.verified
    if "fixture_fallback" in source:
        verification = VerificationStatus.unverified
        notes.append("Preferred contact context is partially using seeded fixture data.")
    if not preferred_channel:
        notes.append("No preferred merchant contact channel is currently recorded on the local case.")
    if chargeback_id and not chargeback_row:
        notes.append("Pinned chargeback detail could not be confirmed from the current dispute tables.")
    if refund_id and not refund_row:
        notes.append("Pinned refund detail could not be confirmed from the current refund tables.")

    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="draft_merchant_update",
        merchant_id=merchant_id,
        data={
            "status": "ready",
            "subject": subject,
            "body": body,
            "channel_hint": preferred_channel,
            "ticket_reference": ticket_reference or None,
        },
        evidence_ids=_support_tool_evidence(evidence_ids, validated.case_id, merchant_id=merchant_id, source=source),
        notes=notes,
    )


def list_settlements(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementListInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    limit = bounded_limit(validated.limit, minimum=1, maximum=100)
    payload = settlements_repository.list_settlements(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        limit=limit,
    )
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    notes: list[str] = []
    if not rows:
        notes.append("No settlements matched the requested merchant and date window.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=VerificationStatus.verified,
        tool_name="list_settlements",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "rows": rows,
            "row_count": len(rows),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])] if isinstance(payload, dict) else [],
        notes=notes,
    )


def get_settlement_detail(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    settlement_id = str(validated.settlement_id or "").strip()
    if not settlement_id:
        raise ValueError("settlement_id is required")
    payload = settlements_repository.get_settlement_detail(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    row = payload.get("row") if isinstance(payload.get("row"), dict) else None
    reconciliation = payload.get("reconciliation") if isinstance(payload.get("reconciliation"), list) else []
    notes: list[str] = []
    verification = VerificationStatus.verified if row else VerificationStatus.unverified
    if row is None:
        notes.append("Settlement detail was not found for the requested merchant and settlement id.")
    elif not reconciliation:
        notes.append("No reconciliation rows were found for this settlement.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_settlement_detail",
        merchant_id=merchant_id,
        data={
            "settlement": row,
            "reconciliation": reconciliation,
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])] if isinstance(payload, dict) else [],
        notes=notes,
    )


def get_settlement_reconciliation(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    settlement_id = str(validated.settlement_id or "").strip()
    if not settlement_id:
        raise ValueError("settlement_id is required")
    payload = settlements_repository.get_settlement_reconciliation(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    notes: list[str] = []
    error = str(payload.get("error") or "").strip()
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    if error:
        notes.append("Settlement reconciliation could not be read from the current schema.")
    elif not rows:
        notes.append("No reconciliation rows were found for this settlement.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_settlement_reconciliation",
        merchant_id=merchant_id,
        data={
            "settlement_id": settlement_id,
            "reconciliation": rows,
            "total_rows": int(payload.get("total_rows") or 0),
            "open_row_count": int(payload.get("open_row_count") or 0),
            "top_reason": payload.get("top_reason"),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])],
        notes=notes,
        error_message=error or None,
    )


def get_hold_reason(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    settlement_id = str(validated.settlement_id or "").strip()
    if not settlement_id:
        raise ValueError("settlement_id is required")
    payload = settlements_repository.get_hold_reason(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    notes: list[str] = []
    verification = VerificationStatus.verified if payload.get("evidence") else VerificationStatus.unverified
    if not payload.get("evidence"):
        notes.append("Settlement detail was not found for hold-reason review.")
    elif not payload.get("hold_reason"):
        notes.append("No explicit hold reason is recorded on the settlement row.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_hold_reason",
        merchant_id=merchant_id,
        data={
            "settlement_id": settlement_id,
            "status": payload.get("status"),
            "expected_date": payload.get("expected_date"),
            "settled_at": payload.get("settled_at"),
            "reference": payload.get("reference"),
            "payment_mode": payload.get("payment_mode"),
            "hold_reason": payload.get("hold_reason"),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])],
        notes=notes,
    )


def get_settlement_timeline(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    settlement_id = str(validated.settlement_id or "").strip()
    if not settlement_id:
        raise ValueError("settlement_id is required")
    payload = settlements_repository.get_settlement_timeline(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    error = str(payload.get("error") or "").strip()
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Settlement timeline could not be assembled for the requested settlement.")
    elif not payload.get("events"):
        notes.append("No settlement lifecycle events were available beyond the current row snapshot.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_settlement_timeline",
        merchant_id=merchant_id,
        data={
            "settlement_id": settlement_id,
            "status": payload.get("status"),
            "current_stage": payload.get("current_stage"),
            "summary": payload.get("summary"),
            "expected_date": payload.get("expected_date"),
            "settled_at": payload.get("settled_at"),
            "delay_state": payload.get("delay_state"),
            "delay_days": payload.get("delay_days"),
            "open_reconciliation_rows": payload.get("open_reconciliation_rows"),
            "events": payload.get("events") or [],
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])],
        notes=notes,
        error_message=error or None,
    )


def get_payout_delay_context(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    settlement_id = str(validated.settlement_id or "").strip()
    if not settlement_id:
        raise ValueError("settlement_id is required")
    payload = settlements_repository.get_payout_delay_context(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    error = str(payload.get("error") or "").strip()
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Payout delay context is incomplete for this settlement.")
    elif not payload.get("is_delayed"):
        notes.append("Settlement is not currently delayed beyond its expected date.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_payout_delay_context",
        merchant_id=merchant_id,
        data={
            "settlement_id": settlement_id,
            "status": payload.get("status"),
            "expected_date": payload.get("expected_date"),
            "settled_at": payload.get("settled_at"),
            "delay_state": payload.get("delay_state"),
            "delay_days": payload.get("delay_days"),
            "is_delayed": payload.get("is_delayed"),
            "hold_reason": payload.get("hold_reason"),
            "reference": payload.get("reference"),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])],
        notes=notes,
        error_message=error or None,
    )


def get_reconciliation_breaks(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    settlement_id = str(validated.settlement_id or "").strip()
    if not settlement_id:
        raise ValueError("settlement_id is required")
    payload = settlements_repository.get_reconciliation_breaks(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    error = str(payload.get("error") or "").strip()
    verification = VerificationStatus.verified if not error else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Reconciliation break review could not be fully read from the current schema.")
    elif not payload.get("breaks"):
        notes.append("No unresolved reconciliation breaks were found for this settlement.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_reconciliation_breaks",
        merchant_id=merchant_id,
        data={
            "settlement_id": settlement_id,
            "settlement_status": payload.get("settlement_status"),
            "expected_date": payload.get("expected_date"),
            "breaks": payload.get("breaks") or [],
            "total_break_rows": int(payload.get("total_break_rows") or 0),
            "distinct_break_count": int(payload.get("distinct_break_count") or 0),
            "top_break": payload.get("top_break"),
            "summary": payload.get("summary"),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])],
        notes=notes,
        error_message=error or None,
    )


def get_deduction_breakdown(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementDetailInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    settlement_id = str(validated.settlement_id or "").strip()
    if not settlement_id:
        raise ValueError("settlement_id is required")
    payload = settlements_repository.get_deduction_breakdown(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    error = str(payload.get("error") or "").strip()
    verification = VerificationStatus.verified if not error and payload.get("difference_amount") is not None else VerificationStatus.unverified
    notes: list[str] = []
    if error:
        notes.append("Settlement deduction breakdown could not be computed.")
    elif payload.get("difference_amount") is None:
        notes.append("Gross and net payout fields are incomplete for this settlement.")
    return ToolEnvelope(
        status=ToolStatus.ok if not error else ToolStatus.error,
        verification=verification,
        tool_name="get_deduction_breakdown",
        merchant_id=merchant_id,
        data={
            "settlement_id": settlement_id,
            "status": payload.get("status"),
            "expected_date": payload.get("expected_date"),
            "settled_at": payload.get("settled_at"),
            "gross_amount": payload.get("gross_amount"),
            "net_settlement_amount": payload.get("net_settlement_amount"),
            "difference_amount": payload.get("difference_amount"),
            "explained_amount": payload.get("explained_amount"),
            "unexplained_amount": payload.get("unexplained_amount"),
            "hold_reason": payload.get("hold_reason"),
            "payment_mode": payload.get("payment_mode"),
            "txn_count": payload.get("txn_count"),
            "refund_count": payload.get("refund_count"),
            "components": payload.get("components"),
            "summary": payload.get("summary"),
            "recommended_actions": payload.get("recommended_actions"),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])],
        notes=notes,
        error_message=error or None,
    )


def get_settlement_cashflow_snapshot(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = MerchantWindowInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    payload = settlements_repository.cashflow_snapshot(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
    )
    verification = VerificationStatus.verified
    notes: list[str] = []
    if payload.get("by_status") is None and payload.get("recent") is None:
        verification = VerificationStatus.unverified
        notes.append("Settlement cashflow snapshot is incomplete in the current schema.")
    return ToolEnvelope(
        status=ToolStatus.ok,
        verification=verification,
        tool_name="get_settlement_cashflow_snapshot",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "by_status": payload.get("by_status"),
            "past_expected": payload.get("past_expected"),
            "amounts": payload.get("amounts"),
            "recent": payload.get("recent"),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])] if isinstance(payload, dict) else [],
        notes=notes,
    )


def explain_settlement_shortfall(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = SettlementShortfallInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    limit = bounded_limit(validated.limit, minimum=1, maximum=100)
    payload = settlements_repository.explain_settlement_shortfall(
        engine,
        merchant_id=merchant_id,
        from_date=start_date.isoformat(),
        to_date=end_date.isoformat(),
        expected_amount=validated.expected_amount,
        received_amount=validated.received_amount,
        limit=limit,
    )
    verification = VerificationStatus.verified if payload.get("verified") else VerificationStatus.unverified
    notes: list[str] = []
    if payload.get("directional_support") and verification != VerificationStatus.verified:
        notes.append("Shortfall explanation has directional support but is not fully verified.")
    if payload.get("error"):
        notes.append(str(payload.get("error")))
    return ToolEnvelope(
        status=ToolStatus.ok if not payload.get("error") else ToolStatus.error,
        verification=verification,
        tool_name="explain_settlement_shortfall",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "shortfall": payload.get("shortfall"),
            "summary": payload.get("summary"),
            "recommended_actions": payload.get("recommended_actions"),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])] if isinstance(payload, dict) else [],
        notes=notes,
        error_message=str(payload.get("error") or "") or None,
    )


def submit_settlement_intervention(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    return _settlement_action_wrapper(
        engine,
        tool_name="submit_settlement_intervention",
        arguments=arguments,
        allowed_case_types={"held_settlement", "processed_unsettled_payout", "delayed_payout_exception"},
        default_action_type="SETTLEMENT_ESCALATION",
    )


def submit_reconciliation_review(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    return _settlement_action_wrapper(
        engine,
        tool_name="submit_reconciliation_review",
        arguments=arguments,
        allowed_case_types={"settlement_shortfall_review", "reconciliation_mismatch"},
        default_action_type="RECONCILIATION_REVIEW",
    )


def run_verified_sql(engine: Any, arguments: dict[str, Any]) -> ToolEnvelope:
    validated = VerifiedSQLInput.model_validate(arguments)
    merchant_id = require_merchant_id(validated.merchant_id)
    start_date, end_date = bounded_window(validated.start_date, validated.end_date, max_days=180)
    limit = bounded_limit(validated.limit, minimum=1, maximum=200)
    payload = execute_verified_sql(
        engine,
        merchant_id=merchant_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        query=validated.query,
        parameters=validated.parameters,
        limit=limit,
        allowed_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )
    notes: list[str] = [
        "run_verified_sql v1 only allows single-table analytics on the configured transaction fact table."
    ]
    if not payload.get("rows") and not payload.get("error"):
        notes.append("Verified query returned no rows for the requested merchant and window.")
    verification = VerificationStatus.verified if payload.get("verified") else VerificationStatus.unverified
    return ToolEnvelope(
        status=ToolStatus.ok if not payload.get("error") else ToolStatus.error,
        verification=verification,
        tool_name="run_verified_sql",
        merchant_id=merchant_id,
        window=_window_payload(start_date, end_date),
        data={
            "rows": payload.get("rows", []),
            "columns": payload.get("columns", []),
            "row_count": int(payload.get("row_count") or 0),
        },
        evidence_ids=[str(item) for item in payload.get("evidence", [])],
        notes=notes,
        error_message=str(payload.get("error") or "") or None,
    )


TOOLS: dict[str, tuple[MCPToolDescriptor, ToolHandler]] = {
    "get_merchant_profile": (
        MCPToolDescriptor(
            name="get_merchant_profile",
            description="Fetch a compact merchant profile, KYC snapshot, and risk summary.",
            input_schema=MerchantScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_merchant_profile,
    ),
    "get_risk_profile": (
        MCPToolDescriptor(
            name="get_risk_profile",
            description="Fetch the latest merchant risk profile from the current risk tables.",
            input_schema=MerchantScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_risk_profile,
    ),
    "get_kyc_status": (
        MCPToolDescriptor(
            name="get_kyc_status",
            description="Fetch the latest merchant KYC status and nearest expiry from the current KYC tables.",
            input_schema=MerchantScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_kyc_status,
    ),
    "get_watchlist_hits": (
        MCPToolDescriptor(
            name="get_watchlist_hits",
            description="Return seeded watchlist-hit context for the requested merchant when live screening integration is blocked.",
            input_schema=MerchantScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_watchlist_hits,
    ),
    "get_screening_results": (
        MCPToolDescriptor(
            name="get_screening_results",
            description="Return seeded screening-result context for the requested merchant when live screening integration is blocked.",
            input_schema=MerchantScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_screening_results,
    ),
    "get_aml_case_context": (
        MCPToolDescriptor(
            name="get_aml_case_context",
            description="Return AML-relevant case context, pinned entities, and recent timeline state for a single ops case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_aml_case_context,
    ),
    "get_velocity_anomalies": (
        MCPToolDescriptor(
            name="get_velocity_anomalies",
            description="Return transaction-velocity anomalies and concentration signals for a bounded merchant window.",
            input_schema=MerchantWindowInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_velocity_anomalies,
    ),
    "get_dispute_risk_signals": (
        MCPToolDescriptor(
            name="get_dispute_risk_signals",
            description="Return dispute-linked risk signals from merchant chargeback and refund tables in a bounded window.",
            input_schema=MerchantWindowInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_dispute_risk_signals,
    ),
    "retrieve_compliance_guidance": (
        MCPToolDescriptor(
            name="retrieve_compliance_guidance",
            description="Return seeded compliance guidance for the requested topic when live guidance retrieval is blocked.",
            input_schema=ComplianceGuidanceInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        retrieve_compliance_guidance,
    ),
    "get_background_refresh_health": (
        MCPToolDescriptor(
            name="get_background_refresh_health",
            description="Return current background proactive refresh status and stored card counts for the merchant.",
            input_schema=MerchantDaysInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_background_refresh_health,
    ),
    "get_window_kpis": (
        MCPToolDescriptor(
            name="get_window_kpis",
            description="Return bounded merchant KPI metrics for a requested date window.",
            input_schema=MerchantWindowInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_window_kpis,
    ),
    "get_failure_breakdown": (
        MCPToolDescriptor(
            name="get_failure_breakdown",
            description="Return a bounded failure breakdown by response code or payment mode.",
            input_schema=FailureBreakdownInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_failure_breakdown,
    ),
    "get_payment_mode_mix": (
        MCPToolDescriptor(
            name="get_payment_mode_mix",
            description="Return bounded payment-mode mix, success, and failure skew for the requested merchant window.",
            input_schema=PaymentModeMixInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_payment_mode_mix,
    ),
    "get_recent_transactions": (
        MCPToolDescriptor(
            name="get_recent_transactions",
            description="List recent merchant transactions in a bounded window with optional payment-mode or terminal filtering.",
            input_schema=RecentTransactionsInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_recent_transactions,
    ),
    "get_transaction_detail": (
        MCPToolDescriptor(
            name="get_transaction_detail",
            description="Fetch a single merchant-scoped transaction row for exception review.",
            input_schema=TransactionDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_transaction_detail,
    ),
    "get_terminal_profile": (
        MCPToolDescriptor(
            name="get_terminal_profile",
            description="Fetch a terminal profile with latest health snapshot and observed transaction summary.",
            input_schema=TerminalProfileInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_terminal_profile,
    ),
    "get_terminal_health_summary": (
        MCPToolDescriptor(
            name="get_terminal_health_summary",
            description="Return a bounded terminal-health summary with optional terminal scoping.",
            input_schema=TerminalHealthSummaryInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_terminal_health_summary,
    ),
    "get_terminal_failure_breakdown": (
        MCPToolDescriptor(
            name="get_terminal_failure_breakdown",
            description="Return terminal-scoped failure attribution by response code or payment mode.",
            input_schema=TerminalFailureBreakdownInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_terminal_failure_breakdown,
    ),
    "retrieve_payments_knowledge": (
        MCPToolDescriptor(
            name="retrieve_payments_knowledge",
            description="Retrieve payments-domain knowledge snippets relevant to the merchant issue under review.",
            input_schema=PaymentsKnowledgeInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        retrieve_payments_knowledge,
    ),
    "get_chargeback_summary": (
        MCPToolDescriptor(
            name="get_chargeback_summary",
            description="Return bounded chargeback counts, exposure, and due-state summary for the merchant.",
            input_schema=MerchantWindowInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_chargeback_summary,
    ),
    "list_chargebacks": (
        MCPToolDescriptor(
            name="list_chargebacks",
            description="List merchant-scoped chargebacks in a bounded window.",
            input_schema=ChargebackListInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        list_chargebacks,
    ),
    "get_chargeback_detail": (
        MCPToolDescriptor(
            name="get_chargeback_detail",
            description="Fetch a single merchant-scoped chargeback row.",
            input_schema=ChargebackDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_chargeback_detail,
    ),
    "get_refund_summary": (
        MCPToolDescriptor(
            name="get_refund_summary",
            description="Return bounded refund count, amount, and latest refund context for the merchant.",
            input_schema=MerchantWindowInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_refund_summary,
    ),
    "list_refunds": (
        MCPToolDescriptor(
            name="list_refunds",
            description="List merchant-scoped refunds in a bounded window.",
            input_schema=RefundListInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        list_refunds,
    ),
    "get_refund_detail": (
        MCPToolDescriptor(
            name="get_refund_detail",
            description="Fetch a single merchant-scoped refund row.",
            input_schema=RefundDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_refund_detail,
    ),
    "get_support_case_history": (
        MCPToolDescriptor(
            name="get_support_case_history",
            description="Return recent related support and dispute case history for the current merchant case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_support_case_history,
    ),
    "get_contact_and_escalation_context": (
        MCPToolDescriptor(
            name="get_contact_and_escalation_context",
            description="Return recorded contact details and the latest escalation chain for a single support case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_contact_and_escalation_context,
    ),
    "get_customer_service_context": (
        MCPToolDescriptor(
            name="get_customer_service_context",
            description="Return combined merchant support history, contact channel, and escalation context for a single case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_customer_service_context,
    ),
    "get_case_detail": (
        MCPToolDescriptor(
            name="get_case_detail",
            description="Return the normalized case substrate for one case, including work item, timeline, tasks, approvals, memory, and connector runs.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_case_detail,
    ),
    "get_case_timeline": (
        MCPToolDescriptor(
            name="get_case_timeline",
            description="Return the ordered timeline for one case with the latest event and approval context.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_case_timeline,
    ),
    "get_case_tasks": (
        MCPToolDescriptor(
            name="get_case_tasks",
            description="Return the current task list, task summary, and runbook progress for one case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_case_tasks,
    ),
    "get_case_memory": (
        MCPToolDescriptor(
            name="get_case_memory",
            description="Return the persisted memory state for one case, including pinned context and the latest saved summary.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_case_memory,
    ),
    "get_sla_snapshot": (
        MCPToolDescriptor(
            name="get_sla_snapshot",
            description="Return the current SLA timers, warning or breach state, and queue attention fields for one case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_sla_snapshot,
    ),
    "list_ops_queue": (
        MCPToolDescriptor(
            name="list_ops_queue",
            description="List bounded merchant-scoped queue cases, queue summary, and pending approvals for the requested ops filters.",
            input_schema=OpsQueueListInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        list_ops_queue,
    ),
    "list_connector_runs": (
        MCPToolDescriptor(
            name="list_connector_runs",
            description="Return the full connector execution history and latest run state for one case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        list_connector_runs,
    ),
    "summarize_case_timeline": (
        MCPToolDescriptor(
            name="summarize_case_timeline",
            description="Summarize the latest timeline, task, and approval state for a single ops case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.drafting,
        ),
        summarize_case_timeline,
    ),
    "get_policy_rule_explanation": (
        MCPToolDescriptor(
            name="get_policy_rule_explanation",
            description="Explain the current runbook, SLA policy, and approval state for a single ops case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_policy_rule_explanation,
    ),
    "get_connector_health": (
        MCPToolDescriptor(
            name="get_connector_health",
            description="Return the latest connector run state and run history summary for a single ops case.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_connector_health,
    ),
    "get_api_health": (
        MCPToolDescriptor(
            name="get_api_health",
            description="Return internal API health signals for one ops case using connector telemetry and fixture fallback when monitoring is blocked.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_api_health,
    ),
    "get_monitoring_alerts": (
        MCPToolDescriptor(
            name="get_monitoring_alerts",
            description="Return bounded internal monitoring alerts for one ops case with seeded fixture fallback when external monitoring is blocked.",
            input_schema=CaseScopedLimitInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_monitoring_alerts,
    ),
    "get_incident_context": (
        MCPToolDescriptor(
            name="get_incident_context",
            description="Return internal incident context for one ops case, including refresh, connector, task, and approval state.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_incident_context,
    ),
    "get_job_failures": (
        MCPToolDescriptor(
            name="get_job_failures",
            description="Return bounded internal job failures and overdue operations for one ops case.",
            input_schema=CaseScopedLimitInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_job_failures,
    ),
    "get_data_quality_checks": (
        MCPToolDescriptor(
            name="get_data_quality_checks",
            description="Run bounded transaction data quality checks for the requested merchant and date window.",
            input_schema=MerchantWindowInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_data_quality_checks,
    ),
    "draft_case_note": (
        MCPToolDescriptor(
            name="draft_case_note",
            description="Draft an internal operator note from the current ops case state.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.drafting,
        ),
        draft_case_note,
    ),
    "draft_approval_request": (
        MCPToolDescriptor(
            name="draft_approval_request",
            description="Draft a follow-up approval payload from the current ops case state.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.drafting,
        ),
        draft_approval_request,
    ),
    "draft_merchant_update": (
        MCPToolDescriptor(
            name="draft_merchant_update",
            description="Draft a merchant-facing support update from the current case state.",
            input_schema=CaseScopedInput.model_json_schema(),
            classification=ToolClassification.drafting,
        ),
        draft_merchant_update,
    ),
    "list_settlements": (
        MCPToolDescriptor(
            name="list_settlements",
            description="List bounded merchant settlements for the requested date window.",
            input_schema=SettlementListInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        list_settlements,
    ),
    "get_settlement_detail": (
        MCPToolDescriptor(
            name="get_settlement_detail",
            description="Fetch a single merchant-scoped settlement row and related reconciliation detail.",
            input_schema=SettlementDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_settlement_detail,
    ),
    "get_settlement_reconciliation": (
        MCPToolDescriptor(
            name="get_settlement_reconciliation",
            description="Return reconciliation counts and the top reason for a single settlement.",
            input_schema=SettlementDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_settlement_reconciliation,
    ),
    "get_settlement_timeline": (
        MCPToolDescriptor(
            name="get_settlement_timeline",
            description="Return lifecycle milestones and the current stage for a single settlement.",
            input_schema=SettlementDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_settlement_timeline,
    ),
    "get_reconciliation_breaks": (
        MCPToolDescriptor(
            name="get_reconciliation_breaks",
            description="Return unresolved reconciliation break buckets for a single settlement.",
            input_schema=SettlementDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_reconciliation_breaks,
    ),
    "get_hold_reason": (
        MCPToolDescriptor(
            name="get_hold_reason",
            description="Return the explicit hold reason and settlement status for a single settlement.",
            input_schema=SettlementDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_hold_reason,
    ),
    "get_payout_delay_context": (
        MCPToolDescriptor(
            name="get_payout_delay_context",
            description="Return delay state, days past expected date, and hold context for a single settlement.",
            input_schema=SettlementDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_payout_delay_context,
    ),
    "get_deduction_breakdown": (
        MCPToolDescriptor(
            name="get_deduction_breakdown",
            description="Break down gross, net, deductions, and unexplained payout delta for a single settlement.",
            input_schema=SettlementDetailInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_deduction_breakdown,
    ),
    "get_settlement_cashflow_snapshot": (
        MCPToolDescriptor(
            name="get_settlement_cashflow_snapshot",
            description="Return a bounded settlement cashflow snapshot and recent settlement rows.",
            input_schema=MerchantWindowInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        get_settlement_cashflow_snapshot,
    ),
    "explain_settlement_shortfall": (
        MCPToolDescriptor(
            name="explain_settlement_shortfall",
            description="Explain a merchant settlement shortfall using bounded settlement rows and deduction fields.",
            input_schema=SettlementShortfallInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        explain_settlement_shortfall,
    ),
    "submit_settlement_intervention": (
        MCPToolDescriptor(
            name="submit_settlement_intervention",
            description="Prepare the approval-gated settlement intervention wrapper without executing any downstream write.",
            input_schema=SettlementCaseActionInput.model_json_schema(),
            classification=ToolClassification.write,
            approval_required=True,
            downstream_target="settlement_ops_core",
            idempotency_expectation="Connector idempotency is derived from approval_id + case_id + action_type after approval.",
        ),
        submit_settlement_intervention,
    ),
    "submit_reconciliation_review": (
        MCPToolDescriptor(
            name="submit_reconciliation_review",
            description="Prepare the approval-gated reconciliation review wrapper without executing any downstream write.",
            input_schema=SettlementCaseActionInput.model_json_schema(),
            classification=ToolClassification.write,
            approval_required=True,
            downstream_target="settlement_ops_core",
            idempotency_expectation="Connector idempotency is derived from approval_id + case_id + action_type after approval.",
        ),
        submit_reconciliation_review,
    ),
    "run_verified_sql": (
        MCPToolDescriptor(
            name="run_verified_sql",
            description="Run bounded verified SQL over the configured transaction fact table with merchant and date scoping.",
            input_schema=VerifiedSQLInput.model_json_schema(),
            classification=ToolClassification.read,
        ),
        run_verified_sql,
    ),
}

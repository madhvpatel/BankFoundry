from __future__ import annotations

import datetime as dt
import re
from typing import Any

from app.agent.bank_ops_contracts import tool_filter_for_agent
from app.agent.mcp_client import (
    BankFoundryMCPClient,
    OpsCaseCopilotMCPAgent,
    _dedupe_text,
    _derive_case_window,
)
from app.mcp_server import BankFoundryMCPServer, ToolEnvelope, ToolStatus, VerificationStatus


SETTLEMENT_CASE_TYPES = {
    "held_settlement",
    "processed_unsettled_payout",
    "settlement_shortfall_review",
    "reconciliation_mismatch",
    "delayed_payout_exception",
}
DISPUTE_CASE_TYPES = {"chargeback_review", "refund_exception"}
PAYMENTS_CASE_TYPES = {"payment_exception", "payment_mode_skew", "terminal_linked_failures", "terminal_failure_review"}
SUPPORT_CASE_TYPES = {"merchant_support_case"}
RISK_CASE_TYPES = {"risk_triage", "kyc_review"}
AML_CASE_TYPES = {"aml_investigation", "aml_review", "screening_review", "watchlist_review"}
CONNECTOR_CASE_TYPES = {"connector_follow_up"}
INCIDENT_CASE_TYPES = {"background_refresh_issue", "incident_response"}
GENERIC_ENTITY_TOKENS = {
    "delay",
    "review",
    "exception",
    "issue",
    "issues",
    "status",
    "shortfall",
    "mismatch",
    "held",
    "payout",
    "settlement",
    "chargeback",
    "refund",
    "transaction",
    "failure",
    "failures",
    "support",
    "case",
}

SETTLEMENT_ID_PATTERN = re.compile(r"\bsettlement\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)
CHARGEBACK_ID_PATTERN = re.compile(r"\bchargeback\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)
REFUND_ID_PATTERN = re.compile(r"\brefund\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)
TRANSACTION_ID_PATTERN = re.compile(r"\b(?:tx|transaction)\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)
SHORTFALL_CASE_TYPES = {"settlement_shortfall_review", "reconciliation_mismatch"}
AML_TRIGGER_TOKENS = ("aml", "watchlist", "screening", "sanction", "pep", "compliance")


def _work_item(case_detail: dict[str, Any]) -> dict[str, Any]:
    value = case_detail.get("work_item")
    return value if isinstance(value, dict) else {}


def _source_payload(case_detail: dict[str, Any]) -> dict[str, Any]:
    value = _work_item(case_detail).get("source_payload")
    return value if isinstance(value, dict) else {}


def _case_memory(case_detail: dict[str, Any]) -> dict[str, Any]:
    value = case_detail.get("memory")
    return value if isinstance(value, dict) else {}


def _case_type(case_detail: dict[str, Any]) -> str:
    return str(_work_item(case_detail).get("case_type") or "manual_ops_review").strip().lower()


def _lane(case_detail: dict[str, Any]) -> str:
    return str(_work_item(case_detail).get("lane") or "").strip().lower()


def _approval_state(case_detail: dict[str, Any]) -> dict[str, Any]:
    value = case_detail.get("approval_state")
    return value if isinstance(value, dict) else {}


def _looks_like_explicit_entity_id(value: Any) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    if any(ch.isspace() for ch in raw):
        return False
    return raw.lower() not in GENERIC_ENTITY_TOKENS


def _looks_like_text_entity_id(value: Any) -> bool:
    raw = str(value or "").strip()
    if not _looks_like_explicit_entity_id(raw):
        return False
    return any(ch.isdigit() for ch in raw)


def _extract_settlement_id(case_detail: dict[str, Any]) -> str | None:
    work_item = _work_item(case_detail)
    source_payload = _source_payload(case_detail)
    memory = _case_memory(case_detail)
    pinned_entities = memory.get("pinned_entities") if isinstance(memory.get("pinned_entities"), dict) else {}
    pinned_settlement_id = str(pinned_entities.get("settlement_id") or "").strip()
    if pinned_settlement_id:
        return pinned_settlement_id

    candidates: list[str] = []
    candidates.extend(str(item) for item in (work_item.get("evidence_ids") or []) if item)
    candidates.extend(str(item) for item in (memory.get("confirmed_evidence_ids") or []) if item)
    candidates.extend(
        str(item)
        for item in (source_payload.get("evidence_ids") or source_payload.get("sources") or [])
        if item
    )
    for item in candidates:
        if item.lower().startswith("settlement:"):
            settlement_id = item.split(":", 1)[1].strip()
            if settlement_id:
                return settlement_id

    explicit_settlement_id = str(source_payload.get("settlement_id") or "").strip()
    if _looks_like_explicit_entity_id(explicit_settlement_id):
        return explicit_settlement_id

    for item in (source_payload.get("source_ref"), work_item.get("source_ref")):
        raw = str(item or "").strip()
        if raw:
            match = SETTLEMENT_ID_PATTERN.search(raw)
            if match and _looks_like_text_entity_id(match.group(1)):
                return match.group(1)

    text_candidates = [
        work_item.get("title"),
        work_item.get("summary"),
        source_payload.get("title"),
        source_payload.get("summary"),
        source_payload.get("body"),
    ]
    for item in text_candidates:
        raw = str(item or "").strip()
        if not raw:
            continue
        match = SETTLEMENT_ID_PATTERN.search(raw)
        if match and _looks_like_text_entity_id(match.group(1)):
            return match.group(1)
    return None


def _extract_entity_id(
    case_detail: dict[str, Any],
    *,
    pinned_key: str,
    evidence_prefix: str,
    pattern: re.Pattern[str],
    explicit_keys: list[str] | None = None,
) -> str | None:
    work_item = _work_item(case_detail)
    source_payload = _source_payload(case_detail)
    memory = _case_memory(case_detail)
    pinned_entities = memory.get("pinned_entities") if isinstance(memory.get("pinned_entities"), dict) else {}
    pinned_value = str(pinned_entities.get(pinned_key) or "").strip()
    if pinned_value:
        return pinned_value

    candidates: list[str] = []
    candidates.extend(str(item) for item in (work_item.get("evidence_ids") or []) if item)
    candidates.extend(str(item) for item in (memory.get("confirmed_evidence_ids") or []) if item)
    candidates.extend(str(item) for item in (source_payload.get("evidence_ids") or source_payload.get("sources") or []) if item)
    for item in candidates:
        if item.lower().startswith(f"{evidence_prefix}:"):
            value = item.split(":", 1)[1].strip()
            if value:
                return value

    for key in explicit_keys or []:
        raw = str(source_payload.get(key) or work_item.get(key) or "").strip()
        if raw:
            if key != "source_ref" and _looks_like_explicit_entity_id(raw):
                return raw
            match = pattern.search(raw)
            if match and _looks_like_text_entity_id(match.group(1)):
                return match.group(1)

    text_candidates = [
        work_item.get("title"),
        work_item.get("summary"),
        source_payload.get("title"),
        source_payload.get("summary"),
        source_payload.get("body"),
    ]
    for item in text_candidates:
        raw = str(item or "").strip()
        if not raw:
            continue
        match = pattern.search(raw)
        if match and _looks_like_text_entity_id(match.group(1)):
            return match.group(1)
    return None


def _extract_chargeback_id(case_detail: dict[str, Any]) -> str | None:
    return _extract_entity_id(
        case_detail,
        pinned_key="chargeback_id",
        evidence_prefix="chargeback",
        pattern=CHARGEBACK_ID_PATTERN,
        explicit_keys=["chargeback_id", "source_ref"],
    )


def _extract_refund_id(case_detail: dict[str, Any]) -> str | None:
    return _extract_entity_id(
        case_detail,
        pinned_key="refund_id",
        evidence_prefix="refund",
        pattern=REFUND_ID_PATTERN,
        explicit_keys=["refund_id", "source_ref"],
    )


def _extract_transaction_id(case_detail: dict[str, Any]) -> str | None:
    return _extract_entity_id(
        case_detail,
        pinned_key="tx_id",
        evidence_prefix="tx",
        pattern=TRANSACTION_ID_PATTERN,
        explicit_keys=["tx_id", "transaction_id", "source_ref"],
    )


def _extract_terminal_id(case_detail: dict[str, Any]) -> str | None:
    work_item = _work_item(case_detail)
    source_payload = _source_payload(case_detail)
    memory = _case_memory(case_detail)
    pinned_entities = memory.get("pinned_entities") if isinstance(memory.get("pinned_entities"), dict) else {}

    for value in (
        pinned_entities.get("terminal_id"),
        work_item.get("terminal_id"),
        work_item.get("tid"),
        source_payload.get("terminal_id"),
        source_payload.get("tid"),
    ):
        terminal_id = str(value or "").strip()
        if terminal_id:
            return terminal_id

    candidates: list[str] = []
    candidates.extend(str(item) for item in (work_item.get("evidence_ids") or []) if item)
    candidates.extend(str(item) for item in (memory.get("confirmed_evidence_ids") or []) if item)
    candidates.extend(str(item) for item in (source_payload.get("evidence_ids") or source_payload.get("sources") or []) if item)
    for item in candidates:
        lowered = item.lower()
        if lowered.startswith("terminal:") or lowered.startswith("tid:"):
            terminal_id = item.split(":", 1)[1].strip()
            if terminal_id:
                return terminal_id
    return None


def _tool_call_entry(envelope: ToolEnvelope) -> dict[str, Any]:
    return {
        "tool_name": envelope.tool_name,
        "verification": envelope.verification.value,
    }


def _format_money(value: Any) -> str:
    try:
        amount = float(value)
    except Exception:
        return ""
    return f"Rs {amount:,.2f}"


def _format_issue_label(value: Any) -> str:
    return str(value or "").strip().replace("_", " ")


def _parse_iso_date(value: Any) -> dt.date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return dt.date.fromisoformat(raw[:10])
    except Exception:
        return None


def _settlement_context_window(
    settlement_row: dict[str, Any],
    *,
    fallback_start: str,
    fallback_end: str,
    days: int = 7,
) -> tuple[str, str]:
    anchor_date = _parse_iso_date(settlement_row.get("expected_date")) or _parse_iso_date(settlement_row.get("settled_at"))
    if anchor_date is None:
        return fallback_start, fallback_end
    start = anchor_date - dt.timedelta(days=days)
    end = anchor_date + dt.timedelta(days=days + 1)
    return start.isoformat(), end.isoformat()


def _is_unresolved_settlement_row(row: dict[str, Any]) -> bool:
    if str(row.get("settled_at") or "").strip():
        return False
    normalized = str(row.get("status") or "").strip().upper()
    return not normalized.startswith(("SETTLED", "PAID", "SUCCESS"))


def _append_envelope(
    envelope: ToolEnvelope | None,
    *,
    tool_calls: list[dict[str, Any]],
    evidence_ids: list[str],
    notes: list[str],
    verification_flags: list[VerificationStatus],
) -> None:
    if envelope is None:
        return
    tool_calls.append(_tool_call_entry(envelope))
    evidence_ids.extend(str(item) for item in envelope.evidence_ids)
    notes.extend(str(item) for item in envelope.notes)
    verification_flags.append(envelope.verification)


def _base_next_action(case_detail: dict[str, Any]) -> str:
    approval_state = _approval_state(case_detail)
    if str(approval_state.get("status") or "").lower() == "pending":
        return "Review the pending approval so the settlement action can move to connector execution."
    runbook_steps = case_detail.get("runbook_steps")
    if isinstance(runbook_steps, list):
        pending_step = next(
            (
                step
                for step in runbook_steps
                if isinstance(step, dict) and str(step.get("status") or "").upper() != "DONE"
            ),
            None,
        )
        if isinstance(pending_step, dict):
            return f"{pending_step.get('title')}: {pending_step.get('description')}"
    return "Review the latest settlement evidence and close the case if no further intervention is required."


def _settlement_case_setup(
    case_detail: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], str, str, str, str, str, str]:
    work_item = _work_item(case_detail)
    source_payload = _source_payload(case_detail)
    merchant_id = str(work_item.get("merchant_id") or "").strip()
    if not merchant_id:
        raise ValueError("case detail is missing merchant_id")
    case_type = _case_type(case_detail)
    settlement_id = _extract_settlement_id(case_detail) or ""
    start_date, end_date, window_reason = _derive_case_window(case_detail)
    return work_item, source_payload, merchant_id, case_type, settlement_id, start_date, end_date, window_reason


def _build_memory_snapshot(case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    work_item = _work_item(case_detail)
    existing_memory = _case_memory(case_detail)
    existing_pinned = existing_memory.get("pinned_entities") if isinstance(existing_memory.get("pinned_entities"), dict) else {}
    existing_window = existing_memory.get("active_window") if isinstance(existing_memory.get("active_window"), dict) else {}
    existing_summary = existing_memory.get("latest_summary") if isinstance(existing_memory.get("latest_summary"), dict) else {}
    context = summary.get("case_context") if isinstance(summary.get("case_context"), dict) else {}
    summary_window = summary.get("window") if isinstance(summary.get("window"), dict) else {}
    answer_sections = summary.get("answer_sections") if isinstance(summary.get("answer_sections"), dict) else {}

    settlement_id = str(context.get("settlement_id") or existing_pinned.get("settlement_id") or "").strip()
    chargeback_id = str(context.get("chargeback_id") or existing_pinned.get("chargeback_id") or "").strip()
    refund_id = str(context.get("refund_id") or existing_pinned.get("refund_id") or "").strip()
    active_window = {
        "start_date": str(summary_window.get("start_date") or existing_window.get("start_date") or "").strip(),
        "end_date": str(summary_window.get("end_date") or existing_window.get("end_date") or "").strip(),
        "reason": str(summary_window.get("reason") or existing_window.get("reason") or "").strip(),
    }

    confirmed_evidence_ids = _dedupe_text(
        [str(item) for item in (existing_memory.get("confirmed_evidence_ids") or [])]
        + [str(item) for item in (work_item.get("evidence_ids") or [])]
        + [str(item) for item in (summary.get("evidence_ids") or [])]
    )

    return {
        "pinned_entities": {
            "merchant_id": str(work_item.get("merchant_id") or existing_pinned.get("merchant_id") or "").strip(),
            "terminal_id": str(work_item.get("terminal_id") or existing_pinned.get("terminal_id") or "").strip() or None,
            "case_type": str(work_item.get("case_type") or existing_pinned.get("case_type") or "").strip(),
            "settlement_id": settlement_id or None,
            "chargeback_id": chargeback_id or None,
            "refund_id": refund_id or None,
        },
        "active_window": active_window,
        "confirmed_evidence_ids": confirmed_evidence_ids,
        "latest_summary": {
            "executive_summary": str(
                answer_sections.get("executive_summary")
                or summary.get("summary")
                or existing_summary.get("executive_summary")
                or ""
            ).strip(),
            "verification": str(summary.get("verification") or existing_summary.get("verification") or "").strip(),
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        },
        "latest_tool_calls": [dict(item) for item in (summary.get("tool_calls") or []) if isinstance(item, dict)],
    }


def _draft_generic_operator_note(case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    work_item = _work_item(case_detail)
    findings = summary.get("answer_sections", {}).get("key_findings") or []
    lines = [
        f"Case review for {work_item.get('title') or work_item.get('case_id')}:",
    ]
    for finding in findings[:3]:
        lines.append(f"- {finding}")
    next_action = str(summary.get("answer_sections", {}).get("next_best_action") or "").strip()
    if next_action:
        lines.append(f"Next action: {next_action}")
    return {
        "status": "ready",
        "body": "\n".join(lines),
    }


def _draft_generic_approval_request(case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    work_item = _work_item(case_detail)
    approval_state = _approval_state(case_detail)
    if str(work_item.get("status") or "").upper() in {"RESOLVED", "CLOSED"}:
        return {
            "status": "blocked",
            "reason": "Case is already resolved.",
        }
    if str(approval_state.get("status") or "").lower() == "pending":
        return {
            "status": "blocked",
            "reason": "This case already has a pending approval.",
        }
    next_action = str(summary.get("answer_sections", {}).get("next_best_action") or "Review the case for follow-through.").strip()
    return {
        "status": "ready",
        "action_type": "SETTLEMENT_ESCALATION",
        "payload_summary": f"Review {work_item.get('title') or work_item.get('case_id')}",
        "payload": {
            "case_id": work_item.get("case_id"),
            "merchant_id": work_item.get("merchant_id"),
            "lane": work_item.get("lane"),
            "evidence_ids": summary.get("evidence_ids") or work_item.get("evidence_ids") or [],
            "recommended_action": next_action,
        },
    }


class SettlementCaseSummaryAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        (
            work_item,
            source_payload,
            merchant_id,
            case_type,
            settlement_id,
            start_date,
            end_date,
            window_reason,
        ) = _settlement_case_setup(case_detail)

        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        cashflow = self._client.call_tool(
            "get_settlement_cashflow_snapshot",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date},
        )
        settlement_detail: ToolEnvelope | None = None
        hold_reason: ToolEnvelope | None = None
        reconciliation: ToolEnvelope | None = None
        if settlement_id:
            settlement_detail = self._client.call_tool(
                "get_settlement_detail",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )
            hold_reason = self._client.call_tool(
                "get_hold_reason",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )
            reconciliation = self._client.call_tool(
                "get_settlement_reconciliation",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )

        shortfall: ToolEnvelope | None = None
        should_explain_shortfall = case_type in SHORTFALL_CASE_TYPES or any(
            token in str(prompt or "").lower()
            for token in ("shortfall", "deduction", "net amount", "reconciliation", "gross")
        )
        if should_explain_shortfall:
            shortfall_args: dict[str, Any] = {
                "merchant_id": merchant_id,
                "start_date": start_date,
                "end_date": end_date,
            }
            expected_amount = source_payload.get("expected_amount")
            received_amount = source_payload.get("received_amount")
            if expected_amount is not None:
                shortfall_args["expected_amount"] = expected_amount
            if received_amount is not None:
                shortfall_args["received_amount"] = received_amount
            shortfall = self._client.call_tool("explain_settlement_shortfall", shortfall_args)

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        settlement_row = (
            settlement_detail.data.get("settlement")
            if settlement_detail and isinstance(settlement_detail.data.get("settlement"), dict)
            else {}
        )
        reconciliation_rows = (
            reconciliation.data.get("reconciliation")
            if reconciliation and isinstance(reconciliation.data.get("reconciliation"), list)
            else []
        )
        reconciliation_total = (
            int(reconciliation.data.get("total_rows") or 0)
            if reconciliation and isinstance(reconciliation.data, dict)
            else 0
        )
        hold_reason_value = (
            str(hold_reason.data.get("hold_reason") or "").strip()
            if hold_reason and isinstance(hold_reason.data, dict)
            else ""
        )
        cashflow_data = cashflow.data if isinstance(cashflow.data, dict) else {}
        shortfall_data = shortfall.data if shortfall and isinstance(shortfall.data, dict) else {}
        shortfall_summary = str(shortfall_data.get("summary") or "").strip()
        shortfall_object = shortfall_data.get("shortfall") if isinstance(shortfall_data.get("shortfall"), dict) else {}

        findings: list[str] = []
        if city:
            findings.append(f"Merchant context: {trade_name} in {city}.")
        else:
            findings.append(f"Merchant context: {trade_name}.")

        if settlement_id and settlement_row:
            status = str(settlement_row.get("status") or "UNKNOWN").upper()
            expected_date = str(settlement_row.get("expected_date") or "").strip()
            amount = _format_money(settlement_row.get("amount_rupees"))
            detail_bits = [f"Settlement {settlement_id} is {status}"]
            if expected_date:
                detail_bits.append(f"expected on {expected_date}")
            if amount:
                detail_bits.append(f"for {amount}")
            settled_at = str(settlement_row.get("settled_at") or "").strip()
            if settled_at:
                detail_bits.append(f"settled at {settled_at}")
            findings.append(", ".join(detail_bits) + ".")
        elif settlement_id:
            findings.append(f"Settlement {settlement_id} could not be confirmed from the current settlement rows.")
        else:
            findings.append("No settlement id is pinned to this case yet, so the review is using merchant-level settlement context.")

        past_expected = cashflow_data.get("past_expected") if isinstance(cashflow_data.get("past_expected"), dict) else {}
        amounts = cashflow_data.get("amounts") if isinstance(cashflow_data.get("amounts"), dict) else {}
        if past_expected:
            past_expected_count = int(past_expected.get("past_expected_count") or 0)
            past_expected_amount = _format_money(past_expected.get("past_expected_amount"))
            findings.append(
                f"Merchant settlement backlog shows {past_expected_count} past-expected payouts"
                + (f" totaling {past_expected_amount}." if past_expected_amount else ".")
            )
        if amounts:
            pending_amount = _format_money(amounts.get("pending_amount"))
            settled_amount = _format_money(amounts.get("settled_amount"))
            if pending_amount or settled_amount:
                findings.append(
                    "Current settlement cashflow in this window: "
                    + ", ".join(
                        item
                        for item in (
                            f"pending {pending_amount}" if pending_amount else "",
                            f"settled {settled_amount}" if settled_amount else "",
                        )
                        if item
                    )
                    + "."
                )

        if reconciliation_rows:
            top_reason = (
                reconciliation.data.get("top_reason")
                if reconciliation and isinstance(reconciliation.data.get("top_reason"), dict)
                else reconciliation_rows[0]
            )
            reason = str(top_reason.get("reason") or top_reason.get("status") or "unknown").strip()
            findings.append(
                f"Top reconciliation signal for this settlement is {reason} across "
                f"{int(top_reason.get('count') or reconciliation_total or 0)} row(s)."
            )
        if hold_reason_value:
            findings.append(f"Recorded hold reason on the settlement row is {hold_reason_value}.")

        if shortfall_summary:
            findings.append(shortfall_summary)
        elif shortfall_object:
            difference_amount = _format_money(shortfall_object.get("difference_amount"))
            if difference_amount:
                findings.append(f"Computed payout delta for the selected settlement is {difference_amount}.")

        next_best_action = _base_next_action(case_detail)
        shortfall_actions = shortfall_data.get("recommended_actions") if isinstance(shortfall_data.get("recommended_actions"), list) else []
        if shortfall_actions:
            next_best_action = str(shortfall_actions[0]).strip() or next_best_action

        caveats: list[str] = []
        if settlement_id and settlement_detail and settlement_detail.verification != VerificationStatus.verified:
            caveats.append("The pinned settlement row could not be fully verified from the current settlement tables.")
        if settlement_id and hold_reason and hold_reason.verification != VerificationStatus.verified:
            caveats.append("The explicit hold reason could not be confirmed from the settlement row.")
        if not settlement_id:
            caveats.append("A settlement-specific escalation draft should wait until a settlement id is pinned on the case.")
        if shortfall is not None and shortfall.verification != VerificationStatus.verified:
            caveats.append("Shortfall attribution is directional and should be confirmed before escalation.")
        if cashflow.verification != VerificationStatus.verified:
            caveats.append("Settlement cashflow context is incomplete in the current schema.")
        if reconciliation and reconciliation.verification != VerificationStatus.verified:
            caveats.append("Reconciliation context is incomplete in the current schema.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(cashflow, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(settlement_detail, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(hold_reason, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(reconciliation, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(shortfall, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        if settlement_id:
            executive_summary = (
                f"{work_item.get('title') or 'Settlement case'} is currently {case_status} for {trade_name}. "
                f"The review is anchored on settlement {settlement_id} and the latest settlement cashflow context."
            )
        else:
            executive_summary = (
                f"{work_item.get('title') or 'Settlement case'} is currently {case_status} for {trade_name}. "
                "The case is still using merchant-level settlement context because no settlement id is pinned yet."
            )

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {
                "start_date": start_date,
                "end_date": end_date,
                "reason": window_reason,
            },
            "case_context": {
                "case_type": case_type,
                "settlement_id": settlement_id,
            },
        }


class ReconciliationInvestigationAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        (
            work_item,
            source_payload,
            merchant_id,
            case_type,
            settlement_id,
            start_date,
            end_date,
            window_reason,
        ) = _settlement_case_setup(case_detail)

        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        settlement_detail: ToolEnvelope | None = None
        deductions: ToolEnvelope | None = None
        reconciliation: ToolEnvelope | None = None
        reconciliation_breaks: ToolEnvelope | None = None
        nearby_settlements: ToolEnvelope | None = None
        if settlement_id:
            settlement_detail = self._client.call_tool(
                "get_settlement_detail",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )
            deductions = self._client.call_tool(
                "get_deduction_breakdown",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )
            reconciliation = self._client.call_tool(
                "get_settlement_reconciliation",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )
            reconciliation_breaks = self._client.call_tool(
                "get_reconciliation_breaks",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )

        shortfall_args: dict[str, Any] = {
            "merchant_id": merchant_id,
            "start_date": start_date,
            "end_date": end_date,
        }
        expected_amount = source_payload.get("expected_amount")
        received_amount = source_payload.get("received_amount")
        if expected_amount is not None:
            shortfall_args["expected_amount"] = expected_amount
        if received_amount is not None:
            shortfall_args["received_amount"] = received_amount
        should_explain_shortfall = (
            case_type == "settlement_shortfall_review"
            or expected_amount is not None
            or received_amount is not None
            or any(
                token in str(prompt or "").lower()
                for token in ("shortfall", "deduction", "net amount", "gross amount")
            )
        )
        shortfall = (
            self._client.call_tool("explain_settlement_shortfall", shortfall_args)
            if should_explain_shortfall
            else None
        )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        settlement_row = (
            settlement_detail.data.get("settlement")
            if settlement_detail and isinstance(settlement_detail.data.get("settlement"), dict)
            else {}
        )
        if settlement_id:
            context_start, context_end = _settlement_context_window(
                settlement_row,
                fallback_start=start_date,
                fallback_end=end_date,
            )
            nearby_settlements = self._client.call_tool(
                "list_settlements",
                {
                    "merchant_id": merchant_id,
                    "start_date": context_start,
                    "end_date": context_end,
                    "limit": 10,
                },
            )
        deduction_data = deductions.data if deductions and isinstance(deductions.data, dict) else {}
        reconciliation_data = reconciliation.data if reconciliation and isinstance(reconciliation.data, dict) else {}
        reconciliation_breaks_data = (
            reconciliation_breaks.data if reconciliation_breaks and isinstance(reconciliation_breaks.data, dict) else {}
        )
        shortfall_data = shortfall.data if shortfall and isinstance(shortfall.data, dict) else {}
        nearby_rows = nearby_settlements.data.get("rows") if nearby_settlements and isinstance(nearby_settlements.data.get("rows"), list) else []

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        if settlement_id and settlement_row:
            status = str(settlement_row.get("status") or "UNKNOWN").upper()
            expected_date = str(settlement_row.get("expected_date") or "").strip()
            findings.append(
                f"Settlement {settlement_id} is {status}"
                + (f" with expected date {expected_date}." if expected_date else ".")
            )
        elif settlement_id:
            findings.append(f"Settlement {settlement_id} could not be confirmed from the current settlement rows.")
        else:
            findings.append("No settlement id is pinned to this case yet, so reconciliation review is still merchant-scoped.")

        difference_amount = _format_money(deduction_data.get("difference_amount"))
        gross_amount = _format_money(deduction_data.get("gross_amount"))
        net_amount = _format_money(deduction_data.get("net_settlement_amount"))
        if difference_amount:
            findings.append(
                f"Settlement payout delta is {difference_amount}"
                + (
                    f" from gross {gross_amount} to net {net_amount}."
                    if gross_amount or net_amount
                    else "."
                )
            )
        components = deduction_data.get("components") if isinstance(deduction_data.get("components"), list) else []
        if components:
            top_components = ", ".join(
                f"{item.get('label')} {_format_money(item.get('amount_rupees'))}"
                for item in components[:3]
                if _format_money(item.get("amount_rupees"))
            )
            if top_components:
                findings.append(f"Known deduction components: {top_components}.")
        if deduction_data.get("hold_reason"):
            findings.append(f"Hold context on the settlement row is {deduction_data.get('hold_reason')}.")

        reconciliation_rows = reconciliation_data.get("reconciliation") if isinstance(reconciliation_data.get("reconciliation"), list) else []
        if reconciliation_rows:
            top_reason = reconciliation_data.get("top_reason") if isinstance(reconciliation_data.get("top_reason"), dict) else reconciliation_rows[0]
            reason = str(top_reason.get("reason") or top_reason.get("status") or "unknown").strip()
            findings.append(
                f"Reconciliation shows {int(reconciliation_data.get('total_rows') or 0)} row(s), "
                f"with top reason {reason} across {int(top_reason.get('count') or 0)} row(s)."
            )
        break_rows = (
            reconciliation_breaks_data.get("breaks")
            if isinstance(reconciliation_breaks_data.get("breaks"), list)
            else []
        )
        if break_rows:
            top_breaks = ", ".join(
                f"{item.get('reason') or item.get('status')} ({int(item.get('count') or 0)})"
                for item in break_rows[:3]
            )
            findings.append(f"Unresolved reconciliation breaks are concentrated in: {top_breaks}.")

        if nearby_rows:
            unresolved_count = sum(1 for row in nearby_rows if isinstance(row, dict) and _is_unresolved_settlement_row(row))
            findings.append(
                f"Nearby settlement context shows {len(nearby_rows)} settlement(s) in the anchor window, "
                f"with {unresolved_count} still unsettled."
            )

        shortfall_summary = str(shortfall_data.get("summary") or "").strip()
        if shortfall_summary:
            findings.append(shortfall_summary)

        next_best_action = _base_next_action(case_detail)
        deduction_actions = deduction_data.get("recommended_actions") if isinstance(deduction_data.get("recommended_actions"), list) else []
        shortfall_actions = shortfall_data.get("recommended_actions") if isinstance(shortfall_data.get("recommended_actions"), list) else []
        if break_rows:
            top_break = break_rows[0]
            break_reason = str(top_break.get("reason") or top_break.get("status") or "the leading reconciliation break").strip()
            next_best_action = f"Resolve the leading reconciliation break ({break_reason}) before asking for downstream intervention."
        for action_list in (deduction_actions, shortfall_actions):
            if action_list:
                next_best_action = str(action_list[0]).strip() or next_best_action
                break

        caveats: list[str] = []
        if not settlement_id:
            caveats.append("A settlement id should be pinned before escalating a reconciliation review.")
        if deductions and deductions.verification != VerificationStatus.verified:
            caveats.append("Deduction attribution is incomplete and should be checked before escalation.")
        if reconciliation and reconciliation.verification != VerificationStatus.verified:
            caveats.append("Reconciliation context could not be fully verified from the current schema.")
        if reconciliation_breaks and reconciliation_breaks.verification != VerificationStatus.verified:
            caveats.append("Reconciliation break buckets are incomplete and should be confirmed before escalation.")
        if shortfall and shortfall.verification != VerificationStatus.verified:
            caveats.append("Shortfall explanation is directional and should be confirmed before approval.")
        if nearby_settlements and nearby_settlements.verification != VerificationStatus.verified:
            caveats.append("Nearby settlement context is incomplete in the current settlement list.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(settlement_detail, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(deductions, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(reconciliation, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(reconciliation_breaks, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(nearby_settlements, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(shortfall, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Reconciliation case").strip()
        executive_summary = (
            f"{title} is currently {case_status} for {trade_name}. "
            + (
                f"The investigation is anchored on settlement {settlement_id}."
                if settlement_id
                else "The investigation is still using merchant-level settlement context."
            )
        )

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {
                "start_date": start_date,
                "end_date": end_date,
                "reason": window_reason,
            },
            "case_context": {
                "case_type": case_type,
                "settlement_id": settlement_id or None,
            },
        }


class DelayedPayoutAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        (
            work_item,
            _source_payload,
            merchant_id,
            case_type,
            settlement_id,
            start_date,
            end_date,
            window_reason,
        ) = _settlement_case_setup(case_detail)

        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        delay_context: ToolEnvelope | None = None
        hold_reason: ToolEnvelope | None = None
        reconciliation: ToolEnvelope | None = None
        timeline: ToolEnvelope | None = None
        nearby_settlements: ToolEnvelope | None = None
        if settlement_id:
            delay_context = self._client.call_tool(
                "get_payout_delay_context",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )
            hold_reason = self._client.call_tool(
                "get_hold_reason",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )
            reconciliation = self._client.call_tool(
                "get_settlement_reconciliation",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )
            timeline = self._client.call_tool(
                "get_settlement_timeline",
                {"merchant_id": merchant_id, "settlement_id": settlement_id},
            )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        delay_data = delay_context.data if delay_context and isinstance(delay_context.data, dict) else {}
        hold_data = hold_reason.data if hold_reason and isinstance(hold_reason.data, dict) else {}
        reconciliation_data = reconciliation.data if reconciliation and isinstance(reconciliation.data, dict) else {}
        timeline_data = timeline.data if timeline and isinstance(timeline.data, dict) else {}
        if settlement_id:
            context_start, context_end = _settlement_context_window(
                {
                    "expected_date": delay_data.get("expected_date"),
                    "settled_at": delay_data.get("settled_at"),
                },
                fallback_start=start_date,
                fallback_end=end_date,
            )
            nearby_settlements = self._client.call_tool(
                "list_settlements",
                {
                    "merchant_id": merchant_id,
                    "start_date": context_start,
                    "end_date": context_end,
                    "limit": 10,
                },
            )
        nearby_rows = nearby_settlements.data.get("rows") if nearby_settlements and isinstance(nearby_settlements.data.get("rows"), list) else []

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        if settlement_id and delay_data:
            status = str(delay_data.get("status") or "UNKNOWN").upper()
            expected_date = str(delay_data.get("expected_date") or "").strip()
            settled_at = str(delay_data.get("settled_at") or "").strip()
            delay_days = delay_data.get("delay_days")
            if delay_data.get("is_delayed"):
                findings.append(
                    f"Settlement {settlement_id} is {status} and {int(delay_days or 0)} day(s) past the expected date {expected_date}."
                    + (f" The latest settled timestamp is {settled_at}." if settled_at else " It still has no settled timestamp.")
                )
            elif expected_date:
                findings.append(
                    f"Settlement {settlement_id} is {status} with expected date {expected_date} and is not currently past due."
                )
        elif settlement_id:
            findings.append(f"Settlement {settlement_id} could not be confirmed for payout-delay review.")
        else:
            findings.append("No settlement id is pinned to this case yet, so payout-delay review is still merchant-scoped.")

        hold_reason_value = str(hold_data.get("hold_reason") or "").strip()
        if hold_reason_value:
            findings.append(f"Explicit hold reason on the settlement row is {hold_reason_value}.")

        reconciliation_rows = reconciliation_data.get("reconciliation") if isinstance(reconciliation_data.get("reconciliation"), list) else []
        if reconciliation_rows:
            top_reason = reconciliation_data.get("top_reason") if isinstance(reconciliation_data.get("top_reason"), dict) else reconciliation_rows[0]
            reason = str(top_reason.get("reason") or top_reason.get("status") or "unknown").strip()
            findings.append(
                f"Reconciliation context shows {int(reconciliation_data.get('total_rows') or 0)} row(s), "
                f"with top reason {reason}."
            )
        timeline_summary = str(timeline_data.get("summary") or "").strip()
        if timeline_summary:
            findings.append(timeline_summary)
        if nearby_rows:
            unresolved_count = sum(1 for row in nearby_rows if isinstance(row, dict) and _is_unresolved_settlement_row(row))
            findings.append(
                f"Nearby settlement context shows {len(nearby_rows)} settlement(s) in the anchor window, "
                f"with {unresolved_count} still unsettled."
            )

        next_best_action = _base_next_action(case_detail)
        if delay_data.get("is_delayed") and hold_reason_value:
            next_best_action = "Review the hold reason and escalate the delayed payout with the attached delay evidence."
        elif delay_data.get("is_delayed"):
            next_best_action = "Escalate the delayed payout and attach the payout timeline and reconciliation evidence."

        caveats: list[str] = []
        if not settlement_id:
            caveats.append("A settlement id should be pinned before escalating a payout delay.")
        if delay_context and delay_context.verification != VerificationStatus.verified:
            caveats.append("Expected-date or delay context could not be fully verified from the settlement row.")
        if reconciliation and reconciliation.verification != VerificationStatus.verified:
            caveats.append("Reconciliation context is incomplete in the current schema.")
        if timeline and timeline.verification != VerificationStatus.verified:
            caveats.append("Settlement timeline context is incomplete in the current schema.")
        if nearby_settlements and nearby_settlements.verification != VerificationStatus.verified:
            caveats.append("Nearby settlement context is incomplete in the current settlement list.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(delay_context, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(hold_reason, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(reconciliation, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(timeline, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(nearby_settlements, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Delayed payout case").strip()
        executive_summary = (
            f"{title} is currently {case_status} for {trade_name}. "
            + (
                f"The investigation is anchored on settlement {settlement_id}."
                if settlement_id
                else "The investigation is still using merchant-level settlement context."
            )
        )

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {
                "start_date": start_date,
                "end_date": end_date,
                "reason": window_reason,
            },
            "case_context": {
                "case_type": case_type,
                "settlement_id": settlement_id or None,
            },
        }


class ChargebackReviewAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        if not merchant_id:
            raise ValueError("case detail is missing merchant_id")
        case_type = _case_type(case_detail)
        chargeback_id = _extract_chargeback_id(case_detail) or ""
        start_date, end_date, window_reason = _derive_case_window(case_detail)

        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        customer_service = self._client.call_tool(
            "get_customer_service_context",
            {"merchant_id": merchant_id, "case_id": str(work_item.get("case_id") or "")},
        )
        summary = self._client.call_tool(
            "get_chargeback_summary",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date},
        )
        chargebacks = self._client.call_tool(
            "list_chargebacks",
            {
                "merchant_id": merchant_id,
                "start_date": start_date,
                "end_date": end_date,
                "status": "all",
                "limit": 10,
            },
        )
        chargeback_detail: ToolEnvelope | None = None
        if chargeback_id:
            chargeback_detail = self._client.call_tool(
                "get_chargeback_detail",
                {"merchant_id": merchant_id, "chargeback_id": chargeback_id},
            )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        service_data = customer_service.data if isinstance(customer_service.data, dict) else {}
        summary_data = summary.data if isinstance(summary.data, dict) else {}
        rows = chargebacks.data.get("rows") if isinstance(chargebacks.data.get("rows"), list) else []
        detail_row = chargeback_detail.data.get("chargeback") if chargeback_detail and isinstance(chargeback_detail.data.get("chargeback"), dict) else {}

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        findings.append(
            f"Chargeback window {start_date} to {end_date} shows {int(summary_data.get('chargebacks_count') or 0)} total chargeback(s), "
            f"{int(summary_data.get('open_chargebacks_count') or 0)} open, and "
            f"{int(summary_data.get('overdue_chargebacks_count') or 0)} overdue."
        )
        chargeback_amount = _format_money(summary_data.get("chargebacks_amount"))
        if chargeback_amount:
            findings.append(f"Chargeback exposure in the active window is {chargeback_amount}.")

        top_reason = summary_data.get("top_reason") if isinstance(summary_data.get("top_reason"), dict) else {}
        if top_reason:
            findings.append(
                f"Top chargeback reason in this window is {top_reason.get('reason_code')} across "
                f"{int(top_reason.get('count') or 0)} row(s)."
            )
        preferred_channel = str(service_data.get("preferred_channel") or "").strip()
        if preferred_channel:
            findings.append(f"Preferred merchant contact channel for follow-up is {preferred_channel}.")
        open_escalation_count = int(service_data.get("open_escalation_count") or 0)
        if open_escalation_count > 0:
            findings.append(f"There are {open_escalation_count} open support escalation(s) already tied to this customer-service context.")

        if detail_row:
            amount = _format_money(detail_row.get("amount_rupees"))
            findings.append(
                f"Pinned chargeback {chargeback_id} is {str(detail_row.get('status') or 'UNKNOWN').upper()}"
                + (f" for {amount}" if amount else "")
                + (f", due by {detail_row.get('due_by')}" if detail_row.get("due_by") else "")
                + "."
            )
            if detail_row.get("reason_code"):
                findings.append(f"Recorded reason code is {detail_row.get('reason_code')}.")
        elif chargeback_id:
            findings.append(f"Chargeback {chargeback_id} could not be confirmed from the current dispute tables.")
        elif rows:
            first_row = rows[0]
            findings.append(
                f"Earliest listed chargeback is {first_row.get('chargeback_id')} with status "
                f"{str(first_row.get('status') or 'UNKNOWN').upper()}."
            )
        else:
            findings.append("No chargeback rows are currently available for detailed review.")

        next_best_action = _base_next_action(case_detail)
        if int(summary_data.get("overdue_chargebacks_count") or 0) > 0:
            next_best_action = "Prioritize the overdue chargeback response and attach the dispute evidence bundle."
        elif open_escalation_count > 0:
            next_best_action = "Send a merchant update and keep the active escalation under review while the dispute response is prepared."
        elif chargeback_id:
            next_best_action = "Review the pinned chargeback deadline and prepare the next response package."

        caveats: list[str] = []
        if chargeback_detail and chargeback_detail.verification != VerificationStatus.verified:
            caveats.append("The pinned chargeback could not be fully confirmed from the dispute tables.")
        if summary.verification != VerificationStatus.verified or chargebacks.verification != VerificationStatus.verified:
            caveats.append("Dispute context is incomplete in the current schema and should be confirmed before escalation.")
        if customer_service.verification != VerificationStatus.verified:
            caveats.append("Merchant contact and support-follow-up context is incomplete for this case.")
        if not chargeback_id:
            caveats.append("A pinned chargeback id would make the review more specific.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        tool_calls.append(_tool_call_entry(customer_service))
        evidence_ids.extend(str(item) for item in customer_service.evidence_ids)
        notes.extend(str(item) for item in customer_service.notes)
        _append_envelope(summary, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(chargebacks, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(chargeback_detail, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Chargeback review").strip()
        executive_summary = f"{title} is currently {case_status} for {trade_name}. The review is focused on chargeback exposure, response readiness, and merchant follow-up."

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {"start_date": start_date, "end_date": end_date, "reason": window_reason},
            "case_context": {
                "case_type": case_type,
                "chargeback_id": chargeback_id or None,
            },
        }


class RefundExceptionAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        if not merchant_id:
            raise ValueError("case detail is missing merchant_id")
        case_type = _case_type(case_detail)
        refund_id = _extract_refund_id(case_detail) or ""
        start_date, end_date, window_reason = _derive_case_window(case_detail)

        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        customer_service = self._client.call_tool(
            "get_customer_service_context",
            {"merchant_id": merchant_id, "case_id": str(work_item.get("case_id") or "")},
        )
        summary = self._client.call_tool(
            "get_refund_summary",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date},
        )
        refunds = self._client.call_tool(
            "list_refunds",
            {
                "merchant_id": merchant_id,
                "start_date": start_date,
                "end_date": end_date,
                "limit": 10,
            },
        )
        refund_detail: ToolEnvelope | None = None
        if refund_id:
            refund_detail = self._client.call_tool(
                "get_refund_detail",
                {"merchant_id": merchant_id, "refund_id": refund_id},
            )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        service_data = customer_service.data if isinstance(customer_service.data, dict) else {}
        summary_data = summary.data if isinstance(summary.data, dict) else {}
        rows = refunds.data.get("rows") if isinstance(refunds.data.get("rows"), list) else []
        detail_row = refund_detail.data.get("refund") if refund_detail and isinstance(refund_detail.data.get("refund"), dict) else {}

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        findings.append(
            f"Refund window {start_date} to {end_date} shows {int(summary_data.get('refunds_count') or 0)} refund row(s)"
            + (f" totaling {_format_money(summary_data.get('refunds_amount'))}." if _format_money(summary_data.get("refunds_amount")) else ".")
        )
        latest_refund = summary_data.get("latest_refund") if isinstance(summary_data.get("latest_refund"), dict) else {}
        if latest_refund:
            findings.append(
                f"Latest refund in the active window is {latest_refund.get('refund_id')} with status "
                f"{str(latest_refund.get('status') or 'UNKNOWN').upper()}."
            )
        preferred_channel = str(service_data.get("preferred_channel") or "").strip()
        if preferred_channel:
            findings.append(f"Preferred merchant contact channel for follow-up is {preferred_channel}.")
        open_escalation_count = int(service_data.get("open_escalation_count") or 0)
        if open_escalation_count > 0:
            findings.append(f"There are {open_escalation_count} open support escalation(s) already tied to this customer-service context.")

        if detail_row:
            amount = _format_money(detail_row.get("amount_rupees"))
            findings.append(
                f"Pinned refund {refund_id} is {str(detail_row.get('status') or 'UNKNOWN').upper()}"
                + (f" for {amount}" if amount else "")
                + (f", created at {detail_row.get('created_at')}" if detail_row.get("created_at") else "")
                + "."
            )
        elif refund_id:
            findings.append(f"Refund {refund_id} could not be confirmed from the current refund tables.")
        elif rows:
            findings.append(f"Most recent refund row is {rows[0].get('refund_id')} for follow-up.")
        else:
            findings.append("No refund rows are currently available for detailed review.")

        next_best_action = _base_next_action(case_detail)
        if refund_id:
            next_best_action = "Review the pinned refund state and confirm whether merchant follow-up is required."
            if open_escalation_count > 0:
                next_best_action = "Send a merchant update and keep the active escalation under review while the refund status is confirmed."
        elif int(summary_data.get("refunds_count") or 0) > 0:
            next_best_action = "Review the latest refund rows and confirm whether the exception is isolated or part of a broader pattern."

        caveats: list[str] = []
        if refund_detail and refund_detail.verification != VerificationStatus.verified:
            caveats.append("The pinned refund could not be fully confirmed from the refund tables.")
        if summary.verification != VerificationStatus.verified or refunds.verification != VerificationStatus.verified:
            caveats.append("Refund context is incomplete in the current schema and should be confirmed before escalation.")
        if customer_service.verification != VerificationStatus.verified:
            caveats.append("Merchant contact and support-follow-up context is incomplete for this case.")
        if not refund_id:
            caveats.append("A pinned refund id would make the review more specific.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        tool_calls.append(_tool_call_entry(customer_service))
        evidence_ids.extend(str(item) for item in customer_service.evidence_ids)
        notes.extend(str(item) for item in customer_service.notes)
        _append_envelope(summary, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(refunds, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(refund_detail, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Refund exception").strip()
        executive_summary = f"{title} is currently {case_status} for {trade_name}. The review is focused on refund activity, merchant follow-up, and the next support-safe update."

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {"start_date": start_date, "end_date": end_date, "reason": window_reason},
            "case_context": {
                "case_type": case_type,
                "refund_id": refund_id or None,
            },
        }


class PaymentsExceptionAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        if not merchant_id:
            raise ValueError("case detail is missing merchant_id")

        case_type = _case_type(case_detail)
        terminal_id = _extract_terminal_id(case_detail) or ""
        tx_id = _extract_transaction_id(case_detail) or ""
        start_date, end_date, window_reason = _derive_case_window(case_detail)

        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        payment_mode_mix = self._client.call_tool(
            "get_payment_mode_mix",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date, "limit": 10},
        )
        recent_arguments: dict[str, Any] = {
            "merchant_id": merchant_id,
            "start_date": start_date,
            "end_date": end_date,
            "status": "FAILURE",
            "limit": 10,
        }
        if terminal_id:
            recent_arguments["terminal_id"] = terminal_id
        recent_transactions = self._client.call_tool("get_recent_transactions", recent_arguments)

        transaction_detail: ToolEnvelope | None = None
        if tx_id:
            transaction_detail = self._client.call_tool(
                "get_transaction_detail",
                {"merchant_id": merchant_id, "tx_id": tx_id},
            )

        terminal_profile: ToolEnvelope | None = None
        terminal_health: ToolEnvelope | None = None
        terminal_failures: ToolEnvelope | None = None
        if terminal_id:
            terminal_profile = self._client.call_tool(
                "get_terminal_profile",
                {"merchant_id": merchant_id, "terminal_id": terminal_id},
            )
            terminal_health = self._client.call_tool(
                "get_terminal_health_summary",
                {
                    "merchant_id": merchant_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "terminal_id": terminal_id,
                    "group_by": "tid",
                    "limit": 5,
                },
            )
            terminal_failures = self._client.call_tool(
                "get_terminal_failure_breakdown",
                {
                    "merchant_id": merchant_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "terminal_id": terminal_id,
                    "dimension": "response_code",
                    "limit": 5,
                },
            )

        mix_data = payment_mode_mix.data if isinstance(payment_mode_mix.data, dict) else {}
        mix_rows = [dict(item) for item in (mix_data.get("rows") or []) if isinstance(item, dict)]
        recent_rows = [dict(item) for item in (recent_transactions.data.get("rows") or []) if isinstance(item, dict)]
        terminal_profile_data = terminal_profile.data if terminal_profile and isinstance(terminal_profile.data, dict) else {}
        terminal_data = terminal_profile_data.get("terminal") if isinstance(terminal_profile_data.get("terminal"), dict) else {}
        latest_health = terminal_profile_data.get("latest_health") if isinstance(terminal_profile_data.get("latest_health"), dict) else {}
        tx_summary = terminal_profile_data.get("tx_summary") if isinstance(terminal_profile_data.get("tx_summary"), dict) else {}
        health_rows = [dict(item) for item in (terminal_health.data.get("rows") or [])] if terminal_health and isinstance(terminal_health.data, dict) else []
        terminal_failure_rows = [dict(item) for item in (terminal_failures.data.get("breakdown") or [])] if terminal_failures and isinstance(terminal_failures.data, dict) else []
        detail_row = transaction_detail.data.get("transaction") if transaction_detail and isinstance(transaction_detail.data.get("transaction"), dict) else {}
        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()

        top_mode = mix_rows[0] if mix_rows else {}
        highest_failure_share = max(mix_rows, key=lambda row: float(row.get("failure_share_pct") or 0.0), default={})
        latest_failed_tx = recent_rows[0] if recent_rows else {}
        terminal_health_row = health_rows[0] if health_rows else {}
        terminal_failure_row = terminal_failure_rows[0] if terminal_failure_rows else {}

        knowledge_query_parts = [
            str(work_item.get("title") or "").strip(),
            str(work_item.get("summary") or "").strip(),
            prompt or "",
            str(top_mode.get("payment_mode") or "").strip(),
            str(terminal_failure_row.get("driver") or "").strip(),
        ]
        if terminal_id:
            knowledge_query_parts.append("terminal failures low network retries")
        else:
            knowledge_query_parts.append("payment failures smart routing retries")
        knowledge_query = " ".join(part for part in knowledge_query_parts if part).strip()
        payments_knowledge = self._client.call_tool(
            "retrieve_payments_knowledge",
            {"merchant_id": merchant_id, "query": knowledge_query, "top_k": 3},
        )
        knowledge_results = [dict(item) for item in (payments_knowledge.data.get("results") or []) if isinstance(item, dict)]

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        if mix_rows:
            findings.append(
                f"Payments window {start_date} to {end_date} shows {int((mix_data.get('summary') or {}).get('attempts') or 0)} attempts. "
                f"Top mode is {top_mode.get('payment_mode')} at {float(top_mode.get('attempt_share_pct') or 0.0):.2f}% of attempts."
            )
            if highest_failure_share:
                findings.append(
                    f"{highest_failure_share.get('payment_mode')} accounts for {float(highest_failure_share.get('failure_share_pct') or 0.0):.2f}% of failures "
                    f"with a {float(highest_failure_share.get('success_rate_pct') or 0.0):.2f}% success rate."
                )
        else:
            findings.append("No payment-mode mix rows were available in the active investigation window.")

        if latest_failed_tx:
            findings.append(
                f"{len(recent_rows)} recent failed transaction(s) matched the scoped review. Latest failure is "
                f"{latest_failed_tx.get('tx_id')} on terminal {latest_failed_tx.get('terminal_id') or 'N/A'} "
                f"with response code {latest_failed_tx.get('response_code') or 'UNKNOWN'}."
            )
        else:
            findings.append("No recent failed transaction rows matched the scoped review.")

        if detail_row:
            findings.append(
                f"Pinned transaction {tx_id} is {str(detail_row.get('status') or 'UNKNOWN').upper()} "
                f"on {detail_row.get('payment_mode') or 'UNKNOWN'}"
                + (f" for {_format_money(detail_row.get('amount_rupees'))}" if _format_money(detail_row.get("amount_rupees")) else "")
                + (f" with response code {detail_row.get('response_code') or 'UNKNOWN'}." if detail_row.get("response_code") is not None else ".")
            )

        if terminal_id:
            findings.append(
                f"Terminal {terminal_id} profile is present with {int(tx_summary.get('attempts') or 0)} observed transaction attempt(s)."
            )
            if terminal_health_row:
                findings.append(
                    f"Terminal health shows {float(terminal_health_row.get('low_network_pct') or 0.0):.2f}% low-network snapshots"
                    + (
                        f" and average battery {float(terminal_health_row.get('avg_battery_pct') or 0.0):.2f}%."
                        if terminal_health_row.get("avg_battery_pct") is not None
                        else "."
                    )
                )
            elif latest_health:
                findings.append(
                    f"Latest terminal health snapshot is from {latest_health.get('captured_at')}."
                )
            if terminal_failure_row:
                findings.append(
                    f"Terminal-linked failure breakdown is led by response code {terminal_failure_row.get('driver')} "
                    f"with {int(terminal_failure_row.get('failed_txns') or 0)} failure(s)."
                )
            elif terminal_data:
                findings.append("Terminal profile is present, but no terminal-specific failed transactions were found in the active window.")

        if knowledge_results:
            findings.append(
                f"Relevant payments guidance is available in {knowledge_results[0].get('title')} "
                f"({knowledge_results[0].get('source_path')})."
            )

        next_best_action = _base_next_action(case_detail)
        low_network_pct = float(terminal_health_row.get("low_network_pct") or 0.0) if terminal_health_row else 0.0
        avg_battery_pct = float(terminal_health_row.get("avg_battery_pct") or 0.0) if terminal_health_row and terminal_health_row.get("avg_battery_pct") is not None else None
        if terminal_id and (low_network_pct >= 50.0 or (avg_battery_pct is not None and avg_battery_pct <= 25.0)):
            next_best_action = f"Inspect terminal {terminal_id} for connectivity or battery instability before treating the failures as issuer-side."
        elif terminal_failure_row:
            next_best_action = (
                f"Review the {terminal_failure_row.get('driver')} response-code cluster on terminal {terminal_id} "
                "and confirm whether routing or field diagnostics are needed."
            )
        elif highest_failure_share:
            next_best_action = (
                f"Investigate why {highest_failure_share.get('payment_mode')} is over-indexing on failures and compare routing or checkout behavior."
            )

        caveats: list[str] = []
        if payment_mode_mix.verification != VerificationStatus.verified:
            caveats.append("Payment-mode skew could not be fully verified from the current transaction schema.")
        if recent_transactions.verification != VerificationStatus.verified:
            caveats.append("Recent transaction evidence is incomplete for the requested scope.")
        if transaction_detail and transaction_detail.verification != VerificationStatus.verified:
            caveats.append("The pinned transaction could not be fully confirmed from the transaction fact table.")
        if terminal_profile and terminal_profile.verification != VerificationStatus.verified:
            caveats.append("Terminal profile context is incomplete for the requested terminal.")
        if terminal_health and terminal_health.verification != VerificationStatus.verified:
            caveats.append("Terminal health snapshots are incomplete for the active window.")
        if terminal_failures and terminal_failures.verification != VerificationStatus.verified:
            caveats.append("Terminal-linked failure attribution is incomplete for the active window.")
        if not terminal_id:
            caveats.append("A pinned terminal id would make terminal-linked diagnosis more specific.")
        if not tx_id:
            caveats.append("A pinned transaction id would make the RCA more specific.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(payment_mode_mix, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(recent_transactions, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(transaction_detail, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(terminal_profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(terminal_health, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(terminal_failures, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        if payments_knowledge is not None:
            tool_calls.append(_tool_call_entry(payments_knowledge))
            evidence_ids.extend(str(item) for item in payments_knowledge.evidence_ids)
            notes.extend(str(item) for item in payments_knowledge.notes)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Payments exception").strip()
        executive_summary = (
            f"{title} is currently {case_status} for {trade_name}. "
            "The review is focused on payment-mode skew, recent failed transactions, and terminal-linked failure context."
        )

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {"start_date": start_date, "end_date": end_date, "reason": window_reason},
            "case_context": {
                "case_type": case_type,
                "terminal_id": terminal_id or None,
                "tx_id": tx_id or None,
            },
        }


class MerchantSupportCaseAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            raise ValueError("case detail is missing merchant or case identity")
        case_type = _case_type(case_detail)
        chargeback_id = _extract_chargeback_id(case_detail) or ""
        refund_id = _extract_refund_id(case_detail) or ""

        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        support_history = self._client.call_tool(
            "get_support_case_history",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        contact_context = self._client.call_tool(
            "get_contact_and_escalation_context",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        customer_service = self._client.call_tool(
            "get_customer_service_context",
            {"merchant_id": merchant_id, "case_id": case_id},
        )

        chargeback_detail: ToolEnvelope | None = None
        refund_detail: ToolEnvelope | None = None
        if chargeback_id:
            chargeback_detail = self._client.call_tool(
                "get_chargeback_detail",
                {"merchant_id": merchant_id, "chargeback_id": chargeback_id},
            )
        if refund_id:
            refund_detail = self._client.call_tool(
                "get_refund_detail",
                {"merchant_id": merchant_id, "refund_id": refund_id},
            )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        history_data = support_history.data if isinstance(support_history.data, dict) else {}
        contact_data = contact_context.data if isinstance(contact_context.data, dict) else {}
        service_data = customer_service.data if isinstance(customer_service.data, dict) else {}
        recent_cases = history_data.get("recent_cases") if isinstance(history_data.get("recent_cases"), list) else []
        contacts = contact_data.get("contacts") if isinstance(contact_data.get("contacts"), list) else []
        escalations = contact_data.get("escalations") if isinstance(contact_data.get("escalations"), list) else []
        preferred_channel = str(service_data.get("preferred_channel") or "").strip()
        ticket_reference = str(service_data.get("ticket_reference") or contact_data.get("ticket_id") or work_item.get("source_ref") or "").strip()
        chargeback_row = (
            chargeback_detail.data.get("chargeback")
            if chargeback_detail and isinstance(chargeback_detail.data.get("chargeback"), dict)
            else {}
        )
        refund_row = (
            refund_detail.data.get("refund")
            if refund_detail and isinstance(refund_detail.data.get("refund"), dict)
            else {}
        )

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        findings.append(
            f"Support case {ticket_reference or case_id} is {str(work_item.get('status') or 'OPEN').replace('_', ' ').lower()}."
        )
        if recent_cases:
            latest_related = recent_cases[0] if isinstance(recent_cases[0], dict) else {}
            findings.append(
                f"Related support history shows {int(history_data.get('related_case_count') or len(recent_cases))} recent case(s), "
                f"with latest item {latest_related.get('ticket_id') or latest_related.get('case_id') or 'unknown'} "
                f"in status {str(latest_related.get('status') or 'UNKNOWN').upper()}."
            )
        else:
            findings.append("No recent related support history was found beyond the current case.")
        if preferred_channel:
            findings.append(f"Preferred merchant contact channel is {preferred_channel}.")
        if contacts:
            findings.append(
                f"{len(contacts)} merchant contact record(s) are available for this case context."
            )
        if escalations:
            findings.append(
                f"{len(escalations)} escalation item(s) are currently linked to this support context."
            )
        if chargeback_row:
            findings.append(
                f"Pinned chargeback {chargeback_id} is {str(chargeback_row.get('status') or 'UNKNOWN').upper()}"
                + (f" and due by {chargeback_row.get('due_by')}" if chargeback_row.get("due_by") else "")
                + "."
            )
        elif chargeback_id:
            findings.append(f"Pinned chargeback {chargeback_id} could not be confirmed from the current dispute tables.")
        if refund_row:
            findings.append(
                f"Pinned refund {refund_id} is {str(refund_row.get('status') or 'UNKNOWN').upper()}"
                + (f" and was created at {refund_row.get('created_at')}" if refund_row.get("created_at") else "")
                + "."
            )
        elif refund_id:
            findings.append(f"Pinned refund {refund_id} could not be confirmed from the current refund tables.")

        next_best_action = _base_next_action(case_detail)
        if int(service_data.get("open_escalation_count") or 0) > 0 and preferred_channel:
            next_best_action = f"Send the next merchant update on {preferred_channel} and keep the current escalation under review until the owning team replies."
        elif chargeback_id or refund_id:
            next_best_action = "Send a concise merchant update with the current dispute or refund status and confirm the next review checkpoint."
        elif preferred_channel:
            next_best_action = f"Send the next merchant-safe update on {preferred_channel} and capture any new reply on the current case."

        caveats: list[str] = []
        if support_history.verification != VerificationStatus.verified:
            caveats.append("Related support history is incomplete and may be using seeded fixture context.")
        if contact_context.verification != VerificationStatus.verified:
            caveats.append("Contact or escalation context is incomplete and should be confirmed before external outreach.")
        if customer_service.verification != VerificationStatus.verified:
            caveats.append("Customer-service context is incomplete for this case.")
        if chargeback_detail and chargeback_detail.verification != VerificationStatus.verified:
            caveats.append("The pinned chargeback could not be fully confirmed from the dispute tables.")
        if refund_detail and refund_detail.verification != VerificationStatus.verified:
            caveats.append("The pinned refund could not be fully confirmed from the refund tables.")
        if not preferred_channel:
            caveats.append("A preferred merchant contact channel would make the next update safer to send.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(support_history, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(contact_context, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(customer_service, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(chargeback_detail, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(refund_detail, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Merchant support case").strip()
        executive_summary = (
            f"{title} is currently {case_status} for {trade_name}. "
            "The review is focused on merchant support context and the next safe customer update."
        )

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {"start_date": "", "end_date": "", "reason": "support_case_context"},
            "case_context": {
                "case_type": case_type,
                "chargeback_id": chargeback_id or None,
                "refund_id": refund_id or None,
                "ticket_reference": ticket_reference or None,
            },
        }


def _case_text_blob(case_detail: dict[str, Any], prompt: str | None = None) -> str:
    work_item = _work_item(case_detail)
    source_payload = _source_payload(case_detail)
    text_parts = [
        prompt or "",
        work_item.get("title") or "",
        work_item.get("summary") or "",
        work_item.get("source_ref") or "",
        source_payload.get("title") or "",
        source_payload.get("summary") or "",
        source_payload.get("body") or "",
        source_payload.get("source_ref") or "",
    ]
    text_parts.extend(str(item) for item in (work_item.get("evidence_ids") or []) if item)
    return " ".join(str(item).strip().lower() for item in text_parts if str(item or "").strip())


def _signal_items(envelope: ToolEnvelope | None, key: str) -> list[dict[str, Any]]:
    if envelope is None or not isinstance(envelope.data, dict):
        return []
    value = envelope.data.get(key)
    return [dict(item) for item in value] if isinstance(value, list) else []


def _has_high_signal(items: list[dict[str, Any]]) -> bool:
    return any(str(item.get("severity") or "").strip().lower() in {"high", "critical"} for item in items)


def _needs_screening_context(case_detail: dict[str, Any], prompt: str | None, risk_band: str) -> bool:
    if _case_type(case_detail) in AML_CASE_TYPES:
        return True
    if str(risk_band or "").strip().upper() in {"HIGH", "WATCHLIST", "AT RISK"}:
        return True
    text_blob = _case_text_blob(case_detail, prompt)
    return any(token in text_blob for token in AML_TRIGGER_TOKENS)


def _screening_requires_review(results: list[dict[str, Any]]) -> bool:
    review_states = {"needs_review", "potential_match", "positive_match", "manual_review"}
    return any(str(item.get("status") or "").strip().lower() in review_states for item in results)


class RiskTriageAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            raise ValueError("case detail is missing merchant or case identity")

        case_type = _case_type(case_detail)
        start_date, end_date, window_reason = _derive_case_window(case_detail)
        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        risk_profile = self._client.call_tool("get_risk_profile", {"merchant_id": merchant_id})
        kyc_status = self._client.call_tool("get_kyc_status", {"merchant_id": merchant_id})
        velocity = self._client.call_tool(
            "get_velocity_anomalies",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date},
        )
        dispute_signals = self._client.call_tool(
            "get_dispute_risk_signals",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date},
        )
        policy = self._client.call_tool(
            "get_policy_rule_explanation",
            {"merchant_id": merchant_id, "case_id": case_id},
        )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        risk_data = risk_profile.data.get("risk_profile") if isinstance(risk_profile.data.get("risk_profile"), dict) else {}
        kyc_data = kyc_status.data.get("kyc") if isinstance(kyc_status.data.get("kyc"), dict) else {}
        policy_data = policy.data if isinstance(policy.data, dict) else {}
        velocity_data = velocity.data if isinstance(velocity.data, dict) else {}
        dispute_data = dispute_signals.data if isinstance(dispute_signals.data, dict) else {}

        risk_band = str(risk_data.get("band") or "").strip()
        should_pull_screening = _needs_screening_context(case_detail, prompt, risk_band)
        watchlist_hits: ToolEnvelope | None = None
        compliance_guidance: ToolEnvelope | None = None
        if should_pull_screening:
            watchlist_hits = self._client.call_tool("get_watchlist_hits", {"merchant_id": merchant_id})
            compliance_guidance = self._client.call_tool(
                "retrieve_compliance_guidance",
                {"merchant_id": merchant_id, "topic": "merchant_screening"},
            )

        watchlist_data = watchlist_hits.data if watchlist_hits and isinstance(watchlist_hits.data, dict) else {}
        guidance_data = compliance_guidance.data if compliance_guidance and isinstance(compliance_guidance.data, dict) else {}

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        if risk_data:
            findings.append(
                f"Latest merchant risk profile is {str(risk_data.get('band') or 'unknown').lower()} "
                f"with score {risk_data.get('score')}."
            )
        else:
            findings.append("No explicit merchant risk profile was found in the current risk tables.")
        if kyc_data:
            findings.append(
                f"KYC status is {str(kyc_data.get('status') or 'UNKNOWN').upper()}"
                + (f" with next expiry at {kyc_data.get('next_expiry_at')}." if kyc_data.get("next_expiry_at") else ".")
            )
        else:
            findings.append("No KYC snapshot was found in the current KYC tables.")

        velocity_summary = str(velocity_data.get("summary") or "").strip()
        if velocity_summary:
            findings.append(velocity_summary)
        dispute_summary = str(dispute_data.get("summary") or "").strip()
        if dispute_summary:
            findings.append(dispute_summary)

        if should_pull_screening:
            hit_count = int(watchlist_data.get("hit_count") or 0)
            if hit_count:
                findings.append(f"Fixture-backed watchlist context shows {hit_count} potential hit(s) that still need live confirmation.")
            else:
                findings.append("Fixture-backed watchlist context does not show a seeded hit for this merchant.")
            guidance = guidance_data.get("guidance") if isinstance(guidance_data.get("guidance"), list) else []
            if guidance:
                findings.append(f"Compliance guidance: {str(guidance[0]).strip()}")

        explanation_lines = policy_data.get("explanation_lines") if isinstance(policy_data.get("explanation_lines"), list) else []
        if explanation_lines:
            findings.append(str(explanation_lines[0]))

        velocity_items = _signal_items(velocity, "anomalies")
        dispute_items = _signal_items(dispute_signals, "signals")
        next_best_action = _base_next_action(case_detail)
        if watchlist_hits and int(watchlist_data.get("hit_count") or 0) > 0:
            next_best_action = "Do not clear the merchant from screening until live evidence is confirmed and an auditable reviewer decision is recorded."
        elif kyc_data and str(kyc_data.get("status") or "").upper() not in {"APPROVED", "ACTIVE"}:
            next_best_action = "Review the KYC state and decide whether the case should move to formal risk follow-up."
        elif str(risk_band or "").upper() in {"HIGH", "WATCHLIST", "AT RISK"} or _has_high_signal(velocity_items + dispute_items):
            next_best_action = "Prepare a formal risk follow-up package with the current velocity, dispute, and merchant evidence."

        caveats: list[str] = []
        if risk_profile.verification != VerificationStatus.verified:
            caveats.append("Risk-profile context is incomplete in the current risk tables.")
        if kyc_status.verification != VerificationStatus.verified:
            caveats.append("KYC context is incomplete in the current KYC tables.")
        if velocity.verification != VerificationStatus.verified:
            caveats.append("Velocity analysis is incomplete and should be confirmed before escalation.")
        if dispute_signals.verification != VerificationStatus.verified:
            caveats.append("Dispute-linked risk signals are incomplete and should be confirmed before escalation.")
        if watchlist_hits and watchlist_hits.verification != VerificationStatus.verified:
            caveats.append("Watchlist context is fixture-backed and not a live screening decision.")
        if compliance_guidance and compliance_guidance.verification != VerificationStatus.verified:
            caveats.append("Compliance guidance is fixture-backed and advisory only.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(risk_profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(kyc_status, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(velocity, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(dispute_signals, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(watchlist_hits, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(compliance_guidance, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(policy, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Risk review").strip()
        executive_summary = (
            f"{title} is currently {case_status} for {trade_name}. "
            "The review is focused on merchant risk, KYC state, and recent velocity or dispute signals."
        )

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {"start_date": start_date, "end_date": end_date, "reason": window_reason},
            "case_context": {"case_type": case_type},
        }


class AMLInvestigationAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            raise ValueError("case detail is missing merchant or case identity")

        case_type = _case_type(case_detail)
        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        aml_context = self._client.call_tool(
            "get_aml_case_context",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        watchlist_hits = self._client.call_tool("get_watchlist_hits", {"merchant_id": merchant_id})
        screening_results = self._client.call_tool("get_screening_results", {"merchant_id": merchant_id})
        compliance_guidance = self._client.call_tool(
            "retrieve_compliance_guidance",
            {"merchant_id": merchant_id, "topic": "aml_investigation"},
        )
        policy = self._client.call_tool(
            "get_policy_rule_explanation",
            {"merchant_id": merchant_id, "case_id": case_id},
        )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        aml_data = aml_context.data if isinstance(aml_context.data, dict) else {}
        watchlist_data = watchlist_hits.data if isinstance(watchlist_hits.data, dict) else {}
        screening_data = screening_results.data if isinstance(screening_results.data, dict) else {}
        guidance_data = compliance_guidance.data if isinstance(compliance_guidance.data, dict) else {}
        policy_data = policy.data if isinstance(policy.data, dict) else {}

        screening_rows = _signal_items(screening_results, "results")
        guidance_rows = guidance_data.get("guidance") if isinstance(guidance_data.get("guidance"), list) else []

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        findings.append(
            f"AML case context is {str(aml_data.get('status') or work_item.get('status') or 'OPEN').upper()} "
            f"with {int(aml_data.get('open_task_count') or 0)} open task(s)."
        )
        if int(watchlist_data.get("hit_count") or 0) > 0:
            findings.append(f"Fixture-backed watchlist context shows {int(watchlist_data.get('hit_count') or 0)} potential hit(s).")
        else:
            findings.append("Fixture-backed watchlist context does not show a seeded hit for this merchant.")
        if int(screening_data.get("results_count") or 0) > 0:
            findings.append(
                f"Fixture-backed screening results are currently {str(screening_data.get('overall_status') or 'needs_review').lower()} "
                f"across {int(screening_data.get('results_count') or 0)} result(s)."
            )
        else:
            findings.append("No seeded screening result is available for this merchant yet.")
        if guidance_rows:
            findings.append(f"Compliance guidance: {str(guidance_rows[0]).strip()}")

        explanation_lines = policy_data.get("explanation_lines") if isinstance(policy_data.get("explanation_lines"), list) else []
        if explanation_lines:
            findings.append(str(explanation_lines[0]))

        next_best_action = _base_next_action(case_detail)
        if int(watchlist_data.get("hit_count") or 0) > 0 or _screening_requires_review(screening_rows):
            next_best_action = "Keep the case in analyst review until live screening evidence is confirmed and the reviewer decision is recorded."
        elif guidance_rows:
            next_best_action = "Use the compliance guidance to complete the next analyst review step and attach the missing live evidence."

        caveats: list[str] = []
        if aml_context.verification != VerificationStatus.verified:
            caveats.append("AML case context is incomplete and should be confirmed from the current case state.")
        if watchlist_hits.verification != VerificationStatus.verified:
            caveats.append("Watchlist context is fixture-backed and does not count as live screening evidence.")
        if screening_results.verification != VerificationStatus.verified:
            caveats.append("Screening results are fixture-backed and do not count as a live clearance decision.")
        if compliance_guidance.verification != VerificationStatus.verified:
            caveats.append("Compliance guidance is fixture-backed and advisory only.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(aml_context, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(watchlist_hits, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(screening_results, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(compliance_guidance, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(policy, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "AML investigation").strip()
        executive_summary = (
            f"{title} is currently {case_status} for {trade_name}. "
            "The review is focused on screening evidence, AML case context, and compliance guidance."
        )

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {"start_date": "", "end_date": "", "reason": "aml_case_context"},
            "case_context": {"case_type": case_type},
        }


class ConnectorSupervisorAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            raise ValueError("case detail is missing merchant or case identity")

        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        timeline = self._client.call_tool(
            "get_case_timeline",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        connector_runs = self._client.call_tool(
            "list_connector_runs",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        sla_snapshot = self._client.call_tool(
            "get_sla_snapshot",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        api_health = self._client.call_tool(
            "get_api_health",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        monitoring_alerts = self._client.call_tool(
            "get_monitoring_alerts",
            {"merchant_id": merchant_id, "case_id": case_id, "limit": 5},
        )
        job_failures = self._client.call_tool(
            "get_job_failures",
            {"merchant_id": merchant_id, "case_id": case_id, "limit": 5},
        )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        connector_data = connector_runs.data if isinstance(connector_runs.data, dict) else {}
        timeline_data = timeline.data if isinstance(timeline.data, dict) else {}
        sla_data = sla_snapshot.data.get("sla") if isinstance(sla_snapshot.data.get("sla"), dict) else {}
        api_data = api_health.data if isinstance(api_health.data, dict) else {}
        alert_data = monitoring_alerts.data if isinstance(monitoring_alerts.data, dict) else {}
        job_data = job_failures.data if isinstance(job_failures.data, dict) else {}
        latest_run = connector_data.get("latest_run") if isinstance(connector_data.get("latest_run"), dict) else {}
        latest_probe = api_data.get("latest_probe") if isinstance(api_data.get("latest_probe"), dict) else {}
        alerts = [dict(item) for item in (alert_data.get("alerts") or []) if isinstance(item, dict)]
        failures = [dict(item) for item in (job_data.get("job_failures") or []) if isinstance(item, dict)]

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        findings.append(
            f"API health is {str(api_data.get('status') or 'unknown').upper()} "
            f"from {str(api_data.get('source') or 'internal_state').replace('_', ' ')} signals."
        )
        if latest_probe.get("http_status_code") is not None:
            findings.append(f"Latest API probe returned HTTP {latest_probe.get('http_status_code')}.")
        if int(connector_data.get("run_count") or 0) > 0:
            findings.append(
                f"Connector history shows {int(connector_data.get('run_count') or 0)} run(s), "
                f"with latest status {str(latest_run.get('status') or 'UNKNOWN').upper()}."
            )
            if latest_run.get("http_status_code"):
                findings.append(f"Latest connector HTTP status was {latest_run.get('http_status_code')}.")
            if latest_run.get("error_message"):
                findings.append(f"Latest connector error was: {latest_run.get('error_message')}.")
        else:
            findings.append("No connector runs have been recorded for this case yet.")

        if alerts:
            top_alert = alerts[0]
            findings.append(
                f"Top monitoring alert: {top_alert.get('summary')}"
                + (
                    f" ({str(top_alert.get('severity') or 'unknown').upper()})."
                    if top_alert.get("severity")
                    else "."
                )
            )
        if failures:
            findings.append(f"Latest job failure: {failures[0].get('summary')}")

        if sla_data:
            if sla_data.get("sla_breached"):
                findings.append("Case SLA is currently breached and needs immediate operator follow-up.")
            elif sla_data.get("sla_warning"):
                findings.append("Case is inside the SLA warning window and should be prioritized.")
            else:
                findings.append(
                    f"Case SLA target is {int(sla_data.get('target_hours') or 0)} hour(s)"
                    + (
                        f" with about {float(sla_data.get('hours_to_due') or 0.0):.1f} hour(s) remaining."
                        if sla_data.get("hours_to_due") is not None
                        else "."
                    )
                )

        for line in (timeline_data.get("summary_lines") or [])[:2]:
            findings.append(str(line))

        next_best_action = _base_next_action(case_detail)
        if bool(job_data.get("attention_required")) or bool(api_data.get("attention_required")):
            next_best_action = "Review the failed connector run and decide whether to retry or escalate manually."
        elif sla_data.get("sla_breached"):
            next_best_action = "Prioritize this case now because it is already past its SLA target."
        elif int(connector_data.get("run_count") or 0) == 0:
            next_best_action = "Confirm whether this case should dispatch to the connector or remain operator-only."

        caveats: list[str] = []
        if connector_runs.verification != VerificationStatus.verified:
            caveats.append("Connector run history is incomplete for this case.")
        if sla_snapshot.verification != VerificationStatus.verified:
            caveats.append("SLA state needs confirmation before using it for escalation priority.")
        if api_health.verification != VerificationStatus.verified:
            caveats.append("API health is fixture-backed because external monitoring is blocked.")
        if monitoring_alerts.verification != VerificationStatus.verified:
            caveats.append("Monitoring alerts are fixture-backed because external monitoring is blocked.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(timeline, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(connector_runs, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(sla_snapshot, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(api_health, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(monitoring_alerts, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(job_failures, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Connector follow-up").strip()
        executive_summary = f"{title} is currently {case_status} for {trade_name}. The review is focused on connector execution state and follow-up readiness."

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {"start_date": "", "end_date": "", "reason": "connector_run_history"},
            "case_context": {"case_type": _case_type(case_detail)},
        }


class IncidentResponseAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            raise ValueError("case detail is missing merchant or case identity")

        start_date, end_date, window_reason = _derive_case_window(case_detail)
        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        incident_context = self._client.call_tool(
            "get_incident_context",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        api_health = self._client.call_tool(
            "get_api_health",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        monitoring_alerts = self._client.call_tool(
            "get_monitoring_alerts",
            {"merchant_id": merchant_id, "case_id": case_id, "limit": 5},
        )
        job_failures = self._client.call_tool(
            "get_job_failures",
            {"merchant_id": merchant_id, "case_id": case_id, "limit": 5},
        )
        data_quality = self._client.call_tool(
            "get_data_quality_checks",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date},
        )
        policy = self._client.call_tool(
            "get_policy_rule_explanation",
            {"merchant_id": merchant_id, "case_id": case_id},
        )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id).strip()
        city = str(merchant.get("business_city") or "").strip()
        incident_data = incident_context.data if isinstance(incident_context.data, dict) else {}
        api_data = api_health.data if isinstance(api_health.data, dict) else {}
        alert_data = monitoring_alerts.data if isinstance(monitoring_alerts.data, dict) else {}
        job_data = job_failures.data if isinstance(job_failures.data, dict) else {}
        data_quality_data = data_quality.data if isinstance(data_quality.data, dict) else {}
        refresh_status = incident_data.get("refresh_status") if isinstance(incident_data.get("refresh_status"), dict) else {}
        connector_summary = incident_data.get("connector_summary") if isinstance(incident_data.get("connector_summary"), dict) else {}
        latest_run = connector_summary.get("latest_run") if isinstance(connector_summary.get("latest_run"), dict) else {}
        alerts = [dict(item) for item in (alert_data.get("alerts") or []) if isinstance(item, dict)]
        failures = [dict(item) for item in (job_data.get("job_failures") or []) if isinstance(item, dict)]
        dq_issues = [str(item) for item in (data_quality_data.get("issues") or []) if str(item or "").strip()]
        dq_metrics = data_quality_data.get("metrics") if isinstance(data_quality_data.get("metrics"), dict) else {}

        findings: list[str] = []
        findings.append(f"Merchant context: {trade_name}{f' in {city}' if city else ''}.")
        for line in (incident_data.get("summary_lines") or [])[:3]:
            findings.append(str(line))
        findings.append(
            f"API health is {str(api_data.get('status') or 'unknown').upper()} "
            f"from {str(api_data.get('source') or 'internal_state').replace('_', ' ')} signals."
        )
        if alerts:
            findings.append(f"Top monitoring alert: {alerts[0].get('summary')}")
        if failures:
            findings.append(f"Latest job failure: {failures[0].get('summary')}")
        if dq_issues:
            findings.append(
                f"Data quality checks found {len(dq_issues)} issue type(s): "
                + ", ".join(_format_issue_label(item) for item in dq_issues[:3])
                + "."
            )
        elif int(dq_metrics.get("total_rows") or 0) > 0:
            findings.append("No transaction data quality issues were detected in the current incident window.")
        if latest_run:
            findings.append(
                f"Latest connector state in incident context is {str(latest_run.get('status') or 'UNKNOWN').upper()}."
            )

        next_best_action = _base_next_action(case_detail)
        if dq_issues:
            next_best_action = "Fix the transaction data quality issues before treating the incident metrics as final."
        elif refresh_status.get("due"):
            next_best_action = "Review why the background refresh is overdue and decide whether to trigger or investigate it manually."
        elif bool(job_data.get("attention_required")) or bool(api_data.get("attention_required")):
            next_best_action = "Review the failed connector run and decide whether to retry or escalate manually."
        elif alerts:
            next_best_action = "Review the active monitoring alert and confirm whether to escalate or recover manually."

        caveats: list[str] = []
        if api_health.verification != VerificationStatus.verified:
            caveats.append("API health is fixture-backed because external monitoring is blocked.")
        if monitoring_alerts.verification != VerificationStatus.verified:
            caveats.append("Monitoring alerts are fixture-backed because external monitoring is blocked.")
        if not refresh_status:
            caveats.append("Background refresh schedule state is incomplete for this merchant.")
        if data_quality.verification != VerificationStatus.verified:
            caveats.append("Transaction data quality checks did not complete cleanly for this incident window.")

        verification_flags: list[VerificationStatus] = []
        tool_calls: list[dict[str, Any]] = []
        evidence_ids = [str(item) for item in (work_item.get("evidence_ids") or []) if str(item or "").strip()]
        notes: list[str] = []
        _append_envelope(profile, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(incident_context, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(api_health, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(monitoring_alerts, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(job_failures, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(data_quality, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)
        _append_envelope(policy, tool_calls=tool_calls, evidence_ids=evidence_ids, notes=notes, verification_flags=verification_flags)

        verification = VerificationStatus.verified
        if any(flag != VerificationStatus.verified for flag in verification_flags):
            verification = VerificationStatus.unverified

        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()
        title = str(work_item.get("title") or "Incident response").strip()
        executive_summary = f"{title} is currently {case_status} for {trade_name}. The review is focused on internal operational state and follow-up readiness."

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text(evidence_ids),
            "notes": _dedupe_text(notes),
            "window": {"start_date": start_date, "end_date": end_date, "reason": window_reason},
            "case_context": {"case_type": _case_type(case_detail)},
        }


class CaseNoteDraftAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def draft(self, *, case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            return {"status": "blocked", "reason": "Case detail is missing merchant or case identity."}
        envelope = self._client.call_tool(
            "draft_case_note",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        return envelope.data


class ApprovalReviewerAssistant:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def draft(self, *, case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            return {"status": "blocked", "reason": "Case detail is missing merchant or case identity."}
        envelope = self._client.call_tool(
            "draft_approval_request",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        return envelope.data


class MerchantUpdateDraftAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def draft(self, *, case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            return {"status": "blocked", "reason": "Case detail is missing merchant or case identity."}
        envelope = self._client.call_tool(
            "draft_merchant_update",
            {"merchant_id": merchant_id, "case_id": case_id},
        )
        return envelope.data


class SettlementOperatorNoteAgent:
    def draft(self, *, case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        context = summary.get("case_context") if isinstance(summary.get("case_context"), dict) else {}
        settlement_id = str(context.get("settlement_id") or "").strip()
        findings = summary.get("answer_sections", {}).get("key_findings") or []
        next_action = str(summary.get("answer_sections", {}).get("next_best_action") or "").strip()
        lines = [
            f"Operator review for case {work_item.get('case_id')}: {work_item.get('title') or 'Settlement case'}",
        ]
        if settlement_id:
            lines.append(f"Pinned settlement: {settlement_id}.")
        for finding in findings[:3]:
            lines.append(f"- {finding}")
        if next_action:
            lines.append(f"Recommended next action: {next_action}")
        return {
            "status": "ready",
            "body": "\n".join(lines),
        }


class SettlementApprovalDraftAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def _fallback_draft(self, *, case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        approval_state = _approval_state(case_detail)
        if str(work_item.get("status") or "").upper() in {"RESOLVED", "CLOSED"}:
            return {"status": "blocked", "reason": "Case is already resolved."}
        if str(approval_state.get("status") or "").lower() == "pending":
            return {"status": "blocked", "reason": "A settlement approval is already pending for this case."}

        context = summary.get("case_context") if isinstance(summary.get("case_context"), dict) else {}
        settlement_id = str(context.get("settlement_id") or "").strip()
        case_type = str(context.get("case_type") or "").strip().lower()
        if case_type in {"settlement_shortfall_review", "reconciliation_mismatch"}:
            action_type = "RECONCILIATION_REVIEW"
        elif case_type in {"processed_unsettled_payout", "delayed_payout_exception"}:
            action_type = "PAYOUT_DELAY_INTERVENTION"
        else:
            action_type = "SETTLEMENT_ESCALATION"

        next_action = str(summary.get("answer_sections", {}).get("next_best_action") or "Review the case for settlement follow-through.").strip()
        title = str(work_item.get("title") or work_item.get("case_id") or "Settlement case").strip()
        payload_summary = f"{title}" + (f" for settlement {settlement_id}" if settlement_id else "")
        return {
            "status": "ready",
            "action_type": action_type,
            "payload_summary": payload_summary,
            "payload": {
                "case_id": work_item.get("case_id"),
                "merchant_id": work_item.get("merchant_id"),
                "lane": work_item.get("lane"),
                "settlement_id": settlement_id or None,
                "evidence_ids": summary.get("evidence_ids") or work_item.get("evidence_ids") or [],
                "recommended_action": next_action,
                "case_title": title,
            },
            "approval_required": True,
            "approval_state": str(approval_state.get("status") or "not_requested").lower() or "not_requested",
            "dispatch_readiness": "approval_required",
            "downstream_target": "settlement_ops_core",
            "idempotency_expectation": "Connector idempotency is derived from approval_id + case_id + action_type after approval.",
        }

    def draft(self, *, case_detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
        work_item = _work_item(case_detail)
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        case_id = str(work_item.get("case_id") or "").strip()
        if not merchant_id or not case_id:
            return {"status": "blocked", "reason": "Case detail is missing merchant or case identity."}
        context = summary.get("case_context") if isinstance(summary.get("case_context"), dict) else {}
        settlement_id = str(context.get("settlement_id") or "").strip()
        case_type = str(context.get("case_type") or "").strip().lower()
        next_action = str(summary.get("answer_sections", {}).get("next_best_action") or "Review the case for settlement follow-through.").strip()
        title = str(work_item.get("title") or work_item.get("case_id") or "Settlement case").strip()
        tool_name = (
            "submit_reconciliation_review"
            if case_type in {"settlement_shortfall_review", "reconciliation_mismatch"}
            else "submit_settlement_intervention"
        )
        envelope = self._client.call_tool(
            tool_name,
            {
                "merchant_id": merchant_id,
                "case_id": case_id,
                "settlement_id": settlement_id or None,
                "evidence_ids": summary.get("evidence_ids") or work_item.get("evidence_ids") or [],
                "recommended_action": next_action,
                "payload_summary": f"{title}" + (f" for settlement {settlement_id}" if settlement_id else ""),
            },
        )
        data = envelope.data if isinstance(envelope.data, dict) else {}
        if envelope.status != ToolStatus.ok or str(data.get("reason") or "").strip() == "Ops case was not found for the requested case id.":
            return self._fallback_draft(case_detail=case_detail, summary=summary)
        if str(data.get("status") or "").lower() == "blocked":
            return {
                "status": "blocked",
                "reason": str(data.get("reason") or "Settlement approval wrapper is currently blocked.").strip(),
            }
        return {
            "status": "ready",
            "action_type": data.get("action_type"),
            "payload_summary": data.get("payload_summary"),
            "payload": data.get("payload"),
            "approval_required": bool(data.get("approval_required")),
            "approval_state": data.get("approval_state"),
            "dispatch_readiness": data.get("dispatch_readiness"),
            "downstream_target": data.get("downstream_target"),
            "idempotency_expectation": data.get("idempotency_expectation"),
        }


class BankOpsCaseCopilotRouter:
    def __init__(self, server: BankFoundryMCPServer):
        self._server = server

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        case_type = _case_type(case_detail)
        if case_type in SETTLEMENT_CASE_TYPES:
            approval_client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("settlement_approval_draft_agent"),
            )
            if case_type in {"settlement_shortfall_review", "reconciliation_mismatch"}:
                client = BankFoundryMCPClient(
                    self._server,
                    tool_filter=tool_filter_for_agent("reconciliation_investigation_agent"),
                )
                summary = ReconciliationInvestigationAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
                summary["agents"] = [
                    {"name": "reconciliation_investigation_agent", "purpose": "Investigate payout delta and reconciliation context for settlement cases."},
                    {"name": "settlement_operator_note_agent", "purpose": "Draft an operator note from the settlement findings."},
                    {"name": "settlement_approval_draft_agent", "purpose": "Draft the next approval payload for settlement follow-through."},
                ]
            elif case_type in {"processed_unsettled_payout", "delayed_payout_exception"}:
                client = BankFoundryMCPClient(
                    self._server,
                    tool_filter=tool_filter_for_agent("delayed_payout_agent"),
                )
                summary = DelayedPayoutAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
                summary["agents"] = [
                    {"name": "delayed_payout_agent", "purpose": "Investigate payout delay state, hold context, and reconciliation status."},
                    {"name": "settlement_operator_note_agent", "purpose": "Draft an operator note from the settlement findings."},
                    {"name": "settlement_approval_draft_agent", "purpose": "Draft the next approval payload for settlement follow-through."},
                ]
            else:
                client = BankFoundryMCPClient(
                    self._server,
                    tool_filter=tool_filter_for_agent("settlement_case_summary_agent"),
                )
                summary = SettlementCaseSummaryAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
                summary["agents"] = [
                    {"name": "settlement_case_summary_agent", "purpose": "Summarize the case using settlement-specific MCP tools."},
                    {"name": "settlement_operator_note_agent", "purpose": "Draft an operator note from the settlement findings."},
                    {"name": "settlement_approval_draft_agent", "purpose": "Draft the next approval payload for settlement follow-through."},
                ]
            summary["drafts"] = {
                "operator_note": SettlementOperatorNoteAgent().draft(case_detail=case_detail, summary=summary),
                "approval_request": SettlementApprovalDraftAgent(approval_client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        if case_type == "chargeback_review":
            client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("chargeback_review_agent"),
            )
            summary = ChargebackReviewAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
            summary["agents"] = [
                {"name": "chargeback_review_agent", "purpose": "Investigate chargeback exposure, deadlines, dispute readiness, and merchant follow-up context."},
                {"name": "case_note_draft_agent", "purpose": "Draft an internal case note from the current case state."},
                {"name": "approval_reviewer_assistant", "purpose": "Draft the next approval payload from the current case state."},
                {"name": "merchant_update_draft_agent", "purpose": "Draft the next merchant-facing support update from the current case state."},
            ]
            summary["drafts"] = {
                "operator_note": CaseNoteDraftAgent(client).draft(case_detail=case_detail, summary=summary),
                "approval_request": ApprovalReviewerAssistant(client).draft(case_detail=case_detail, summary=summary),
                "merchant_update": MerchantUpdateDraftAgent(client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        if case_type == "refund_exception":
            client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("refund_exception_agent"),
            )
            summary = RefundExceptionAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
            summary["agents"] = [
                {"name": "refund_exception_agent", "purpose": "Investigate refund volume, latest refund state, and merchant follow-up needs."},
                {"name": "case_note_draft_agent", "purpose": "Draft an internal case note from the current case state."},
                {"name": "approval_reviewer_assistant", "purpose": "Draft the next approval payload from the current case state."},
                {"name": "merchant_update_draft_agent", "purpose": "Draft the next merchant-facing support update from the current case state."},
            ]
            summary["drafts"] = {
                "operator_note": CaseNoteDraftAgent(client).draft(case_detail=case_detail, summary=summary),
                "approval_request": ApprovalReviewerAssistant(client).draft(case_detail=case_detail, summary=summary),
                "merchant_update": MerchantUpdateDraftAgent(client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        if case_type in PAYMENTS_CASE_TYPES:
            client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("payments_exception_agent"),
            )
            summary = PaymentsExceptionAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
            summary["agents"] = [
                {"name": "payments_exception_agent", "purpose": "Investigate payment-mode skew, recent failed transactions, and terminal-linked failure context."},
                {"name": "case_note_draft_agent", "purpose": "Draft an internal case note from the current case state."},
                {"name": "approval_reviewer_assistant", "purpose": "Draft the next approval payload from the current case state."},
            ]
            summary["drafts"] = {
                "operator_note": CaseNoteDraftAgent(client).draft(case_detail=case_detail, summary=summary),
                "approval_request": ApprovalReviewerAssistant(client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        if case_type in SUPPORT_CASE_TYPES or (_lane(case_detail) == "support" and case_type == "manual_ops_review"):
            client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("merchant_support_case_agent"),
            )
            summary = MerchantSupportCaseAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
            summary["agents"] = [
                {"name": "merchant_support_case_agent", "purpose": "Review support history, contact context, and the next safe merchant update."},
                {"name": "merchant_update_draft_agent", "purpose": "Draft the next merchant-facing support update from the current case state."},
            ]
            summary["drafts"] = {
                "merchant_update": MerchantUpdateDraftAgent(client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        if case_type in AML_CASE_TYPES:
            client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("aml_investigation_agent"),
            )
            summary = AMLInvestigationAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
            summary["agents"] = [
                {"name": "aml_investigation_agent", "purpose": "Review screening evidence, AML case context, and compliance guidance for AML-oriented cases."},
                {"name": "case_note_draft_agent", "purpose": "Draft an internal case note from the current case state."},
                {"name": "approval_reviewer_assistant", "purpose": "Draft the next approval payload from the current case state."},
            ]
            summary["drafts"] = {
                "operator_note": CaseNoteDraftAgent(client).draft(case_detail=case_detail, summary=summary),
                "approval_request": ApprovalReviewerAssistant(client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        if case_type in RISK_CASE_TYPES:
            client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("risk_triage_agent"),
            )
            summary = RiskTriageAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
            summary["agents"] = [
                {"name": "risk_triage_agent", "purpose": "Review merchant risk profile, KYC state, and current risk signals for risk-oriented cases."},
                {"name": "case_note_draft_agent", "purpose": "Draft an internal case note from the current case state."},
                {"name": "approval_reviewer_assistant", "purpose": "Draft the next approval payload from the current case state."},
            ]
            summary["drafts"] = {
                "operator_note": CaseNoteDraftAgent(client).draft(case_detail=case_detail, summary=summary),
                "approval_request": ApprovalReviewerAssistant(client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        if case_type in CONNECTOR_CASE_TYPES:
            client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("connector_supervisor_agent"),
            )
            summary = ConnectorSupervisorAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
            summary["agents"] = [
                {"name": "connector_supervisor_agent", "purpose": "Review connector execution history and operator follow-up needs."},
                {"name": "case_note_draft_agent", "purpose": "Draft an internal case note from the current case state."},
                {"name": "approval_reviewer_assistant", "purpose": "Draft the next approval payload from the current case state."},
            ]
            summary["drafts"] = {
                "operator_note": CaseNoteDraftAgent(client).draft(case_detail=case_detail, summary=summary),
                "approval_request": ApprovalReviewerAssistant(client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        if case_type in INCIDENT_CASE_TYPES:
            client = BankFoundryMCPClient(
                self._server,
                tool_filter=tool_filter_for_agent("incident_response_agent"),
            )
            summary = IncidentResponseAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
            summary["agents"] = [
                {"name": "incident_response_agent", "purpose": "Review background refresh, connector, and case-timeline state for ops incidents."},
                {"name": "case_note_draft_agent", "purpose": "Draft an internal case note from the current case state."},
                {"name": "approval_reviewer_assistant", "purpose": "Draft the next approval payload from the current case state."},
            ]
            summary["drafts"] = {
                "operator_note": CaseNoteDraftAgent(client).draft(case_detail=case_detail, summary=summary),
                "approval_request": ApprovalReviewerAssistant(client).draft(case_detail=case_detail, summary=summary),
            }
            summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
            return summary

        client = BankFoundryMCPClient(
            self._server,
            tool_filter=tool_filter_for_agent("generic_ops_case_copilot_agent"),
        )
        summary = OpsCaseCopilotMCPAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)
        summary["agents"] = [
            {"name": "generic_ops_case_copilot_agent", "purpose": "Summarize non-settlement cases with the base MCP toolset."},
        ]
        summary["drafts"] = {
            "operator_note": _draft_generic_operator_note(case_detail, summary),
            "approval_request": _draft_generic_approval_request(case_detail, summary),
        }
        summary["memory_snapshot"] = _build_memory_snapshot(case_detail, summary)
        return summary


def build_bank_ops_case_copilot_summary(
    engine: Any,
    case_detail: dict[str, Any],
    *,
    prompt: str | None = None,
) -> dict[str, Any]:
    server = BankFoundryMCPServer(engine)
    return BankOpsCaseCopilotRouter(server).summarize_case(case_detail=case_detail, prompt=prompt)

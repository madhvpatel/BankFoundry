from __future__ import annotations

from typing import Any

from app.data.evidence import normalize_evidence_ids
from app.data.ops import repository as ops_repository
from app.data.proactive import repository as proactive_repository
from app.ontology.ops import case_type_from_source, runbook_for_case_type


SETTLEMENT_CASE_TYPES = {
    "held_settlement",
    "processed_unsettled_payout",
    "settlement_shortfall_review",
    "reconciliation_mismatch",
    "delayed_payout_exception",
}


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _priority_from_card(card: dict[str, Any]) -> str:
    impact = float(card.get("impact_rupees") or 0.0)
    title = str(card.get("title") or "").strip().lower()
    if impact >= 100000:
        return "critical"
    if impact >= 10000 or any(token in title for token in ("held", "delay", "shortfall", "reconciliation")):
        return "high"
    return "medium"


def _is_settlement_ops_card(card: dict[str, Any]) -> bool:
    lane = str(card.get("lane") or "").strip().lower()
    if lane != "operations":
        return False
    case_type = case_type_from_source(
        "proactive_card",
        {
            "title": card.get("title"),
            "source_ref": card.get("dedupe_key"),
        },
    )
    return case_type in SETTLEMENT_CASE_TYPES


def _seed_tasks(case_type: str, priority: str) -> list[dict[str, Any]]:
    runbook = runbook_for_case_type(case_type)
    return [
        {
            "title": step.title,
            "description": step.description,
            "priority": priority,
            "metadata": {"step_id": step.step_id, "action_type": step.action_type},
        }
        for step in runbook.steps
    ]


def auto_intake_settlement_ops_cases(
    engine: Any,
    merchant_id: str,
    *,
    cards: list[dict[str, Any]],
    actor_id: str = "proactive_monitor",
    actor_role: str = "system",
) -> dict[str, Any]:
    created = 0
    refreshed = 0
    linked = 0
    case_ids: list[str] = []

    for card in cards:
        if not isinstance(card, dict) or not _is_settlement_ops_card(card):
            continue
        case_type = case_type_from_source(
            "proactive_card",
            {
                "title": card.get("title"),
                "source_ref": card.get("dedupe_key"),
            },
        )
        priority = _priority_from_card(card)
        links = [{"link_type": "proactive_card", "ref": str(card.get("dedupe_key") or ""), "label": str(card.get("title") or "Proactive signal")}]
        linked_action_id = _clean_text(card.get("linked_action_id") or card.get("converted_action_id"))
        if linked_action_id:
            links.append({"link_type": "merchant_action", "ref": linked_action_id, "label": "Related Action Center item"})

        result = ops_repository.upsert_case_from_source(
            engine,
            merchant_id=merchant_id,
            lane="operations",
            case_type=case_type,
            title=str(card.get("title") or "Settlement ops case"),
            summary=str(card.get("body") or "").strip(),
            actor_id=actor_id,
            actor_role=actor_role,
            terminal_id=_clean_text(card.get("terminal_id")),
            priority=priority,
            source="proactive",
            source_ref=str(card.get("dedupe_key") or ""),
            source_payload=dict(card),
            evidence_ids=normalize_evidence_ids(card.get("evidence_ids")),
            links=links,
            runbook_code=runbook_for_case_type(case_type).code,
            tasks=_seed_tasks(case_type, priority),
        )
        case_id = str(result.get("case_id") or "").strip()
        if result.get("created"):
            created += 1
        elif result.get("reused") or result.get("refreshed"):
            refreshed += 1
        if case_id:
            case_ids.append(case_id)
            link_result = proactive_repository.link_background_proactive_card_case(
                engine,
                merchant_id,
                dedupe_key=str(card.get("dedupe_key") or ""),
                case_id=case_id,
            )
            if link_result.get("updated"):
                linked += 1

    return {
        "created_count": created,
        "refreshed_count": refreshed,
        "linked_count": linked,
        "case_ids": case_ids,
    }

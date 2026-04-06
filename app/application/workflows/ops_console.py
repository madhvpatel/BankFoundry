from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.application.kernel.request_models import CanonicalRequest
from app.data.ops import repository as ops_repository
from app.ontology.ops import case_type_from_source, runbook_for_case_type


ROLE_LANES: dict[str, set[str]] = {
    "admin": {"operations", "support", "risk"},
    "acquiring_ops": {"operations"},
    "support": {"operations", "support"},
    "risk_fraud": {"operations", "risk"},
    "operator": {"operations"},
}


@dataclass
class OpsConsoleDeps:
    engine: Any
    json_safe: Callable[[Any], Any]
    get_background_proactive_card: Callable[..., dict[str, Any] | None]
    get_existing_action: Callable[..., dict[str, Any] | None]
    case_copilot_summary: Callable[[dict[str, Any], str | None], dict[str, Any]]


def _actor_role(request: CanonicalRequest) -> str:
    return str(request.actor.role or "operator").strip().lower() or "operator"


def _assert_lane_access(role: str, lane: str) -> None:
    normalized_role = str(role or "operator").strip().lower()
    normalized_lane = str(lane or "operations").strip().lower()
    allowed = ROLE_LANES.get(normalized_role, ROLE_LANES["operator"])
    if normalized_lane not in allowed:
        raise PermissionError(f"role {normalized_role} cannot access lane {normalized_lane}")


def _approval_state(approvals: list[dict[str, Any]]) -> dict[str, Any]:
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


def _runbook_steps_with_progress(case_type: str, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    runbook = runbook_for_case_type(case_type)
    task_by_step = {
        str((task.get("metadata") or {}).get("step_id") or ""): task
        for task in tasks
        if isinstance(task, dict)
    }
    steps: list[dict[str, Any]] = []
    for step in runbook.steps:
        task = task_by_step.get(step.step_id, {})
        steps.append(
            {
                "step_id": step.step_id,
                "title": step.title,
                "description": step.description,
                "action_type": step.action_type,
                "status": str(task.get("status") or "OPEN").upper(),
                "task_id": task.get("task_id"),
                "owner": task.get("owner"),
            }
        )
    return steps


def _queue_payload(
    request: CanonicalRequest,
    deps: OpsConsoleDeps,
) -> dict[str, Any]:
    lane = str(request.lane.value if request.lane else request.payload.get("lane") or "operations").strip().lower()
    role = _actor_role(request)
    _assert_lane_access(role, lane)
    listing = ops_repository.list_cases(
        deps.engine,
        merchant_id=str(request.workspace.merchant_id or ""),
        lane=lane,
        status=str(request.payload.get("status") or "").strip() or None,
        owner=str(request.payload.get("owner") or "").strip() or None,
        limit=int(request.payload.get("limit") or 25),
    )
    approvals = ops_repository.list_approvals(
        deps.engine,
        merchant_id=str(request.workspace.merchant_id or ""),
        lane=lane,
        status="PENDING",
        limit=10,
    )
    return {
        "lane": lane,
        "role": role,
        "cases": deps.json_safe(listing["cases"]),
        "queue_summary": deps.json_safe(listing["queue_summary"]),
        "approvals": deps.json_safe(approvals),
    }


def handle_ops_queue(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    return _queue_payload(request, deps)


def handle_ops_case_detail(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    case_id = str(request.case_id or request.payload.get("case_id") or "").strip()
    detail = ops_repository.get_case_detail(deps.engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    lane = str(detail["case"].get("lane") or "operations").strip().lower()
    _assert_lane_access(_actor_role(request), lane)
    return {
        "work_item": deps.json_safe(detail["case"]),
        "tasks": deps.json_safe(detail["tasks"]),
        "timeline": deps.json_safe(detail["timeline"]),
        "approvals": deps.json_safe(detail["approvals"]),
        "connector_runs": deps.json_safe(detail.get("connector_runs") or []),
        "memory": deps.json_safe(detail.get("memory") or {}),
        "approval_state": deps.json_safe(_approval_state(detail["approvals"])),
        "runbook_steps": deps.json_safe(_runbook_steps_with_progress(str(detail["case"].get("case_type") or ""), detail["tasks"])),
    }


def handle_ops_case_create(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    lane = str(request.lane.value if request.lane else request.payload.get("lane") or "operations").strip().lower()
    role = _actor_role(request)
    _assert_lane_access(role, lane)
    case_type = str(request.payload.get("case_type") or "manual_ops_review").strip().lower()
    priority = str(request.payload.get("priority") or "medium").strip().lower()
    created = ops_repository.create_case(
        deps.engine,
        merchant_id=str(request.workspace.merchant_id or ""),
        terminal_id=str(request.workspace.terminal_id or request.payload.get("terminal_id") or "").strip() or None,
        lane=lane,
        case_type=case_type,
        title=str(request.payload.get("title") or "Untitled ops case"),
        summary=str(request.payload.get("summary") or ""),
        actor_id=str(request.actor.actor_id),
        actor_role=role,
        priority=priority,
        severity=str(request.payload.get("severity") or "").strip() or None,
        owner=str(request.payload.get("owner") or "").strip() or None,
        source=str(request.source.value if request.source else request.payload.get("source") or "manual"),
        source_ref=str(request.payload.get("source_ref") or "").strip() or None,
        source_payload=dict(request.payload.get("source_payload") or {}),
        evidence_ids=[str(item) for item in (request.payload.get("evidence_ids") or []) if str(item or "").strip()],
        links=[dict(item) for item in (request.payload.get("links") or []) if isinstance(item, dict)],
        runbook_code=str(request.payload.get("runbook_code") or "").strip() or None,
        tasks=_seed_tasks(case_type, priority),
    )
    detail_request = request.model_copy(update={"case_id": created["case_id"]})
    detail = handle_ops_case_detail(detail_request, deps)
    return {
        **detail,
        "created": created["created"],
        "reused": created["reused"],
    }


def _promote_payload_from_source(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    source_type = str(request.payload.get("source_type") or "").strip().lower()
    source_ref = str(request.payload.get("source_ref") or "").strip()
    source_payload = dict(request.payload.get("source_payload") or {})
    merchant_id = str(request.workspace.merchant_id or "")
    if source_type == "proactive_card":
        card = deps.get_background_proactive_card(deps.engine, merchant_id, dedupe_key=source_ref)
        if not card:
            raise LookupError("proactive card not found")
        return {
            "title": str(card.get("title") or "Settlement ops case"),
            "summary": str(card.get("body") or ""),
            "evidence_ids": list(card.get("evidence_ids") or []),
            "terminal_id": card.get("terminal_id"),
            "case_type": case_type_from_source(source_type, {"title": card.get("title"), "source_ref": source_ref}),
            "priority": "high",
            "links": [{"link_type": "proactive_card", "ref": source_ref, "label": str(card.get("title") or "Proactive card")}],
            "source_payload": card,
        }
    if source_type == "merchant_action":
        action = deps.get_existing_action(deps.engine, merchant_id, action_id=source_ref)
        if not action:
            raise LookupError("merchant action not found")
        evidence_payload = dict(action.get("evidence_payload") or {})
        title = str(action.get("title") or action.get("action_type") or "Merchant action").strip()
        summary = str(action.get("description") or evidence_payload.get("summary") or "").strip()
        evidence_ids = [str(item) for item in (evidence_payload.get("evidence_ids") or []) if str(item or "").strip()]
        return {
            "title": title,
            "summary": summary,
            "evidence_ids": evidence_ids,
            "terminal_id": evidence_payload.get("terminal_id"),
            "case_type": case_type_from_source(source_type, {"title": title, "source_ref": source_ref}),
            "priority": "medium",
            "links": [{"link_type": "merchant_action", "ref": source_ref, "label": title}],
            "source_payload": action,
        }
    if source_type == "chat_finding":
        title = str(source_payload.get("title") or source_payload.get("question") or "Chat finding").strip()
        summary = str(source_payload.get("summary") or source_payload.get("answer") or "").strip()
        evidence_ids = [str(item) for item in (source_payload.get("evidence_ids") or source_payload.get("sources") or []) if str(item or "").strip()]
        return {
            "title": title,
            "summary": summary,
            "evidence_ids": evidence_ids,
            "terminal_id": source_payload.get("terminal_id"),
            "case_type": str(source_payload.get("case_type") or case_type_from_source(source_type, {"title": title, "source_ref": source_ref})),
            "priority": str(source_payload.get("priority") or "medium"),
            "links": [{"link_type": "chat_finding", "ref": source_ref or "manual_chat", "label": title}],
            "source_payload": source_payload,
        }
    raise ValueError("unsupported promotion source")


def handle_ops_case_promote(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    lane = str(request.lane.value if request.lane else request.payload.get("lane") or "operations").strip().lower()
    role = _actor_role(request)
    _assert_lane_access(role, lane)
    promotion = _promote_payload_from_source(request, deps)
    create_request = request.model_copy(
        update={
            "payload": {
                **request.payload,
                "title": promotion["title"],
                "summary": promotion["summary"],
                "terminal_id": promotion.get("terminal_id"),
                "case_type": promotion["case_type"],
                "priority": promotion["priority"],
                "evidence_ids": promotion["evidence_ids"],
                "links": promotion["links"],
                "source_ref": str(request.payload.get("source_ref") or "").strip() or None,
                "source_payload": promotion["source_payload"],
            }
        }
    )
    return handle_ops_case_create(create_request, deps)


def handle_ops_case_assign(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    case_id = str(request.case_id or request.payload.get("case_id") or "").strip()
    detail = ops_repository.get_case_detail(deps.engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    _assert_lane_access(_actor_role(request), str(detail["case"].get("lane") or "operations"))
    result = ops_repository.assign_case(
        deps.engine,
        case_id=case_id,
        owner=str(request.payload.get("owner") or ""),
        actor_id=str(request.actor.actor_id),
        actor_role=_actor_role(request),
    )
    return {**result, **handle_ops_case_detail(request.model_copy(update={"case_id": case_id}), deps)}


def handle_ops_case_note(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    case_id = str(request.case_id or request.payload.get("case_id") or "").strip()
    detail = ops_repository.get_case_detail(deps.engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    _assert_lane_access(_actor_role(request), str(detail["case"].get("lane") or "operations"))
    result = ops_repository.add_case_note(
        deps.engine,
        case_id=case_id,
        body=str(request.payload.get("body") or ""),
        actor_id=str(request.actor.actor_id),
        actor_role=_actor_role(request),
    )
    return {**result, **handle_ops_case_detail(request.model_copy(update={"case_id": case_id}), deps)}


def handle_ops_case_copilot(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    case_id = str(request.case_id or request.payload.get("case_id") or "").strip()
    detail = ops_repository.get_case_detail(deps.engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    _assert_lane_access(_actor_role(request), str(detail["case"].get("lane") or "operations"))
    rendered_detail = handle_ops_case_detail(request.model_copy(update={"case_id": case_id}), deps)
    copilot = deps.case_copilot_summary(rendered_detail, str(request.payload.get("prompt") or "").strip() or None)
    memory_snapshot = ops_repository.upsert_case_memory(
        deps.engine,
        case_id=case_id,
        memory=dict(copilot.get("memory_snapshot") or {}),
    )
    return {
        **rendered_detail,
        "memory": deps.json_safe(memory_snapshot),
        "copilot": deps.json_safe(copilot),
    }


def handle_ops_case_memory_update(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    case_id = str(request.case_id or request.payload.get("case_id") or "").strip()
    detail = ops_repository.get_case_detail(deps.engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    _assert_lane_access(_actor_role(request), str(detail["case"].get("lane") or "operations"))
    memory = ops_repository.update_case_memory_context(
        deps.engine,
        case_id=case_id,
        actor_id=str(request.actor.actor_id),
        actor_role=_actor_role(request),
        settlement_id=request.payload.get("settlement_id"),
        start_date=request.payload.get("start_date"),
        end_date=request.payload.get("end_date"),
        evidence_ids=[str(item) for item in (request.payload.get("evidence_ids") or []) if str(item or "").strip()],
        clear_pinned_context=bool(request.payload.get("clear_pinned_context")),
        clear_window=bool(request.payload.get("clear_window")),
        clear_evidence=bool(request.payload.get("clear_evidence")),
    )
    return {
        **handle_ops_case_detail(request.model_copy(update={"case_id": case_id}), deps),
        "memory": deps.json_safe(memory),
    }


def handle_ops_case_request_approval(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    case_id = str(request.case_id or request.payload.get("case_id") or "").strip()
    detail = ops_repository.get_case_detail(deps.engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    _assert_lane_access(_actor_role(request), str(detail["case"].get("lane") or "operations"))
    result = ops_repository.request_case_approval(
        deps.engine,
        case_id=case_id,
        action_type=str(request.payload.get("action_type") or "FOLLOW_UP"),
        payload_summary=str(request.payload.get("payload_summary") or "Approval requested"),
        payload=dict(request.payload.get("payload") or {}),
        actor_id=str(request.actor.actor_id),
        actor_role=_actor_role(request),
    )
    return {**result, **handle_ops_case_detail(request.model_copy(update={"case_id": case_id}), deps)}


def handle_ops_case_resolve(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    case_id = str(request.case_id or request.payload.get("case_id") or "").strip()
    detail = ops_repository.get_case_detail(deps.engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    _assert_lane_access(_actor_role(request), str(detail["case"].get("lane") or "operations"))
    result = ops_repository.resolve_case(
        deps.engine,
        case_id=case_id,
        actor_id=str(request.actor.actor_id),
        actor_role=_actor_role(request),
        resolution_note=str(request.payload.get("resolution_note") or "").strip() or None,
        status=str(request.payload.get("status") or "RESOLVED"),
    )
    return {**result, **handle_ops_case_detail(request.model_copy(update={"case_id": case_id}), deps)}


def handle_ops_approvals(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    lane = str(request.lane.value if request.lane else request.payload.get("lane") or "operations").strip().lower()
    _assert_lane_access(_actor_role(request), lane)
    approvals = ops_repository.list_approvals(
        deps.engine,
        merchant_id=str(request.workspace.merchant_id or ""),
        lane=lane,
        status=str(request.payload.get("status") or "PENDING"),
        limit=int(request.payload.get("limit") or 25),
    )
    return {
        "lane": lane,
        "approvals": deps.json_safe(approvals),
        "queue_summary": {"pending": len([item for item in approvals if str(item.get("status") or "").upper() == "PENDING"])},
    }


def handle_ops_approval_decision(request: CanonicalRequest, deps: OpsConsoleDeps) -> dict[str, Any]:
    approval_id = str(request.work_item_id or request.payload.get("approval_id") or "").strip()
    result = ops_repository.decide_approval(
        deps.engine,
        approval_id=approval_id,
        decision=str(request.payload.get("decision") or ""),
        actor_id=str(request.actor.actor_id),
        actor_role=_actor_role(request),
        notes=str(request.payload.get("notes") or "").strip() or None,
    )
    approvals = handle_ops_approvals(request, deps)
    case_id = str((result.get("connector_result") or {}).get("case_id") or request.payload.get("case_id") or "").strip()
    if not case_id:
        pending = [item for item in approvals.get("approvals") or [] if str(item.get("approval_id") or "") == approval_id]
        if pending:
            case_id = str(pending[0].get("case_id") or "").strip()
    if case_id:
        detail = handle_ops_case_detail(request.model_copy(update={"case_id": case_id}), deps)
        return {**result, **approvals, **detail}
    return {**result, **approvals}

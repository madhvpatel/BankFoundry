from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.application.kernel.request_models import CanonicalRequest


@dataclass
class LiveSurfaceDeps:
    engine: Any
    json_safe: Callable[[Any], Any]
    pick_default_merchant_id: Callable[[Any], str | None]
    merchant_options: Callable[[int], list[dict[str, Any]]]
    merchant_snapshot: Callable[..., dict[str, Any]]
    merchant_label: Callable[[dict[str, Any], str], str]
    report_payload: Callable[[dict[str, Any]], dict[str, Any]]
    run_agent_turn: Callable[..., dict[str, Any]]
    ensure_background_proactive_refresh: Callable[..., Any]
    list_background_proactive_cards: Callable[..., Any]
    update_background_proactive_card_state: Callable[..., Any]
    preview_background_proactive_card_action: Callable[..., Any]
    confirm_background_proactive_card_action: Callable[..., Any]
    preview_merchant_action: Callable[..., Any]
    confirm_merchant_action: Callable[..., Any]
    update_existing_action_status: Callable[..., Any]
    update_existing_action_details: Callable[..., Any]
    cleanup_legacy_actions: Callable[..., Any]
    dashboard_metrics: Callable[..., dict[str, Any]]
    chat_memory_service: Any | None = None


def handle_merchant_options(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    limit = int(request.payload.get("limit") or 25)
    merchants = deps.merchant_options(limit)
    return {
        "default_merchant_id": deps.pick_default_merchant_id(deps.engine),
        "merchants": merchants,
    }


def handle_workspace_refresh(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    terminal_id = request.workspace.terminal_id
    days = int(request.payload.get("days") or 30)
    snapshot = deps.merchant_snapshot(merchant_id, terminal_id, days=days, refresh=True)
    return {
        "merchant_id": merchant_id,
        "merchant_label": deps.merchant_label(snapshot, merchant_id),
        "terminal_id": terminal_id,
        "snapshot": deps.json_safe(snapshot),
    }


def handle_report_build(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    terminal_id = request.workspace.terminal_id
    days = int(request.payload.get("days") or 30)
    snapshot = deps.merchant_snapshot(merchant_id, terminal_id, days=days, refresh=False)
    return {
        "merchant_id": merchant_id,
        "terminal_id": terminal_id,
        **deps.report_payload(snapshot),
    }


def handle_terminal_options(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    days = int(request.payload.get("days") or 30)
    snapshot = deps.merchant_snapshot(merchant_id, None, days=days, refresh=False)
    options = [{"terminal_id": "", "label": "All terminals"}]
    options.extend({"terminal_id": tid, "label": tid} for tid in snapshot.get("terminal_options") or [])
    return {"merchant_id": merchant_id, "terminals": options}


def handle_chat_turn(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    terminal_id = request.workspace.terminal_id
    prompt = str(request.payload.get("prompt") or "")
    request_history = list(request.payload.get("history") or [])
    session_key = request.session.session_key
    thread_scope = request.session.thread_scope or "default"

    merged_history = request_history
    memory_context: dict[str, Any] | None = None
    updated_memory: dict[str, Any] | None = None

    if deps.chat_memory_service is not None:
        session_bundle = deps.chat_memory_service.load_session(
            session_key=session_key,
            merchant_id=merchant_id,
            terminal_id=terminal_id,
            thread_scope=thread_scope,
            request_type=request.request_type.value,
            surface=request.surface.value,
        )
        merged_history = deps.chat_memory_service.merged_history(session_bundle, request_history=request_history)
        memory_context = deps.chat_memory_service.agent_memory_context(session_bundle, prompt=prompt)

    payload = deps.run_agent_turn(
        deps.engine,
        merchant_id=merchant_id,
        prompt=prompt,
        terminal_id=terminal_id,
        history=merged_history,
        memory_context=memory_context,
        debug=bool(request.debug),
    )
    if deps.chat_memory_service is not None:
        updated_bundle = deps.chat_memory_service.remember_turn(
            session_key=session_key,
            merchant_id=merchant_id,
            terminal_id=terminal_id,
            thread_scope=thread_scope,
            prompt=prompt,
            payload=payload,
        )
        updated_memory = deps.chat_memory_service.response_memory(updated_bundle, prompt=prompt)

    response = {
        "merchant_id": merchant_id,
        "prompt": prompt,
        "terminal_id": terminal_id,
        "session_key": session_key,
        "thread_scope": thread_scope,
        "answer": payload.get("answer") or "",
        "verification_status": payload.get("verification_status") or "",
        "sources": deps.json_safe(payload.get("sources") or []),
        "structured_result": deps.json_safe(payload.get("structured_result")),
        "follow_ups": deps.json_safe(payload.get("follow_ups") or []),
        "action_preview": deps.json_safe(payload.get("action_preview")),
        "scope": deps.json_safe(payload.get("scope") or {"merchant_id": merchant_id, "terminal_id": terminal_id}),
        "memory": deps.json_safe(updated_memory or {}),
        "intent": payload.get("intent") or "agent_turn",
        "answer_source": payload.get("answer_source") or "agent",
        "verification_summary": payload.get("verification_summary") or "",
        "validation_status": payload.get("validation_status") or "clean",
        "validation_issues": deps.json_safe(payload.get("validation_issues") or []),
        "display_notice": deps.json_safe(payload.get("display_notice")),
        "answer_sections": deps.json_safe(payload.get("answer_sections") or {}),
        "clarifying_question": deps.json_safe(payload.get("clarifying_question")),
        "trace": deps.json_safe(payload.get("trace") or {}),
    }
    if isinstance(response["trace"], dict):
        response["trace"]["session_key"] = session_key
        response["trace"]["thread_scope"] = thread_scope
        if isinstance(memory_context, dict):
            response["trace"]["memory_turn_count_before"] = memory_context.get("turn_count")
    if request.debug:
        response["debug"] = deps.json_safe(payload.get("debug") or {})
    return response


def handle_proactive_list(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    terminal_id = request.workspace.terminal_id
    lane = str(request.payload.get("lane") or "operations")
    days = int(request.payload.get("days") or 30)
    refresh_status = deps.ensure_background_proactive_refresh(deps.engine, merchant_id, days=days, force=False)
    snapshot = deps.merchant_snapshot(merchant_id, terminal_id, days=days, refresh=False)
    cards = list(snapshot.get("proactive_cards") or [])
    if lane != "all":
        cards = [card for card in cards if str(card.get("lane") or "").lower() == lane]
    nudges = []
    for card in cards:
        if isinstance(card, dict):
            title = str(card.get("title") or "").strip()
            body = str(card.get("body") or "").strip()
            nudges.append(f"{title}: {body}" if title and body else title or body)
    return {
        "merchant_id": merchant_id,
        "lane": lane,
        "terminal_id": terminal_id,
        "proactive_summary": f"{len(cards)} proactive card(s) available.",
        "nudges": nudges,
        "cards": deps.json_safe(cards),
        "refresh_status": deps.json_safe(refresh_status),
        "evidence": deps.json_safe([e for card in cards if isinstance(card, dict) for e in (card.get("evidence_ids") or [])]),
    }


def handle_proactive_refresh(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    days = int(request.payload.get("days") or 30)
    force = bool(request.payload.get("force"))
    result = deps.ensure_background_proactive_refresh(deps.engine, merchant_id, days=days, force=force)
    cards = deps.list_background_proactive_cards(deps.engine, merchant_id, limit=8)
    return {
        "merchant_id": merchant_id,
        "refresh_status": deps.json_safe(result),
        "cards": deps.json_safe(cards),
    }


def handle_proactive_card_state(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    return deps.json_safe(
        deps.update_background_proactive_card_state(
            deps.engine,
            merchant_id,
            dedupe_key=str(request.payload.get("dedupe_key") or ""),
            state=str(request.payload.get("state") or ""),
            card_notes=request.payload.get("card_notes"),
        )
    )


def handle_proactive_action_preview(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    return deps.json_safe(
        deps.preview_background_proactive_card_action(
            deps.engine,
            merchant_id,
            dedupe_key=str(request.payload.get("dedupe_key") or ""),
        )
    )


def handle_proactive_action_confirm(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    return deps.json_safe(
        deps.confirm_background_proactive_card_action(
            deps.engine,
            merchant_id,
            dedupe_key=str(request.payload.get("dedupe_key") or ""),
            confirmation_token=str(request.payload.get("confirmation_token") or ""),
        )
    )


def handle_action_preview(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    return deps.json_safe(
        deps.preview_merchant_action(
            deps.engine,
            merchant_id,
            action_type=str(request.payload.get("action_type") or ""),
            payload=dict(request.payload.get("payload") or {}),
        )
    )


def handle_action_confirm(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    return deps.json_safe(
        deps.confirm_merchant_action(
            deps.engine,
            merchant_id,
            confirmation_token=str(request.payload.get("confirmation_token") or ""),
        )
    )


def handle_action_status(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    return deps.json_safe(
        deps.update_existing_action_status(
            deps.engine,
            merchant_id,
            action_id=request.payload.get("action_id"),
            status=str(request.payload.get("status") or ""),
        )
    )


def handle_action_details(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    return deps.json_safe(
        deps.update_existing_action_details(
            deps.engine,
            merchant_id,
            action_id=request.payload.get("action_id"),
            owner=request.payload.get("owner"),
            notes=request.payload.get("notes"),
            blocked_reason=request.payload.get("blocked_reason"),
            follow_up_date=request.payload.get("follow_up_date"),
        )
    )


def handle_action_cleanup(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    return deps.json_safe(deps.cleanup_legacy_actions(deps.engine, merchant_id))


def handle_dashboard_query(request: CanonicalRequest, deps: LiveSurfaceDeps) -> dict[str, Any]:
    merchant_id = str(request.workspace.merchant_id or "")
    terminal_id = request.workspace.terminal_id
    return deps.dashboard_metrics(merchant_id=merchant_id, terminal_id=terminal_id)

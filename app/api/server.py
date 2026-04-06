import logging
import uuid
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine

from app.application.control_plane import ControlPlaneRouter, build_session_key
from app.application.control_plane.chat_memory import ChatMemoryService
from app.application.kernel.request_models import (
    ActorContext,
    CanonicalRequest,
    DeliveryContext,
    Lane,
    PolicyContext,
    RequestType,
    RequestSource,
    SessionContext,
    Surface,
    TenantContext,
    WorkspaceContext,
)
from app.application.workflows.live_context import LiveContextDeps
from app.application.workflows.merchant_surface import LiveSurfaceDeps
from app.application.workflows.bank_surface import OpsConsoleDeps
from app.application.workflows import bank_surface, live_context, merchant_surface
from app.data.actions import get_existing_action
from app.data.merchants import load_merchant_options
from app.data.proactive import get_background_proactive_card
from app.data.transactions import fetch_dashboard_metrics
from config import Config
from app.agent import run_agent_turn
from app.agent.bank_ops_agents import build_bank_ops_case_copilot_summary
from app.copilot.validation_server import _json_safe, pick_default_merchant_id
from app.dispute import extract_receipt_fields
from app.merchant_os import (
    build_report_briefs,
    build_report_packs,
    cleanup_legacy_actions,
    confirm_background_proactive_card_action,
    confirm_merchant_action,
    ensure_background_proactive_refresh,
    get_background_refresh_status,
    get_merchant_os_snapshot,
    list_background_proactive_cards,
    preview_background_proactive_card_action,
    preview_merchant_action,
    scope_snapshot_to_terminal,
    terminal_scope_options,
    update_background_proactive_card_state,
    update_existing_action_details,
    update_existing_action_status,
)

logger = logging.getLogger("copilot_api")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Bank Foundry API",
    description="Bank Foundry backend for separate merchant and bank-facing surfaces.",
    version="1.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.CORS_ALLOW_ORIGINS.split(",") if Config.CORS_ALLOW_ORIGINS != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = create_engine(Config.DATABASE_URL)


class AskRequest(BaseModel):
    merchant_id: Optional[str] = None
    prompt: str
    terminal_id: Optional[str] = None
    thread_scope: Optional[str] = None
    history: list[dict[str, str]] = Field(default_factory=list)
    debug: bool = False


class ProactiveRefreshRequest(BaseModel):
    merchant_id: Optional[str] = None
    days: int = 30
    force: bool = True


class ProactiveCardStateRequest(BaseModel):
    merchant_id: Optional[str] = None
    dedupe_key: str
    state: str = Field(..., pattern="^(NEW|ACKNOWLEDGED|DISMISSED|CONVERTED)$")
    card_notes: Optional[str] = None


class ProactiveCardPreviewRequest(BaseModel):
    merchant_id: Optional[str] = None
    dedupe_key: str


class ProactiveCardConfirmRequest(BaseModel):
    merchant_id: Optional[str] = None
    dedupe_key: str
    confirmation_token: str


class ActionPreviewRequest(BaseModel):
    merchant_id: Optional[str] = None
    action_type: str
    payload: dict[str, Any] = Field(default_factory=dict)


class ActionConfirmRequest(BaseModel):
    merchant_id: Optional[str] = None
    confirmation_token: str


class ActionStatusRequest(BaseModel):
    merchant_id: Optional[str] = None
    action_id: Any
    status: str


class ActionDetailsRequest(BaseModel):
    merchant_id: Optional[str] = None
    action_id: Any
    owner: Optional[str] = None
    notes: Optional[str] = None
    blocked_reason: Optional[str] = None
    follow_up_date: Optional[str] = None


class CleanupActionsRequest(BaseModel):
    merchant_id: Optional[str] = None


class OpsQueueRequest(BaseModel):
    merchant_id: Optional[str] = None
    lane: str = Field(default="operations", pattern="^(operations|support|risk)$")
    role: str = "acquiring_ops"
    status: Optional[str] = None
    owner: Optional[str] = None
    limit: int = Field(default=25, ge=1, le=100)


class OpsCaseCreateRequest(BaseModel):
    merchant_id: Optional[str] = None
    terminal_id: Optional[str] = None
    lane: str = Field(default="operations", pattern="^(operations|support|risk)$")
    role: str = "acquiring_ops"
    case_type: str = "manual_ops_review"
    title: str
    summary: str
    priority: str = "medium"
    severity: Optional[str] = None
    owner: Optional[str] = None
    evidence_ids: list[str] = Field(default_factory=list)
    links: list[dict[str, Any]] = Field(default_factory=list)


class OpsCasePromoteRequest(BaseModel):
    merchant_id: Optional[str] = None
    lane: str = Field(default="operations", pattern="^(operations|support|risk)$")
    role: str = "acquiring_ops"
    source_type: str = Field(..., pattern="^(proactive_card|merchant_action|chat_finding)$")
    source_ref: Optional[str] = None
    source_payload: dict[str, Any] = Field(default_factory=dict)


class OpsCaseAssignRequest(BaseModel):
    merchant_id: Optional[str] = None
    role: str = "acquiring_ops"
    owner: str


class OpsCaseNoteRequest(BaseModel):
    merchant_id: Optional[str] = None
    role: str = "acquiring_ops"
    body: str


class OpsCaseCopilotRequest(BaseModel):
    merchant_id: Optional[str] = None
    role: str = "acquiring_ops"
    prompt: Optional[str] = None


class OpsCaseMemoryUpdateRequest(BaseModel):
    merchant_id: Optional[str] = None
    role: str = "acquiring_ops"
    settlement_id: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    evidence_ids: list[str] = Field(default_factory=list)
    clear_pinned_context: bool = False
    clear_window: bool = False
    clear_evidence: bool = False


class OpsCaseApprovalRequest(BaseModel):
    merchant_id: Optional[str] = None
    role: str = "acquiring_ops"
    action_type: str
    payload_summary: str
    payload: dict[str, Any] = Field(default_factory=dict)


class OpsCaseResolveRequest(BaseModel):
    merchant_id: Optional[str] = None
    role: str = "acquiring_ops"
    resolution_note: Optional[str] = None
    status: str = Field(default="RESOLVED", pattern="^(RESOLVED|CLOSED)$")


class OpsApprovalDecisionRequest(BaseModel):
    merchant_id: Optional[str] = None
    lane: str = Field(default="operations", pattern="^(operations|support|risk)$")
    role: str = "acquiring_ops"
    decision: str = Field(..., pattern="^(APPROVED|REJECTED)$")
    notes: Optional[str] = None


@app.get("/health")
async def health():
    return {"status": "ok"}


def _resolved_merchant_id(merchant_id: Optional[str]) -> str:
    resolved = str(merchant_id or "").strip() or pick_default_merchant_id(engine)
    if not resolved:
        raise HTTPException(status_code=400, detail="Merchant ID required")
    return resolved


def _live_context_deps() -> LiveContextDeps:
    return LiveContextDeps(
        engine=engine,
        json_safe=_json_safe,
        ensure_background_proactive_refresh=ensure_background_proactive_refresh,
        get_merchant_os_snapshot=get_merchant_os_snapshot,
        terminal_scope_options=terminal_scope_options,
        scope_snapshot_to_terminal=scope_snapshot_to_terminal,
        get_background_refresh_status=get_background_refresh_status,
        build_report_briefs=build_report_briefs,
        build_report_packs=build_report_packs,
    )


def _coerce_int_param(value: Any, default: int) -> int:
    raw = getattr(value, "default", value)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _canonical_request(
    *,
    request_type: RequestType,
    surface: Surface,
    merchant_id: str | None = None,
    terminal_id: str | None = None,
    payload: dict[str, Any] | None = None,
    allow_write: bool = False,
    debug: bool = False,
    thread_scope: str | None = None,
    window_label: str | None = None,
    report_kind: str | None = None,
    job_name: str | None = None,
    actor_id: str = "api_caller",
    actor_role: str = "operator",
    lane: Lane | None = None,
    case_id: str | None = None,
    work_item_id: str | None = None,
    source: RequestSource | None = None,
) -> CanonicalRequest:
    resolved_mid = str(merchant_id or "").strip() or None
    session_key = build_session_key(
        request_type=request_type,
        surface=surface,
        merchant_id=resolved_mid,
        terminal_id=terminal_id,
        thread_scope=thread_scope,
        window_label=window_label,
        report_kind=report_kind,
        job_name=job_name,
        lane=lane.value if lane else None,
        case_id=case_id,
        work_item_id=work_item_id,
        source=source.value if source else None,
    )
    tenant_id = resolved_mid or "platform"
    tenant_type = "merchant" if resolved_mid else "platform"
    return CanonicalRequest(
        request_id=f"req_{uuid.uuid4().hex[:12]}",
        request_type=request_type,
        surface=surface,
        actor=ActorContext(actor_id=actor_id, actor_type="user", role=actor_role),
        tenant=TenantContext(tenant_id=tenant_id, tenant_type=tenant_type),
        workspace=WorkspaceContext(merchant_id=resolved_mid, terminal_id=terminal_id),
        session=SessionContext(session_key=session_key, thread_scope=thread_scope),
        lane=lane,
        case_id=case_id,
        work_item_id=work_item_id,
        source=source,
        payload=payload or {},
        policy_context=PolicyContext(allow_write=allow_write, debug=debug),
        delivery=DeliveryContext(mode="sync_response", target="http_response"),
        debug=debug,
    )


def _live_surface_deps() -> LiveSurfaceDeps:
    context_deps = _live_context_deps()
    return LiveSurfaceDeps(
        engine=engine,
        json_safe=_json_safe,
        pick_default_merchant_id=pick_default_merchant_id,
        merchant_options=lambda limit: load_merchant_options(
            engine,
            limit=limit,
            query_source_table=Config.QUERY_SOURCE_TABLE,
            default_merchant_id_loader=pick_default_merchant_id,
        ),
        merchant_snapshot=lambda merchant_id, terminal_id, *, days=30, refresh=True: live_context.build_merchant_snapshot(
            merchant_id,
            terminal_id,
            days=days,
            refresh=refresh,
            deps=context_deps,
        ),
        merchant_label=live_context.merchant_label,
        report_payload=lambda snapshot: live_context.build_report_payload(snapshot, deps=context_deps),
        run_agent_turn=run_agent_turn,
        ensure_background_proactive_refresh=ensure_background_proactive_refresh,
        list_background_proactive_cards=list_background_proactive_cards,
        update_background_proactive_card_state=update_background_proactive_card_state,
        preview_background_proactive_card_action=preview_background_proactive_card_action,
        confirm_background_proactive_card_action=confirm_background_proactive_card_action,
        preview_merchant_action=preview_merchant_action,
        confirm_merchant_action=confirm_merchant_action,
        update_existing_action_status=update_existing_action_status,
        update_existing_action_details=update_existing_action_details,
        cleanup_legacy_actions=cleanup_legacy_actions,
        dashboard_metrics=lambda **kwargs: fetch_dashboard_metrics(engine, **kwargs),
        chat_memory_service=ChatMemoryService(engine),
    )


def _bank_surface_deps() -> OpsConsoleDeps:
    return OpsConsoleDeps(
        engine=engine,
        json_safe=_json_safe,
        get_background_proactive_card=get_background_proactive_card,
        get_existing_action=get_existing_action,
        case_copilot_summary=lambda case_detail, prompt=None: build_bank_ops_case_copilot_summary(engine, case_detail, prompt=prompt),
    )


def _handle_merchant_options(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_merchant_options(request, deps=_live_surface_deps())


def _handle_workspace_refresh(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_workspace_refresh(request, deps=_live_surface_deps())


def _handle_report_build(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_report_build(request, deps=_live_surface_deps())


def _handle_terminal_options(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_terminal_options(request, deps=_live_surface_deps())


def _handle_chat_turn(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_chat_turn(request, deps=_live_surface_deps())


def _handle_proactive_list(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_proactive_list(request, deps=_live_surface_deps())


def _handle_proactive_refresh(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_proactive_refresh(request, deps=_live_surface_deps())


def _handle_proactive_card_state(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_proactive_card_state(request, deps=_live_surface_deps())


def _handle_proactive_action_preview(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_proactive_action_preview(request, deps=_live_surface_deps())


def _handle_proactive_action_confirm(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_proactive_action_confirm(request, deps=_live_surface_deps())


def _handle_action_preview(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_action_preview(request, deps=_live_surface_deps())


def _handle_action_confirm(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_action_confirm(request, deps=_live_surface_deps())


def _handle_action_status(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_action_status(request, deps=_live_surface_deps())


def _handle_action_details(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_action_details(request, deps=_live_surface_deps())


def _handle_action_cleanup(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_action_cleanup(request, deps=_live_surface_deps())


def _handle_dashboard_query(request: CanonicalRequest) -> dict[str, Any]:
    return merchant_surface.handle_dashboard_query(request, deps=_live_surface_deps())


def _handle_ops_queue(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_queue(request, deps=_bank_surface_deps())


def _handle_ops_case_detail(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_detail(request, deps=_bank_surface_deps())


def _handle_ops_case_create(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_create(request, deps=_bank_surface_deps())


def _handle_ops_case_promote(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_promote(request, deps=_bank_surface_deps())


def _handle_ops_case_assign(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_assign(request, deps=_bank_surface_deps())


def _handle_ops_case_note(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_note(request, deps=_bank_surface_deps())


def _handle_ops_case_copilot(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_copilot(request, deps=_bank_surface_deps())


def _handle_ops_case_memory_update(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_memory_update(request, deps=_bank_surface_deps())


def _handle_ops_case_request_approval(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_request_approval(request, deps=_bank_surface_deps())


def _handle_ops_case_resolve(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_case_resolve(request, deps=_bank_surface_deps())


def _handle_ops_approvals(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_approvals(request, deps=_bank_surface_deps())


def _handle_ops_approval_decision(request: CanonicalRequest) -> dict[str, Any]:
    return bank_surface.handle_ops_approval_decision(request, deps=_bank_surface_deps())


_CONTROL_PLANE_ROUTER = ControlPlaneRouter(
    handlers={
        RequestType.merchant_options.value: _handle_merchant_options,
        RequestType.workspace_refresh.value: _handle_workspace_refresh,
        RequestType.report_build.value: _handle_report_build,
        RequestType.terminal_options.value: _handle_terminal_options,
        RequestType.chat_turn.value: _handle_chat_turn,
        RequestType.proactive_list.value: _handle_proactive_list,
        RequestType.proactive_refresh.value: _handle_proactive_refresh,
        RequestType.proactive_card_state.value: _handle_proactive_card_state,
        RequestType.proactive_action_preview.value: _handle_proactive_action_preview,
        RequestType.proactive_action_confirm.value: _handle_proactive_action_confirm,
        RequestType.action_preview.value: _handle_action_preview,
        RequestType.action_confirm.value: _handle_action_confirm,
        RequestType.action_status.value: _handle_action_status,
        RequestType.action_details.value: _handle_action_details,
        RequestType.action_cleanup.value: _handle_action_cleanup,
        RequestType.dashboard_query.value: _handle_dashboard_query,
        RequestType.ops_queue.value: _handle_ops_queue,
        RequestType.ops_case_detail.value: _handle_ops_case_detail,
        RequestType.ops_case_create.value: _handle_ops_case_create,
        RequestType.ops_case_promote.value: _handle_ops_case_promote,
        RequestType.ops_case_assign.value: _handle_ops_case_assign,
        RequestType.ops_case_note.value: _handle_ops_case_note,
        RequestType.ops_case_copilot.value: _handle_ops_case_copilot,
        RequestType.ops_case_memory_update.value: _handle_ops_case_memory_update,
        RequestType.ops_case_request_approval.value: _handle_ops_case_request_approval,
        RequestType.ops_case_resolve.value: _handle_ops_case_resolve,
        RequestType.ops_approvals.value: _handle_ops_approvals,
        RequestType.ops_approval_decision.value: _handle_ops_approval_decision,
    }
)


@app.get("/api/v1/merchants/options")
async def get_merchant_options(limit: int = Query(25, ge=1, le=100)):
    limit_value = _coerce_int_param(limit, 25)
    request = _canonical_request(
        request_type=RequestType.merchant_options,
        surface=Surface.api,
        payload={"limit": limit_value},
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.get("/api/v1/merchant/snapshot")
async def get_merchant_snapshot(
    merchant_id: Optional[str] = None,
    terminal_id: Optional[str] = None,
    days: int = Query(30, ge=7, le=90),
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    days_value = _coerce_int_param(days, 30)
    request = _canonical_request(
        request_type=RequestType.workspace_refresh,
        surface=Surface.workspace,
        merchant_id=resolved_mid,
        terminal_id=terminal_id,
        payload={"days": days_value},
        window_label=f"{days_value}d",
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.get("/api/v1/merchant/reports")
async def get_merchant_reports(
    merchant_id: Optional[str] = None,
    terminal_id: Optional[str] = None,
    days: int = Query(30, ge=7, le=90),
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    days_value = _coerce_int_param(days, 30)
    request = _canonical_request(
        request_type=RequestType.report_build,
        surface=Surface.reports,
        merchant_id=resolved_mid,
        terminal_id=terminal_id,
        payload={"days": days_value},
        report_kind="merchant_reports",
        window_label=f"{days_value}d",
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.get("/api/v1/merchant/terminals")
async def get_terminal_options(
    merchant_id: Optional[str] = None,
    days: int = Query(30, ge=7, le=90),
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    days_value = _coerce_int_param(days, 30)
    request = _canonical_request(
        request_type=RequestType.terminal_options,
        surface=Surface.workspace,
        merchant_id=resolved_mid,
        payload={"days": days_value},
        window_label=f"{days_value}d",
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/ask")
async def ask_endpoint(req: AskRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    terminal_id = req.terminal_id or None
    try:
        request = _canonical_request(
            request_type=RequestType.chat_turn,
            surface=Surface.web_chat,
            merchant_id=merchant_id,
            terminal_id=terminal_id,
            payload={"prompt": req.prompt, "history": req.history, "thread_scope": req.thread_scope},
            debug=bool(req.debug),
            thread_scope=req.thread_scope or "default",
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        logger.exception("Error in ask_endpoint")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/copilot/proactive")
async def get_proactive_nudges(
    merchant_id: Optional[str] = None,
    terminal_id: Optional[str] = None,
    lane: str = Query("operations", pattern="^(operations|growth|all)$"),
    days: int = Query(30, ge=7, le=90),
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    days_value = _coerce_int_param(days, 30)
    request = _canonical_request(
        request_type=RequestType.proactive_list,
        surface=Surface.proactive_inbox,
        merchant_id=resolved_mid,
        terminal_id=terminal_id,
        payload={"lane": lane, "days": days_value},
        window_label=f"{days_value}d",
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/copilot/proactive/refresh")
async def refresh_proactive_cards(req: ProactiveRefreshRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.proactive_refresh,
            surface=Surface.proactive_inbox,
            merchant_id=merchant_id,
            payload={"days": int(req.days), "force": bool(req.force)},
            window_label=f"{int(req.days)}d",
            job_name="refresh_proactive_cards",
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        logger.exception("Error refreshing proactive cards")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/v1/copilot/proactive/card/state")
async def set_proactive_card_state(req: ProactiveCardStateRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    request = _canonical_request(
        request_type=RequestType.proactive_card_state,
        surface=Surface.proactive_inbox,
        merchant_id=merchant_id,
        payload={"dedupe_key": req.dedupe_key, "state": req.state, "card_notes": req.card_notes},
        allow_write=True,
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/copilot/proactive/card/preview-action")
async def preview_proactive_card_action(req: ProactiveCardPreviewRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    request = _canonical_request(
        request_type=RequestType.proactive_action_preview,
        surface=Surface.proactive_inbox,
        merchant_id=merchant_id,
        payload={"dedupe_key": req.dedupe_key},
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/copilot/proactive/card/confirm-action")
async def confirm_proactive_card_action(req: ProactiveCardConfirmRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    request = _canonical_request(
        request_type=RequestType.proactive_action_confirm,
        surface=Surface.proactive_inbox,
        merchant_id=merchant_id,
        payload={"dedupe_key": req.dedupe_key, "confirmation_token": req.confirmation_token},
        allow_write=True,
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/actions/preview")
async def preview_action(req: ActionPreviewRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    request = _canonical_request(
        request_type=RequestType.action_preview,
        surface=Surface.action_center,
        merchant_id=merchant_id,
        payload={"action_type": req.action_type, "payload": req.payload},
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/actions/confirm")
async def confirm_action(req: ActionConfirmRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    request = _canonical_request(
        request_type=RequestType.action_confirm,
        surface=Surface.action_center,
        merchant_id=merchant_id,
        payload={"confirmation_token": req.confirmation_token},
        allow_write=True,
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/actions/status")
async def set_action_status(req: ActionStatusRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    request = _canonical_request(
        request_type=RequestType.action_status,
        surface=Surface.action_center,
        merchant_id=merchant_id,
        payload={"action_id": req.action_id, "status": req.status},
        allow_write=True,
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/actions/details")
async def set_action_details(req: ActionDetailsRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    request = _canonical_request(
        request_type=RequestType.action_details,
        surface=Surface.action_center,
        merchant_id=merchant_id,
        payload={
            "action_id": req.action_id,
            "owner": req.owner,
            "notes": req.notes,
            "blocked_reason": req.blocked_reason,
            "follow_up_date": req.follow_up_date,
        },
        allow_write=True,
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


@app.post("/api/v1/actions/cleanup-legacy")
async def hide_legacy_actions(req: CleanupActionsRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    request = _canonical_request(
        request_type=RequestType.action_cleanup,
        surface=Surface.action_center,
        merchant_id=merchant_id,
        allow_write=True,
    )
    return _CONTROL_PLANE_ROUTER.handle(request).payload


def _ops_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, PermissionError):
        return HTTPException(status_code=403, detail=str(exc))
    if isinstance(exc, LookupError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, ValueError):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/ops/queue")
async def get_ops_queue(
    merchant_id: Optional[str] = None,
    lane: str = Query("operations", pattern="^(operations|support|risk)$"),
    role: str = Query("acquiring_ops"),
    status: Optional[str] = None,
    owner: Optional[str] = None,
    limit: int = Query(25, ge=1, le=100),
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    limit_value = _coerce_int_param(limit, 25)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_queue,
            surface=Surface.ops_console,
            merchant_id=resolved_mid,
            payload={"lane": lane, "status": status, "owner": owner, "limit": limit_value},
            actor_role=role,
            lane=Lane(lane),
            source=RequestSource.manual,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.get("/api/v1/ops/cases/{case_id}")
async def get_ops_case_detail(
    case_id: str,
    merchant_id: Optional[str] = None,
    role: str = Query("acquiring_ops"),
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_case_detail,
            surface=Surface.ops_console,
            merchant_id=resolved_mid,
            actor_role=role,
            case_id=case_id,
            payload={"case_id": case_id},
            source=RequestSource.manual,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/cases")
async def create_ops_case(req: OpsCaseCreateRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_case_create,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            terminal_id=req.terminal_id,
            payload={
                "lane": req.lane,
                "case_type": req.case_type,
                "title": req.title,
                "summary": req.summary,
                "priority": req.priority,
                "severity": req.severity,
                "owner": req.owner,
                "evidence_ids": req.evidence_ids,
                "links": req.links,
            },
            actor_role=req.role,
            lane=Lane(req.lane),
            source=RequestSource.manual,
            allow_write=True,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/cases/promote")
async def promote_ops_case(req: OpsCasePromoteRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        source_kind = (
            RequestSource.manual
            if req.source_type == "chat_finding"
            else RequestSource.proactive
            if req.source_type == "proactive_card"
            else RequestSource.connector
        )
        request = _canonical_request(
            request_type=RequestType.ops_case_promote,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            payload={
                "lane": req.lane,
                "source_type": req.source_type,
                "source_ref": req.source_ref,
                "source_payload": req.source_payload,
            },
            actor_role=req.role,
            lane=Lane(req.lane),
            source=source_kind,
            allow_write=True,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/cases/{case_id}/assign")
async def assign_ops_case(case_id: str, req: OpsCaseAssignRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_case_assign,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            payload={"case_id": case_id, "owner": req.owner},
            actor_role=req.role,
            case_id=case_id,
            source=RequestSource.manual,
            allow_write=True,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/cases/{case_id}/notes")
async def add_ops_case_note(case_id: str, req: OpsCaseNoteRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_case_note,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            payload={"case_id": case_id, "body": req.body},
            actor_role=req.role,
            case_id=case_id,
            source=RequestSource.manual,
            allow_write=True,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/cases/{case_id}/copilot")
async def get_ops_case_copilot(case_id: str, req: OpsCaseCopilotRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_case_copilot,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            payload={"case_id": case_id, "prompt": req.prompt},
            actor_role=req.role,
            case_id=case_id,
            source=RequestSource.manual,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/cases/{case_id}/memory")
async def update_ops_case_memory(case_id: str, req: OpsCaseMemoryUpdateRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_case_memory_update,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            payload={
                "case_id": case_id,
                "settlement_id": req.settlement_id,
                "start_date": req.start_date,
                "end_date": req.end_date,
                "evidence_ids": req.evidence_ids,
                "clear_pinned_context": req.clear_pinned_context,
                "clear_window": req.clear_window,
                "clear_evidence": req.clear_evidence,
            },
            actor_role=req.role,
            case_id=case_id,
            source=RequestSource.manual,
            allow_write=True,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/cases/{case_id}/approval")
async def request_ops_case_approval(case_id: str, req: OpsCaseApprovalRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_case_request_approval,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            payload={
                "case_id": case_id,
                "action_type": req.action_type,
                "payload_summary": req.payload_summary,
                "payload": req.payload,
            },
            actor_role=req.role,
            case_id=case_id,
            source=RequestSource.manual,
            allow_write=True,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/cases/{case_id}/resolve")
async def resolve_ops_case(case_id: str, req: OpsCaseResolveRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_case_resolve,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            payload={"case_id": case_id, "resolution_note": req.resolution_note, "status": req.status},
            actor_role=req.role,
            case_id=case_id,
            source=RequestSource.manual,
            allow_write=True,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.get("/api/v1/ops/approvals")
async def get_ops_approvals(
    merchant_id: Optional[str] = None,
    lane: str = Query("operations", pattern="^(operations|support|risk)$"),
    role: str = Query("acquiring_ops"),
    status: str = Query("PENDING"),
    limit: int = Query(25, ge=1, le=100),
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    limit_value = _coerce_int_param(limit, 25)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_approvals,
            surface=Surface.ops_console,
            merchant_id=resolved_mid,
            payload={"lane": lane, "status": status, "limit": limit_value},
            actor_role=role,
            lane=Lane(lane),
            source=RequestSource.manual,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.post("/api/v1/ops/approvals/{approval_id}/decision")
async def decide_ops_approval(approval_id: str, req: OpsApprovalDecisionRequest):
    merchant_id = _resolved_merchant_id(req.merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.ops_approval_decision,
            surface=Surface.ops_console,
            merchant_id=merchant_id,
            payload={"approval_id": approval_id, "decision": req.decision, "notes": req.notes, "lane": req.lane},
            actor_role=req.role,
            lane=Lane(req.lane),
            work_item_id=approval_id,
            source=RequestSource.manual,
            allow_write=True,
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        raise _ops_http_error(exc)


@app.get("/api/v1/analytics/dashboard")
async def get_dashboard_metrics(
    merchant_id: Optional[str] = None,
    terminal_id: Optional[str] = None,
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    try:
        request = _canonical_request(
            request_type=RequestType.dashboard_query,
            surface=Surface.dashboard,
            merchant_id=resolved_mid,
            terminal_id=terminal_id,
            window_label="30d",
        )
        return _CONTROL_PLANE_ROUTER.handle(request).payload
    except Exception as exc:
        logger.exception("Error in dashboard metrics")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/v1/dispute/upload-receipt")
async def upload_dispute_receipt(
    file: UploadFile = File(...),
    merchant_id: Optional[str] = Form(None),
    context: Optional[str] = Form(None),
):
    resolved_mid = _resolved_merchant_id(merchant_id)
    try:
        image_bytes = await file.read()
        mime_type = file.content_type or "image/jpeg"
        
        ctx_dict = {}
        if context:
            import json
            try:
                ctx_dict = json.loads(context)
            except Exception:
                pass

        result = extract_receipt_fields(
            image_bytes=image_bytes,
            mime_type=mime_type,
            merchant_id=resolved_mid,
            context=ctx_dict,
        )
        
        if not result["ok"]:
            raise HTTPException(status_code=422, detail=result.get("error", "Failed to extract receipt data"))
            
        return {"status": "ok", "evidence": result}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Error processing dispute receipt")
        raise HTTPException(status_code=500, detail=str(exc))


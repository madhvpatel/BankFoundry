from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class KernelModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class RequestType(str, Enum):
    chat_turn = "chat_turn"
    merchant_options = "merchant_options"
    workspace_refresh = "workspace_refresh"
    report_build = "report_build"
    terminal_options = "terminal_options"
    proactive_list = "proactive_list"
    proactive_refresh = "proactive_refresh"
    proactive_card_state = "proactive_card_state"
    proactive_action_preview = "proactive_action_preview"
    proactive_action_confirm = "proactive_action_confirm"
    action_preview = "action_preview"
    action_confirm = "action_confirm"
    action_status = "action_status"
    action_details = "action_details"
    action_cleanup = "action_cleanup"
    dashboard_query = "dashboard_query"
    ops_queue = "ops_queue"
    ops_case_detail = "ops_case_detail"
    ops_case_create = "ops_case_create"
    ops_case_promote = "ops_case_promote"
    ops_case_assign = "ops_case_assign"
    ops_case_note = "ops_case_note"
    ops_case_copilot = "ops_case_copilot"
    ops_case_memory_update = "ops_case_memory_update"
    ops_case_request_approval = "ops_case_request_approval"
    ops_case_resolve = "ops_case_resolve"
    ops_approvals = "ops_approvals"
    ops_approval_decision = "ops_approval_decision"


class Surface(str, Enum):
    api = "api"
    web_chat = "web_chat"
    workspace = "workspace"
    proactive_inbox = "proactive_inbox"
    action_center = "action_center"
    reports = "reports"
    dashboard = "dashboard"
    scheduler = "scheduler"
    ops_console = "ops_console"


class Lane(str, Enum):
    operations = "operations"
    support = "support"
    risk = "risk"


class RequestSource(str, Enum):
    manual = "manual"
    proactive = "proactive"
    connector = "connector"
    scheduled = "scheduled"


class ActorContext(KernelModel):
    actor_id: str
    actor_type: str = "user"
    role: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TenantContext(KernelModel):
    tenant_id: str
    tenant_type: str = "merchant"
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkspaceContext(KernelModel):
    merchant_id: str | None = None
    terminal_id: str | None = None
    workspace_kind: str = "merchant_workspace"
    metadata: dict[str, Any] = Field(default_factory=dict)


class SessionContext(KernelModel):
    session_key: str
    thread_scope: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeliveryContext(KernelModel):
    mode: str = "sync_response"
    target: str = "http_response"
    metadata: dict[str, Any] = Field(default_factory=dict)


class PolicyContext(KernelModel):
    allow_write: bool = False
    debug: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalRequest(KernelModel):
    request_id: str
    request_type: RequestType
    surface: Surface
    actor: ActorContext
    tenant: TenantContext
    workspace: WorkspaceContext
    session: SessionContext
    lane: Lane | None = None
    case_id: str | None = None
    work_item_id: str | None = None
    source: RequestSource | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    policy_context: PolicyContext = Field(default_factory=PolicyContext)
    delivery: DeliveryContext = Field(default_factory=DeliveryContext)
    debug: bool = False

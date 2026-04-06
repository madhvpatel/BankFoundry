from __future__ import annotations

from app.application.kernel.request_models import RequestType, Surface


def build_session_key(
    *,
    request_type: RequestType,
    surface: Surface,
    merchant_id: str | None = None,
    terminal_id: str | None = None,
    thread_scope: str | None = None,
    window_label: str | None = None,
    report_kind: str | None = None,
    job_name: str | None = None,
    lane: str | None = None,
    case_id: str | None = None,
    work_item_id: str | None = None,
    source: str | None = None,
) -> str:
    request_name = request_type.value
    surface_name = surface.value
    merchant = str(merchant_id or "").strip()
    terminal = str(terminal_id or "").strip()
    scope = str(thread_scope or "").strip() or "default"
    window = str(window_label or "").strip() or "default"
    report = str(report_kind or "").strip() or "default"
    job = str(job_name or "").strip() or "default"
    lane_name = str(lane or "").strip() or "default"
    case_name = str(case_id or "").strip()
    work_item = str(work_item_id or "").strip()
    source_name = str(source or "").strip() or "default"

    if request_type == RequestType.chat_turn and merchant:
        if terminal:
            return f"merchant:{merchant}:chat:{surface_name}:terminal:{terminal}"
        return f"merchant:{merchant}:chat:{surface_name}:{scope}"

    if request_type in {RequestType.workspace_refresh, RequestType.terminal_options} and merchant:
        if terminal:
            return f"merchant:{merchant}:terminal:{terminal}:workspace"
        return f"merchant:{merchant}:workspace"

    if request_type == RequestType.report_build and merchant:
        if terminal:
            return f"merchant:{merchant}:reports:{report}:terminal:{terminal}:{window}"
        return f"merchant:{merchant}:reports:{report}:{window}"

    if request_type in {RequestType.proactive_list, RequestType.proactive_refresh, RequestType.proactive_card_state, RequestType.proactive_action_preview, RequestType.proactive_action_confirm} and merchant:
        return f"merchant:{merchant}:proactive:{window}"

    if request_type in {
        RequestType.action_preview,
        RequestType.action_confirm,
        RequestType.action_status,
        RequestType.action_details,
        RequestType.action_cleanup,
    } and merchant:
        return f"merchant:{merchant}:actions"

    if request_type == RequestType.dashboard_query and merchant:
        if terminal:
            return f"merchant:{merchant}:dashboard:terminal:{terminal}:{window}"
        return f"merchant:{merchant}:dashboard:{window}"

    if request_type == RequestType.merchant_options:
        return f"catalog:merchants:{surface_name}"

    if request_type == RequestType.proactive_refresh and merchant:
        return f"merchant:{merchant}:job:{job}:{window}"

    if request_type in {
        RequestType.ops_queue,
        RequestType.ops_case_detail,
        RequestType.ops_case_create,
        RequestType.ops_case_promote,
        RequestType.ops_case_assign,
        RequestType.ops_case_note,
        RequestType.ops_case_copilot,
        RequestType.ops_case_request_approval,
        RequestType.ops_case_resolve,
        RequestType.ops_approvals,
        RequestType.ops_approval_decision,
    }:
        if case_name:
            return f"ops:{merchant or 'platform'}:{lane_name}:case:{case_name}"
        if work_item:
            return f"ops:{merchant or 'platform'}:{lane_name}:work:{work_item}"
        return f"ops:{merchant or 'platform'}:{lane_name}:{request_name}:{source_name}"

    if merchant:
        return f"merchant:{merchant}:{request_name}:{surface_name}"
    return f"platform:{request_name}:{surface_name}"

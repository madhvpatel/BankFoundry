from __future__ import annotations

import datetime as dt
import json
import uuid
from typing import Any

from sqlalchemy import text

from app.data.connectors import dispatch_settlement_approval, list_connector_runs_for_case
from app.data.evidence import merge_evidence_ids, normalize_evidence_ids
from app.ontology.ops import runbook_for_case_type, sla_policy_for_priority
from app.project_paths import repo_path

ACTIVE_CASE_STATUSES = {"OPEN", "IN_PROGRESS", "BLOCKED", "AWAITING_APPROVAL"}
RESOLVED_CASE_STATUSES = {"RESOLVED", "CLOSED"}
SUPPORT_HISTORY_FIXTURE = repo_path("tests", "fixtures", "bank_foundry", "support_case_history.json")
MAX_CASE_EVIDENCE_JSON_CHARS = 32768
OPS_CASE_COLUMNS = (
    "case_id",
    "merchant_id",
    "terminal_id",
    "lane",
    "case_type",
    "title",
    "summary",
    "status",
    "priority",
    "severity",
    "owner",
    "source",
    "source_ref",
    "source_payload_json",
    "evidence_ids_json",
    "links_json",
    "approval_state",
    "runbook_code",
    "blocked_reason",
    "opened_at",
    "due_at",
    "resolved_at",
    "last_activity_at",
    "updated_at",
    "created_at",
)


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat(timespec="seconds")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _json_dump(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, default=str)


def _json_load_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_load_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _ops_case_select_clause(*, bound_evidence_json: bool) -> str:
    parts: list[str] = []
    for column in OPS_CASE_COLUMNS:
        if bound_evidence_json and column == "evidence_ids_json":
            parts.append(
                f"CASE WHEN COALESCE(LENGTH({column}), 0) > {MAX_CASE_EVIDENCE_JSON_CHARS} THEN '[]' ELSE {column} END AS {column}"
            )
        else:
            parts.append(column)
    return ", ".join(parts)


def _normalized_source_payload(value: Any) -> dict[str, Any]:
    payload = dict(value) if isinstance(value, dict) else {}
    if "evidence_ids" in payload:
        payload["evidence_ids"] = normalize_evidence_ids(payload.get("evidence_ids"))
    return payload


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _parse_datetime(value: Any) -> dt.datetime | None:
    text_value = _clean_text(value)
    if text_value is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text_value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _compute_due_at(priority: str, now: dt.datetime | None = None) -> str:
    now = now or _utc_now()
    policy = sla_policy_for_priority(priority)
    return (now + dt.timedelta(hours=policy.target_hours)).isoformat(timespec="seconds")


def _priority_rank(value: str) -> int:
    normalized = str(value or "medium").strip().lower()
    return {"low": 1, "medium": 2, "high": 3, "critical": 4}.get(normalized, 2)


def _priority_max(left: str, right: str) -> str:
    return left if _priority_rank(left) >= _priority_rank(right) else right


def _merge_unique_text(left: list[Any] | None, right: list[Any] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for source in (left or [], right or []):
        text_value = str(source or "").strip()
        if text_value and text_value not in seen:
            seen.add(text_value)
            out.append(text_value)
    return out


def _merge_links(left: list[dict[str, Any]] | None, right: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in (left or []) + (right or []):
        if not isinstance(item, dict):
            continue
        key = (
            str(item.get("link_type") or "").strip().lower(),
            str(item.get("ref") or "").strip(),
            str(item.get("label") or "").strip(),
        )
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        out.append(dict(item))
    return out


def _case_links_from_json(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in _json_load_list(value) if isinstance(item, dict)]


def _task_metadata_from_json(value: Any) -> dict[str, Any]:
    return _json_load_object(value)


def _event_metadata_from_json(value: Any) -> dict[str, Any]:
    return _json_load_object(value)


def _empty_case_memory() -> dict[str, Any]:
    return {
        "pinned_entities": {},
        "active_window": {},
        "confirmed_evidence_ids": [],
        "latest_summary": {},
        "latest_tool_calls": [],
    }


def _normalize_case_memory(value: Any) -> dict[str, Any]:
    payload = _json_load_object(value)
    normalized = _empty_case_memory()
    pinned_entities = payload.get("pinned_entities")
    active_window = payload.get("active_window")
    latest_summary = payload.get("latest_summary")
    latest_tool_calls = payload.get("latest_tool_calls")
    confirmed_evidence_ids = payload.get("confirmed_evidence_ids")
    normalized["pinned_entities"] = pinned_entities if isinstance(pinned_entities, dict) else {}
    normalized["active_window"] = active_window if isinstance(active_window, dict) else {}
    normalized["latest_summary"] = latest_summary if isinstance(latest_summary, dict) else {}
    normalized["latest_tool_calls"] = [dict(item) for item in latest_tool_calls] if isinstance(latest_tool_calls, list) else []
    normalized["confirmed_evidence_ids"] = normalize_evidence_ids(confirmed_evidence_ids)
    return normalized


def ensure_ops_schema(engine: Any) -> None:
    create_statements = [
        """
        CREATE TABLE IF NOT EXISTS ops_cases (
            case_id TEXT PRIMARY KEY,
            merchant_id TEXT NOT NULL,
            terminal_id TEXT NULL,
            lane TEXT NOT NULL,
            case_type TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            status TEXT NOT NULL,
            priority TEXT NOT NULL,
            severity TEXT NULL,
            owner TEXT NULL,
            source TEXT NOT NULL,
            source_ref TEXT NULL,
            source_payload_json TEXT NOT NULL,
            evidence_ids_json TEXT NOT NULL,
            links_json TEXT NOT NULL,
            approval_state TEXT NOT NULL,
            runbook_code TEXT NOT NULL,
            blocked_reason TEXT NULL,
            opened_at TEXT NOT NULL,
            due_at TEXT NULL,
            resolved_at TEXT NULL,
            last_activity_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ops_tasks (
            task_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            owner TEXT NULL,
            priority TEXT NOT NULL,
            due_at TEXT NULL,
            metadata_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ops_case_events (
            event_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            actor_role TEXT NOT NULL,
            body TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ops_approvals (
            approval_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            status TEXT NOT NULL,
            action_type TEXT NOT NULL,
            payload_summary TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            requested_by TEXT NOT NULL,
            requested_role TEXT NOT NULL,
            requested_at TEXT NOT NULL,
            reviewed_by TEXT NULL,
            reviewed_role TEXT NULL,
            reviewed_at TEXT NULL,
            decision_notes TEXT NULL,
            receipt_ref TEXT NULL,
            connector_status TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ops_case_memory (
            case_id TEXT PRIMARY KEY,
            memory_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """,
    ]
    with engine.begin() as conn:
        for statement in create_statements:
            conn.execute(text(statement))


def _insert_case_event(
    conn: Any,
    *,
    case_id: str,
    event_type: str,
    actor_id: str,
    actor_role: str,
    body: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = {
        "event_id": _new_id("evt"),
        "case_id": case_id,
        "event_type": str(event_type or "note").strip().lower(),
        "actor_id": actor_id,
        "actor_role": actor_role,
        "body": str(body or "").strip(),
        "metadata_json": _json_dump(metadata or {}),
        "created_at": _iso_now(),
    }
    conn.execute(
        text(
            """
            INSERT INTO ops_case_events (
                event_id, case_id, event_type, actor_id, actor_role, body, metadata_json, created_at
            ) VALUES (
                :event_id, :case_id, :event_type, :actor_id, :actor_role, :body, :metadata_json, :created_at
            )
            """
        ),
        event,
    )
    return event


def _insert_case_task(
    conn: Any,
    *,
    case_id: str,
    title: str,
    description: str,
    priority: str = "medium",
    owner: str | None = None,
    due_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task = {
        "task_id": _new_id("task"),
        "case_id": case_id,
        "title": str(title or "").strip(),
        "description": str(description or "").strip(),
        "status": "OPEN",
        "owner": _clean_text(owner),
        "priority": str(priority or "medium").strip().lower(),
        "due_at": due_at,
        "metadata_json": _json_dump(metadata or {}),
        "updated_at": _iso_now(),
        "created_at": _iso_now(),
    }
    conn.execute(
        text(
            """
            INSERT INTO ops_tasks (
                task_id, case_id, title, description, status, owner, priority, due_at, metadata_json, updated_at, created_at
            ) VALUES (
                :task_id, :case_id, :title, :description, :status, :owner, :priority, :due_at, :metadata_json, :updated_at, :created_at
            )
            """
        ),
        task,
    )
    return task


def _serialize_case_row(row: dict[str, Any], *, now: dt.datetime | None = None) -> dict[str, Any]:
    current_time = now or _utc_now()
    due_at = _parse_datetime(row.get("due_at"))
    opened_at = _parse_datetime(row.get("opened_at"))
    age_hours = None
    if opened_at is not None:
        age_hours = round((current_time - opened_at).total_seconds() / 3600, 1)
    breached = bool(due_at and current_time > due_at and str(row.get("status") or "").upper() not in RESOLVED_CASE_STATUSES)
    warning = False
    if due_at and not breached:
        policy = sla_policy_for_priority(str(row.get("priority") or "medium"))
        if opened_at is not None:
            elapsed_hours = max((current_time - opened_at).total_seconds() / 3600, 0)
            warning = elapsed_hours >= policy.warning_hours
    return {
        **dict(row),
        "evidence_ids": normalize_evidence_ids(row.get("evidence_ids_json")),
        "links": _case_links_from_json(row.get("links_json")),
        "source_payload": _json_load_object(row.get("source_payload_json")),
        "age_hours": age_hours,
        "sla_breached": breached,
        "sla_warning": warning,
    }


def _task_stats_by_case(engine: Any, case_ids: list[str], *, now: dt.datetime | None = None) -> dict[str, dict[str, Any]]:
    if not case_ids:
        return {}
    current_time = now or _utc_now()
    placeholders = ", ".join(f":case_id_{idx}" for idx in range(len(case_ids)))
    params = {f"case_id_{idx}": case_id for idx, case_id in enumerate(case_ids)}
    query = text(
        f"""
        SELECT case_id, status, due_at
        FROM ops_tasks
        WHERE case_id IN ({placeholders})
        """
    )
    stats: dict[str, dict[str, Any]] = {
        case_id: {
            "task_count": 0,
            "open_task_count": 0,
            "done_task_count": 0,
            "overdue_task_count": 0,
        }
        for case_id in case_ids
    }
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    for row in rows:
        case_id = str(row.get("case_id") or "").strip()
        if not case_id:
            continue
        item = stats.setdefault(
            case_id,
            {"task_count": 0, "open_task_count": 0, "done_task_count": 0, "overdue_task_count": 0},
        )
        item["task_count"] += 1
        status = str(row.get("status") or "").upper()
        if status == "DONE":
            item["done_task_count"] += 1
        else:
            item["open_task_count"] += 1
            due_at = _parse_datetime(row.get("due_at"))
            if due_at and current_time > due_at:
                item["overdue_task_count"] += 1
    return stats


def _latest_approval_by_case(engine: Any, case_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not case_ids:
        return {}
    placeholders = ", ".join(f":case_id_{idx}" for idx in range(len(case_ids)))
    params = {f"case_id_{idx}": case_id for idx, case_id in enumerate(case_ids)}
    query = text(
        f"""
        SELECT *
        FROM ops_approvals
        WHERE case_id IN ({placeholders})
        ORDER BY requested_at DESC
        """
    )
    latest: dict[str, dict[str, Any]] = {}
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    for row in rows:
        case_id = str(row.get("case_id") or "").strip()
        if case_id and case_id not in latest:
            item = dict(row)
            item["payload"] = _json_load_object(item.get("payload_json"))
            latest[case_id] = item
    return latest


def _latest_connector_by_case(engine: Any, case_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not case_ids:
        return {}
    latest: dict[str, dict[str, Any]] = {}
    for case_id in case_ids:
        runs = list_connector_runs_for_case(engine, case_id)
        if runs:
            latest[case_id] = dict(runs[0])
    return latest


def _queue_attention(
    case_row: dict[str, Any],
    *,
    latest_approval: dict[str, Any] | None,
    latest_connector: dict[str, Any] | None,
    task_stats: dict[str, Any] | None,
) -> dict[str, Any]:
    status = str(case_row.get("status") or "").upper()
    blocked_reason = _clean_text(case_row.get("blocked_reason"))
    owner = _clean_text(case_row.get("owner"))
    approval_pending = str(case_row.get("approval_state") or "").lower() == "pending"
    connector_status = str((latest_connector or {}).get("status") or "").upper()
    connector_attention = connector_status in {"FAILED", "SKIPPED"}
    open_task_count = int((task_stats or {}).get("open_task_count") or 0)
    overdue_task_count = int((task_stats or {}).get("overdue_task_count") or 0)

    waiting_on: str | None = None
    if connector_attention:
        waiting_on = f"connector_{connector_status.lower()}"
    elif approval_pending:
        waiting_on = "approval_decision"
    elif status == "BLOCKED":
        waiting_on = blocked_reason or "operator_follow_up"
    elif not owner and status in ACTIVE_CASE_STATUSES:
        waiting_on = "assignment"
    elif overdue_task_count > 0:
        waiting_on = "overdue_task_follow_up"
    elif open_task_count > 0 and status in ACTIVE_CASE_STATUSES:
        waiting_on = "runbook_follow_up"

    attention_level = "resolved"
    if status not in RESOLVED_CASE_STATUSES:
        if case_row.get("sla_breached") or connector_attention:
            attention_level = "critical"
        elif status == "BLOCKED" or approval_pending or overdue_task_count > 0:
            attention_level = "high"
        elif case_row.get("sla_warning") or not owner:
            attention_level = "warning"
        else:
            attention_level = "normal"

    return {
        "approval_pending": approval_pending,
        "latest_approval_status": str((latest_approval or {}).get("status") or "").upper() or None,
        "latest_approval_id": (latest_approval or {}).get("approval_id"),
        "connector_status": connector_status or None,
        "connector_attention": connector_attention,
        "blocked_reason": blocked_reason,
        "unassigned": not owner,
        "open_task_count": open_task_count,
        "done_task_count": int((task_stats or {}).get("done_task_count") or 0),
        "overdue_task_count": overdue_task_count,
        "task_count": int((task_stats or {}).get("task_count") or 0),
        "waiting_on": waiting_on,
        "attention_level": attention_level,
    }


def _queue_sort_key(case_row: dict[str, Any]) -> tuple[Any, ...]:
    status = str(case_row.get("status") or "").upper()
    attention_level = str(case_row.get("attention_level") or "normal").lower()
    priority = str(case_row.get("priority") or "medium").lower()
    attention_rank = {"critical": 0, "high": 1, "warning": 2, "normal": 3, "resolved": 4}.get(attention_level, 3)
    status_rank = {
        "BLOCKED": 0,
        "AWAITING_APPROVAL": 1,
        "IN_PROGRESS": 2,
        "OPEN": 3,
        "RESOLVED": 4,
        "CLOSED": 5,
    }.get(status, 3)
    age_rank = -(float(case_row.get("age_hours") or 0.0))
    activity_value = str(case_row.get("last_activity_at") or "")
    return (
        attention_rank,
        status_rank,
        -_priority_rank(priority),
        -int(case_row.get("overdue_task_count") or 0),
        age_rank,
        activity_value,
    )


def _queue_summary_from_cases(cases: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "total": len(cases),
        "open": 0,
        "in_progress": 0,
        "blocked": 0,
        "awaiting_approval": 0,
        "resolved": 0,
        "sla_breached": 0,
        "sla_warning": 0,
        "unassigned": 0,
        "connector_attention": 0,
        "overdue_task_cases": 0,
        "high_priority_active": 0,
    }
    for item in cases:
        status = str(item.get("status") or "").upper()
        if status == "OPEN":
            summary["open"] += 1
        elif status == "IN_PROGRESS":
            summary["in_progress"] += 1
        elif status == "BLOCKED":
            summary["blocked"] += 1
        elif status == "AWAITING_APPROVAL":
            summary["awaiting_approval"] += 1
        elif status in RESOLVED_CASE_STATUSES:
            summary["resolved"] += 1
        if item.get("sla_breached"):
            summary["sla_breached"] += 1
        if item.get("sla_warning"):
            summary["sla_warning"] += 1
        if item.get("unassigned") and status in ACTIVE_CASE_STATUSES:
            summary["unassigned"] += 1
        if item.get("connector_attention"):
            summary["connector_attention"] += 1
        if int(item.get("overdue_task_count") or 0) > 0:
            summary["overdue_task_cases"] += 1
        if status in ACTIVE_CASE_STATUSES and _priority_rank(str(item.get("priority") or "medium")) >= _priority_rank("high"):
            summary["high_priority_active"] += 1
    return summary


def list_cases(
    engine: Any,
    *,
    merchant_id: str | None = None,
    lane: str | None = None,
    status: str | None = None,
    owner: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    where_parts: list[str] = ["1 = 1"]
    params: dict[str, Any] = {"limit": int(limit)}
    if merchant_id:
        where_parts.append("merchant_id = :merchant_id")
        params["merchant_id"] = merchant_id
    if lane:
        where_parts.append("lane = :lane")
        params["lane"] = str(lane).strip().lower()
    if status:
        normalized = str(status).strip().upper()
        if normalized == "ACTIVE":
            where_parts.append("status IN ('OPEN', 'IN_PROGRESS', 'BLOCKED', 'AWAITING_APPROVAL')")
        else:
            where_parts.append("status = :status")
            params["status"] = normalized
    if owner:
        where_parts.append("owner = :owner")
        params["owner"] = owner
    query = text(
        f"""
        SELECT {_ops_case_select_clause(bound_evidence_json=True)}
        FROM ops_cases
        WHERE {' AND '.join(where_parts)}
        ORDER BY
            CASE priority
                WHEN 'critical' THEN 1
                WHEN 'high' THEN 2
                WHEN 'medium' THEN 3
                ELSE 4
            END,
            last_activity_at DESC,
            created_at DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    now = _utc_now()
    cases = [_serialize_case_row(dict(row), now=now) for row in rows]
    case_ids = [str(item.get("case_id") or "").strip() for item in cases if str(item.get("case_id") or "").strip()]
    task_stats = _task_stats_by_case(engine, case_ids, now=now)
    latest_approvals = _latest_approval_by_case(engine, case_ids)
    latest_connectors = _latest_connector_by_case(engine, case_ids)
    enriched_cases: list[dict[str, Any]] = []
    for item in cases:
        case_id = str(item.get("case_id") or "").strip()
        queue_state = _queue_attention(
            item,
            latest_approval=latest_approvals.get(case_id),
            latest_connector=latest_connectors.get(case_id),
            task_stats=task_stats.get(case_id),
        )
        enriched_cases.append({**item, **queue_state})
    enriched_cases.sort(key=_queue_sort_key)
    return {"cases": enriched_cases, "queue_summary": _queue_summary_from_cases(enriched_cases)}


def _find_open_case_by_source(
    conn: Any,
    *,
    merchant_id: str,
    lane: str,
    source: str,
    source_ref: str,
) -> dict[str, Any] | None:
    query = text(
        f"""
        SELECT {_ops_case_select_clause(bound_evidence_json=True)}
        FROM ops_cases
        WHERE merchant_id = :merchant_id
          AND lane = :lane
          AND source = :source
          AND source_ref = :source_ref
          AND status IN ('OPEN', 'IN_PROGRESS', 'BLOCKED', 'AWAITING_APPROVAL')
        ORDER BY updated_at DESC
        LIMIT 1
        """
    )
    return conn.execute(
        query,
        {
            "merchant_id": merchant_id,
            "lane": lane,
            "source": source,
            "source_ref": source_ref,
        },
    ).mappings().first()


def create_case(
    engine: Any,
    *,
    merchant_id: str,
    lane: str,
    case_type: str,
    title: str,
    summary: str,
    actor_id: str,
    actor_role: str,
    terminal_id: str | None = None,
    priority: str = "medium",
    severity: str | None = None,
    owner: str | None = None,
    source: str = "manual",
    source_ref: str | None = None,
    source_payload: dict[str, Any] | None = None,
    evidence_ids: list[str] | None = None,
    links: list[dict[str, Any]] | None = None,
    runbook_code: str | None = None,
    tasks: list[dict[str, Any]] | None = None,
    due_at: str | None = None,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    normalized_lane = str(lane or "operations").strip().lower()
    normalized_source = str(source or "manual").strip().lower()
    normalized_priority = str(priority or "medium").strip().lower()
    normalized_status = "OPEN"
    runbook = runbook_for_case_type(case_type)
    runbook_code = str(runbook_code or runbook.code).strip()
    normalized_source_payload = _normalized_source_payload(source_payload)
    normalized_evidence_ids = normalize_evidence_ids(evidence_ids)

    with engine.begin() as conn:
        existing = None
        if source_ref:
            existing = _find_open_case_by_source(
                conn,
                merchant_id=merchant_id,
                lane=normalized_lane,
                source=normalized_source,
                source_ref=source_ref,
            )
        if existing:
            _insert_case_event(
                conn,
                case_id=str(existing["case_id"]),
                event_type="case_reused",
                actor_id=actor_id,
                actor_role=actor_role,
                body="Existing open case reused for the same source reference.",
                metadata={"source": normalized_source, "source_ref": source_ref},
            )
            return {"created": False, "case_id": str(existing["case_id"]), "reused": True}

        case_id = _new_id("case")
        opened_at = _iso_now()
        due_value = due_at or _compute_due_at(normalized_priority)
        params = {
            "case_id": case_id,
            "merchant_id": merchant_id,
            "terminal_id": _clean_text(terminal_id),
            "lane": normalized_lane,
            "case_type": str(case_type or "manual_ops_review").strip().lower(),
            "title": str(title or "Untitled ops case").strip(),
            "summary": str(summary or "").strip(),
            "status": normalized_status,
            "priority": normalized_priority,
            "severity": _clean_text(severity),
            "owner": _clean_text(owner),
            "source": normalized_source,
            "source_ref": _clean_text(source_ref),
            "source_payload_json": _json_dump(normalized_source_payload),
            "evidence_ids_json": _json_dump(normalized_evidence_ids),
            "links_json": _json_dump(links or []),
            "approval_state": "not_requested",
            "runbook_code": runbook_code,
            "blocked_reason": None,
            "opened_at": opened_at,
            "due_at": due_value,
            "resolved_at": None,
            "last_activity_at": opened_at,
            "updated_at": opened_at,
            "created_at": opened_at,
        }
        conn.execute(
            text(
                """
                INSERT INTO ops_cases (
                    case_id, merchant_id, terminal_id, lane, case_type, title, summary, status, priority, severity,
                    owner, source, source_ref, source_payload_json, evidence_ids_json, links_json, approval_state,
                    runbook_code, blocked_reason, opened_at, due_at, resolved_at, last_activity_at, updated_at, created_at
                ) VALUES (
                    :case_id, :merchant_id, :terminal_id, :lane, :case_type, :title, :summary, :status, :priority, :severity,
                    :owner, :source, :source_ref, :source_payload_json, :evidence_ids_json, :links_json, :approval_state,
                    :runbook_code, :blocked_reason, :opened_at, :due_at, :resolved_at, :last_activity_at, :updated_at, :created_at
                )
                """
            ),
            params,
        )
        _insert_case_event(
            conn,
            case_id=case_id,
            event_type="case_created",
            actor_id=actor_id,
            actor_role=actor_role,
            body=f"Case opened in {normalized_lane} lane.",
            metadata={"case_type": params["case_type"], "source": normalized_source},
        )
        for task in tasks or []:
            _insert_case_task(
                conn,
                case_id=case_id,
                title=str(task.get("title") or "").strip(),
                description=str(task.get("description") or "").strip(),
                priority=str(task.get("priority") or normalized_priority).strip().lower(),
                owner=_clean_text(task.get("owner")),
                due_at=_clean_text(task.get("due_at")) or due_value,
                metadata=dict(task.get("metadata") or {}),
            )
    return {"created": True, "case_id": case_id, "reused": False}


def upsert_case_from_source(
    engine: Any,
    *,
    merchant_id: str,
    lane: str,
    case_type: str,
    title: str,
    summary: str,
    actor_id: str,
    actor_role: str,
    terminal_id: str | None = None,
    priority: str = "medium",
    severity: str | None = None,
    owner: str | None = None,
    source: str = "proactive",
    source_ref: str | None = None,
    source_payload: dict[str, Any] | None = None,
    evidence_ids: list[str] | None = None,
    links: list[dict[str, Any]] | None = None,
    runbook_code: str | None = None,
    tasks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    normalized_lane = str(lane or "operations").strip().lower()
    normalized_source = str(source or "proactive").strip().lower()
    normalized_priority = str(priority or "medium").strip().lower()
    normalized_source_payload = _normalized_source_payload(source_payload)
    normalized_evidence_ids = normalize_evidence_ids(evidence_ids)
    with engine.begin() as conn:
        existing = None
        if source_ref:
            existing = _find_open_case_by_source(
                conn,
                merchant_id=merchant_id,
                lane=normalized_lane,
                source=normalized_source,
                source_ref=source_ref,
            )
        if not existing:
            return create_case(
                engine,
                merchant_id=merchant_id,
                lane=normalized_lane,
                case_type=case_type,
                title=title,
                summary=summary,
                actor_id=actor_id,
                actor_role=actor_role,
                terminal_id=terminal_id,
                priority=normalized_priority,
                severity=severity,
                owner=owner,
                source=normalized_source,
                source_ref=source_ref,
                source_payload=source_payload,
                evidence_ids=evidence_ids,
                links=links,
                runbook_code=runbook_code,
                tasks=tasks,
            )

        case_id = str(existing["case_id"])
        existing_evidence = normalize_evidence_ids(existing.get("evidence_ids_json"))
        existing_links = _case_links_from_json(existing.get("links_json"))
        merged_priority = _priority_max(str(existing.get("priority") or "medium"), normalized_priority)
        now = _iso_now()
        merged_evidence = merge_evidence_ids(existing_evidence, normalized_evidence_ids)
        merged_links = _merge_links(existing_links, links or [])
        next_title = str(title or existing.get("title") or "").strip() or str(existing.get("title") or "Ops case")
        next_summary = str(summary or existing.get("summary") or "").strip() or str(existing.get("summary") or "")
        next_terminal = _clean_text(terminal_id) or _clean_text(existing.get("terminal_id"))
        next_runbook = str(runbook_code or existing.get("runbook_code") or runbook_for_case_type(case_type).code).strip()

        conn.execute(
            text(
                """
                UPDATE ops_cases
                SET title = :title,
                    summary = :summary,
                    priority = :priority,
                    severity = COALESCE(:severity, severity),
                    owner = COALESCE(owner, :owner),
                    terminal_id = :terminal_id,
                    source_payload_json = :source_payload_json,
                    evidence_ids_json = :evidence_ids_json,
                    links_json = :links_json,
                    runbook_code = :runbook_code,
                    last_activity_at = :now,
                    updated_at = :now
                WHERE case_id = :case_id
                """
            ),
            {
                "title": next_title,
                "summary": next_summary,
                "priority": merged_priority,
                "severity": _clean_text(severity),
                "owner": _clean_text(owner),
                "terminal_id": next_terminal,
                "source_payload_json": _json_dump(normalized_source_payload),
                "evidence_ids_json": _json_dump(merged_evidence),
                "links_json": _json_dump(merged_links),
                "runbook_code": next_runbook,
                "now": now,
                "case_id": case_id,
            },
        )
        _insert_case_event(
            conn,
            case_id=case_id,
            event_type="source_refreshed",
            actor_id=actor_id,
            actor_role=actor_role,
            body="Background source refreshed the active case.",
            metadata={"source": normalized_source, "source_ref": source_ref},
        )
    return {"created": False, "case_id": case_id, "reused": True, "refreshed": True}


def list_approvals(
    engine: Any,
    *,
    merchant_id: str | None = None,
    lane: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    ensure_ops_schema(engine)
    where_parts = ["1 = 1"]
    params: dict[str, Any] = {"limit": int(limit)}
    if status:
        where_parts.append("a.status = :status")
        params["status"] = str(status).strip().upper()
    if merchant_id:
        where_parts.append("c.merchant_id = :merchant_id")
        params["merchant_id"] = merchant_id
    if lane:
        where_parts.append("c.lane = :lane")
        params["lane"] = str(lane).strip().lower()
    query = text(
        f"""
        SELECT a.*, c.merchant_id, c.lane, c.title AS case_title, c.status AS case_status
        FROM ops_approvals a
        JOIN ops_cases c ON c.case_id = a.case_id
        WHERE {' AND '.join(where_parts)}
        ORDER BY a.requested_at DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    approvals: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["payload"] = _json_load_object(item.get("payload_json"))
        approvals.append(item)
    return approvals


def get_case_detail(engine: Any, case_id: str) -> dict[str, Any] | None:
    ensure_ops_schema(engine)
    with engine.connect() as conn:
        case_row = conn.execute(
            text(
                f"""
                SELECT {_ops_case_select_clause(bound_evidence_json=True)}
                FROM ops_cases
                WHERE case_id = :case_id
                LIMIT 1
                """
            ),
            {"case_id": case_id},
        ).mappings().first()
        if not case_row:
            return None
        memory_row = conn.execute(
            text("SELECT * FROM ops_case_memory WHERE case_id = :case_id LIMIT 1"),
            {"case_id": case_id},
        ).mappings().first()
        task_rows = conn.execute(
            text("SELECT * FROM ops_tasks WHERE case_id = :case_id ORDER BY created_at ASC"),
            {"case_id": case_id},
        ).mappings().all()
        event_rows = conn.execute(
            text("SELECT * FROM ops_case_events WHERE case_id = :case_id ORDER BY created_at ASC"),
            {"case_id": case_id},
        ).mappings().all()
        approval_rows = conn.execute(
            text("SELECT * FROM ops_approvals WHERE case_id = :case_id ORDER BY requested_at DESC"),
            {"case_id": case_id},
        ).mappings().all()
    case_payload = _serialize_case_row(dict(case_row))
    tasks = []
    for row in task_rows:
        item = dict(row)
        item["metadata"] = _task_metadata_from_json(item.get("metadata_json"))
        tasks.append(item)
    timeline = []
    for row in event_rows:
        item = dict(row)
        item["metadata"] = _event_metadata_from_json(item.get("metadata_json"))
        timeline.append(item)
    approvals = []
    for row in approval_rows:
        item = dict(row)
        item["payload"] = _json_load_object(item.get("payload_json"))
        approvals.append(item)
    connector_runs = list_connector_runs_for_case(engine, case_id)
    task_stats = _task_stats_by_case(engine, [case_id]).get(case_id)
    latest_approval = approvals[0] if approvals else None
    latest_connector = connector_runs[0] if connector_runs else None
    case_payload = {
        **case_payload,
        **_queue_attention(
            case_payload,
            latest_approval=latest_approval,
            latest_connector=latest_connector,
            task_stats=task_stats,
        ),
    }
    return {
        "case": case_payload,
        "tasks": tasks,
        "timeline": timeline,
        "approvals": approvals,
        "connector_runs": connector_runs,
        "memory": {
            **_normalize_case_memory(memory_row.get("memory_json") if memory_row else {}),
            "updated_at": memory_row.get("updated_at") if memory_row else None,
            "created_at": memory_row.get("created_at") if memory_row else None,
        },
    }


def _load_support_history_fixture(merchant_id: str) -> dict[str, Any]:
    try:
        payload = json.loads(SUPPORT_HISTORY_FIXTURE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if str(payload.get("merchant_id") or "").strip() != merchant_id:
        return {}
    return payload


def _support_case_snapshot(case_row: dict[str, Any]) -> dict[str, Any]:
    source_payload = case_row.get("source_payload") if isinstance(case_row.get("source_payload"), dict) else {}
    return {
        "case_id": case_row.get("case_id"),
        "ticket_id": source_payload.get("ticket_id") or case_row.get("source_ref"),
        "lane": case_row.get("lane"),
        "case_type": case_row.get("case_type"),
        "title": case_row.get("title"),
        "status": case_row.get("status"),
        "priority": case_row.get("priority"),
        "channel": source_payload.get("channel"),
        "opened_at": case_row.get("opened_at"),
        "last_activity_at": case_row.get("last_activity_at"),
        "waiting_on": case_row.get("waiting_on"),
        "approval_state": case_row.get("approval_state"),
        "evidence_ids": [str(item) for item in (case_row.get("evidence_ids") or []) if str(item or "").strip()],
    }


def _support_fixture_case_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "case_id": None,
        "ticket_id": _clean_text(item.get("ticket_id")),
        "lane": "support",
        "case_type": "merchant_support_case",
        "title": _clean_text(item.get("summary")) or _clean_text(item.get("category")) or "Support history case",
        "status": _clean_text(item.get("status")),
        "priority": None,
        "channel": _clean_text(item.get("channel")),
        "opened_at": _clean_text(item.get("opened_at")),
        "last_activity_at": _clean_text(item.get("last_updated_at")) or _clean_text(item.get("opened_at")),
        "waiting_on": None,
        "approval_state": None,
        "evidence_ids": [],
        "category": _clean_text(item.get("category")),
    }


def _normalize_support_contacts(value: Any) -> list[dict[str, Any]]:
    raw_items = value if isinstance(value, list) else ([value] if isinstance(value, dict) else [])
    contacts: list[dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        normalized = {
            "contact_id": _clean_text(item.get("contact_id")) or _clean_text(item.get("id")),
            "name": _clean_text(item.get("name")),
            "role": _clean_text(item.get("role")),
            "channel": _clean_text(item.get("channel")) or _clean_text(item.get("preferred_channel")),
            "email": _clean_text(item.get("email")),
            "phone": _clean_text(item.get("phone")),
            "last_contact_at": _clean_text(item.get("last_contact_at")) or _clean_text(item.get("contacted_at")),
            "notes": _clean_text(item.get("notes")) or _clean_text(item.get("summary")),
        }
        if any(value is not None for value in normalized.values()):
            contacts.append(normalized)
    return contacts


def _normalize_support_escalations(value: Any) -> list[dict[str, Any]]:
    raw_items = value if isinstance(value, list) else ([value] if isinstance(value, dict) else [])
    escalations: list[dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        normalized = {
            "escalation_id": _clean_text(item.get("escalation_id")) or _clean_text(item.get("id")),
            "target_team": _clean_text(item.get("target_team")) or _clean_text(item.get("team")),
            "status": _clean_text(item.get("status")),
            "opened_at": _clean_text(item.get("opened_at")),
            "updated_at": _clean_text(item.get("updated_at")) or _clean_text(item.get("last_updated_at")),
            "summary": _clean_text(item.get("summary")) or _clean_text(item.get("notes")),
        }
        if any(value is not None for value in normalized.values()):
            escalations.append(normalized)
    return escalations


def _merge_support_source(left: str, right: str) -> str:
    labels = [item for item in (left, right) if item and item != "none"]
    if not labels:
        return "none"
    unique = []
    for item in labels:
        if item not in unique:
            unique.append(item)
    return "+".join(unique)


def list_related_support_cases(
    engine: Any,
    *,
    merchant_id: str,
    exclude_case_id: str | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    ensure_ops_schema(engine)
    normalized_limit = max(1, min(int(limit or 5), 10))
    where_parts = [
        "merchant_id = :merchant_id",
        "(lane = 'support' OR case_type IN ('merchant_support_case', 'chargeback_review', 'refund_exception'))",
    ]
    params: dict[str, Any] = {"merchant_id": merchant_id, "limit": normalized_limit}
    if exclude_case_id:
        where_parts.append("case_id != :exclude_case_id")
        params["exclude_case_id"] = exclude_case_id
    query = text(
        f"""
        SELECT *
        FROM ops_cases
        WHERE {' AND '.join(where_parts)}
        ORDER BY last_activity_at DESC, opened_at DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    now = _utc_now()
    cases = [_serialize_case_row(dict(row), now=now) for row in rows]
    case_ids = [str(item.get("case_id") or "").strip() for item in cases if str(item.get("case_id") or "").strip()]
    task_stats = _task_stats_by_case(engine, case_ids, now=now)
    latest_approvals = _latest_approval_by_case(engine, case_ids)
    latest_connectors = _latest_connector_by_case(engine, case_ids)
    snapshots: list[dict[str, Any]] = []
    for item in cases:
        case_id = str(item.get("case_id") or "").strip()
        enriched = {
            **item,
            **_queue_attention(
                item,
                latest_approval=latest_approvals.get(case_id),
                latest_connector=latest_connectors.get(case_id),
                task_stats=task_stats.get(case_id),
            ),
        }
        snapshots.append(_support_case_snapshot(enriched))
    return snapshots


def get_support_case_history_context(
    engine: Any,
    *,
    merchant_id: str,
    case_id: str,
    limit: int = 5,
) -> dict[str, Any]:
    detail = get_case_detail(engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    current_case = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    local_cases = list_related_support_cases(
        engine,
        merchant_id=merchant_id,
        exclude_case_id=case_id,
        limit=limit,
    )
    fixture = _load_support_history_fixture(merchant_id)
    fixture_cases = [_support_fixture_case_snapshot(item) for item in (fixture.get("cases") or []) if isinstance(item, dict)]
    cases = local_cases or fixture_cases[:limit]
    source = "ops_case_history" if local_cases else ("fixture_fallback" if fixture_cases else "none")
    open_related_case_count = sum(
        1
        for item in cases
        if str(item.get("status") or "").upper() not in RESOLVED_CASE_STATUSES
    )
    return {
        "case_id": case_id,
        "current_case": _support_case_snapshot(current_case),
        "recent_cases": cases,
        "related_case_count": len(cases),
        "open_related_case_count": open_related_case_count,
        "source": source,
    }


def get_contact_and_escalation_context(
    engine: Any,
    *,
    merchant_id: str,
    case_id: str,
) -> dict[str, Any]:
    detail = get_case_detail(engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    source_payload = case_row.get("source_payload") if isinstance(case_row.get("source_payload"), dict) else {}
    approvals = detail.get("approvals") if isinstance(detail.get("approvals"), list) else []

    raw_contacts: list[Any] = []
    for key in ("contacts", "customer_contact", "merchant_contact", "contact"):
        value = source_payload.get(key)
        if isinstance(value, list):
            raw_contacts.extend(value)
        elif isinstance(value, dict):
            raw_contacts.append(value)
    contacts = _normalize_support_contacts(raw_contacts)
    source = "ops_case" if contacts else ""

    escalations: list[dict[str, Any]] = []
    latest_approval = approvals[0] if approvals else None
    if latest_approval:
        escalations.append(
            {
                "escalation_id": latest_approval.get("approval_id"),
                "target_team": "approval_queue",
                "status": str(latest_approval.get("status") or "").upper() or None,
                "opened_at": latest_approval.get("requested_at"),
                "updated_at": latest_approval.get("reviewed_at") or latest_approval.get("requested_at"),
                "summary": latest_approval.get("payload_summary"),
                "action_type": latest_approval.get("action_type"),
            }
        )
        source = _merge_support_source(source, "ops_case")

    fixture = _load_support_history_fixture(merchant_id)
    if not contacts:
        contacts = _normalize_support_contacts(fixture.get("contacts"))
        if contacts:
            source = _merge_support_source(source, "fixture_fallback")
    if not escalations:
        escalations = _normalize_support_escalations(fixture.get("escalations"))
        if escalations:
            source = _merge_support_source(source, "fixture_fallback")

    return {
        "case_id": case_id,
        "ticket_id": source_payload.get("ticket_id") or case_row.get("source_ref"),
        "current_owner": case_row.get("owner"),
        "approval_state": case_row.get("approval_state"),
        "contacts": contacts,
        "escalations": escalations,
        "latest_approval": latest_approval,
        "source": source or "none",
    }


def get_customer_service_context(
    engine: Any,
    *,
    merchant_id: str,
    case_id: str,
) -> dict[str, Any]:
    detail = get_case_detail(engine, case_id)
    if detail is None:
        raise LookupError("case not found")
    case_row = detail.get("case") if isinstance(detail.get("case"), dict) else {}
    source_payload = case_row.get("source_payload") if isinstance(case_row.get("source_payload"), dict) else {}
    history = get_support_case_history_context(engine, merchant_id=merchant_id, case_id=case_id)
    contact_context = get_contact_and_escalation_context(engine, merchant_id=merchant_id, case_id=case_id)

    contacts = contact_context.get("contacts") if isinstance(contact_context.get("contacts"), list) else []
    escalations = contact_context.get("escalations") if isinstance(contact_context.get("escalations"), list) else []
    preferred_channel = (
        _clean_text(source_payload.get("channel"))
        or _clean_text((contacts[0] or {}).get("channel") if contacts else None)
        or _clean_text((history.get("recent_cases") or [{}])[0].get("channel") if history.get("recent_cases") else None)
    )
    open_escalation_count = sum(
        1
        for item in escalations
        if str(item.get("status") or "").upper() not in {"APPROVED", "REJECTED", "RESOLVED", "CLOSED"}
    )

    return {
        "case_id": case_id,
        "ticket_reference": source_payload.get("ticket_id") or case_row.get("source_ref"),
        "current_case": _support_case_snapshot(case_row),
        "recent_support_cases": history.get("recent_cases") or [],
        "related_case_count": int(history.get("related_case_count") or 0),
        "open_related_case_count": int(history.get("open_related_case_count") or 0),
        "preferred_channel": preferred_channel,
        "contacts": contacts,
        "escalations": escalations,
        "open_escalation_count": open_escalation_count,
        "history_source": history.get("source"),
        "contact_source": contact_context.get("source"),
        "source": _merge_support_source(str(history.get("source") or ""), str(contact_context.get("source") or "")),
    }


def get_case_memory(engine: Any, case_id: str) -> dict[str, Any]:
    ensure_ops_schema(engine)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM ops_case_memory WHERE case_id = :case_id LIMIT 1"),
            {"case_id": case_id},
        ).mappings().first()
    if not row:
        return {
            **_empty_case_memory(),
            "updated_at": None,
            "created_at": None,
        }
    return {
        **_normalize_case_memory(row.get("memory_json")),
        "updated_at": row.get("updated_at"),
        "created_at": row.get("created_at"),
    }


def upsert_case_memory(
    engine: Any,
    *,
    case_id: str,
    memory: dict[str, Any] | None,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    now = _iso_now()
    payload = _normalize_case_memory(memory or {})
    with engine.begin() as conn:
        case_exists = conn.execute(
            text("SELECT case_id FROM ops_cases WHERE case_id = :case_id LIMIT 1"),
            {"case_id": case_id},
        ).scalar()
        if not case_exists:
            raise LookupError("case not found")
        existing = conn.execute(
            text("SELECT created_at FROM ops_case_memory WHERE case_id = :case_id LIMIT 1"),
            {"case_id": case_id},
        ).mappings().first()
        if existing:
            conn.execute(
                text(
                    """
                    UPDATE ops_case_memory
                    SET memory_json = :memory_json,
                        updated_at = :updated_at
                    WHERE case_id = :case_id
                    """
                ),
                {
                    "case_id": case_id,
                    "memory_json": _json_dump(payload),
                    "updated_at": now,
                },
            )
            created_at = existing.get("created_at")
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO ops_case_memory (
                        case_id, memory_json, updated_at, created_at
                    ) VALUES (
                        :case_id, :memory_json, :updated_at, :created_at
                    )
                    """
                ),
                {
                    "case_id": case_id,
                    "memory_json": _json_dump(payload),
                    "updated_at": now,
                    "created_at": now,
                },
            )
            created_at = now
    return {
        **payload,
        "updated_at": now,
        "created_at": created_at,
    }


def update_case_memory_context(
    engine: Any,
    *,
    case_id: str,
    actor_id: str,
    actor_role: str,
    settlement_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    evidence_ids: list[str] | None = None,
    clear_pinned_context: bool = False,
    clear_window: bool = False,
    clear_evidence: bool = False,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    now = _iso_now()
    with engine.begin() as conn:
        case_row = conn.execute(
            text(
                f"""
                SELECT {_ops_case_select_clause(bound_evidence_json=True)}
                FROM ops_cases
                WHERE case_id = :case_id
                LIMIT 1
                """
            ),
            {"case_id": case_id},
        ).mappings().first()
        if not case_row:
            raise LookupError("case not found")

        memory_row = conn.execute(
            text("SELECT * FROM ops_case_memory WHERE case_id = :case_id LIMIT 1"),
            {"case_id": case_id},
        ).mappings().first()
        existing_memory = _normalize_case_memory(memory_row.get("memory_json") if memory_row else {})

        pinned_entities = dict(existing_memory.get("pinned_entities") or {})
        pinned_entities["merchant_id"] = str(case_row.get("merchant_id") or "").strip()
        pinned_entities["case_type"] = str(case_row.get("case_type") or "").strip()
        terminal_id = _clean_text(case_row.get("terminal_id"))
        if terminal_id is not None:
            pinned_entities["terminal_id"] = terminal_id
        else:
            pinned_entities.pop("terminal_id", None)

        if clear_pinned_context:
            pinned_entities.pop("settlement_id", None)
        else:
            normalized_settlement = _clean_text(settlement_id)
            if settlement_id is not None:
                if normalized_settlement is None:
                    pinned_entities.pop("settlement_id", None)
                else:
                    pinned_entities["settlement_id"] = normalized_settlement

        active_window = dict(existing_memory.get("active_window") or {})
        if clear_window:
            active_window = {}
        elif start_date is not None or end_date is not None:
            normalized_start = _clean_text(start_date)
            normalized_end = _clean_text(end_date)
            if bool(normalized_start) != bool(normalized_end):
                raise ValueError("start_date and end_date must both be provided together")
            if normalized_start and normalized_end:
                active_window = {
                    "start_date": normalized_start,
                    "end_date": normalized_end,
                    "reason": "operator_pinned_window",
                }
            else:
                active_window = {}

        confirmed_evidence_ids = normalize_evidence_ids(existing_memory.get("confirmed_evidence_ids"))
        if clear_evidence:
            confirmed_evidence_ids = []
        elif evidence_ids is not None:
            confirmed_evidence_ids = normalize_evidence_ids(evidence_ids)

        payload = {
            "pinned_entities": pinned_entities,
            "active_window": active_window,
            "confirmed_evidence_ids": confirmed_evidence_ids,
            "latest_summary": dict(existing_memory.get("latest_summary") or {}),
            "latest_tool_calls": [dict(item) for item in (existing_memory.get("latest_tool_calls") or []) if isinstance(item, dict)],
        }

        if memory_row:
            conn.execute(
                text(
                    """
                    UPDATE ops_case_memory
                    SET memory_json = :memory_json,
                        updated_at = :updated_at
                    WHERE case_id = :case_id
                    """
                ),
                {
                    "case_id": case_id,
                    "memory_json": _json_dump(payload),
                    "updated_at": now,
                },
            )
            created_at = memory_row.get("created_at")
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO ops_case_memory (
                        case_id, memory_json, updated_at, created_at
                    ) VALUES (
                        :case_id, :memory_json, :updated_at, :created_at
                    )
                    """
                ),
                {
                    "case_id": case_id,
                    "memory_json": _json_dump(payload),
                    "updated_at": now,
                    "created_at": now,
                },
            )
            created_at = now

        conn.execute(
            text(
                """
                UPDATE ops_cases
                SET last_activity_at = :now,
                    updated_at = :now
                WHERE case_id = :case_id
                """
            ),
            {"case_id": case_id, "now": now},
        )
        _insert_case_event(
            conn,
            case_id=case_id,
            event_type="memory_updated",
            actor_id=actor_id,
            actor_role=actor_role,
            body="Operator updated pinned case context.",
            metadata={
                "settlement_id": payload["pinned_entities"].get("settlement_id"),
                "window": payload["active_window"],
                "evidence_count": len(payload["confirmed_evidence_ids"]),
                "clear_pinned_context": bool(clear_pinned_context),
                "clear_window": bool(clear_window),
                "clear_evidence": bool(clear_evidence),
            },
        )
    return {
        **payload,
        "updated_at": now,
        "created_at": created_at,
    }


def assign_case(
    engine: Any,
    *,
    case_id: str,
    owner: str,
    actor_id: str,
    actor_role: str,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    owner_value = _clean_text(owner)
    if owner_value is None:
        raise ValueError("owner is required")
    now = _iso_now()
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                UPDATE ops_cases
                SET owner = :owner, status = CASE WHEN status = 'OPEN' THEN 'IN_PROGRESS' ELSE status END,
                    last_activity_at = :now, updated_at = :now
                WHERE case_id = :case_id
                """
            ),
            {"owner": owner_value, "case_id": case_id, "now": now},
        )
        if not getattr(result, "rowcount", 0):
            raise LookupError("case not found")
        _insert_case_event(
            conn,
            case_id=case_id,
            event_type="assignment",
            actor_id=actor_id,
            actor_role=actor_role,
            body=f"Assigned case to {owner_value}.",
            metadata={"owner": owner_value},
        )
    return {"updated": True, "case_id": case_id, "owner": owner_value}


def add_case_note(
    engine: Any,
    *,
    case_id: str,
    body: str,
    actor_id: str,
    actor_role: str,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    note_body = _clean_text(body)
    if note_body is None:
        raise ValueError("note body is required")
    now = _iso_now()
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                UPDATE ops_cases
                SET last_activity_at = :now, updated_at = :now
                WHERE case_id = :case_id
                """
            ),
            {"case_id": case_id, "now": now},
        )
        if not getattr(result, "rowcount", 0):
            raise LookupError("case not found")
        event = _insert_case_event(
            conn,
            case_id=case_id,
            event_type="note",
            actor_id=actor_id,
            actor_role=actor_role,
            body=note_body,
        )
    return {"updated": True, "case_id": case_id, "note": event}


def request_case_approval(
    engine: Any,
    *,
    case_id: str,
    action_type: str,
    payload_summary: str,
    payload: dict[str, Any],
    actor_id: str,
    actor_role: str,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    now = _iso_now()
    approval = {
        "approval_id": _new_id("apr"),
        "case_id": case_id,
        "status": "PENDING",
        "action_type": str(action_type or "FOLLOW_UP").strip().upper(),
        "payload_summary": str(payload_summary or "Approval requested").strip(),
        "payload_json": _json_dump(payload or {}),
        "requested_by": actor_id,
        "requested_role": actor_role,
        "requested_at": now,
        "reviewed_by": None,
        "reviewed_role": None,
        "reviewed_at": None,
        "decision_notes": None,
        "receipt_ref": None,
        "connector_status": "NOT_SENT",
    }
    with engine.begin() as conn:
        case_exists = conn.execute(text("SELECT case_id FROM ops_cases WHERE case_id = :case_id"), {"case_id": case_id}).scalar()
        if not case_exists:
            raise LookupError("case not found")
        conn.execute(
            text(
                """
                INSERT INTO ops_approvals (
                    approval_id, case_id, status, action_type, payload_summary, payload_json, requested_by, requested_role,
                    requested_at, reviewed_by, reviewed_role, reviewed_at, decision_notes, receipt_ref, connector_status
                ) VALUES (
                    :approval_id, :case_id, :status, :action_type, :payload_summary, :payload_json, :requested_by, :requested_role,
                    :requested_at, :reviewed_by, :reviewed_role, :reviewed_at, :decision_notes, :receipt_ref, :connector_status
                )
                """
            ),
            approval,
        )
        conn.execute(
            text(
                """
                UPDATE ops_cases
                SET approval_state = 'pending', status = 'AWAITING_APPROVAL', blocked_reason = NULL,
                    last_activity_at = :now, updated_at = :now
                WHERE case_id = :case_id
                """
            ),
            {"case_id": case_id, "now": now},
        )
        _insert_case_event(
            conn,
            case_id=case_id,
            event_type="approval_requested",
            actor_id=actor_id,
            actor_role=actor_role,
            body=approval["payload_summary"],
            metadata={"approval_id": approval["approval_id"], "action_type": approval["action_type"]},
        )
    return {"updated": True, "approval": approval}


def decide_approval(
    engine: Any,
    *,
    approval_id: str,
    decision: str,
    actor_id: str,
    actor_role: str,
    notes: str | None = None,
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    normalized_decision = str(decision or "").strip().upper()
    if normalized_decision not in {"APPROVED", "REJECTED"}:
        raise ValueError("decision must be APPROVED or REJECTED")
    now = _iso_now()
    case_id: str | None = None
    approval_action_type = ""
    approval_payload: dict[str, Any] = {}
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT * FROM ops_approvals WHERE approval_id = :approval_id LIMIT 1"),
            {"approval_id": approval_id},
        ).mappings().first()
        if not row:
            raise LookupError("approval not found")
        case_id = str(row["case_id"])
        approval_action_type = str(row.get("action_type") or "").strip().upper()
        approval_payload = _json_load_object(row.get("payload_json"))
        receipt_ref = _new_id("receipt") if normalized_decision == "APPROVED" else None
        connector_status = "QUEUED" if normalized_decision == "APPROVED" else "NOT_SENT"
        conn.execute(
            text(
                """
                UPDATE ops_approvals
                SET status = :status, reviewed_by = :reviewed_by, reviewed_role = :reviewed_role,
                    reviewed_at = :reviewed_at, decision_notes = :decision_notes, receipt_ref = :receipt_ref,
                    connector_status = :connector_status
                WHERE approval_id = :approval_id
                """
            ),
            {
                "status": normalized_decision,
                "reviewed_by": actor_id,
                "reviewed_role": actor_role,
                "reviewed_at": now,
                "decision_notes": _clean_text(notes),
                "receipt_ref": receipt_ref,
                "connector_status": connector_status,
                "approval_id": approval_id,
            },
        )
        case_status = "IN_PROGRESS" if normalized_decision == "APPROVED" else "BLOCKED"
        approval_state = "approved" if normalized_decision == "APPROVED" else "rejected"
        blocked_reason = None if normalized_decision == "APPROVED" else (_clean_text(notes) or "Approval was rejected and needs operator follow-up.")
        conn.execute(
            text(
                """
                UPDATE ops_cases
                SET approval_state = :approval_state, status = :status, blocked_reason = :blocked_reason,
                    last_activity_at = :now, updated_at = :now
                WHERE case_id = :case_id
                """
            ),
            {
                "approval_state": approval_state,
                "status": case_status,
                "blocked_reason": blocked_reason,
                "now": now,
                "case_id": str(row["case_id"]),
            },
        )
        _insert_case_event(
            conn,
            case_id=case_id,
            event_type="approval_decision",
            actor_id=actor_id,
            actor_role=actor_role,
            body=f"Approval {normalized_decision.lower()}.",
            metadata={"approval_id": approval_id, "decision": normalized_decision, "receipt_ref": receipt_ref},
        )

    connector_result = None
    if normalized_decision == "APPROVED" and case_id:
        connector_result = dispatch_settlement_approval(
            engine,
            approval_id=approval_id,
            case_id=case_id,
            action_type=approval_action_type,
            payload=approval_payload,
            requested_by=actor_id,
        )
        connector_status = str(connector_result.get("connector_status") or connector_status)
        receipt_ref = str(connector_result.get("receipt_ref") or receipt_ref or "")
        with engine.begin() as conn:
            next_case_status = "IN_PROGRESS"
            next_blocked_reason = None
            if connector_status in {"FAILED", "SKIPPED"}:
                next_case_status = "BLOCKED"
                next_blocked_reason = _clean_text(connector_result.get("error_message")) or (
                    "Connector dispatch needs manual follow-up."
                )
            conn.execute(
                text(
                    """
                    UPDATE ops_approvals
                    SET connector_status = :connector_status,
                        receipt_ref = :receipt_ref
                    WHERE approval_id = :approval_id
                    """
                ),
                {
                    "connector_status": connector_status,
                    "receipt_ref": receipt_ref or None,
                    "approval_id": approval_id,
                },
            )
            conn.execute(
                text(
                    """
                    UPDATE ops_cases
                    SET status = :status,
                        blocked_reason = :blocked_reason,
                        last_activity_at = :now,
                        updated_at = :now
                    WHERE case_id = :case_id
                    """
                ),
                {
                    "status": next_case_status,
                    "blocked_reason": next_blocked_reason,
                    "now": _iso_now(),
                    "case_id": case_id,
                },
            )
            _insert_case_event(
                conn,
                case_id=case_id,
                event_type="connector_dispatch",
                actor_id=actor_id,
                actor_role=actor_role,
                body=f"Settlement connector dispatch {connector_status.lower()}.",
                metadata={
                    "approval_id": approval_id,
                    "connector_status": connector_status,
                    "connector_run_id": connector_result.get("run_id"),
                    "external_ref": connector_result.get("external_ref"),
                },
            )
    return {
        "updated": True,
        "approval_id": approval_id,
        "decision": normalized_decision,
        "receipt_ref": receipt_ref,
        "connector_status": connector_status,
        "connector_result": connector_result,
    }


def resolve_case(
    engine: Any,
    *,
    case_id: str,
    actor_id: str,
    actor_role: str,
    resolution_note: str | None = None,
    status: str = "RESOLVED",
) -> dict[str, Any]:
    ensure_ops_schema(engine)
    normalized_status = str(status or "RESOLVED").strip().upper()
    if normalized_status not in RESOLVED_CASE_STATUSES:
        raise ValueError("status must be RESOLVED or CLOSED")
    now = _iso_now()
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                UPDATE ops_cases
                SET status = :status, blocked_reason = NULL, resolved_at = :now, last_activity_at = :now, updated_at = :now
                WHERE case_id = :case_id
                """
            ),
            {"status": normalized_status, "now": now, "case_id": case_id},
        )
        if not getattr(result, "rowcount", 0):
            raise LookupError("case not found")
        conn.execute(
            text(
                """
                UPDATE ops_tasks
                SET status = CASE WHEN status = 'DONE' THEN status ELSE 'DONE' END, updated_at = :now
                WHERE case_id = :case_id
                """
            ),
            {"case_id": case_id, "now": now},
        )
        _insert_case_event(
            conn,
            case_id=case_id,
            event_type="case_resolved",
            actor_id=actor_id,
            actor_role=actor_role,
            body=_clean_text(resolution_note) or f"Case {normalized_status.lower()} by operator.",
            metadata={"status": normalized_status},
        )
    return {"updated": True, "case_id": case_id, "status": normalized_status}

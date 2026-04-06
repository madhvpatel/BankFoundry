from __future__ import annotations

import datetime as dt
import json
import re
import uuid
from typing import Any

import httpx
from sqlalchemy import text

from app.data.evidence import normalize_evidence_ids
from app.data.settlements import repository as settlements_repository
from config import Config


SETTLEMENT_ACTION_PREFIXES = ("SETTLEMENT_", "PAYOUT_", "RECONCILIATION_")
SETTLEMENT_EVIDENCE_PATTERN = re.compile(r"^settlement:(?P<settlement_id>[A-Za-z0-9_-]+)$", re.IGNORECASE)


def _iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


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


def _is_supported_action(action_type: str) -> bool:
    normalized = str(action_type or "").strip().upper()
    return normalized.startswith(SETTLEMENT_ACTION_PREFIXES)


def _extract_settlement_id(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, dict):
            nested = _extract_settlement_id(
                value.get("settlement_id"),
                value.get("source_ref"),
                value.get("evidence_ids"),
            )
            if nested:
                return nested
            continue
        if isinstance(value, (list, tuple, set)):
            nested = _extract_settlement_id(*list(value))
            if nested:
                return nested
            continue
        text_value = str(value or "").strip()
        if not text_value:
            continue
        match = SETTLEMENT_EVIDENCE_PATTERN.match(text_value)
        if match:
            return match.group("settlement_id")
        if re.fullmatch(r"[A-Za-z0-9_-]+", text_value):
            return text_value
        if text_value.lower().startswith("settlement"):
            parts = re.split(r"[:#\s-]+", text_value, maxsplit=1)
            if len(parts) == 2 and parts[1].strip():
                return parts[1].strip()
    return None


def _load_case_context(engine: Any, case_id: str) -> dict[str, Any]:
    with engine.connect() as conn:
        case_row = conn.execute(
            text(
                """
                SELECT case_id, merchant_id, terminal_id, lane, case_type, title, summary,
                       evidence_ids_json, source_payload_json
                FROM ops_cases
                WHERE case_id = :case_id
                LIMIT 1
                """
            ),
            {"case_id": case_id},
        ).mappings().first()
        memory_row = conn.execute(
            text(
                """
                SELECT memory_json
                FROM ops_case_memory
                WHERE case_id = :case_id
                LIMIT 1
                """
            ),
            {"case_id": case_id},
        ).mappings().first()
    case_payload = dict(case_row) if case_row else {}
    memory_payload = _json_load_object(memory_row.get("memory_json")) if memory_row else {}
    return {
        "case": case_payload,
        "memory": memory_payload,
    }


def _enrich_payload_from_db(engine: Any, *, case_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    base_payload = dict(payload or {})
    context = _load_case_context(engine, case_id)
    case_row = context.get("case") if isinstance(context.get("case"), dict) else {}
    memory = context.get("memory") if isinstance(context.get("memory"), dict) else {}
    pinned_entities = memory.get("pinned_entities") if isinstance(memory.get("pinned_entities"), dict) else {}
    source_payload = _json_load_object(case_row.get("source_payload_json"))
    case_evidence_ids = normalize_evidence_ids(case_row.get("evidence_ids_json"))
    payload_evidence_ids = normalize_evidence_ids(base_payload.get("evidence_ids"))
    memory_evidence_ids = normalize_evidence_ids(memory.get("confirmed_evidence_ids"))

    merchant_id = (
        str(base_payload.get("merchant_id") or case_row.get("merchant_id") or pinned_entities.get("merchant_id") or "").strip()
        or None
    )
    settlement_id = _extract_settlement_id(
        base_payload.get("settlement_id"),
        pinned_entities.get("settlement_id"),
        base_payload.get("source_ref"),
        source_payload.get("settlement_id"),
        source_payload.get("source_ref"),
        payload_evidence_ids,
        case_evidence_ids,
        memory_evidence_ids,
    )

    enriched_payload = {
        **base_payload,
        "merchant_id": merchant_id,
        "settlement_id": settlement_id,
        "case_context": {
            "case_id": case_row.get("case_id") or case_id,
            "case_type": case_row.get("case_type"),
            "lane": case_row.get("lane"),
            "title": case_row.get("title"),
            "summary": case_row.get("summary"),
            "terminal_id": case_row.get("terminal_id"),
        },
        "evidence_ids": normalize_evidence_ids([payload_evidence_ids, case_evidence_ids, memory_evidence_ids]),
    }

    if merchant_id and settlement_id:
        settlement_detail = settlements_repository.get_settlement_detail(
            engine,
            merchant_id=merchant_id,
            settlement_id=settlement_id,
        )
        settlement_reconciliation = settlements_repository.get_settlement_reconciliation(
            engine,
            merchant_id=merchant_id,
            settlement_id=settlement_id,
        )
        deduction_breakdown = settlements_repository.get_deduction_breakdown(
            engine,
            merchant_id=merchant_id,
            settlement_id=settlement_id,
        )
        payout_delay = settlements_repository.get_payout_delay_context(
            engine,
            merchant_id=merchant_id,
            settlement_id=settlement_id,
        )
        enriched_payload["settlement_context"] = {
            "settlement": settlement_detail.get("row"),
            "reconciliation": settlement_reconciliation.get("rows"),
            "deduction_breakdown": {
                "difference_amount": deduction_breakdown.get("difference_amount"),
                "explained_amount": deduction_breakdown.get("explained_amount"),
                "unexplained_amount": deduction_breakdown.get("unexplained_amount"),
                "components": deduction_breakdown.get("components"),
                "summary": deduction_breakdown.get("summary"),
            },
            "payout_delay": {
                "delay_state": payout_delay.get("delay_state"),
                "delay_days": payout_delay.get("delay_days"),
                "is_delayed": payout_delay.get("is_delayed"),
                "hold_reason": payout_delay.get("hold_reason"),
            },
        }
    else:
        enriched_payload["settlement_context"] = {
            "settlement": None,
            "reconciliation": None,
            "deduction_breakdown": None,
            "payout_delay": None,
        }
    return enriched_payload


def _connector_mode() -> str:
    raw = str(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_MODE", "simulated") or "simulated").strip().lower()
    if raw in {"real", "http", "live"}:
        return "http"
    return raw or "simulated"


def _connector_endpoint_url() -> str:
    base_url = str(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_BASE_URL", "") or "").strip()
    endpoint = str(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_ENDPOINT", "/v1/settlements/interventions") or "").strip()
    if not base_url and not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        raise ValueError("SETTLEMENT_OPS_CONNECTOR_BASE_URL is not configured")
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    normalized_endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    return f"{base_url.rstrip('/')}{normalized_endpoint}"


def _idempotency_key(*, approval_id: str, case_id: str, action_type: str) -> str:
    normalized_action = str(action_type or "").strip().upper() or "FOLLOW_UP"
    return f"bank-foundry:{approval_id}:{case_id}:{normalized_action}"


def _auth_headers() -> dict[str, str]:
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    auth_mode = str(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_AUTH_MODE", "none") or "none").strip().lower()
    if auth_mode in {"", "none"}:
        pass
    elif auth_mode == "bearer":
        token = str(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_BEARER_TOKEN", "") or "").strip()
        if not token:
            raise ValueError("SETTLEMENT_OPS_CONNECTOR_BEARER_TOKEN is not configured")
        headers["Authorization"] = f"Bearer {token}"
    elif auth_mode == "api_key":
        api_key = str(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_API_KEY", "") or "").strip()
        header_name = str(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_API_KEY_HEADER", "X-API-Key") or "X-API-Key").strip()
        if not api_key:
            raise ValueError("SETTLEMENT_OPS_CONNECTOR_API_KEY is not configured")
        headers[header_name or "X-API-Key"] = api_key
    else:
        raise ValueError(f"Unsupported settlement connector auth mode: {auth_mode}")
    partner_id = str(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_PARTNER_ID", "") or "").strip()
    if partner_id:
        headers["X-Partner-Id"] = partner_id
    return headers


def _request_payload(
    *,
    approval_id: str,
    case_id: str,
    action_type: str,
    payload: dict[str, Any],
    requested_by: str,
    requested_at: str,
    idempotency_key: str,
) -> dict[str, Any]:
    return {
        "request_id": approval_id,
        "case_id": case_id,
        "action_type": str(action_type or "").strip().upper(),
        "requested_by": requested_by,
        "requested_at": requested_at,
        "idempotency_key": idempotency_key,
        "payload": payload or {},
    }


def _parse_json_response(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except Exception:
        return {"raw_text": response.text}
    return payload if isinstance(payload, dict) else {"data": payload}


def _dispatch_http_connector(
    *,
    approval_id: str,
    case_id: str,
    action_type: str,
    payload: dict[str, Any],
    requested_by: str,
    requested_at: str,
) -> dict[str, Any]:
    endpoint_url = _connector_endpoint_url()
    idempotency_value = _idempotency_key(
        approval_id=approval_id,
        case_id=case_id,
        action_type=action_type,
    )
    headers = _auth_headers()
    idempotency_header = str(
        getattr(Config, "SETTLEMENT_OPS_CONNECTOR_IDEMPOTENCY_HEADER", "Idempotency-Key") or "Idempotency-Key"
    ).strip() or "Idempotency-Key"
    headers[idempotency_header] = idempotency_value
    request_payload = _request_payload(
        approval_id=approval_id,
        case_id=case_id,
        action_type=action_type,
        payload=payload,
        requested_by=requested_by,
        requested_at=requested_at,
        idempotency_key=idempotency_value,
    )
    timeout_seconds = float(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_TIMEOUT_SECONDS", 10) or 10)
    verify_ssl = bool(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_VERIFY_SSL", True))

    try:
        response = httpx.post(
            endpoint_url,
            json=request_payload,
            headers=headers,
            timeout=timeout_seconds,
            verify=verify_ssl,
            trust_env=False,
        )
        response_payload = _parse_json_response(response)
        http_status_code = int(response.status_code)
        receipt_ref = (
            str(response_payload.get("receipt_ref") or response_payload.get("receipt_id") or "").strip() or None
        )
        external_ref = (
            str(response_payload.get("external_ref") or response_payload.get("request_ref") or "").strip() or None
        )
        if http_status_code == 202:
            status = "QUEUED"
            error_message = None
        elif 200 <= http_status_code < 300:
            status = "SUCCESS"
            error_message = None
        else:
            status = "FAILED"
            error_message = (
                str(response_payload.get("error") or response_payload.get("message") or "").strip()
                or f"Connector returned HTTP {http_status_code}"
            )
        return {
            "connector_name": "settlement_ops_core",
            "connector_mode": "http",
            "connector_status": status,
            "request_payload": request_payload,
            "response_payload": response_payload,
            "receipt_ref": receipt_ref,
            "external_ref": external_ref,
            "error_message": error_message,
            "endpoint_url": endpoint_url,
            "idempotency_key": idempotency_value,
            "http_status_code": http_status_code,
        }
    except httpx.TimeoutException as exc:
        return {
            "connector_name": "settlement_ops_core",
            "connector_mode": "http",
            "connector_status": "FAILED",
            "request_payload": request_payload,
            "response_payload": {"message": "connector timeout"},
            "receipt_ref": None,
            "external_ref": None,
            "error_message": f"Connector timed out: {exc}",
            "endpoint_url": endpoint_url,
            "idempotency_key": idempotency_value,
            "http_status_code": None,
        }
    except httpx.HTTPError as exc:
        return {
            "connector_name": "settlement_ops_core",
            "connector_mode": "http",
            "connector_status": "FAILED",
            "request_payload": request_payload,
            "response_payload": {"message": "connector request failed"},
            "receipt_ref": None,
            "external_ref": None,
            "error_message": str(exc),
            "endpoint_url": endpoint_url,
            "idempotency_key": idempotency_value,
            "http_status_code": None,
        }


def ensure_connector_schema(engine: Any) -> None:
    statement = """
    CREATE TABLE IF NOT EXISTS ops_connector_runs (
        run_id TEXT PRIMARY KEY,
        approval_id TEXT NOT NULL,
        case_id TEXT NOT NULL,
        connector_name TEXT NOT NULL,
        connector_mode TEXT NOT NULL,
        action_type TEXT NOT NULL,
        status TEXT NOT NULL,
        request_payload_json TEXT NOT NULL,
        response_payload_json TEXT NOT NULL,
        receipt_ref TEXT NULL,
        external_ref TEXT NULL,
        endpoint_url TEXT NULL,
        idempotency_key TEXT NULL,
        http_status_code INTEGER NULL,
        error_message TEXT NULL,
        dispatched_at TEXT NOT NULL,
        completed_at TEXT NULL,
        updated_at TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """
    with engine.begin() as conn:
        conn.execute(text(statement))
        alter_statements = [
            "ALTER TABLE ops_connector_runs ADD COLUMN endpoint_url TEXT NULL",
            "ALTER TABLE ops_connector_runs ADD COLUMN idempotency_key TEXT NULL",
            "ALTER TABLE ops_connector_runs ADD COLUMN http_status_code INTEGER NULL",
        ]
        for alter_statement in alter_statements:
            try:
                conn.execute(text(alter_statement))
            except Exception:
                pass


def dispatch_settlement_approval(
    engine: Any,
    *,
    approval_id: str,
    case_id: str,
    action_type: str,
    payload: dict[str, Any],
    requested_by: str,
) -> dict[str, Any]:
    ensure_connector_schema(engine)
    now = _iso_now()
    connector_name = "settlement_ops_core"
    connector_mode = _connector_mode()
    enabled = bool(getattr(Config, "SETTLEMENT_OPS_CONNECTOR_ENABLED", True))
    supported = _is_supported_action(action_type)
    enriched_payload = _enrich_payload_from_db(engine, case_id=case_id, payload=payload or {})
    request_payload = enriched_payload
    endpoint_url = None
    idempotency_value = None
    http_status_code = None

    if not enabled:
        status = "SKIPPED"
        response_payload = {"message": "connector disabled", "mode": connector_mode}
        receipt_ref = None
        external_ref = None
        error_message = None
    elif not supported:
        status = "SKIPPED"
        response_payload = {"message": "unsupported action type", "mode": connector_mode}
        receipt_ref = None
        external_ref = None
        error_message = None
    elif connector_mode == "simulated":
        idempotency_value = _idempotency_key(approval_id=approval_id, case_id=case_id, action_type=action_type)
        request_payload = _request_payload(
            approval_id=approval_id,
            case_id=case_id,
            action_type=action_type,
            payload=enriched_payload,
            requested_by=requested_by,
            requested_at=now,
            idempotency_key=idempotency_value,
        )
        status = "SUCCESS"
        receipt_ref = _new_id("receipt")
        external_ref = _new_id("settlement_req")
        response_payload = {
            "message": "simulated settlement ops dispatch succeeded",
            "connector": connector_name,
            "mode": connector_mode,
            "submitted_by": requested_by,
            "external_ref": external_ref,
        }
        error_message = None
    elif connector_mode == "http":
        try:
            dispatch = _dispatch_http_connector(
                approval_id=approval_id,
                case_id=case_id,
                action_type=action_type,
                payload=enriched_payload,
                requested_by=requested_by,
                requested_at=now,
            )
            status = str(dispatch.get("connector_status") or "FAILED").strip().upper()
            response_payload = dict(dispatch.get("response_payload") or {})
            request_payload = dict(dispatch.get("request_payload") or {})
            receipt_ref = dispatch.get("receipt_ref")
            external_ref = dispatch.get("external_ref")
            error_message = dispatch.get("error_message")
            endpoint_url = dispatch.get("endpoint_url")
            idempotency_value = dispatch.get("idempotency_key")
            http_status_code = dispatch.get("http_status_code")
        except ValueError as exc:
            status = "FAILED"
            response_payload = {"message": "connector configuration invalid", "mode": connector_mode}
            receipt_ref = None
            external_ref = None
            error_message = str(exc)
    else:
        status = "FAILED"
        response_payload = {"message": "connector mode not implemented", "mode": connector_mode}
        receipt_ref = None
        external_ref = None
        error_message = f"Unsupported connector mode: {connector_mode}"

    record = {
        "run_id": _new_id("conn"),
        "approval_id": approval_id,
        "case_id": case_id,
        "connector_name": connector_name,
        "connector_mode": connector_mode,
        "action_type": str(action_type or "").strip().upper(),
        "status": status,
        "request_payload_json": _json_dump(request_payload),
        "response_payload_json": _json_dump(response_payload),
        "receipt_ref": receipt_ref,
        "external_ref": external_ref,
        "endpoint_url": endpoint_url,
        "idempotency_key": idempotency_value,
        "http_status_code": http_status_code,
        "error_message": error_message,
        "dispatched_at": now,
        "completed_at": now if status in {"SUCCESS", "FAILED", "SKIPPED"} else None,
        "updated_at": now,
        "created_at": now,
    }
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ops_connector_runs (
                    run_id, approval_id, case_id, connector_name, connector_mode, action_type, status,
                    request_payload_json, response_payload_json, receipt_ref, external_ref, endpoint_url,
                    idempotency_key, http_status_code, error_message,
                    dispatched_at, completed_at, updated_at, created_at
                ) VALUES (
                    :run_id, :approval_id, :case_id, :connector_name, :connector_mode, :action_type, :status,
                    :request_payload_json, :response_payload_json, :receipt_ref, :external_ref, :endpoint_url,
                    :idempotency_key, :http_status_code, :error_message,
                    :dispatched_at, :completed_at, :updated_at, :created_at
                )
                """
            ),
            record,
        )
    return {
        "run_id": record["run_id"],
        "case_id": case_id,
        "connector_name": connector_name,
        "connector_mode": connector_mode,
        "connector_status": status,
        "receipt_ref": receipt_ref,
        "external_ref": external_ref,
        "endpoint_url": endpoint_url,
        "idempotency_key": idempotency_value,
        "http_status_code": http_status_code,
        "error_message": error_message,
        "response_payload": response_payload,
    }


def list_connector_runs_for_case(engine: Any, case_id: str) -> list[dict[str, Any]]:
    ensure_connector_schema(engine)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM ops_connector_runs
                WHERE case_id = :case_id
                ORDER BY created_at DESC
                """
            ),
            {"case_id": case_id},
        ).mappings().all()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["request_payload"] = _json_load_object(item.get("request_payload_json"))
        item["response_payload"] = _json_load_object(item.get("response_payload_json"))
        out.append(item)
    return out

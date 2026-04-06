from __future__ import annotations

import datetime as dt
import json
from typing import Any

from sqlalchemy import bindparam, text

from app.data.evidence import normalize_evidence_ids
from app.data.merchant_ops import repository as merchant_ops_repository


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _evidence_ids_from_payload(value: Any) -> list[str]:
    return normalize_evidence_ids(value)


def _action_meta_from_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    meta = evidence.get("action_meta")
    return meta if isinstance(meta, dict) else {}


def _action_dedupe_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _normalize_text(row.get("category")),
        _normalize_text(row.get("title")),
        _normalize_text(row.get("description")),
    )


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _normalize_follow_up_date(value: Any) -> str | None:
    text_value = _clean_optional_text(value)
    if text_value is None:
        return None
    try:
        return dt.date.fromisoformat(text_value).isoformat()
    except Exception as exc:
        raise ValueError("follow_up_date must be YYYY-MM-DD") from exc


def list_existing_actions(
    engine: Any,
    merchant_id: str,
    *,
    limit: int = 10,
    low_signal_titles: set[str] | frozenset[str],
) -> list[dict[str, Any]]:
    cols = merchant_ops_repository.table_columns(engine, "merchant_actions")
    if not cols:
        return []

    mid_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not mid_col:
        return []

    title_col = "title" if "title" in cols else ("action_type" if "action_type" in cols else "NULL")
    category_col = "category" if "category" in cols else ("action_type" if "action_type" in cols else "NULL")
    description_col = "description" if "description" in cols else ("payload" if "payload" in cols else "NULL")
    status_col = "status" if "status" in cols else "NULL"
    priority_col = "priority_score" if "priority_score" in cols else "NULL"
    created_col = "created_at" if "created_at" in cols else "NULL"
    updated_col = "updated_at" if "updated_at" in cols else created_col
    evidence_col = "evidence" if "evidence" in cols else "NULL"
    owner_col = "owner" if "owner" in cols else "NULL"
    notes_col = "notes" if "notes" in cols else "NULL"
    blocked_reason_col = "blocked_reason" if "blocked_reason" in cols else "NULL"
    follow_up_col = "follow_up_date" if "follow_up_date" in cols else ("followup_date" if "followup_date" in cols else "NULL")

    query = text(
        f"""
        SELECT
          action_id,
          {title_col} AS title,
          {category_col} AS category,
          {description_col} AS description,
          {status_col} AS status,
          {priority_col} AS priority_score,
          {created_col} AS created_at,
          {updated_col} AS updated_at,
          {evidence_col} AS evidence,
          {owner_col} AS owner,
          {notes_col} AS notes,
          {blocked_reason_col} AS blocked_reason,
          {follow_up_col} AS follow_up_date
        FROM merchant_actions
        WHERE {mid_col} = :mid
        ORDER BY {updated_col} DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"mid": merchant_id, "limit": int(limit)}).mappings().all()

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        item = dict(row)
        status = str(item.get("status") or "UNKNOWN").upper()
        if status in {"HIDDEN", "ARCHIVED"}:
            continue
        title = _normalize_text(item.get("title"))
        if title in low_signal_titles:
            continue
        evidence = _json_object(item.get("evidence"))
        action_meta = _action_meta_from_evidence(evidence)
        item["source"] = str(evidence.get("source") or "unknown")
        item["evidence_ids"] = _evidence_ids_from_payload(evidence.get("evidence_ids"))
        item["evidence_payload"] = evidence.get("payload") if isinstance(evidence, dict) else None
        item["owner"] = _clean_optional_text(item.get("owner")) or _clean_optional_text(action_meta.get("owner"))
        item["notes"] = _clean_optional_text(item.get("notes")) or _clean_optional_text(action_meta.get("notes"))
        item["blocked_reason"] = _clean_optional_text(item.get("blocked_reason")) or _clean_optional_text(action_meta.get("blocked_reason"))
        item["follow_up_date"] = _clean_optional_text(item.get("follow_up_date")) or _clean_optional_text(action_meta.get("follow_up_date"))
        dedupe_key = _action_dedupe_key(item)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(item)
    return deduped


def cleanup_legacy_actions(
    engine: Any,
    merchant_id: str,
    *,
    hide_status: str = "HIDDEN",
    low_signal_titles: set[str] | frozenset[str],
    active_queue_statuses: set[str] | frozenset[str],
) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "merchant_actions")
    if not cols:
        return {"updated": False, "hidden_count": 0, "hidden_action_ids": [], "error": "merchant_actions table not found"}
    if "action_id" not in cols or "status" not in cols:
        return {"updated": False, "hidden_count": 0, "hidden_action_ids": [], "error": "merchant_actions table does not support cleanup"}

    mid_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not mid_col:
        return {"updated": False, "hidden_count": 0, "hidden_action_ids": [], "error": "merchant_actions table is missing merchant scope column"}

    title_col = "title" if "title" in cols else ("action_type" if "action_type" in cols else "NULL")
    category_col = "category" if "category" in cols else ("action_type" if "action_type" in cols else "NULL")
    description_col = "description" if "description" in cols else ("payload" if "payload" in cols else "NULL")
    created_col = "created_at" if "created_at" in cols else "NULL"
    updated_col = "updated_at" if "updated_at" in cols else created_col

    query = text(
        f"""
        SELECT
          action_id,
          {title_col} AS title,
          {category_col} AS category,
          {description_col} AS description,
          status,
          {created_col} AS created_at,
          {updated_col} AS updated_at
        FROM merchant_actions
        WHERE {mid_col} = :mid
        ORDER BY {updated_col} DESC, action_id DESC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"mid": merchant_id}).mappings().all()

    hidden_action_ids: list[Any] = []
    seen_active: set[tuple[str, str, str]] = set()
    for row in rows:
        item = dict(row)
        status = str(item.get("status") or "OPEN").strip().upper()
        if status in {"HIDDEN", "ARCHIVED", "CLOSED", "RESOLVED", "DONE"}:
            continue

        dedupe_key = _action_dedupe_key(item)
        title = dedupe_key[1]
        should_hide = False
        if title in low_signal_titles:
            should_hide = True
        elif status in active_queue_statuses:
            if dedupe_key in seen_active:
                should_hide = True
            else:
                seen_active.add(dedupe_key)

        if should_hide and item.get("action_id") is not None:
            hidden_action_ids.append(item["action_id"])

    if not hidden_action_ids:
        return {
            "updated": False,
            "hidden_count": 0,
            "hidden_action_ids": [],
            "merchant_id": merchant_id,
            "status": hide_status,
        }

    set_updated_expr = ", updated_at = CURRENT_TIMESTAMP" if "updated_at" in cols else ""
    update_query = text(
        f"""
        UPDATE merchant_actions
        SET status = :status{set_updated_expr}
        WHERE {mid_col} = :mid
          AND action_id IN :action_ids
        """
    ).bindparams(bindparam("action_ids", expanding=True))
    with engine.begin() as conn:
        result = conn.execute(
            update_query,
            {"status": str(hide_status or "HIDDEN").strip().upper(), "mid": merchant_id, "action_ids": hidden_action_ids},
        )

    return {
        "updated": bool(getattr(result, "rowcount", 0)),
        "hidden_count": len(hidden_action_ids),
        "hidden_action_ids": hidden_action_ids,
        "merchant_id": merchant_id,
        "status": str(hide_status or "HIDDEN").strip().upper(),
    }


def create_merchant_action(
    engine: Any,
    *,
    merchant_id: str,
    preview: dict[str, Any],
) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "merchant_actions")
    if not cols:
        return {"error": "Could not create merchant action: merchant_actions table not found", "preview": preview, "evidence": []}

    dialect = str(getattr(getattr(engine, "dialect", None), "name", "")).lower()
    payload_json = json.dumps(preview.get("payload") or {}, ensure_ascii=False, default=str)

    with engine.connect() as conn:
        tx = conn.begin()
        try:
            row = None
            action_id = None
            if {"merchant_id", "action_type", "payload"}.issubset(cols):
                if "sqlite" in dialect:
                    conn.execute(
                        text(
                            """
                            INSERT INTO merchant_actions (merchant_id, action_type, payload, status)
                            VALUES (:mid, :atype, :payload, 'REQUESTED')
                            """
                        ),
                        {"mid": merchant_id, "atype": preview.get("action_type"), "payload": payload_json},
                    )
                    if "action_id" in cols:
                        action_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()
                else:
                    row = conn.execute(
                        text(
                            """
                            INSERT INTO merchant_actions (merchant_id, action_type, payload, status)
                            VALUES (:mid, :atype, CAST(:payload AS jsonb), 'REQUESTED')
                            RETURNING action_id
                            """
                        ),
                        {"mid": merchant_id, "atype": preview.get("action_type"), "payload": payload_json},
                    ).fetchone()
                    action_id = row[0] if row else None
            else:
                payload = preview.get("payload") if isinstance(preview.get("payload"), dict) else {}
                fields: dict[str, Any] = {}
                if "mid" in cols:
                    fields["mid"] = merchant_id
                elif "merchant_id" in cols:
                    fields["merchant_id"] = merchant_id
                if "category" in cols:
                    fields["category"] = str(payload.get("category") or str(preview.get("action_type") or "workflow").lower())
                if "title" in cols:
                    fields["title"] = str(payload.get("title") or str(preview.get("action_type") or "Action").replace("_", " ").title())
                if "description" in cols:
                    fields["description"] = str(payload.get("description") or payload_json)
                if "impact_rupees" in cols:
                    fields["impact_rupees"] = float(payload.get("impact_rupees") or 0.0)
                if "confidence" in cols:
                    fields["confidence"] = float(payload.get("confidence") or 0.0)
                if "priority_score" in cols:
                    fields["priority_score"] = float(payload.get("priority_score") or 0.0)
                if "owner" in cols:
                    fields["owner"] = str(payload.get("owner") or "merchant_ui")
                if "evidence" in cols:
                    fields["evidence"] = json.dumps(payload.get("evidence") or payload, ensure_ascii=False, default=str)
                if "status" in cols:
                    fields["status"] = "REQUESTED"

                if not fields:
                    raise ValueError("merchant_actions schema is not supported")

                column_list = ", ".join(fields.keys())
                value_list = ", ".join(f":{key}" for key in fields)
                insert_sql = f"INSERT INTO merchant_actions ({column_list}) VALUES ({value_list})"
                if "action_id" in cols and "sqlite" not in dialect:
                    row = conn.execute(text(insert_sql + " RETURNING action_id"), fields).fetchone()
                    action_id = row[0] if row else None
                else:
                    conn.execute(text(insert_sql), fields)
                    if "action_id" in cols and "sqlite" in dialect:
                        action_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            tx.commit()
        except Exception as exc:
            tx.rollback()
            return {"error": f"Could not create merchant action: {exc}", "preview": preview, "evidence": []}

    return {"action_id": action_id, "preview": preview, "evidence": [f"action:{action_id}"] if action_id else []}


def update_existing_action_status(engine: Any, merchant_id: str, *, action_id: Any, status: str) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "merchant_actions")
    if not cols:
        return {"updated": False, "error": "merchant_actions table not found"}
    if "action_id" not in cols or "status" not in cols:
        return {"updated": False, "error": "merchant_actions table does not support status updates"}

    mid_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not mid_col:
        return {"updated": False, "error": "merchant_actions table is missing merchant scope column"}

    normalized_status = str(status or "").strip().upper()
    if not normalized_status:
        return {"updated": False, "error": "status is required"}

    query = text(
        f"""
        UPDATE merchant_actions
        SET status = :status{", updated_at = CURRENT_TIMESTAMP" if "updated_at" in cols else ""}
        WHERE action_id = :action_id
          AND {mid_col} = :mid
        """
    )
    with engine.begin() as conn:
        result = conn.execute(
            query,
            {"status": normalized_status, "action_id": action_id, "mid": merchant_id},
        )

    updated = bool(getattr(result, "rowcount", 0))
    if not updated:
        return {"updated": False, "error": "no matching action found", "action_id": action_id, "status": normalized_status}
    return {"updated": True, "action_id": action_id, "status": normalized_status}


def update_existing_action_details(
    engine: Any,
    merchant_id: str,
    *,
    action_id: Any,
    owner: Any = None,
    notes: Any = None,
    blocked_reason: Any = None,
    follow_up_date: Any = None,
) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "merchant_actions")
    if not cols:
        return {"updated": False, "error": "merchant_actions table not found"}
    if "action_id" not in cols:
        return {"updated": False, "error": "merchant_actions table does not support action updates"}

    mid_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not mid_col:
        return {"updated": False, "error": "merchant_actions table is missing merchant scope column"}

    normalized_owner = _clean_optional_text(owner)
    normalized_notes = _clean_optional_text(notes)
    normalized_blocked_reason = _clean_optional_text(blocked_reason)
    try:
        normalized_follow_up_date = _normalize_follow_up_date(follow_up_date)
    except ValueError as exc:
        return {"updated": False, "error": str(exc), "action_id": action_id}

    updates: dict[str, Any] = {}
    if "owner" in cols:
        updates["owner"] = normalized_owner
    if "notes" in cols:
        updates["notes"] = normalized_notes
    if "blocked_reason" in cols:
        updates["blocked_reason"] = normalized_blocked_reason
    if "follow_up_date" in cols:
        updates["follow_up_date"] = normalized_follow_up_date
    elif "followup_date" in cols:
        updates["followup_date"] = normalized_follow_up_date

    if "evidence" in cols:
        with engine.connect() as conn:
            existing_row = conn.execute(
                text(
                    f"""
                    SELECT evidence
                    FROM merchant_actions
                    WHERE action_id = :action_id
                      AND {mid_col} = :mid
                    """
                ),
                {"action_id": action_id, "mid": merchant_id},
            ).mappings().first()
        if not existing_row:
            return {"updated": False, "error": "no matching action found", "action_id": action_id}
        evidence_payload = _json_object(existing_row.get("evidence"))
        action_meta = dict(_action_meta_from_evidence(evidence_payload))
        action_meta["owner"] = normalized_owner
        action_meta["notes"] = normalized_notes
        action_meta["blocked_reason"] = normalized_blocked_reason
        action_meta["follow_up_date"] = normalized_follow_up_date
        action_meta = {key: value for key, value in action_meta.items() if value not in {None, ""}}
        if action_meta:
            evidence_payload["action_meta"] = action_meta
        else:
            evidence_payload.pop("action_meta", None)
        updates["evidence"] = json.dumps(evidence_payload, ensure_ascii=False, default=str)

    if "updated_at" in cols:
        updates["updated_at"] = text("CURRENT_TIMESTAMP")

    if not updates:
        return {"updated": False, "error": "merchant_actions table has no editable action detail fields", "action_id": action_id}

    assignments: list[str] = []
    params: dict[str, Any] = {"action_id": action_id, "mid": merchant_id}
    for key, value in updates.items():
        if hasattr(value, "text"):
            assignments.append(f"{key} = CURRENT_TIMESTAMP")
        else:
            assignments.append(f"{key} = :{key}")
            params[key] = value

    query = text(
        f"""
        UPDATE merchant_actions
        SET {', '.join(assignments)}
        WHERE action_id = :action_id
          AND {mid_col} = :mid
        """
    )
    with engine.begin() as conn:
        result = conn.execute(query, params)

    updated = bool(getattr(result, "rowcount", 0))
    if not updated:
        return {"updated": False, "error": "no matching action found", "action_id": action_id}
    return {
        "updated": True,
        "action_id": action_id,
        "owner": normalized_owner,
        "notes": normalized_notes,
        "blocked_reason": normalized_blocked_reason,
        "follow_up_date": normalized_follow_up_date,
    }


def get_existing_action(engine: Any, merchant_id: str, *, action_id: Any) -> dict[str, Any] | None:
    cols = merchant_ops_repository.table_columns(engine, "merchant_actions")
    if not cols or "action_id" not in cols:
        return None
    mid_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not mid_col:
        return None

    query = text(
        f"""
        SELECT *
        FROM merchant_actions
        WHERE action_id = :action_id
          AND {mid_col} = :mid
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"action_id": action_id, "mid": merchant_id}).mappings().first()
    if not row:
        return None
    item = dict(row)
    if "evidence" in item:
        item["evidence_payload"] = _json_object(item.get("evidence"))
    return item

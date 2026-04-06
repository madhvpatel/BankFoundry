from __future__ import annotations

import datetime as dt
import json
from typing import Any, Callable

from sqlalchemy import text

from app.data.evidence import normalize_evidence_ids
from app.data.merchant_ops import repository as merchant_ops_repository


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


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


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _evidence_ids_from_payload(value: Any) -> list[str]:
    return normalize_evidence_ids(value)


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_iso_timestamp(value: Any) -> dt.datetime | None:
    text_value = _clean_optional_text(value)
    if text_value is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text_value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def ensure_proactive_cards_schema(engine: Any) -> set[str]:
    from sqlalchemy import text as sa_text

    if not hasattr(engine, "begin") or not hasattr(engine, "connect"):
        return set()

    create_sql = sa_text(
        """
        CREATE TABLE IF NOT EXISTS proactive_cards (
          dedupe_key TEXT PRIMARY KEY,
          merchant_id TEXT NOT NULL,
          lane TEXT NOT NULL,
          verification_status TEXT NOT NULL,
          evidence_ids TEXT NOT NULL,
          action_preview_token TEXT NULL,
          payload_json TEXT NOT NULL,
          window_from TEXT NOT NULL,
          window_to TEXT NOT NULL,
          card_state TEXT DEFAULT 'NEW',
          card_notes TEXT NULL,
          converted_action_id TEXT NULL,
          linked_case_id TEXT NULL,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(create_sql)

    cols = merchant_ops_repository.table_columns(engine, "proactive_cards")
    alter_statements: list[str] = []
    if "card_state" not in cols:
        alter_statements.append("ALTER TABLE proactive_cards ADD COLUMN card_state TEXT DEFAULT 'NEW'")
    if "card_notes" not in cols:
        alter_statements.append("ALTER TABLE proactive_cards ADD COLUMN card_notes TEXT NULL")
    if "converted_action_id" not in cols:
        alter_statements.append("ALTER TABLE proactive_cards ADD COLUMN converted_action_id TEXT NULL")
    if "linked_case_id" not in cols:
        alter_statements.append("ALTER TABLE proactive_cards ADD COLUMN linked_case_id TEXT NULL")
    if "updated_at" not in cols:
        alter_statements.append("ALTER TABLE proactive_cards ADD COLUMN updated_at TIMESTAMP NULL")

    if alter_statements:
        with engine.begin() as conn:
            for stmt in alter_statements:
                conn.execute(sa_text(stmt))
        cols = merchant_ops_repository.table_columns(engine, "proactive_cards")
    return cols


def ensure_proactive_refresh_schedule_schema(engine: Any) -> set[str]:
    from sqlalchemy import text as sa_text

    if not hasattr(engine, "begin") or not hasattr(engine, "connect"):
        return set()

    create_sql = sa_text(
        """
        CREATE TABLE IF NOT EXISTS proactive_refresh_schedule (
          merchant_id TEXT NOT NULL,
          window_days INTEGER NOT NULL,
          status TEXT DEFAULT 'IDLE',
          last_refresh_at TEXT NULL,
          next_refresh_at TEXT NULL,
          last_window_from TEXT NULL,
          last_window_to TEXT NULL,
          last_generated_count INTEGER DEFAULT 0,
          last_inserted_count INTEGER DEFAULT 0,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (merchant_id, window_days)
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(create_sql)

    cols = merchant_ops_repository.table_columns(engine, "proactive_refresh_schedule")
    alter_statements: list[str] = []
    if "status" not in cols:
        alter_statements.append("ALTER TABLE proactive_refresh_schedule ADD COLUMN status TEXT DEFAULT 'IDLE'")
    if "last_refresh_at" not in cols:
        alter_statements.append("ALTER TABLE proactive_refresh_schedule ADD COLUMN last_refresh_at TEXT NULL")
    if "next_refresh_at" not in cols:
        alter_statements.append("ALTER TABLE proactive_refresh_schedule ADD COLUMN next_refresh_at TEXT NULL")
    if "last_window_from" not in cols:
        alter_statements.append("ALTER TABLE proactive_refresh_schedule ADD COLUMN last_window_from TEXT NULL")
    if "last_window_to" not in cols:
        alter_statements.append("ALTER TABLE proactive_refresh_schedule ADD COLUMN last_window_to TEXT NULL")
    if "last_generated_count" not in cols:
        alter_statements.append("ALTER TABLE proactive_refresh_schedule ADD COLUMN last_generated_count INTEGER DEFAULT 0")
    if "last_inserted_count" not in cols:
        alter_statements.append("ALTER TABLE proactive_refresh_schedule ADD COLUMN last_inserted_count INTEGER DEFAULT 0")
    if "updated_at" not in cols:
        alter_statements.append("ALTER TABLE proactive_refresh_schedule ADD COLUMN updated_at TIMESTAMP NULL")

    if alter_statements:
        with engine.begin() as conn:
            for stmt in alter_statements:
                conn.execute(sa_text(stmt))
        cols = merchant_ops_repository.table_columns(engine, "proactive_refresh_schedule")
    return cols


def get_background_refresh_status(
    engine: Any,
    merchant_id: str,
    *,
    days: int = 30,
    interval_minutes: int,
    auto_enabled: bool,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    current_time = now.astimezone(dt.timezone.utc) if isinstance(now, dt.datetime) and now.tzinfo else (now.replace(tzinfo=dt.timezone.utc) if isinstance(now, dt.datetime) else _utc_now())
    cols = ensure_proactive_refresh_schedule_schema(engine)
    if not cols:
        return {
            "auto_enabled": auto_enabled,
            "due": False,
            "merchant_id": merchant_id,
            "window_days": int(days),
            "min_interval_minutes": interval_minutes,
        }

    query = text(
        """
        SELECT merchant_id, window_days, status, last_refresh_at, next_refresh_at, last_window_from, last_window_to,
               last_generated_count, last_inserted_count, updated_at
        FROM proactive_refresh_schedule
        WHERE merchant_id = :mid
          AND window_days = :days
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"mid": merchant_id, "days": int(days)}).mappings().first()

    last_refresh_at = _parse_iso_timestamp(row.get("last_refresh_at")) if row else None
    next_refresh_at = _parse_iso_timestamp(row.get("next_refresh_at")) if row else None
    has_schedule = row is not None
    due = not has_schedule or next_refresh_at is None or next_refresh_at <= current_time
    return {
        "auto_enabled": auto_enabled,
        "due": bool(due),
        "merchant_id": merchant_id,
        "window_days": int(days),
        "status": str(row.get("status") or "IDLE") if row else "IDLE",
        "last_refresh_at": last_refresh_at.isoformat() if last_refresh_at else None,
        "next_refresh_at": next_refresh_at.isoformat() if next_refresh_at else None,
        "last_window_from": str(row.get("last_window_from") or "") if row else "",
        "last_window_to": str(row.get("last_window_to") or "") if row else "",
        "last_generated_count": int(row.get("last_generated_count") or 0) if row else 0,
        "last_inserted_count": int(row.get("last_inserted_count") or 0) if row else 0,
        "min_interval_minutes": interval_minutes,
    }


def upsert_background_refresh_schedule(
    engine: Any,
    merchant_id: str,
    *,
    days: int,
    current_time: dt.datetime,
    next_refresh_at: dt.datetime,
    window_from: str,
    window_to: str,
    generated_count: int,
    inserted_count: int,
) -> None:
    ensure_proactive_refresh_schedule_schema(engine)
    upsert = text(
        """
        INSERT INTO proactive_refresh_schedule
        (merchant_id, window_days, status, last_refresh_at, next_refresh_at, last_window_from, last_window_to,
         last_generated_count, last_inserted_count, updated_at)
        VALUES
        (:mid, :days, :status, :last_refresh_at, :next_refresh_at, :wf, :wt, :generated_count, :inserted_count, CURRENT_TIMESTAMP)
        ON CONFLICT(merchant_id, window_days) DO UPDATE SET
          status = excluded.status,
          last_refresh_at = excluded.last_refresh_at,
          next_refresh_at = excluded.next_refresh_at,
          last_window_from = excluded.last_window_from,
          last_window_to = excluded.last_window_to,
          last_generated_count = excluded.last_generated_count,
          last_inserted_count = excluded.last_inserted_count,
          updated_at = CURRENT_TIMESTAMP
        """
    )
    params = {
        "mid": merchant_id,
        "days": int(days),
        "status": "REFRESHED",
        "last_refresh_at": current_time.isoformat(),
        "next_refresh_at": next_refresh_at.isoformat(),
        "wf": window_from,
        "wt": window_to,
        "generated_count": int(generated_count or 0),
        "inserted_count": int(inserted_count or 0),
    }
    with engine.begin() as conn:
        conn.execute(upsert, params)


def update_background_proactive_card_state(
    engine: Any,
    merchant_id: str,
    *,
    dedupe_key: str,
    state: str,
    card_notes: Any = None,
    converted_action_id: Any = None,
) -> dict[str, Any]:
    cols = ensure_proactive_cards_schema(engine)
    normalized_state = str(state or "").strip().upper()
    if normalized_state not in {"NEW", "ACKNOWLEDGED", "DISMISSED", "CONVERTED"}:
        return {"updated": False, "error": "invalid proactive card state", "dedupe_key": dedupe_key}

    notes_value = _clean_optional_text(card_notes)
    converted_value = _clean_optional_text(converted_action_id)
    updates = {
        "card_state": normalized_state,
        "card_notes": notes_value,
        "converted_action_id": converted_value,
    }

    assignments = [f"{key} = :{key}" for key in updates.keys() if key in cols]
    params: dict[str, Any] = {"dedupe_key": dedupe_key, "mid": merchant_id}
    for key, value in updates.items():
        if key in cols:
            params[key] = value
    if "updated_at" in cols:
        assignments.append("updated_at = CURRENT_TIMESTAMP")

    query = text(
        f"""
        UPDATE proactive_cards
        SET {', '.join(assignments)}
        WHERE dedupe_key = :dedupe_key
          AND merchant_id = :mid
        """
    )
    with engine.begin() as conn:
        result = conn.execute(query, params)

    updated = bool(getattr(result, "rowcount", 0))
    if not updated:
        return {"updated": False, "error": "no matching proactive card found", "dedupe_key": dedupe_key}
    return {
        "updated": True,
        "dedupe_key": dedupe_key,
        "state": normalized_state,
        "card_notes": notes_value,
        "converted_action_id": converted_value,
    }


def link_background_proactive_card_case(
    engine: Any,
    merchant_id: str,
    *,
    dedupe_key: str,
    case_id: Any,
) -> dict[str, Any]:
    cols = ensure_proactive_cards_schema(engine)
    if "linked_case_id" not in cols:
        return {"updated": False, "error": "linked_case_id not supported", "dedupe_key": dedupe_key}

    query = text(
        """
        UPDATE proactive_cards
        SET linked_case_id = :case_id, updated_at = CURRENT_TIMESTAMP
        WHERE dedupe_key = :dedupe_key
          AND merchant_id = :mid
        """
    )
    with engine.begin() as conn:
        result = conn.execute(query, {"case_id": _clean_optional_text(case_id), "dedupe_key": dedupe_key, "mid": merchant_id})
    updated = bool(getattr(result, "rowcount", 0))
    if not updated:
        return {"updated": False, "error": "no matching proactive card found", "dedupe_key": dedupe_key}
    return {"updated": True, "dedupe_key": dedupe_key, "linked_case_id": _clean_optional_text(case_id)}


def list_background_proactive_cards(engine: Any, merchant_id: str, *, limit: int = 8) -> list[dict[str, Any]]:
    cols = ensure_proactive_cards_schema(engine)
    if not cols:
        return []
    if "merchant_id" not in cols or "payload_json" not in cols or "dedupe_key" not in cols:
        return []

    created_col = "created_at" if "created_at" in cols else "dedupe_key"
    updated_col = "updated_at" if "updated_at" in cols else created_col
    state_col = "card_state" if "card_state" in cols else "'NEW'"
    notes_col = "card_notes" if "card_notes" in cols else "NULL"
    action_id_col = "converted_action_id" if "converted_action_id" in cols else "NULL"
    case_id_col = "linked_case_id" if "linked_case_id" in cols else "NULL"
    query = text(
        f"""
        SELECT dedupe_key, lane, verification_status, evidence_ids, action_preview_token, payload_json, window_from, window_to,
               {created_col} AS created_at, {updated_col} AS updated_at, {state_col} AS card_state,
               {notes_col} AS card_notes, {action_id_col} AS converted_action_id, {case_id_col} AS linked_case_id
        FROM proactive_cards
        WHERE merchant_id = :mid
          AND dedupe_key LIKE :prefix
          AND UPPER(COALESCE({state_col}, 'NEW')) <> 'DISMISSED'
        ORDER BY {updated_col} DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"mid": merchant_id, "prefix": f"bg:{merchant_id}:%", "limit": int(limit)}).mappings().all()

    cards: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        payload = _json_object(item.get("payload_json"))
        evidence_ids = _evidence_ids_from_payload(item.get("evidence_ids"))
        linked_action_id = _clean_optional_text(payload.get("linked_action_id"))
        cards.append(
            {
                "dedupe_key": item.get("dedupe_key"),
                "lane": str(item.get("lane") or payload.get("lane") or "growth"),
                "verification_status": str(item.get("verification_status") or payload.get("verification_status") or "Background signal"),
                "evidence_ids": evidence_ids,
                "action_preview_token": item.get("action_preview_token"),
                "title": str(payload.get("title") or ""),
                "body": str(payload.get("body") or ""),
                "type": str(payload.get("type") or "info"),
                "icon": str(payload.get("icon") or "🔎"),
                "impact_rupees": float(payload.get("impact_rupees") or 0.0) if payload.get("impact_rupees") is not None else None,
                "confidence": float(payload.get("confidence") or 0.0) if payload.get("confidence") is not None else None,
                "actions": payload.get("actions") if isinstance(payload.get("actions"), list) else [],
                "source": str(payload.get("source") or "insight_card_engine"),
                "terminal_id": _clean_optional_text(payload.get("terminal_id")),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "card_state": str(item.get("card_state") or "NEW").upper(),
                "card_notes": _clean_optional_text(item.get("card_notes")),
                "converted_action_id": _clean_optional_text(item.get("converted_action_id")),
                "linked_case_id": _clean_optional_text(item.get("linked_case_id")),
                "linked_action_id": linked_action_id,
                "window": {"from": item.get("window_from"), "to": item.get("window_to")},
            }
        )
    return cards


def get_background_proactive_card(engine: Any, merchant_id: str, *, dedupe_key: str) -> dict[str, Any] | None:
    cards = list_background_proactive_cards(engine, merchant_id, limit=100)
    for card in cards:
        if str(card.get("dedupe_key") or "") == str(dedupe_key or ""):
            return card
    return None


def persist_background_proactive_cards(
    engine: Any,
    merchant_id: str,
    *,
    window_from: str,
    window_to: str,
    ranked_cards: list[dict[str, Any]],
    shortfall_by_card_id: dict[str, dict[str, Any]],
    lane_resolver: Callable[[dict[str, Any]], str],
    create_action_fn: Callable[[str, dict[str, Any]], Any],
) -> dict[str, Any]:
    from sqlalchemy import text as sa_text

    cols = ensure_proactive_cards_schema(engine)
    dialect = str(getattr(getattr(engine, "dialect", None), "name", "")).lower()
    delete_sql = sa_text(
        """
        DELETE FROM proactive_cards
        WHERE merchant_id = :mid
          AND dedupe_key LIKE :prefix
          AND window_from = :wf
          AND window_to = :wt
        """
    )

    inserted = 0
    with engine.begin() as conn:
        existing_rows = conn.execute(
            sa_text(
                """
                SELECT dedupe_key, card_state, card_notes, converted_action_id, linked_case_id
                FROM proactive_cards
                WHERE merchant_id = :mid
                  AND dedupe_key LIKE :prefix
                  AND window_from = :wf
                  AND window_to = :wt
                """
            ),
            {"mid": merchant_id, "prefix": f"bg:{merchant_id}:%", "wf": window_from, "wt": window_to},
        ).mappings().all()
        existing_meta = {str(row["dedupe_key"]): dict(row) for row in existing_rows}
        conn.execute(delete_sql, {"mid": merchant_id, "prefix": f"bg:{merchant_id}:%", "wf": window_from, "wt": window_to})
        for card in ranked_cards:
            card_id = str(card.get("id") or "insight")
            lane = lane_resolver(card)
            dedupe_key = f"bg:{merchant_id}:{lane}:{card_id}:{window_from}:{window_to}"
            preserved = existing_meta.get(dedupe_key, {})
            linked_action_id = None
            shortfall_alert = shortfall_by_card_id.get(card_id, {}) if card_id in shortfall_by_card_id else {}
            evidence_ids = normalize_evidence_ids(
                [
                    f"insight_card:{card_id}:{window_from}:{window_to}",
                    f"window:{window_from}:{window_to}",
                    shortfall_alert.get("evidence") if isinstance(shortfall_alert, dict) else [],
                ]
            )
            linked_action = shortfall_alert.get("action") if isinstance(shortfall_alert, dict) else None
            if isinstance(linked_action, dict):
                linked_action_id = create_action_fn(merchant_id, linked_action)
            payload = {
                "source": str(card.get("source") or "insight_card_engine"),
                "lane": lane,
                "id": card_id,
                "title": card.get("title"),
                "body": card.get("body"),
                "type": card.get("type"),
                "icon": card.get("icon"),
                "impact_rupees": card.get("impact_rupees"),
                "confidence": card.get("confidence"),
                "actions": card.get("actions") or [],
                "drivers": card.get("drivers") or [],
                "verification_status": str(card.get("verification_status") or "Background signal (template-triggered)"),
                "linked_action_id": str(linked_action_id) if linked_action_id is not None else None,
            }
            params = {
                "dedupe_key": dedupe_key,
                "mid": merchant_id,
                "lane": lane,
                "vs": str(card.get("verification_status") or "Background signal (template-triggered)"),
                "evidence_ids": json.dumps(evidence_ids, ensure_ascii=False),
                "token": None,
                "payload": json.dumps(payload, ensure_ascii=False, default=str),
                "wf": window_from,
                "wt": window_to,
            }
            if "card_state" in cols:
                params["card_state"] = str(preserved.get("card_state") or "NEW").upper()
            if "card_notes" in cols:
                params["card_notes"] = preserved.get("card_notes")
            if "converted_action_id" in cols:
                params["converted_action_id"] = preserved.get("converted_action_id")
            if "linked_case_id" in cols:
                params["linked_case_id"] = preserved.get("linked_case_id")

            column_names = [
                "dedupe_key",
                "merchant_id",
                "lane",
                "verification_status",
                "evidence_ids",
                "action_preview_token",
                "payload_json",
                "window_from",
                "window_to",
            ]
            value_names = [
                ":dedupe_key",
                ":mid",
                ":lane",
                ":vs",
                ":evidence_ids",
                ":token",
                ":payload",
                ":wf",
                ":wt",
            ]
            if "card_state" in cols:
                column_names.append("card_state")
                value_names.append(":card_state")
            if "card_notes" in cols:
                column_names.append("card_notes")
                value_names.append(":card_notes")
            if "converted_action_id" in cols:
                column_names.append("converted_action_id")
                value_names.append(":converted_action_id")
            if "linked_case_id" in cols:
                column_names.append("linked_case_id")
                value_names.append(":linked_case_id")
            insert_verb = "INSERT OR REPLACE" if "sqlite" in dialect else "INSERT"
            conn.execute(
                sa_text(
                    f"""
                    {insert_verb} INTO proactive_cards
                    ({', '.join(column_names)})
                    VALUES ({', '.join(value_names)})
                    """
                ),
                params,
            )
            inserted += 1

    return {
        "merchant_id": merchant_id,
        "window": {"from": window_from, "to": window_to},
        "generated_count": len(ranked_cards),
        "inserted_count": inserted,
    }

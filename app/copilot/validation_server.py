from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any

from sqlalchemy import text

from config import Config


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        value = asdict(value)
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


def pick_default_merchant_id(engine: Any) -> str:
    table = str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features"))
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT merchant_id FROM {table} LIMIT 1")).fetchone()
    return str(row[0]).strip() if row and row[0] is not None else ""


def pick_default_terminal_id(engine: Any, merchant_id: str) -> str | None:
    if not str(merchant_id or "").strip():
        return None
    table = str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features"))
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT terminal_id
                FROM {table}
                WHERE merchant_id = :mid
                  AND terminal_id IS NOT NULL
                  AND TRIM(CAST(terminal_id AS TEXT)) <> ''
                ORDER BY terminal_id
                LIMIT 1
                """
            ),
            {"mid": merchant_id},
        ).fetchone()
    return str(row[0]).strip() if row and row[0] is not None else None

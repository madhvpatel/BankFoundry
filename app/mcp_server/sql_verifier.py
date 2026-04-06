from __future__ import annotations

import hashlib
import re
from typing import Any

from sqlalchemy import text

from app.data.merchant_ops import repository as merchant_ops_repository
from app.mcp_server.guards import MCPGuardError

_FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|merge|call|execute|vacuum)\b",
    flags=re.IGNORECASE,
)
_COMMENT_PATTERN = re.compile(r"(--|/\*)")
_TABLE_PATTERN = re.compile(r'\b(?:from|join)\s+"?([a-zA-Z_][\w]*)"?', flags=re.IGNORECASE)


def _normalize_query(query: str) -> str:
    normalized = str(query or "").strip()
    if not normalized:
        raise MCPGuardError("query is required")
    if normalized.endswith(";"):
        normalized = normalized[:-1].strip()
    if ";" in normalized:
        raise MCPGuardError("multiple SQL statements are not allowed")
    if _COMMENT_PATTERN.search(normalized):
        raise MCPGuardError("SQL comments are not allowed")
    lowered = normalized.lower()
    if not (lowered.startswith("select") or lowered.startswith("with ")):
        raise MCPGuardError("run_verified_sql only allows read-only SELECT queries")
    if _FORBIDDEN_SQL_PATTERN.search(normalized):
        raise MCPGuardError("query contains non-read-only SQL keywords")
    return normalized


def _extract_tables(query: str) -> set[str]:
    return {match.group(1).lower() for match in _TABLE_PATTERN.finditer(query)}


def validate_verified_sql_query(
    engine: Any,
    *,
    query: str,
    allowed_table: str,
) -> str:
    normalized = _normalize_query(query)
    lowered = normalized.lower()
    if " join " in lowered:
        raise MCPGuardError("run_verified_sql v1 does not allow joins")

    required_placeholders = (":mid", ":start_date", ":end_date")
    for placeholder in required_placeholders:
        if placeholder not in normalized:
            raise MCPGuardError(f"query must include {placeholder}")

    if "merchant_id" not in lowered:
        raise MCPGuardError("query must scope on merchant_id")
    if "p_date" not in lowered:
        raise MCPGuardError("query must bound the window using p_date")

    tables = _extract_tables(normalized)
    allowed = str(allowed_table or "").strip().lower()
    if not tables:
        raise MCPGuardError("query must reference the configured source table")
    if tables != {allowed}:
        raise MCPGuardError(f"run_verified_sql v1 only allows the {allowed_table} table")

    cols = merchant_ops_repository.table_columns(engine, allowed_table)
    required_cols = {"merchant_id", "p_date"}
    if not required_cols.issubset(cols):
        raise MCPGuardError(f"{allowed_table} is missing required scoped analytics columns")
    return normalized


def execute_verified_sql(
    engine: Any,
    *,
    merchant_id: str,
    start_date: str,
    end_date: str,
    query: str,
    parameters: dict[str, Any] | None,
    limit: int,
    allowed_table: str,
) -> dict[str, Any]:
    normalized = validate_verified_sql_query(engine, query=query, allowed_table=allowed_table)
    safe_limit = max(1, min(int(limit or 100), 200))
    sql_params = dict(parameters or {}) if isinstance(parameters, dict) else {}
    sql_params["mid"] = merchant_id
    sql_params["start_date"] = start_date
    sql_params["end_date"] = end_date
    sql_params["_verified_limit"] = safe_limit

    query_hash = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    wrapped = text(f"SELECT * FROM ({normalized}) AS verified_query LIMIT :_verified_limit")

    try:
        with engine.connect() as conn:
            rows = conn.execute(wrapped, sql_params).mappings().all()
    except Exception as exc:
        return {
            "verified": False,
            "rows": [],
            "columns": [],
            "row_count": 0,
            "error": str(exc),
            "evidence": [f"sql:{query_hash}", f"merchant:{merchant_id}", f"window:{start_date}:{end_date}"],
        }

    materialized = [dict(row) for row in rows]
    columns = list(materialized[0].keys()) if materialized else []
    return {
        "verified": True,
        "rows": materialized,
        "columns": columns,
        "row_count": len(materialized),
        "error": None,
        "evidence": [f"sql:{query_hash}", f"merchant:{merchant_id}", f"window:{start_date}:{end_date}"],
    }

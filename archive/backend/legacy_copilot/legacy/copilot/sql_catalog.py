from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from sqlalchemy import text

from config import Config

logger = logging.getLogger("copilot_sql_catalog")

DEFAULT_CATALOG_PATH = Path(__file__).with_name("sql_catalog.json")
DEFAULT_DISCOVERY_PREFIXES = ("fact_", "dim_", "merchant_", "payment_", "terminal_", "transaction_", "kpi_")


def _json_load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": "1.0", "tables": [], "recommended_views": []}


def _discover_tables_postgres(engine: Any) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
        ).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def _discover_tables_sqlite(engine: Any) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                ORDER BY name
                """
            )
        ).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def _table_columns(engine: Any, table_name: str) -> list[str]:
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :t
                    ORDER BY ordinal_position
                    """
                ),
                {"t": table_name},
            ).fetchall()
        cols = [str(r[0]) for r in rows if r and r[0]]
        if cols:
            return cols
    except Exception:
        pass

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        return [str(r[1]) for r in rows if len(r) > 1 and r[1]]
    except Exception:
        return []


def _normalize_table_entry(raw: dict[str, Any]) -> dict[str, Any]:
    name = str(raw.get("name") or "").strip()
    if not name:
        return {}
    cols = raw.get("columns") if isinstance(raw.get("columns"), list) else []
    return {
        "name": name,
        "description": str(raw.get("description") or "").strip(),
        "columns": [str(c) for c in cols if str(c).strip()],
    }


def _merge_catalog(curated: dict[str, Any], discovered: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, dict[str, Any]] = {}

    for t in curated.get("tables") or []:
        row = _normalize_table_entry(t if isinstance(t, dict) else {})
        if row:
            merged[row["name"]] = row

    for t in discovered:
        row = _normalize_table_entry(t)
        if not row:
            continue
        if row["name"] in merged:
            existing = merged[row["name"]]
            seen = {c.lower() for c in existing.get("columns") or []}
            for c in row.get("columns") or []:
                if c.lower() not in seen:
                    existing.setdefault("columns", []).append(c)
                    seen.add(c.lower())
        else:
            merged[row["name"]] = row

    tables = sorted(merged.values(), key=lambda x: x.get("name", ""))
    recommended = [str(v) for v in (curated.get("recommended_views") or []) if str(v).strip()]
    recommended_set = {v.lower() for v in recommended}
    for t in tables:
        name = str(t.get("name") or "")
        if name.lower() not in recommended_set and (
            name.startswith(DEFAULT_DISCOVERY_PREFIXES) or name in {"transaction_features", "settlements", "chargebacks", "refunds", "merchants"}
        ):
            recommended.append(name)
            recommended_set.add(name.lower())

    return {"version": str(curated.get("version") or "1.0"), "tables": tables, "recommended_views": recommended}


def load_catalog(engine: Any) -> dict[str, Any]:
    catalog_path = Path(str(getattr(Config, "SQL_GRAPH_CATALOG_PATH", str(DEFAULT_CATALOG_PATH)) or str(DEFAULT_CATALOG_PATH)))
    curated = _json_load(catalog_path if catalog_path.exists() else DEFAULT_CATALOG_PATH)

    if not bool(getattr(Config, "SQL_GRAPH_AUTO_DISCOVER_TABLES", True)):
        return curated

    discovered_rows: list[dict[str, Any]] = []
    allowlist_raw = str(getattr(Config, "SQL_GRAPH_TABLE_ALLOWLIST", "") or "").strip()
    allowlist = {x.strip() for x in allowlist_raw.split(",") if x.strip()}
    include_prefixes_raw = str(getattr(Config, "SQL_GRAPH_DISCOVERY_PREFIXES", "") or "").strip()
    include_prefixes = tuple(x.strip() for x in include_prefixes_raw.split(",") if x.strip()) or DEFAULT_DISCOVERY_PREFIXES

    try:
        dialect = str(getattr(getattr(engine, "dialect", None), "name", "")).lower()
        if "sqlite" in dialect:
            tables = _discover_tables_sqlite(engine)
        else:
            tables = _discover_tables_postgres(engine)

        for t in tables:
            t_name = str(t or "").strip()
            if not t_name:
                continue
            if allowlist and t_name not in allowlist:
                continue
            if not allowlist and not (t_name.startswith(include_prefixes) or t_name in {"transaction_features", "settlements", "chargebacks", "refunds", "merchants"}):
                continue
            discovered_rows.append(
                {
                    "name": t_name,
                    "description": "Auto-discovered table from active DB schema.",
                    "columns": _table_columns(engine, t_name),
                }
            )
    except Exception as exc:
        logger.debug("Live schema discovery failed: %s", exc)

    return _merge_catalog(curated, discovered_rows)


def render_catalog_for_prompt(catalog: dict[str, Any], *, max_tables: int = 20, max_columns: int = 40) -> str:
    lines: list[str] = []
    tables = catalog.get("tables") if isinstance(catalog.get("tables"), list) else []
    for t in tables[:max_tables]:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name") or "").strip()
        if not name:
            continue
        desc = str(t.get("description") or "").strip()
        cols = t.get("columns") if isinstance(t.get("columns"), list) else []
        cols_txt = ", ".join(str(c) for c in cols[:max_columns]) if cols else "(unknown columns)"
        if desc:
            lines.append(f"- {name}: {desc}")
        else:
            lines.append(f"- {name}")
        lines.append(f"  columns: {cols_txt}")
    rec = catalog.get("recommended_views") if isinstance(catalog.get("recommended_views"), list) else []
    if rec:
        lines.append("")
        lines.append("recommended_views: " + ", ".join(str(v) for v in rec))
    return "\n".join(lines).strip()

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import text

from app.data.providers import ResolvedSource, resolve_settlement_provider, resolve_transaction_provider
from config import Config


def resolve_transaction_source(engine: Any, *, table: str | None = None) -> ResolvedSource:
    preferred = table or str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features") or "transaction_features")
    return resolve_transaction_provider(engine, preferred_table=preferred)


def resolve_settlement_source(engine: Any, *, table: str | None = None) -> ResolvedSource:
    return resolve_settlement_provider(engine, preferred_table=table or "settlements")


def coerce_date(value: Any) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def normalized_text(expr: str, *, uppercase: bool = False, default: str = "UNKNOWN") -> str:
    base = f"COALESCE(NULLIF(TRIM(CAST({expr} AS TEXT)), ''), '{default}')"
    return f"UPPER({base})" if uppercase else base


def resolve_transaction_max_date(
    engine: Any,
    mid: str,
    *,
    table: str | None = None,
) -> tuple[ResolvedSource, dt.date | None]:
    provider = resolve_transaction_source(engine, table=table)
    if provider.missing("merchant_id", "p_date"):
        return provider, None

    with engine.connect() as conn:
        max_dt = conn.execute(
            text(
                f"""
                SELECT MAX({provider.value('p_date')})
                FROM {provider.source_table}
                WHERE {provider.value('merchant_id')} = :mid
                """
            ),
            {"mid": mid},
        ).scalar()

    return provider, coerce_date(max_dt)

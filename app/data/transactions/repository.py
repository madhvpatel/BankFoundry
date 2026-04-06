from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import text

from config import Config

from app.data.providers import ResolvedSource, resolve_transaction_provider


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _dedupe_text_values(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text_value = str(value or "").strip()
        if text_value and text_value not in seen:
            seen.add(text_value)
            out.append(text_value)
    return out


def _transaction_provider(engine: Any, source_table: str | None = None) -> ResolvedSource:
    preferred = source_table or str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features") or "transaction_features")
    return resolve_transaction_provider(engine, preferred_table=preferred)


def _dim_expr(provider: ResolvedSource, dimension: str) -> str | None:
    if dimension == "payment_mode":
        return (
            f"COALESCE(NULLIF(TRIM({provider.value('payment_mode')}), ''), 'UNKNOWN')"
            if provider.has("payment_mode")
            else None
        )
    if dimension == "response_code":
        return (
            f"COALESCE(NULLIF(TRIM({provider.value('response_code')}), ''), 'UNKNOWN')"
            if provider.has("response_code")
            else None
        )
    if dimension == "status":
        return provider.value("status") if provider.has("status") else None
    if dimension == "day":
        return provider.value("p_date") if provider.has("p_date") else None
    if dimension == "hour":
        return provider.value("hour_of_day") if provider.has("hour_of_day") else None
    return None


def fetch_dashboard_metrics(
    engine: Any,
    *,
    merchant_id: str,
    terminal_id: str | None = None,
    lookback_days: int = 30,
    reference_date: date | None = None,
) -> dict[str, Any]:
    date_to = reference_date or datetime.now().date()
    date_from = date_to - timedelta(days=lookback_days)
    provider = _transaction_provider(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return {
            "window": {"from": date_from, "to": date_to},
            "kpis": {"attempts": 0, "success_txns": 0, "fail_txns": 0, "success_gmv": 0, "success_rate_pct": 0},
            "charts": {"payment_modes": []},
            "error": f"{provider.source_table or 'transaction source'} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }

    filter_sql = f"{provider.value('merchant_id')} = :mid"
    params = {"mid": merchant_id, "d1": date_from, "d2": date_to}
    if terminal_id:
        if not provider.has("terminal_id"):
            return {
                "window": {"from": date_from, "to": date_to},
                "kpis": {"attempts": 0, "success_txns": 0, "fail_txns": 0, "success_gmv": 0, "success_rate_pct": 0},
                "charts": {"payment_modes": []},
                "error": f"{provider.source_table or 'transaction source'} does not expose canonical terminal_id",
                "notes": list(provider.notes),
            }
        filter_sql += f" AND {provider.value('terminal_id')} = :tid"
        params["tid"] = terminal_id

    kpi_sql = f"""
        SELECT
            COUNT(*) as attempts,
            SUM(CASE WHEN {provider.value('status')}='SUCCESS' THEN 1 ELSE 0 END) as success_txns,
            SUM(CASE WHEN {provider.value('status')} IN ('FAILURE','FAILED') THEN 1 ELSE 0 END) as fail_txns,
            SUM(CASE WHEN {provider.value('status')}='SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END) as success_gmv
        FROM {provider.source_table}
        WHERE {filter_sql}
          AND {provider.value('p_date')} >= :d1 AND {provider.value('p_date')} <= :d2
    """

    with engine.connect() as conn:
        row = conn.execute(text(kpi_sql), params).fetchone()
        if not row or row[0] == 0:
            kpis = {"attempts": 0, "success_txns": 0, "fail_txns": 0, "success_gmv": 0, "success_rate_pct": 0}
        else:
            kpis = {
                "attempts": row.attempts,
                "success_txns": row.success_txns,
                "fail_txns": row.fail_txns,
                "success_gmv": float(row.success_gmv or 0),
                "success_rate_pct": round((row.success_txns / row.attempts) * 100, 2),
            }

        modes: list[dict[str, Any]] = []
        if provider.has("payment_mode"):
            mode_sql = f"""
                SELECT {provider.value('payment_mode')} as name,
                       SUM(CASE WHEN {provider.value('status')}='SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END) as value
                FROM {provider.source_table}
                WHERE {filter_sql}
                  AND {provider.value('p_date')} >= :d1 AND {provider.value('p_date')} <= :d2
                GROUP BY {provider.value('payment_mode')}
            """
            modes = [dict(r._mapping) for r in conn.execute(text(mode_sql), params).fetchall()]
            for mode in modes:
                mode["value"] = float(mode["value"] or 0)

    return {
        "window": {"from": date_from, "to": date_to},
        "kpis": kpis,
        "charts": {"payment_modes": modes},
        "notes": list(provider.notes),
    }


def verify_failure_drivers(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    by: str = "response_code",
    limit: int = 5,
    terminal_id: str | None = None,
    source_table: str,
) -> dict[str, Any]:
    provider = _transaction_provider(engine, source_table)
    dim_key = "payment_mode" if by == "payment_mode" else "response_code"
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees", dim_key)
    if missing:
        return {
            "verified": False,
            "verification_type": "failure_driver_ranking",
            "dimension": dim_key,
            "rows": [],
            "window": {"from": from_date, "to": to_date},
            "evidence": [],
            "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
            "error": f"{provider.source_table or source_table} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }
    dimension = "payment_mode" if by == "payment_mode" else "response_code"
    dim_expr = _dim_expr(provider, dimension)
    where = [
        f"{provider.value('merchant_id')} = :mid",
        f"{provider.value('p_date')} >= :d1",
        f"{provider.value('p_date')} < :d2",
        f"{provider.value('status')} IN ('FAILED', 'FAILURE')",
    ]
    params: dict[str, Any] = {
        "mid": merchant_id,
        "d1": from_date,
        "d2": to_date,
        "limit": max(1, min(int(limit or 5), 20)),
    }
    if terminal_id:
        if not provider.has("terminal_id"):
            return {
                "verified": False,
                "verification_type": "failure_driver_ranking",
                "dimension": dimension,
                "rows": [],
                "window": {"from": from_date, "to": to_date},
                "evidence": [],
                "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
                "error": f"{provider.source_table or source_table} does not expose canonical terminal_id",
                "notes": list(provider.notes),
            }
        where.append(f"{provider.value('terminal_id')} = :tid")
        params["tid"] = terminal_id

    query = text(
        f"""
        SELECT
          {dim_expr} AS driver,
          COUNT(*) AS failed_txns,
          COALESCE(SUM({provider.value('amount_rupees')}), 0) AS failed_gmv
        FROM {provider.source_table}
        WHERE {' AND '.join(where)}
        GROUP BY 1
        ORDER BY failed_txns DESC, failed_gmv DESC
        LIMIT :limit
        """
    )
    evidence = [f"verify_faildrivers:{dimension}:{from_date}:{to_date}"]
    if terminal_id:
        evidence.append(f"terminal:{terminal_id}")
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()
    except Exception as exc:
        return {
            "verified": False,
            "verification_type": "failure_driver_ranking",
            "dimension": dimension,
            "rows": [],
            "window": {"from": from_date, "to": to_date},
            "evidence": evidence,
            "error": str(exc),
            "notes": list(provider.notes),
        }

    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["failed_txns"] = int(item.get("failed_txns") or 0)
        out.append(item)
    evidence.extend([f"faildriver:{dimension}:{row.get('driver')}" for row in out if row.get("driver") is not None])
    return {
        "verified": bool(out),
        "verification_type": "failure_driver_ranking",
        "dimension": dimension,
        "rows": out,
        "window": {"from": from_date, "to": to_date},
        "evidence": evidence[:80],
        "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
        "error": None,
        "notes": list(provider.notes),
    }


def list_transactions(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    status: str = "ALL",
    payment_mode: str = "ALL",
    limit: int = 50,
    terminal_id: str | None = None,
    source_table: str,
) -> dict[str, Any]:
    provider = _transaction_provider(engine, source_table)
    missing = provider.missing("merchant_id", "p_date", "status", "tx_id")
    if missing:
        return {
            "rows": [],
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "error": f"{provider.source_table or source_table} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }

    where = [
        f"{provider.value('merchant_id')} = :mid",
        f"{provider.value('p_date')} >= :d1",
        f"{provider.value('p_date')} < :d2",
    ]
    params: dict[str, Any] = {
        "mid": merchant_id,
        "d1": from_date,
        "d2": to_date,
        "limit": max(1, min(int(limit or 50), 200)),
    }
    if status == "SUCCESS":
        where.append(f"{provider.value('status')} = 'SUCCESS'")
    elif status == "FAILURE":
        where.append(f"{provider.value('status')} IN ('FAILED', 'FAILURE')")
    if payment_mode != "ALL":
        if not provider.has("payment_mode"):
            return {
                "rows": [],
                "evidence": [],
                "window": {"from": from_date, "to": to_date},
                "error": f"{provider.source_table or source_table} does not expose canonical payment_mode",
                "notes": list(provider.notes),
            }
        where.append(f"{provider.value('payment_mode')} = :pm")
        params["pm"] = payment_mode
    if terminal_id:
        if not provider.has("terminal_id"):
            return {
                "rows": [],
                "evidence": [],
                "window": {"from": from_date, "to": to_date},
                "error": f"{provider.source_table or source_table} does not expose canonical terminal_id",
                "notes": list(provider.notes),
            }
        where.append(f"{provider.value('terminal_id')} = :tid")
        params["tid"] = terminal_id

    select_parts = [
        provider.select("tx_id", alias="tx_id"),
        provider.select("p_date", alias="p_date"),
        provider.select("initiated_at", alias="initiated_at", null_if_missing=True),
        provider.select("completed_at", alias="completed_at", null_if_missing=True),
        provider.select("payment_mode", alias="payment_mode", null_if_missing=True),
        provider.select("status", alias="status"),
        provider.select("response_code", alias="response_code", null_if_missing=True),
        provider.select("amount_rupees", alias="amount_rupees", null_if_missing=True),
        provider.select("terminal_id", alias="terminal_id", null_if_missing=True),
    ]
    order_column = provider.value("initiated_at") if provider.has("initiated_at") else provider.value("p_date")
    query = text(
        f"""
        SELECT
          {", ".join(select_parts)}
        FROM {provider.source_table}
        WHERE {' AND '.join(where)}
        ORDER BY CASE WHEN {order_column} IS NULL THEN 1 ELSE 0 END, {order_column} DESC
        LIMIT :limit
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()
    except Exception as exc:
        return {
            "rows": [],
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "error": str(exc),
            "notes": list(provider.notes),
        }
    out = [dict(row) for row in rows]
    evidence = [f"tx:{row.get('tx_id')}" for row in out if row.get("tx_id")]
    if terminal_id:
        evidence.append(f"terminal:{terminal_id}")
    return {
        "rows": out,
        "evidence": evidence,
        "window": {"from": from_date, "to": to_date},
        "error": None,
        "notes": list(provider.notes),
    }


def compute_kpis(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    group_by: str = "none",
    terminal_id: str | None = None,
    source_table: str,
) -> dict[str, Any]:
    provider = _transaction_provider(engine, source_table)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return {
            "rows": [],
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "group_by": group_by,
            "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
            "error": f"{provider.source_table or source_table} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }
    group_sql = ""
    select_group = ""
    if group_by == "day":
        bucket_expr = _dim_expr(provider, "day")
        select_group = f"{bucket_expr} AS bucket" if bucket_expr else ""
        group_sql = f"GROUP BY {bucket_expr}" if bucket_expr else ""
    elif group_by == "hour":
        bucket_expr = _dim_expr(provider, "hour")
        select_group = f"{bucket_expr} AS bucket" if bucket_expr else ""
        group_sql = f"GROUP BY {bucket_expr}" if bucket_expr else ""
    elif group_by == "payment_mode":
        bucket_expr = _dim_expr(provider, "payment_mode")
        select_group = f"{bucket_expr} AS bucket" if bucket_expr else ""
        group_sql = f"GROUP BY {bucket_expr}" if bucket_expr else ""
    elif group_by == "status":
        bucket_expr = _dim_expr(provider, "status")
        select_group = f"{bucket_expr} AS bucket" if bucket_expr else ""
        group_sql = f"GROUP BY {bucket_expr}" if bucket_expr else ""
    elif group_by == "response_code":
        bucket_expr = _dim_expr(provider, "response_code")
        select_group = f"{bucket_expr} AS bucket" if bucket_expr else ""
        group_sql = f"GROUP BY {bucket_expr}" if bucket_expr else ""
    else:
        bucket_expr = None
    if group_by != "none" and not select_group:
        return {
            "rows": [],
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "group_by": group_by,
            "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
            "error": f"{provider.source_table or source_table} does not expose canonical fields needed for group_by={group_by}",
            "notes": list(provider.notes),
        }

    where = [
        f"{provider.value('merchant_id')} = :mid",
        f"{provider.value('p_date')} >= :d1",
        f"{provider.value('p_date')} < :d2",
    ]
    params: dict[str, Any] = {"mid": merchant_id, "d1": from_date, "d2": to_date}
    if terminal_id:
        if not provider.has("terminal_id"):
            return {
                "rows": [],
                "evidence": [],
                "window": {"from": from_date, "to": to_date},
                "group_by": group_by,
                "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
                "error": f"{provider.source_table or source_table} does not expose canonical terminal_id",
                "notes": list(provider.notes),
            }
        where.append(f"{provider.value('terminal_id')} = :tid")
        params["tid"] = terminal_id

    query = text(
        f"""
        SELECT
          {select_group + ',' if select_group else ''}
          CAST(COUNT(*) AS INTEGER) AS attempts,
          CAST(SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) AS INTEGER) AS success_txns,
          CAST(SUM(CASE WHEN {provider.value('status')} IN ('FAILED', 'FAILURE') THEN 1 ELSE 0 END) AS INTEGER) AS fail_txns,
          ROUND(100.0 * SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
          COALESCE(SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END), 0) AS success_gmv,
          COALESCE(SUM(CASE WHEN {provider.value('status')} IN ('FAILED', 'FAILURE') THEN {provider.value('amount_rupees')} ELSE 0 END), 0) AS failed_gmv
        FROM {provider.source_table}
        WHERE {' AND '.join(where)}
        {group_sql}
        ORDER BY attempts DESC
        LIMIT 200
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    evidence = [f"kpi:{group_by}:{from_date}:{to_date}"]
    if terminal_id:
        evidence.append(f"terminal:{terminal_id}")
    return {
        "rows": [dict(row) for row in rows],
        "evidence": evidence,
        "window": {"from": from_date, "to": to_date},
        "group_by": group_by,
        "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
        "notes": list(provider.notes),
    }


def get_transaction_detail(
    engine: Any,
    *,
    merchant_id: str,
    tx_id: str,
    source_table: str,
) -> dict[str, Any]:
    provider = _transaction_provider(engine, source_table)
    missing = provider.missing("tx_id", "merchant_id")
    if missing:
        return {
            "row": None,
            "evidence": [],
            "error": f"{provider.source_table or source_table} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }

    select_parts = [
        provider.select("tx_id", alias="tx_id"),
        provider.select("merchant_id", alias="merchant_id"),
        provider.select("terminal_id", alias="terminal_id", null_if_missing=True),
        provider.select("source_system", alias="source_system", null_if_missing=True),
        provider.select("source_txn_id", alias="source_txn_id", null_if_missing=True),
        provider.select("p_date", alias="p_date", null_if_missing=True),
        provider.select("initiated_at", alias="initiated_at", null_if_missing=True),
        provider.select("completed_at", alias="completed_at", null_if_missing=True),
        provider.select("payment_mode", alias="payment_mode", null_if_missing=True),
        provider.select("status", alias="status", null_if_missing=True),
        provider.select("response_code", alias="response_code", null_if_missing=True),
        provider.select("response_desc", alias="response_desc", null_if_missing=True),
        provider.select("amount_rupees", alias="amount_rupees", null_if_missing=True),
        provider.select("card_network", alias="card_network", null_if_missing=True),
        provider.select("device_type", alias="device_type", null_if_missing=True),
        provider.select("os_name", alias="os_name", null_if_missing=True),
    ]
    query = text(
        f"""
        SELECT
          {", ".join(select_parts)}
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('tx_id')} = :tx_id
        LIMIT 1
        """
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(query, {"mid": merchant_id, "tx_id": tx_id}).mappings().first()
    except Exception as exc:
        return {"row": None, "evidence": [], "error": str(exc), "notes": list(provider.notes)}
    out = dict(row) if row else None
    evidence = [f"tx:{tx_id}"] if out else []
    return {"row": out, "evidence": evidence, "error": None, "notes": list(provider.notes)}


def get_payment_mode_mix(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    limit: int = 10,
    terminal_id: str | None = None,
    source_table: str,
) -> dict[str, Any]:
    provider = _transaction_provider(engine, source_table)
    if not provider.has("payment_mode"):
        return {
            "rows": [],
            "summary": {"attempts": 0, "fail_txns": 0, "success_gmv": 0.0},
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
            "error": f"{provider.source_table or source_table} does not expose canonical payment_mode",
            "notes": list(provider.notes),
        }
    if terminal_id and not provider.has("terminal_id"):
        return {
            "rows": [],
            "summary": {"attempts": 0, "fail_txns": 0, "success_gmv": 0.0},
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
            "error": f"{provider.source_table or source_table} does not expose canonical terminal_id",
            "notes": list(provider.notes),
        }

    payload = compute_kpis(
        engine,
        merchant_id=merchant_id,
        from_date=from_date,
        to_date=to_date,
        group_by="payment_mode",
        terminal_id=terminal_id,
        source_table=source_table,
    )
    rows = [dict(row) for row in (payload.get("rows") or []) if isinstance(row, dict)]
    total_attempts = sum(_safe_int(row.get("attempts")) for row in rows)
    total_fail_txns = sum(_safe_int(row.get("fail_txns")) for row in rows)
    total_success_gmv = sum(_safe_float(row.get("success_gmv")) for row in rows)

    out: list[dict[str, Any]] = []
    for row in rows[: max(1, min(int(limit or 10), 20))]:
        attempts = _safe_int(row.get("attempts"))
        fail_txns = _safe_int(row.get("fail_txns"))
        success_gmv = _safe_float(row.get("success_gmv"))
        out.append(
            {
                "payment_mode": str(row.get("bucket") or "UNKNOWN").strip() or "UNKNOWN",
                "attempts": attempts,
                "success_txns": _safe_int(row.get("success_txns")),
                "fail_txns": fail_txns,
                "success_rate_pct": _safe_float(row.get("success_rate_pct")),
                "success_gmv": success_gmv,
                "failed_gmv": _safe_float(row.get("failed_gmv")),
                "attempt_share_pct": round((attempts / total_attempts) * 100, 2) if total_attempts else 0.0,
                "failure_share_pct": round((fail_txns / total_fail_txns) * 100, 2) if total_fail_txns else 0.0,
                "success_gmv_share_pct": round((success_gmv / total_success_gmv) * 100, 2) if total_success_gmv else 0.0,
            }
        )

    return {
        "rows": out,
        "summary": {"attempts": total_attempts, "fail_txns": total_fail_txns, "success_gmv": round(total_success_gmv, 2)},
        "evidence": [str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        "window": {"from": from_date, "to": to_date},
        "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
        "error": None,
        "notes": list(provider.notes),
    }


def terminal_performance(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    limit: int = 10,
    terminal_id: str | None = None,
    source_table: str,
) -> dict[str, Any]:
    provider = _transaction_provider(engine, source_table)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees", "terminal_id")
    if missing:
        return {
            "rows": [],
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
            "error": f"{provider.source_table or source_table} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }
    where = [
        f"{provider.value('merchant_id')} = :mid",
        f"{provider.value('p_date')} >= :d1",
        f"{provider.value('p_date')} < :d2",
        f"{provider.value('terminal_id')} IS NOT NULL",
    ]
    params: dict[str, Any] = {
        "mid": merchant_id,
        "d1": from_date,
        "d2": to_date,
        "limit": max(1, min(int(limit or 10), 50)),
    }
    if terminal_id:
        where.append(f"{provider.value('terminal_id')} = :tid")
        params["tid"] = terminal_id
    query = text(
        f"""
        SELECT
          {provider.value('terminal_id')} AS terminal_id,
          CAST(COUNT(*) AS INTEGER) AS attempts,
          ROUND(100.0 * SUM(CASE WHEN {provider.value('status')}='SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS success_rate_pct,
          COALESCE(SUM(CASE WHEN {provider.value('status')}='SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END),0) AS success_gmv
        FROM {provider.source_table}
        WHERE {' AND '.join(where)}
        GROUP BY {provider.value('terminal_id')}
        ORDER BY attempts DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    out = [dict(row) for row in rows]
    evidence = [f"terminal:{row.get('terminal_id')}" for row in out if row.get("terminal_id")]
    if terminal_id and f"terminal:{terminal_id}" not in evidence:
        evidence.append(f"terminal:{terminal_id}")
    evidence.append(f"window:{from_date}:{to_date}")
    return {
        "rows": out,
        "evidence": evidence[:50],
        "window": {"from": from_date, "to": to_date},
        "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
        "notes": list(provider.notes),
    }


def daily_success_gmv(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    source_table: str,
) -> list[dict[str, Any]]:
    provider = _transaction_provider(engine, source_table)
    if provider.missing("merchant_id", "p_date", "status", "amount_rupees"):
        return []
    query = text(
        f"""
        SELECT
          {provider.value('p_date')} AS p_date,
          COALESCE(SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END), 0) AS success_gmv
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('p_date')} >= :d1
          AND {provider.value('p_date')} < :d2
        GROUP BY {provider.value('p_date')}
        ORDER BY {provider.value('p_date')} ASC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"mid": merchant_id, "d1": from_date, "d2": to_date}).mappings().all()
    return [dict(row) for row in rows]


def top_failure_codes(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    limit: int,
    source_table: str,
) -> list[dict[str, Any]]:
    provider = _transaction_provider(engine, source_table)
    if provider.missing("merchant_id", "p_date", "status", "amount_rupees", "response_code"):
        return []
    query = text(
        f"""
        SELECT
          COALESCE(NULLIF(TRIM({provider.value('response_code')}), ''), 'UNKNOWN') AS response_code,
          CAST(COUNT(*) AS INTEGER) AS fail_count,
          COALESCE(SUM({provider.value('amount_rupees')}),0) AS fail_amount
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('p_date')} >= :d1
          AND {provider.value('p_date')} < :d2
          AND {provider.value('status')} IN ('FAILED', 'FAILURE')
        GROUP BY 1
        ORDER BY fail_count DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            query,
            {"mid": merchant_id, "d1": from_date, "d2": to_date, "limit": max(1, min(int(limit or 10), 50))},
        ).mappings().all()
    return [dict(row) for row in rows]


def slice_performance_by_column(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    column: str,
    limit: int,
    source_table: str,
) -> list[dict[str, Any]] | None:
    provider = _transaction_provider(engine, source_table)
    if not provider.source_table:
        return None
    cols = set(provider.columns)
    if column not in cols:
        return None
    query = text(
        f"""
        SELECT
          {column} AS bucket,
          CAST(COUNT(*) AS INTEGER) AS attempts,
          ROUND(100.0 * SUM(CASE WHEN {provider.value('status')}='SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS success_rate_pct,
          COALESCE(SUM(CASE WHEN {provider.value('status')}='SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END),0) AS success_gmv
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('p_date')} >= :d1
          AND {provider.value('p_date')} < :d2
          AND {column} IS NOT NULL
        GROUP BY 1
        ORDER BY attempts DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            query,
            {"mid": merchant_id, "d1": from_date, "d2": to_date, "limit": max(1, min(int(limit or 10), 50))},
        ).mappings().all()
    return [dict(row) for row in rows]


def detect_velocity_anomalies(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    source_table: str,
) -> dict[str, Any]:
    provider = _transaction_provider(engine, source_table)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return {
            "verified": False,
            "anomalies": [],
            "summary": "Velocity analysis could not run because the transaction fact table is missing required columns.",
            "daily_breakdown": [],
            "top_payment_modes": [],
            "top_hours": [],
            "window_metrics": {},
            "evidence": [f"velocity:{merchant_id}:{from_date}:{to_date}"],
            "error": f"{provider.source_table or source_table} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }

    try:
        daily_payload = compute_kpis(
            engine,
            merchant_id=merchant_id,
            from_date=from_date,
            to_date=to_date,
            group_by="day",
            source_table=source_table,
        )
        mode_payload = compute_kpis(
            engine,
            merchant_id=merchant_id,
            from_date=from_date,
            to_date=to_date,
            group_by="payment_mode",
            source_table=source_table,
        )
        hour_payload = (
            compute_kpis(
                engine,
                merchant_id=merchant_id,
                from_date=from_date,
                to_date=to_date,
                group_by="hour",
                source_table=source_table,
            )
            if provider.has("hour_of_day")
            else {"rows": [], "evidence": []}
        )
    except Exception as exc:
        return {
            "verified": False,
            "anomalies": [],
            "summary": "Velocity analysis failed while reading the transaction fact table.",
            "daily_breakdown": [],
            "top_payment_modes": [],
            "top_hours": [],
            "window_metrics": {},
            "evidence": [f"velocity:{merchant_id}:{from_date}:{to_date}"],
            "error": str(exc),
            "notes": list(provider.notes),
        }

    daily_rows = sorted(
        [dict(row) for row in (daily_payload.get("rows") or []) if isinstance(row, dict)],
        key=lambda row: str(row.get("bucket") or ""),
    )
    mode_rows = [dict(row) for row in (mode_payload.get("rows") or []) if isinstance(row, dict)]
    hour_rows = [dict(row) for row in (hour_payload.get("rows") or []) if isinstance(row, dict)]

    total_attempts = sum(_safe_int(row.get("attempts")) for row in daily_rows)
    total_fail_txns = sum(_safe_int(row.get("fail_txns")) for row in daily_rows)
    active_days = len(daily_rows)
    average_attempts = round(total_attempts / active_days, 2) if active_days else 0.0
    average_fail_rate_pct = round((100.0 * total_fail_txns / total_attempts), 2) if total_attempts else 0.0

    anomalies: list[dict[str, Any]] = []
    for row in daily_rows:
        bucket = str(row.get("bucket") or "").strip()
        attempts = _safe_int(row.get("attempts"))
        fail_txns = _safe_int(row.get("fail_txns"))
        success_rate_pct = _safe_float(row.get("success_rate_pct"))
        fail_rate_pct = round(max(0.0, 100.0 - success_rate_pct), 2) if attempts else 0.0
        if active_days > 1 and attempts >= max(3, int(average_attempts * 1.75) or 0):
            anomalies.append(
                {
                    "signal_type": "daily_attempt_spike",
                    "severity": "high" if attempts >= max(5, int(average_attempts * 2.25) or 0) else "medium",
                    "bucket": bucket,
                    "value": attempts,
                    "baseline_value": average_attempts,
                    "description": f"Daily attempts reached {attempts} against an active-day average of {average_attempts}.",
                }
            )
        if attempts >= 3 and fail_txns >= 2 and fail_rate_pct >= max(40.0, average_fail_rate_pct + 20.0):
            anomalies.append(
                {
                    "signal_type": "failure_rate_spike",
                    "severity": "high" if fail_rate_pct >= 60.0 else "medium",
                    "bucket": bucket,
                    "value": fail_rate_pct,
                    "baseline_value": average_fail_rate_pct,
                    "description": f"Failure rate reached {fail_rate_pct}% with {fail_txns} failed transactions.",
                }
            )

    top_mode = mode_rows[0] if mode_rows else None
    if top_mode and total_attempts >= 6:
        mode_attempts = _safe_int(top_mode.get("attempts"))
        mode_share_pct = round((100.0 * mode_attempts / total_attempts), 2) if total_attempts else 0.0
        if mode_share_pct >= 75.0:
            anomalies.append(
                {
                    "signal_type": "payment_mode_concentration",
                    "severity": "medium",
                    "bucket": str(top_mode.get("bucket") or "UNKNOWN"),
                    "value": mode_share_pct,
                    "baseline_value": round(100.0 / max(len(mode_rows), 1), 2),
                    "description": f"{top_mode.get('bucket') or 'UNKNOWN'} accounts for {mode_share_pct}% of attempts in the window.",
                }
            )

    top_hour = hour_rows[0] if hour_rows else None
    if top_hour and total_attempts >= 6:
        hour_attempts = _safe_int(top_hour.get("attempts"))
        hour_share_pct = round((100.0 * hour_attempts / total_attempts), 2) if total_attempts else 0.0
        hour_bucket = _safe_int(top_hour.get("bucket"))
        if hour_share_pct >= 45.0 or (0 <= hour_bucket <= 5 and hour_attempts >= 3):
            anomalies.append(
                {
                    "signal_type": "hourly_concentration",
                    "severity": "medium",
                    "bucket": hour_bucket,
                    "value": hour_share_pct,
                    "baseline_value": round(100.0 / 24.0, 2),
                    "description": f"Hour {hour_bucket:02d} accounts for {hour_share_pct}% of attempts in the window.",
                }
            )

    if anomalies:
        top_signal = anomalies[0]
        summary = (
            f"Detected {len(anomalies)} velocity signal(s) in the current window; "
            f"top signal is {str(top_signal.get('signal_type') or 'unknown').replace('_', ' ')}."
        )
    elif total_attempts:
        summary = "No material velocity anomalies were detected in the current transaction window."
    else:
        summary = "No transactions were available in the requested window, so no velocity anomalies were detected."

    return {
        "verified": True,
        "anomalies": anomalies,
        "summary": summary,
        "daily_breakdown": daily_rows,
        "top_payment_modes": mode_rows[:5],
        "top_hours": hour_rows[:5],
        "window_metrics": {
            "attempts": total_attempts,
            "fail_txns": total_fail_txns,
            "active_days": active_days,
            "average_attempts_per_active_day": average_attempts,
            "average_fail_rate_pct": average_fail_rate_pct,
        },
        "evidence": _dedupe_text_values(
            [f"velocity:{merchant_id}:{from_date}:{to_date}"]
            + list(daily_payload.get("evidence") or [])
            + list(mode_payload.get("evidence") or [])
            + list(hour_payload.get("evidence") or [])
        ),
        "scope": {"merchant_id": merchant_id},
        "error": None,
        "notes": list(provider.notes),
    }

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from app.data.providers import resolve_settlement_provider, resolve_transaction_provider


def table_columns(engine: Any, table: str) -> set[str]:
    table = str(table or "").strip()
    if not table:
        return set()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :t
                    """
                ),
                {"t": table},
            ).fetchall()
        cols = {str(r[0]).lower() for r in rows if r and r[0]}
        if cols:
            return cols
    except Exception:
        pass

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {str(r[1]).lower() for r in rows if len(r) > 1 and r[1]}
    except Exception:
        return set()


def table_exists(engine: Any, table: str) -> bool:
    return bool(table_columns(engine, table))


def terminal_scope_summary_from_source(
    engine: Any,
    merchant_id: str,
    terminal_id: str,
    from_date: str,
    to_date: str,
    *,
    source_table: str,
) -> dict[str, Any]:
    if not hasattr(engine, "connect"):
        return {}
    cols = table_columns(engine, source_table)
    required = {"merchant_id", "terminal_id", "p_date", "status", "amount_rupees"}
    if not required.issubset(cols):
        return {}

    query = text(
        f"""
        SELECT
          CAST(COUNT(*) AS INTEGER) AS attempts,
          CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS INTEGER) AS success_txns,
          CAST(SUM(CASE WHEN status IN ('FAILED', 'FAILURE') THEN 1 ELSE 0 END) AS INTEGER) AS fail_txns,
          ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
          COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount_rupees ELSE 0 END), 0) AS success_gmv,
          COALESCE(SUM(CASE WHEN status IN ('FAILED', 'FAILURE') THEN amount_rupees ELSE 0 END), 0) AS failed_gmv
        FROM {source_table}
        WHERE merchant_id = :mid
          AND terminal_id = :tid
          AND p_date >= :d1
          AND p_date < :d2
        """
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(query, {"mid": merchant_id, "tid": terminal_id, "d1": from_date, "d2": to_date}).mappings().first()
    except Exception:
        return {}
    return dict(row) if row else {}


def terminal_scope_kpis_by_mode(
    engine: Any,
    merchant_id: str,
    terminal_id: str,
    from_date: str,
    to_date: str,
    *,
    source_table: str,
) -> list[dict[str, Any]]:
    if not hasattr(engine, "connect"):
        return []
    cols = table_columns(engine, source_table)
    required = {"merchant_id", "terminal_id", "p_date", "payment_mode", "status", "amount_rupees"}
    if not required.issubset(cols):
        return []

    query = text(
        f"""
        SELECT
          COALESCE(payment_mode, 'UNKNOWN') AS bucket,
          CAST(COUNT(*) AS INTEGER) AS attempts,
          CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS INTEGER) AS success_txns,
          CAST(SUM(CASE WHEN status IN ('FAILED', 'FAILURE') THEN 1 ELSE 0 END) AS INTEGER) AS fail_txns,
          ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
          COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount_rupees ELSE 0 END), 0) AS success_gmv,
          COALESCE(SUM(CASE WHEN status IN ('FAILED', 'FAILURE') THEN amount_rupees ELSE 0 END), 0) AS failed_gmv
        FROM {source_table}
        WHERE merchant_id = :mid
          AND terminal_id = :tid
          AND p_date >= :d1
          AND p_date < :d2
        GROUP BY COALESCE(payment_mode, 'UNKNOWN')
        ORDER BY attempts DESC, failed_gmv DESC
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"mid": merchant_id, "tid": terminal_id, "d1": from_date, "d2": to_date}).mappings().all()
    except Exception:
        return []
    return [dict(row) for row in rows]


def terminal_scope_failure_drivers(
    engine: Any,
    merchant_id: str,
    terminal_id: str,
    from_date: str,
    to_date: str,
    *,
    by: str,
    source_table: str,
    limit: int = 5,
) -> dict[str, Any]:
    if not hasattr(engine, "connect"):
        return {"verified": False, "rows": [], "evidence": []}
    dimension = "payment_mode" if by == "payment_mode" else "response_code"
    cols = table_columns(engine, source_table)
    required = {"merchant_id", "terminal_id", "p_date", "status", "amount_rupees", dimension}
    if not required.issubset(cols):
        return {"verified": False, "rows": [], "evidence": []}

    query = text(
        f"""
        SELECT
          COALESCE({dimension}, 'UNKNOWN') AS driver,
          CAST(COUNT(*) AS INTEGER) AS failed_txns,
          COALESCE(SUM(amount_rupees), 0) AS failed_gmv
        FROM {source_table}
        WHERE merchant_id = :mid
          AND terminal_id = :tid
          AND p_date >= :d1
          AND p_date < :d2
          AND status IN ('FAILED', 'FAILURE')
        GROUP BY COALESCE({dimension}, 'UNKNOWN')
        ORDER BY failed_txns DESC, failed_gmv DESC
        LIMIT :limit
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                query,
                {"mid": merchant_id, "tid": terminal_id, "d1": from_date, "d2": to_date, "limit": int(limit)},
            ).mappings().all()
    except Exception:
        return {"verified": False, "rows": [], "evidence": []}

    out = [dict(row) for row in rows]
    evidence = [f"terminal_scope:{dimension}:{terminal_id}:{from_date}:{to_date}"]
    prefix = "payment_mode" if dimension == "payment_mode" else "response_code"
    evidence.extend([f"terminal_scope:{prefix}:{terminal_id}:{row.get('driver')}" for row in out if row.get("driver")])
    return {
        "verified": bool(out),
        "dimension": dimension,
        "rows": out,
        "evidence": evidence,
        "window": {"from": from_date, "to": to_date},
    }


def count_rows(engine: Any, table: str, merchant_id: str) -> int:
    cols = table_columns(engine, table)
    if not cols:
        return 0
    mid_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not mid_col:
        return 0
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) AS c FROM {table} WHERE {mid_col} = :mid"), {"mid": merchant_id}).fetchone()
    return int(row[0] or 0) if row else 0


def integration_status_from_table(engine: Any, table: str, merchant_id: str, integration_type: str) -> dict[str, Any] | None:
    cols = table_columns(engine, table)
    if not cols:
        return None
    mid_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not mid_col:
        return None

    where = [f"{mid_col} = :mid"]
    params: dict[str, Any] = {"mid": merchant_id}

    type_col = "integration_type" if "integration_type" in cols else ("type" if "type" in cols else "")
    if type_col and table == "merchant_integrations":
        where.append(f"LOWER({type_col}) = :itype")
        params["itype"] = integration_type.lower()

    status_col = "status" if "status" in cols else ""
    provider_col = "provider" if "provider" in cols else ("name" if "name" in cols else "")

    query = f"SELECT * FROM {table} WHERE {' AND '.join(where)} LIMIT 1"
    with engine.connect() as conn:
        row = conn.execute(text(query), params).mappings().first()
    if not row:
        return {"connected": False, "source_table": table, "provider": None, "status": None}

    status_value = str(row.get(status_col) or "").strip() if status_col else ""
    provider_value = str(row.get(provider_col) or "").strip() if provider_col else ""
    connected = True
    if status_value:
        connected = status_value.upper() not in {"DISCONNECTED", "INACTIVE", "ERROR", "FAILED"}

    return {
        "connected": connected,
        "source_table": table,
        "provider": provider_value or None,
        "status": status_value or None,
    }


def detect_connected_systems(
    engine: Any,
    merchant_id: str,
    *,
    integration_table_candidates: dict[str, tuple[str, ...]],
    query_source_table: str,
) -> dict[str, Any]:
    integrations: dict[str, Any] = {}
    for integration_type, tables in integration_table_candidates.items():
        detected = None
        for table in tables:
            if not table_exists(engine, table):
                continue
            detected = integration_status_from_table(engine, table, merchant_id, integration_type)
            if detected and detected.get("connected"):
                break
        integrations[integration_type] = detected or {"connected": False, "source_table": None, "provider": None, "status": None}

    payment_source = resolve_transaction_provider(engine, preferred_table=query_source_table)
    payment_table = payment_source.source_table or query_source_table
    payments_exists = bool(payment_source.source_table and not payment_source.missing("merchant_id", "p_date", "status"))
    payments_rows = count_rows(engine, payment_table, merchant_id) if payments_exists else 0

    settlement_source = resolve_settlement_provider(engine)
    settlement_table = settlement_source.source_table or "settlements"
    settlements_exists = bool(settlement_source.source_table and not settlement_source.missing("merchant_id"))
    settlements_rows = count_rows(engine, settlement_table, merchant_id) if settlements_exists else 0

    refunds_exists = table_exists(engine, "refunds")
    refunds_rows = count_rows(engine, "refunds", merchant_id) if refunds_exists else 0

    chargebacks_exists = table_exists(engine, "chargebacks")
    chargebacks_rows = count_rows(engine, "chargebacks", merchant_id) if chargebacks_exists else 0

    terminals_exists = table_exists(engine, "terminals")
    terminal_health_exists = table_exists(engine, "terminal_health_snapshots")
    terminal_rows = count_rows(engine, "terminals", merchant_id) if terminals_exists else 0
    terminal_health_rows = count_rows(engine, "terminal_health_snapshots", merchant_id) if terminal_health_exists else 0

    data_domains: dict[str, Any] = {
        "payments": {
            "available": payments_exists,
            "source_table": payment_table,
            "row_count": payments_rows,
            "latest_date": None,
        },
        "settlements": {
            "available": settlements_exists,
            "source_table": settlement_table,
            "row_count": settlements_rows,
            "latest_date": None,
        },
        "refunds": {
            "available": refunds_exists,
            "source_table": "refunds",
            "row_count": refunds_rows,
            "latest_date": None,
        },
        "chargebacks": {
            "available": chargebacks_exists,
            "source_table": "chargebacks",
            "row_count": chargebacks_rows,
            "latest_date": None,
        },
        "terminal_ops": {
            "available": terminals_exists or terminal_health_exists,
            "source_table": "terminal_health_snapshots" if terminal_health_exists else ("terminals" if terminals_exists else None),
            "row_count": max(terminal_rows, terminal_health_rows),
            "terminals_table": terminals_exists,
            "health_table": terminal_health_exists,
            "latest_date": None,
        },
    }

    systems: dict[str, Any] = {**integrations}
    systems["payments"] = {
        "connected": bool(data_domains["payments"]["available"]),
        "source_table": payment_table,
        "row_count": payments_rows,
    }
    systems["settlements"] = {
        "connected": bool(data_domains["settlements"]["available"]),
        "source_table": settlement_table,
        "row_count": settlements_rows,
    }
    systems["refunds"] = {
        "connected": bool(data_domains["refunds"]["available"]),
        "source_table": "refunds",
        "row_count": refunds_rows,
    }
    systems["chargebacks"] = {
        "connected": bool(data_domains["chargebacks"]["available"]),
        "source_table": "chargebacks",
        "row_count": chargebacks_rows,
    }
    systems["terminal_ops"] = {
        "connected": bool(data_domains["terminal_ops"]["available"]),
        "source_table": data_domains["terminal_ops"]["source_table"],
        "row_count": data_domains["terminal_ops"]["row_count"],
        "terminals_table": terminals_exists,
        "health_table": terminal_health_exists,
    }

    coverage_label = "Payments only"
    if integrations["erp"]["connected"]:
        coverage_label = "Payments + ERP"
    elif integrations["accounting"]["connected"]:
        coverage_label = "Payments + accounting"
    elif integrations["pos"]["connected"]:
        coverage_label = "Payments + POS"
    elif data_domains["settlements"]["available"] or data_domains["refunds"]["available"] or data_domains["chargebacks"]["available"] or data_domains["terminal_ops"]["available"]:
        coverage_label = "Payments + acquiring ops"

    return {
        "integrations": integrations,
        "data_domains": data_domains,
        "systems": systems,
        "coverage_label": coverage_label,
    }


def operating_signals(
    engine: Any,
    merchant_id: str,
    from_date: str,
    to_date: str,
    *,
    query_source_table: str,
) -> dict[str, Any]:
    cols = table_columns(engine, query_source_table)
    if not cols:
        return {}

    invoice_expr = None
    if "invoice_nr" in cols:
        invoice_expr = "invoice_nr"
    elif "invoice_number" in cols:
        invoice_expr = "invoice_number"

    source_txn_expr = "source_txn_id" if "source_txn_id" in cols else None
    terminal_expr = "terminal_id" if "terminal_id" in cols else None

    select_parts = [
        "COUNT(*)::int AS attempts",
        "COUNT(DISTINCT payment_mode)::int AS payment_mode_count" if "payment_mode" in cols else "0::int AS payment_mode_count",
    ]
    if terminal_expr:
        select_parts.append(f"COUNT(DISTINCT {terminal_expr})::int AS distinct_terminals")
    else:
        select_parts.append("0::int AS distinct_terminals")
    if invoice_expr:
        select_parts.append(
            f"ROUND(100.0 * AVG(CASE WHEN {invoice_expr} IS NOT NULL AND TRIM(CAST({invoice_expr} AS TEXT)) <> '' THEN 1 ELSE 0 END), 2) AS invoice_reference_coverage_pct"
        )
    else:
        select_parts.append("0.0 AS invoice_reference_coverage_pct")
    if source_txn_expr:
        select_parts.append(
            f"ROUND(100.0 * AVG(CASE WHEN {source_txn_expr} IS NOT NULL AND TRIM(CAST({source_txn_expr} AS TEXT)) <> '' THEN 1 ELSE 0 END), 2) AS source_reference_coverage_pct"
        )
    else:
        select_parts.append("0.0 AS source_reference_coverage_pct")

    query = text(
        f"""
        SELECT {', '.join(select_parts)}
        FROM {query_source_table}
        WHERE merchant_id = :mid
          AND p_date >= :d1
          AND p_date < :d2
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"mid": merchant_id, "d1": from_date, "d2": to_date}).mappings().first()
    return dict(row) if row else {}

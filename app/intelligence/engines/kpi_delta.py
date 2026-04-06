from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import text

from ..money import get_amount_scale, scale_inr
from ..source_adapters import normalized_text, resolve_transaction_source
from .operational_signals import resolve_window_from_data


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _delta_pct(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return round(((current - previous) / previous) * 100.0, 2)


def _window_pair(start_date: dt.date, end_date: dt.date) -> tuple[dt.date, dt.date]:
    window = end_date - start_date
    previous_end = start_date
    previous_start = previous_end - window
    return previous_start, previous_end


def _normalize_overall(row: Any, amount_scale: float) -> dict[str, float]:
    return {
        "attempts": float(row.get("attempts") or 0),
        "success_txns": float(row.get("success_txns") or 0),
        "success_gmv": float(scale_inr(row.get("success_gmv"), amount_scale) or 0.0),
        "success_rate_pct": _safe_float(row.get("success_rate_pct")),
        "avg_ticket": float(scale_inr(row.get("avg_ticket"), amount_scale) or 0.0),
    }


def _metric_delta(current: float, previous: float) -> dict[str, float | None]:
    return {
        "current": round(current, 2),
        "previous": round(previous, 2),
        "delta_abs": round(current - previous, 2),
        "delta_pct": _delta_pct(current, previous),
    }


def _overall_sql(provider) -> str:
    return f"""
    SELECT
      COUNT(*) AS attempts,
      SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
      COALESCE(SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END), 0) AS success_gmv,
      ROUND(100.0 * SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
      ROUND(AVG(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} ELSE NULL END), 2) AS avg_ticket
    FROM {provider.source_table}
    WHERE {provider.value('merchant_id')} = :mid
      AND {provider.value('p_date')} >= :start_date
      AND {provider.value('p_date')} < :end_date
    """


def _mode_sql(provider) -> str:
    mode_expr = normalized_text(provider.value("payment_mode"), uppercase=True)
    return f"""
    SELECT
      {mode_expr} AS payment_mode,
      COUNT(*) AS attempts,
      SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
      COALESCE(SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END), 0) AS success_gmv,
      ROUND(100.0 * SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
      ROUND(AVG(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} ELSE NULL END), 2) AS avg_ticket
    FROM {provider.source_table}
    WHERE {provider.value('merchant_id')} = :mid
      AND {provider.value('p_date')} >= :start_date
      AND {provider.value('p_date')} < :end_date
    GROUP BY 1
    ORDER BY attempts DESC
    """


def compute_kpi_delta(
    engine,
    mid: str,
    *,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    window_days: int = 30,
) -> dict[str, Any]:
    provider = resolve_transaction_source(engine)
    window = resolve_window_from_data(engine, mid=mid, table=provider.source_table or "transaction_features", window_days=window_days)
    start_date = start_date or window.start_date
    end_date = end_date or window.end_date
    previous_start, previous_end = _window_pair(start_date, end_date)
    amount_scale = get_amount_scale(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return {
            "engine": "kpi_delta",
            "window": {"start_date": str(start_date), "end_date": str(end_date)},
            "previous_window": {"start_date": str(previous_start), "end_date": str(previous_end)},
            "merchant_level": {},
            "by_payment_mode": [],
            "errors": [f"{provider.source_table or 'transaction source'} missing canonical fields: {', '.join(sorted(missing))}"],
            "notes": list(provider.notes),
        }

    with engine.connect() as conn:
        current_overall = conn.execute(text(_overall_sql(provider)), {"mid": mid, "start_date": start_date, "end_date": end_date}).mappings().first() or {}
        previous_overall = conn.execute(text(_overall_sql(provider)), {"mid": mid, "start_date": previous_start, "end_date": previous_end}).mappings().first() or {}
        if provider.has("payment_mode"):
            current_modes = conn.execute(text(_mode_sql(provider)), {"mid": mid, "start_date": start_date, "end_date": end_date}).mappings().all()
            previous_modes = conn.execute(text(_mode_sql(provider)), {"mid": mid, "start_date": previous_start, "end_date": previous_end}).mappings().all()
        else:
            current_modes = []
            previous_modes = []

    current_norm = _normalize_overall(current_overall, amount_scale)
    previous_norm = _normalize_overall(previous_overall, amount_scale)
    merchant_level = {
        key: _metric_delta(current_norm[key], previous_norm[key])
        for key in ("attempts", "success_txns", "success_gmv", "success_rate_pct", "avg_ticket")
    }

    previous_mode_map = {str(row.get("payment_mode") or "UNKNOWN"): _normalize_overall(row, amount_scale) for row in previous_modes}
    by_payment_mode = []
    seen_modes = {str(row.get("payment_mode") or "UNKNOWN") for row in current_modes} | set(previous_mode_map.keys())
    current_mode_map = {str(row.get("payment_mode") or "UNKNOWN"): _normalize_overall(row, amount_scale) for row in current_modes}
    for mode in sorted(seen_modes):
        current_row = current_mode_map.get(mode, {"attempts": 0.0, "success_txns": 0.0, "success_gmv": 0.0, "success_rate_pct": 0.0, "avg_ticket": 0.0})
        previous_row = previous_mode_map.get(mode, {"attempts": 0.0, "success_txns": 0.0, "success_gmv": 0.0, "success_rate_pct": 0.0, "avg_ticket": 0.0})
        by_payment_mode.append(
            {
                "payment_mode": mode,
                "attempts": _metric_delta(current_row["attempts"], previous_row["attempts"]),
                "success_txns": _metric_delta(current_row["success_txns"], previous_row["success_txns"]),
                "success_gmv": _metric_delta(current_row["success_gmv"], previous_row["success_gmv"]),
                "success_rate_pct": _metric_delta(current_row["success_rate_pct"], previous_row["success_rate_pct"]),
                "avg_ticket": _metric_delta(current_row["avg_ticket"], previous_row["avg_ticket"]),
            }
        )

    errors: list[str] = []
    if not provider.has("payment_mode"):
        errors.append(f"{provider.source_table} missing payment_mode; mode-level KPI delta skipped")

    return {
        "engine": "kpi_delta",
        "window": {"start_date": str(start_date), "end_date": str(end_date)},
        "previous_window": {"start_date": str(previous_start), "end_date": str(previous_end)},
        "merchant_level": merchant_level,
        "by_payment_mode": by_payment_mode,
        "errors": errors,
        "notes": list(provider.notes),
    }

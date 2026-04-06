from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import text

from ..money import get_amount_scale, scale_inr
from ..source_adapters import normalized_text, resolve_transaction_source
from .operational_signals import resolve_window_from_data

SUPPORTED_METRICS = {"success_rate_pct", "failed_gmv", "attempts", "success_gmv"}
SUPPORTED_DIMENSIONS = {"payment_mode", "response_code", "hour_of_day", "terminal_id", "payer_bank_code"}


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _dimension_expr(dimension: str, provider) -> str:
    if dimension == "payment_mode" and provider.has("payment_mode"):
        return normalized_text(provider.value("payment_mode"), uppercase=True)
    if dimension == "response_code" and provider.has("response_code"):
        return normalized_text(provider.value("response_code"), uppercase=True)
    if dimension == "hour_of_day" and provider.has("hour_of_day"):
        return f"CAST({provider.value('hour_of_day')} AS TEXT)"
    if dimension == "terminal_id" and provider.has("terminal_id"):
        return normalized_text(provider.value("terminal_id"))
    if dimension == "payer_bank_code" and provider.has("payer_bank_code"):
        return normalized_text(provider.value("payer_bank_code"), uppercase=True)
    return ""


def _metric_sql(metric: str, provider) -> tuple[str, str]:
    status_expr = provider.value("status")
    amount_expr = provider.value("amount_rupees")
    if metric == "failed_gmv":
        return f"COALESCE(SUM(CASE WHEN {status_expr} IN ('FAILURE','FAILED') THEN {amount_expr} ELSE 0 END), 0)", "gmv"
    if metric == "success_gmv":
        return f"COALESCE(SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN {amount_expr} ELSE 0 END), 0)", "gmv"
    if metric == "attempts":
        return "COUNT(*)", "count"
    return (
        f"ROUND(100.0 * SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)",
        "rate",
    )


def _window_pair(start_date: dt.date, end_date: dt.date) -> tuple[dt.date, dt.date]:
    window = end_date - start_date
    previous_end = start_date
    previous_start = previous_end - window
    return previous_start, previous_end


def _aggregate(engine, provider, mid: str, start_date: dt.date, end_date: dt.date, dimension: str, metric: str) -> dict[str, dict[str, float]]:
    expr = _dimension_expr(dimension, provider)
    if not expr or provider.missing("merchant_id", "p_date", "status", "amount_rupees"):
        return {}
    metric_expr, _ = _metric_sql(metric, provider)
    query = text(
        f"""
        SELECT {expr} AS dim_value,
               {metric_expr} AS metric_value,
               COUNT(*) AS attempts,
               ROUND(AVG(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} ELSE NULL END), 2) AS avg_ticket
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('p_date')} >= :start_date
          AND {provider.value('p_date')} < :end_date
        GROUP BY 1
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"mid": mid, "start_date": start_date, "end_date": end_date}).mappings().all()
    amount_scale = get_amount_scale(engine)
    output: dict[str, dict[str, float]] = {}
    for row in rows:
        value = str(row.get("dim_value") or "UNKNOWN")
        metric_value = _safe_float(row.get("metric_value"))
        if metric in {"failed_gmv", "success_gmv"}:
            metric_value = float(scale_inr(metric_value, amount_scale) or 0.0)
        avg_ticket = float(scale_inr(row.get("avg_ticket"), amount_scale) or 0.0)
        output[value] = {
            "metric_value": metric_value,
            "attempts": float(row.get("attempts") or 0.0),
            "avg_ticket": avg_ticket,
        }
    return output


def compute_attribution(
    engine,
    mid: str,
    *,
    metric: str,
    dimension: str,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    window_days: int = 30,
) -> dict[str, Any]:
    if metric not in SUPPORTED_METRICS:
        return {
            "engine": "attribution",
            "metric": metric,
            "dimension": dimension,
            "window": {},
            "current": 0.0,
            "previous": 0.0,
            "delta": 0.0,
            "attributions": [],
            "errors": [f"unsupported metric: {metric}"],
        }
    if dimension not in SUPPORTED_DIMENSIONS:
        return {
            "engine": "attribution",
            "metric": metric,
            "dimension": dimension,
            "window": {},
            "current": 0.0,
            "previous": 0.0,
            "delta": 0.0,
            "attributions": [],
            "errors": [f"unsupported dimension: {dimension}"],
        }

    provider = resolve_transaction_source(engine)
    window = resolve_window_from_data(engine, mid=mid, table=provider.source_table or "transaction_features", window_days=window_days)
    start_date = start_date or window.start_date
    end_date = end_date or window.end_date
    previous_start, previous_end = _window_pair(start_date, end_date)

    current_map = _aggregate(engine, provider, mid, start_date, end_date, dimension, metric)
    previous_map = _aggregate(engine, provider, mid, previous_start, previous_end, dimension, metric)
    all_values = set(current_map.keys()) | set(previous_map.keys())

    attributions = []
    current_total = 0.0
    previous_total = 0.0
    for value in all_values:
        current_row = current_map.get(value, {"metric_value": 0.0, "attempts": 0.0, "avg_ticket": 0.0})
        previous_row = previous_map.get(value, {"metric_value": 0.0, "attempts": 0.0, "avg_ticket": 0.0})
        current_value = float(current_row.get("metric_value") or 0.0)
        previous_value = float(previous_row.get("metric_value") or 0.0)
        delta = round(current_value - previous_value, 2)
        current_total += current_value
        previous_total += previous_value
        if metric in {"failed_gmv", "success_gmv"}:
            impact_rupees = abs(delta)
        elif metric == "attempts":
            impact_rupees = abs(delta) * max(float(current_row.get("avg_ticket") or 0.0), float(previous_row.get("avg_ticket") or 0.0))
        else:
            baseline_attempts = max(float(current_row.get("attempts") or 0.0), float(previous_row.get("attempts") or 0.0))
            baseline_ticket = max(float(current_row.get("avg_ticket") or 0.0), float(previous_row.get("avg_ticket") or 0.0))
            impact_rupees = abs(delta) / 100.0 * baseline_attempts * baseline_ticket
        attributions.append(
            {
                "dimension": dimension,
                "value": value,
                "current_value": round(current_value, 2),
                "previous_value": round(previous_value, 2),
                "delta": delta,
                "impact_rupees": round(impact_rupees, 2),
            }
        )

    attributions = sorted(
        attributions,
        key=lambda row: (float(row.get("impact_rupees") or 0.0), abs(float(row.get("delta") or 0.0))),
        reverse=True,
    )
    for idx, row in enumerate(attributions, start=1):
        row["contribution_rank"] = idx

    errors: list[str] = []
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        errors.append(f"{provider.source_table or 'transaction source'} missing canonical fields: {', '.join(sorted(missing))}")
    if not _dimension_expr(dimension, provider):
        errors.append(f"{provider.source_table or 'transaction source'} missing canonical dimension: {dimension}")

    return {
        "engine": "attribution",
        "window": {"start_date": str(start_date), "end_date": str(end_date)},
        "metric": metric,
        "current": round(current_total, 2),
        "previous": round(previous_total, 2),
        "delta": round(current_total - previous_total, 2),
        "attributions": attributions[:10],
        "errors": errors,
        "notes": list(provider.notes),
    }

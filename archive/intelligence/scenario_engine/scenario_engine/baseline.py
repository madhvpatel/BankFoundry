from __future__ import annotations

import datetime as dt
import logging
from typing import Any

from sqlalchemy import text

from app.intelligence.constants import FAILED_STATUS_SQL
from app.intelligence.money import get_amount_scale, scale_inr

logger = logging.getLogger("scenario_engine")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _table_columns(conn, table: str) -> set[str]:
    try:
        rows = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                """
            ),
            {"table_name": table},
        ).fetchall()
        cols = {str(r[0]).lower() for r in rows if r and r[0]}
        if cols:
            return cols
    except Exception:
        pass

    try:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {str(r[1]).lower() for r in rows if len(r) > 1 and r[1]}
    except Exception:
        return set()


def _pick_date_col(columns: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c.lower() in columns:
            return c
    return None


def _fetch_count_and_sum(
    conn,
    table: str,
    mid_col: str,
    amount_col: str,
    mid: str,
    start_date: dt.date,
    end_date: dt.date,
    date_col: str | None,
) -> tuple[int, float]:
    where_parts = [f"{mid_col} = :mid"]
    params: dict[str, Any] = {"mid": mid}
    if date_col:
        where_parts.append(f"{date_col} >= :start_date")
        where_parts.append(f"{date_col} < :end_date")
        params["start_date"] = start_date
        params["end_date"] = end_date

    sql = f"""
        SELECT
            COUNT(*) AS c,
            COALESCE(SUM({amount_col}), 0) AS gmv
        FROM {table}
        WHERE {' AND '.join(where_parts)}
    """
    try:
        row = conn.execute(text(sql), params).fetchone()
        return _safe_int(row[0] if row else 0), _safe_float(row[1] if row else 0.0)
    except Exception:
        logger.debug("Skipping optional table metrics for %s", table, exc_info=True)
        return 0, 0.0


def fetch_baseline(engine, mid: str, start_date, end_date) -> dict:
    """
    Baseline metrics for scenario simulation.
    Values are deterministic and sourced from DB only.
    """
    amount_scale = get_amount_scale(engine)

    baseline: dict[str, Any] = {
        "attempts": 0,
        "success_txns": 0,
        "fail_txns": 0,
        "success_rate": 0.0,
        "success_revenue": 0.0,
        "avg_ticket_success": 0.0,
        "payment_modes": [],
        "refund_count": 0,
        "refund_gmv": 0.0,
        "chargeback_count": 0,
        "chargeback_gmv": 0.0,
        "window_start": str(start_date),
        "window_end": str(end_date),
    }

    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS attempts,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
                    SUM(CASE WHEN status IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS fail_txns,
                    ROUND(
                        100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                        2
                    ) AS success_rate,
                    COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount_rupees ELSE 0 END), 0) AS success_revenue,
                    ROUND(AVG(CASE WHEN status = 'SUCCESS' THEN amount_rupees ELSE NULL END), 2) AS avg_ticket_success
                FROM transaction_features
                WHERE merchant_id = :mid
                  AND p_date >= :start_date
                  AND p_date < :end_date
                """
            ),
            {"mid": mid, "start_date": start_date, "end_date": end_date},
        ).fetchone()

        if row:
            baseline["attempts"] = _safe_int(row[0])
            baseline["success_txns"] = _safe_int(row[1])
            baseline["fail_txns"] = _safe_int(row[2])
            baseline["success_rate"] = _safe_float(row[3])
            baseline["success_revenue"] = scale_inr(row[4], amount_scale)
            baseline["avg_ticket_success"] = scale_inr(row[5], amount_scale)

        pm_rows = conn.execute(
            text(
                f"""
                SELECT
                    UPPER(COALESCE(NULLIF(TRIM(payment_mode), ''), 'UNKNOWN')) AS mode,
                    COUNT(*) AS attempts,
                    ROUND(
                        100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                        2
                    ) AS success_rate
                FROM transaction_features
                WHERE merchant_id = :mid
                  AND p_date >= :start_date
                  AND p_date < :end_date
                GROUP BY 1
                ORDER BY attempts DESC
                """
            ),
            {"mid": mid, "start_date": start_date, "end_date": end_date},
        ).fetchall()

        baseline["payment_modes"] = [
            {
                "mode": str(r[0] or "UNKNOWN"),
                "attempts": _safe_int(r[1]),
                "success_rate": _safe_float(r[2]),
            }
            for r in pm_rows
        ]

        refund_cols = _table_columns(conn, "refunds")
        refund_date_col = _pick_date_col(refund_cols, ["refund_date", "created_at", "p_date"])
        refund_count, refund_gmv = _fetch_count_and_sum(
            conn=conn,
            table="refunds",
            mid_col="mid",
            amount_col="refund_amount",
            mid=mid,
            start_date=start_date,
            end_date=end_date,
            date_col=refund_date_col,
        )
        baseline["refund_count"] = refund_count
        baseline["refund_gmv"] = scale_inr(refund_gmv, amount_scale)

        cb_cols = _table_columns(conn, "chargebacks")
        cb_date_col = _pick_date_col(cb_cols, ["chargeback_date", "created_at", "p_date"])
        cb_count, cb_gmv = _fetch_count_and_sum(
            conn=conn,
            table="chargebacks",
            mid_col="mid",
            amount_col="chargeback_amount",
            mid=mid,
            start_date=start_date,
            end_date=end_date,
            date_col=cb_date_col,
        )
        baseline["chargeback_count"] = cb_count
        baseline["chargeback_gmv"] = scale_inr(cb_gmv, amount_scale)

    return baseline

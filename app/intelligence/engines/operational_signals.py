# app/intelligence/engines/operational_signals.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..money import get_amount_scale, scale_inr
from ..response_codes import canonical_response_category, canonical_response_desc, normalize_response_code
from ..source_adapters import normalized_text, resolve_transaction_max_date, resolve_transaction_source

logger = logging.getLogger(__name__)
FAILED_STATUS_SQL = "('FAILURE','FAILED')"


@dataclass
class QueryWindow:
    start_date: date
    end_date: date  # exclusive

    def as_params(self, mid: str) -> dict[str, Any]:
        return {"mid": mid, "start_date": self.start_date, "end_date": self.end_date}


def _fallback_window(window_days: int) -> QueryWindow:
    end = date.today() + timedelta(days=1)
    start = end - timedelta(days=window_days)
    return QueryWindow(start_date=start, end_date=end)


def _window_from_provider(engine: Engine, mid: str, provider, window_days: int) -> QueryWindow:
    if provider.missing("merchant_id", "p_date"):
        return _fallback_window(window_days)

    provider, max_dt = resolve_transaction_max_date(engine, mid, table=provider.source_table)
    if not max_dt:
        return _fallback_window(window_days)

    end = max_dt + timedelta(days=1)
    start = end - timedelta(days=window_days)
    return QueryWindow(start_date=start, end_date=end)


def resolve_window_from_data(
    engine: Engine,
    mid: str,
    table: str = "transaction_features",
    window_days: int = 30,
) -> QueryWindow:
    provider = resolve_transaction_source(engine, table=table)
    return _window_from_provider(engine, mid, provider, window_days)


def _rows_to_dicts(cols: list[str], rows: list[tuple[Any, ...]], limit: int = 50) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows[:limit]:
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def collect_operational_signals(
    engine: Engine,
    mid: str,
    table: str = "transaction_features",
    window_days: int = 30,
    top_n: int = 10,
) -> dict[str, Any]:
    provider = resolve_transaction_source(engine, table=table)
    window = _window_from_provider(engine, mid, provider, window_days)
    params = window.as_params(mid)
    amount_scale = get_amount_scale(engine)
    status_expr = provider.value("status") if provider.has("status") else ""
    merchant_expr = provider.value("merchant_id") if provider.has("merchant_id") else ""
    date_expr = provider.value("p_date") if provider.has("p_date") else ""
    amount_expr = provider.value("amount_rupees") if provider.has("amount_rupees") else ""

    signals: dict[str, Any] = {
        "engine": "operational_signals",
        "merchant_id": mid,
        "window": asdict(window),
        "tables": {"primary": provider.source_table},
        "metrics": {},
        "evidence": {},
        "errors": [],
        "notes": list(provider.notes),
    }

    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        signals["errors"].append(
            f"{provider.source_table or 'transaction source'} is missing canonical fields: {', '.join(sorted(missing))}"
        )
        return signals

    payment_mode_expr = provider.value("payment_mode") if provider.has("payment_mode") else ""
    payment_mode_text = normalized_text(payment_mode_expr, uppercase=True) if payment_mode_expr else "'UNKNOWN'"
    response_code_text = normalized_text(provider.value("response_code"), uppercase=True) if provider.has("response_code") else ""
    response_desc_text = normalized_text(provider.value("response_desc")) if provider.has("response_desc") else "'UNKNOWN'"

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT
                        COUNT(*) AS attempts,
                        SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
                        SUM(CASE WHEN {status_expr} IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS fail_txns,
                        ROUND(100.0 * SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS success_rate_pct,
                        COALESCE(SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN {amount_expr} ELSE 0 END),0) AS success_revenue,
                        ROUND(AVG(CASE WHEN {status_expr} = 'SUCCESS' THEN {amount_expr} ELSE NULL END), 2) AS avg_ticket_success
                    FROM {provider.source_table}
                    WHERE {merchant_expr} = :mid
                      AND {date_expr} >= :start_date
                      AND {date_expr} < :end_date
                    """
                ),
                params,
            ).fetchone()

            if row:
                signals["metrics"].update(
                    {
                        "attempts": int(row[0] or 0),
                        "success_txns": int(row[1] or 0),
                        "fail_txns": int(row[2] or 0),
                        "success_rate_pct": float(row[3] or 0.0),
                        "success_revenue": scale_inr(row[4], amount_scale),
                        "avg_ticket_success": scale_inr(row[5], amount_scale),
                    }
                )

            if payment_mode_expr:
                res = conn.execute(
                    text(
                        f"""
                        SELECT
                            {payment_mode_text} AS payment_mode,
                            COUNT(*) AS attempts,
                            SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
                            SUM(CASE WHEN {status_expr} IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS fail_txns,
                            ROUND(100.0 * SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS success_rate_pct,
                            COALESCE(SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN {amount_expr} ELSE 0 END),0) AS success_revenue
                        FROM {provider.source_table}
                        WHERE {merchant_expr} = :mid
                          AND {date_expr} >= :start_date
                          AND {date_expr} < :end_date
                        GROUP BY 1
                        ORDER BY attempts DESC
                        """
                    ),
                    params,
                )
                cols = list(res.keys())
                rows = res.fetchall()
                signals["evidence"]["by_payment_mode"] = _rows_to_dicts(cols, rows, limit=10)
                for row_dict in signals["evidence"]["by_payment_mode"]:
                    row_dict["success_revenue"] = scale_inr(row_dict.get("success_revenue"), amount_scale)
            else:
                signals["evidence"]["by_payment_mode"] = []
                signals["errors"].append(f"{provider.source_table} missing payment_mode; payment-mode evidence skipped")

            response_code_case = (
                f"""
                CASE
                    WHEN {response_code_text} = 'UNKNOWN'
                    THEN CASE
                        WHEN {payment_mode_text} = 'UPI' THEN 'UPI_FAILURE'
                        ELSE 'UNMAPPED_FAILURE'
                    END
                    ELSE {response_code_text}
                END
                """
                if response_code_text
                else f"CASE WHEN {payment_mode_text} = 'UPI' THEN 'UPI_FAILURE' ELSE 'UNMAPPED_FAILURE' END"
            )
            res = conn.execute(
                text(
                    f"""
                    SELECT
                        {response_code_case} AS response_code,
                        MAX({response_desc_text}) AS response_desc,
                        COUNT(*) AS fail_count,
                        COALESCE(SUM({amount_expr}),0) AS fail_amount
                    FROM {provider.source_table}
                    WHERE {merchant_expr} = :mid
                      AND {status_expr} IN {FAILED_STATUS_SQL}
                      AND {date_expr} >= :start_date
                      AND {date_expr} < :end_date
                    GROUP BY 1
                    ORDER BY fail_count DESC
                    LIMIT :top_n
                    """
                ),
                {**params, "top_n": top_n},
            )
            cols = list(res.keys())
            rows = res.fetchall()
            signals["evidence"]["top_failure_codes"] = _rows_to_dicts(cols, rows, limit=top_n)
            for row_dict in signals["evidence"]["top_failure_codes"]:
                code = normalize_response_code(row_dict.get("response_code"))
                row_dict["response_code"] = code
                row_dict["response_desc"] = canonical_response_desc(code, row_dict.get("response_desc"))
                row_dict["response_category"] = canonical_response_category(code)
                row_dict["fail_amount"] = scale_inr(row_dict.get("fail_amount"), amount_scale)

            if provider.has("hour_of_day"):
                res = conn.execute(
                    text(
                        f"""
                        SELECT
                            {provider.value('hour_of_day')} AS hour_of_day,
                            COUNT(*) AS attempts,
                            ROUND(100.0 * SUM(CASE WHEN {status_expr} IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS failure_rate_pct
                        FROM {provider.source_table}
                        WHERE {merchant_expr} = :mid
                          AND {date_expr} >= :start_date
                          AND {date_expr} < :end_date
                        GROUP BY 1
                        ORDER BY 1
                        """
                    ),
                    params,
                )
                signals["evidence"]["failure_rate_by_hour"] = _rows_to_dicts(list(res.keys()), res.fetchall(), limit=24)
            else:
                signals["evidence"]["failure_rate_by_hour"] = []
                signals["errors"].append(f"{provider.source_table} missing hour_of_day; hourly evidence skipped")

            if provider.has("day_of_week"):
                res = conn.execute(
                    text(
                        f"""
                        SELECT
                            {provider.value('day_of_week')} AS day_of_week,
                            COUNT(*) AS attempts,
                            ROUND(100.0 * SUM(CASE WHEN {status_expr} IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS failure_rate_pct
                        FROM {provider.source_table}
                        WHERE {merchant_expr} = :mid
                          AND {date_expr} >= :start_date
                          AND {date_expr} < :end_date
                        GROUP BY 1
                        ORDER BY 1
                        """
                    ),
                    params,
                )
                signals["evidence"]["failure_rate_by_dow"] = _rows_to_dicts(list(res.keys()), res.fetchall(), limit=7)
            else:
                signals["evidence"]["failure_rate_by_dow"] = []
                signals["errors"].append(f"{provider.source_table} missing day_of_week; weekday evidence skipped")

            if provider.has("terminal_id"):
                res = conn.execute(
                    text(
                        f"""
                        SELECT
                            {normalized_text(provider.value('terminal_id'))} AS terminal_id,
                            COUNT(*) AS attempts,
                            ROUND(100.0 * SUM(CASE WHEN {status_expr} = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS success_rate_pct,
                            ROUND(AVG(CASE WHEN {status_expr} = 'SUCCESS' THEN {amount_expr} ELSE NULL END), 2) AS avg_ticket_success
                        FROM {provider.source_table}
                        WHERE {merchant_expr} = :mid
                          AND {date_expr} >= :start_date
                          AND {date_expr} < :end_date
                          AND {provider.value('terminal_id')} IS NOT NULL
                        GROUP BY 1
                        ORDER BY attempts DESC
                        LIMIT :top_n
                        """
                    ),
                    {**params, "top_n": top_n},
                )
                signals["evidence"]["terminal_health_top"] = _rows_to_dicts(list(res.keys()), res.fetchall(), limit=top_n)
                for row_dict in signals["evidence"]["terminal_health_top"]:
                    row_dict["avg_ticket_success"] = scale_inr(row_dict.get("avg_ticket_success"), amount_scale)
            else:
                signals["evidence"]["terminal_health_top"] = []
                signals["errors"].append(f"{provider.source_table} missing terminal_id; terminal evidence skipped")

            if provider.has("payer_bank_code") and payment_mode_expr:
                payer_bank_expr = normalized_text(provider.value("payer_bank_code"), uppercase=True)
                res = conn.execute(
                    text(
                        f"""
                        SELECT
                            {payer_bank_expr} AS payer_bank_code,
                            COUNT(*) AS fail_count,
                            ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (),0), 2) AS pct_of_failures
                        FROM {provider.source_table}
                        WHERE {merchant_expr} = :mid
                          AND {status_expr} IN {FAILED_STATUS_SQL}
                          AND {payment_mode_text} = 'UPI'
                          AND {payer_bank_expr} <> 'UNKNOWN'
                          AND {date_expr} >= :start_date
                          AND {date_expr} < :end_date
                        GROUP BY 1
                        ORDER BY fail_count DESC
                        LIMIT :top_n
                        """
                    ),
                    {**params, "top_n": top_n},
                )
                signals["evidence"]["top_payer_banks_in_failures"] = _rows_to_dicts(list(res.keys()), res.fetchall(), limit=top_n)

                missing_upi_bank = conn.execute(
                    text(
                        f"""
                        SELECT
                            SUM(CASE WHEN {payer_bank_expr} = 'UNKNOWN' THEN 1 ELSE 0 END) AS missing_bank_failures,
                            ROUND(
                                100.0 * SUM(CASE WHEN {payer_bank_expr} = 'UNKNOWN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                                2
                            ) AS missing_bank_pct
                        FROM {provider.source_table}
                        WHERE {merchant_expr} = :mid
                          AND {status_expr} IN {FAILED_STATUS_SQL}
                          AND {payment_mode_text} = 'UPI'
                          AND {date_expr} >= :start_date
                          AND {date_expr} < :end_date
                        """
                    ),
                    params,
                ).fetchone()
                if missing_upi_bank:
                    signals["metrics"]["upi_failures_missing_bank_code"] = int(missing_upi_bank[0] or 0)
                    signals["metrics"]["upi_failures_missing_bank_code_pct"] = float(missing_upi_bank[1] or 0.0)
            else:
                signals["evidence"]["top_payer_banks_in_failures"] = []
                signals["errors"].append(f"{provider.source_table} missing payer_bank_code or payment_mode; bank evidence skipped")

    except Exception as exc:
        logger.exception("collect_operational_signals failed")
        signals["errors"].append(str(exc))

    return signals

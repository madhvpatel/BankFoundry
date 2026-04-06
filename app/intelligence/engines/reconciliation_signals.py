from __future__ import annotations

import datetime as dt
import logging
from collections import defaultdict
from typing import Any

from sqlalchemy import text

from app.data.merchant_ops import repository as merchant_ops_repository

from ..money import get_amount_scale, scale_inr
from ..source_adapters import normalized_text, resolve_settlement_source, resolve_transaction_source
from .operational_signals import resolve_window_from_data

logger = logging.getLogger("reconciliation_signals")


def _table_columns(engine: Any, table: str) -> set[str]:
    return merchant_ops_repository.table_columns(engine, table)


def _pick(*values: str) -> str:
    for value in values:
        if value:
            return value
    return ""


def _to_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_date(value: Any) -> dt.date | None:
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


def _scaled(amount_scale: float, value: Any) -> float:
    return float(scale_inr(value, amount_scale) or 0.0)


def _metric_defaults() -> dict[str, Any]:
    return {
        "successful_txns": 0,
        "successful_gmv": 0.0,
        "refund_count": 0,
        "refund_gmv": 0.0,
        "chargeback_count": 0,
        "chargeback_gmv": 0.0,
        "settlement_batches": 0,
        "gross_settlement": 0.0,
        "net_settlement": 0.0,
        "known_deductions_total": 0.0,
        "unexplained_residual": 0.0,
        "expected_mdr_pct": None,
        "actual_mdr_pct": None,
        "mdr_variance_pct": None,
        "held_batches": 0,
        "delayed_batches": 0,
    }


def _evidence_defaults() -> dict[str, Any]:
    return {
        "recon_status_breakdown": [],
        "top_recon_exceptions": [],
        "deduction_components": [],
        "settlement_status_breakdown": [],
        "hold_reason_breakdown": [],
        "largest_shortfalls": [],
        "shortfall_aging": [],
    }


def _fetch_window_sum_and_count(
    engine: Any,
    table: str,
    *,
    merchant_col: str,
    amount_col: str,
    count_label: str,
    sum_label: str,
    merchant_id: str,
    start_date: dt.date,
    end_date: dt.date,
    date_col: str = "",
) -> tuple[int, float, str | None]:
    if not table or not merchant_col or not amount_col:
        return 0, 0.0, f"{table or 'table'} missing required columns"

    where_parts = [f"{merchant_col} = :mid"]
    params: dict[str, Any] = {"mid": merchant_id}
    schema_note = None
    if date_col:
        where_parts.extend([f"{date_col} >= :start_date", f"{date_col} < :end_date"])
        params["start_date"] = start_date
        params["end_date"] = end_date
    else:
        schema_note = f"{table} missing date column; using all available rows"

    query = text(
        f"""
        SELECT COUNT(*) AS {count_label}, COALESCE(SUM({amount_col}), 0) AS {sum_label}
        FROM {table}
        WHERE {' AND '.join(where_parts)}
        """
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row[0] or 0), _to_float(row[1]), schema_note
    except Exception as exc:
        return 0, 0.0, f"{table} query failed: {exc}"


def _compute_expected_mdr_pct(
    engine: Any,
    merchant_id: str,
    start_date: dt.date,
    end_date: dt.date,
) -> tuple[float | None, str | None]:
    rate_cols = _table_columns(engine, "mdr_rates")
    txn_provider = resolve_transaction_source(engine)
    if not rate_cols:
        return None, None
    if "mid" not in rate_cols or "payment_mode" not in rate_cols or "mdr_percentage" not in rate_cols:
        return None, "mdr_rates missing required columns; expected MDR not computed"
    missing = txn_provider.missing("merchant_id", "payment_mode", "status", "amount_rupees", "p_date")
    if missing:
        return None, f"{txn_provider.source_table or 'transaction source'} missing canonical fields for MDR: {', '.join(sorted(missing))}"

    payment_mode_expr = normalized_text(txn_provider.value("payment_mode"), uppercase=True)
    card_network_expr = (
        normalized_text(txn_provider.value("card_network"), uppercase=True, default="")
        if txn_provider.has("card_network")
        else "''"
    )

    with engine.connect() as conn:
        tx_rows = conn.execute(
            text(
                f"""
                SELECT
                    {payment_mode_expr} AS payment_mode,
                    {card_network_expr} AS card_network,
                    COALESCE(SUM({txn_provider.value('amount_rupees')}), 0) AS success_gmv
                FROM {txn_provider.source_table}
                WHERE {txn_provider.value('merchant_id')} = :mid
                  AND {txn_provider.value('status')} = 'SUCCESS'
                  AND {txn_provider.value('p_date')} >= :start_date
                  AND {txn_provider.value('p_date')} < :end_date
                GROUP BY 1, 2
                """
            ),
            {"mid": merchant_id, "start_date": start_date, "end_date": end_date},
        ).mappings().all()
        rate_rows = conn.execute(
            text(
                """
                SELECT
                    UPPER(COALESCE(NULLIF(TRIM(payment_mode), ''), 'UNKNOWN')) AS payment_mode,
                    UPPER(COALESCE(NULLIF(TRIM(card_network), ''), '')) AS card_network,
                    COALESCE(mdr_percentage, 0) AS mdr_percentage
                FROM mdr_rates
                WHERE mid = :mid
                """
            ),
            {"mid": merchant_id},
        ).mappings().all()

    if not tx_rows or not rate_rows:
        return None, None

    rate_map: dict[tuple[str, str], float] = {}
    for row in rate_rows:
        rate_map[(str(row.get("payment_mode") or "UNKNOWN"), str(row.get("card_network") or ""))] = _to_float(row.get("mdr_percentage"))

    weighted_value = 0.0
    weighted_base = 0.0
    for row in tx_rows:
        payment_mode = str(row.get("payment_mode") or "UNKNOWN")
        card_network = str(row.get("card_network") or "")
        rate = rate_map.get((payment_mode, card_network))
        if rate is None:
            rate = rate_map.get((payment_mode, ""))
        if rate is None:
            continue
        success_gmv = _to_float(row.get("success_gmv"))
        weighted_value += success_gmv * rate
        weighted_base += success_gmv

    if weighted_base <= 0:
        return None, None
    return round(weighted_value / weighted_base, 4), None


def collect_reconciliation_signals(engine, mid, window_days: int = 30):
    txn_provider = resolve_transaction_source(engine)
    settlement_provider = resolve_settlement_source(engine)
    window = resolve_window_from_data(engine, mid=mid, table=txn_provider.source_table or "transaction_features", window_days=window_days)
    amount_scale = get_amount_scale(engine)
    notes = list(txn_provider.notes)
    for note in settlement_provider.notes:
        if note not in notes:
            notes.append(note)
    signals = {
        "engine": "reconciliation_signals",
        "merchant_id": mid,
        "window": {"start_date": str(window.start_date), "end_date": str(window.end_date)},
        "metrics": _metric_defaults(),
        "evidence": _evidence_defaults(),
        "errors": [],
        "notes": notes,
        "tables": {
            "transactions": txn_provider.source_table,
            "settlements": settlement_provider.source_table,
        },
    }

    txn_missing = txn_provider.missing("merchant_id", "status", "amount_rupees", "p_date")
    if not txn_missing:
        try:
            with engine.connect() as conn:
                success_row = conn.execute(
                    text(
                        f"""
                        SELECT COUNT(*) AS c, COALESCE(SUM({txn_provider.value('amount_rupees')}), 0) AS gmv
                        FROM {txn_provider.source_table}
                        WHERE {txn_provider.value('merchant_id')} = :mid
                          AND {txn_provider.value('status')} = 'SUCCESS'
                          AND {txn_provider.value('p_date')} >= :start_date
                          AND {txn_provider.value('p_date')} < :end_date
                        """
                    ),
                    {"mid": mid, "start_date": window.start_date, "end_date": window.end_date},
                ).fetchone()
            signals["metrics"]["successful_txns"] = int(success_row[0] or 0)
            signals["metrics"]["successful_gmv"] = _scaled(amount_scale, success_row[1])
        except Exception as exc:
            logger.error("reconciliation success transaction query failed: %s", exc)
            signals["errors"].append(f"{txn_provider.source_table} success query failed: {exc}")
    else:
        signals["errors"].append(
            f"{txn_provider.source_table or 'transaction source'} missing canonical fields: {', '.join(sorted(txn_missing))}"
        )

    refund_cols = _table_columns(engine, "refunds")
    refund_count, refund_gmv, refund_note = _fetch_window_sum_and_count(
        engine,
        "refunds",
        merchant_col=_pick("merchant_id" if "merchant_id" in refund_cols else "", "mid" if "mid" in refund_cols else ""),
        amount_col=_pick("amount_rupees" if "amount_rupees" in refund_cols else "", "refund_amount" if "refund_amount" in refund_cols else ""),
        count_label="refund_count",
        sum_label="refund_gmv",
        merchant_id=mid,
        start_date=window.start_date,
        end_date=window.end_date,
        date_col=_pick(
            "refund_date" if "refund_date" in refund_cols else "",
            "created_at" if "created_at" in refund_cols else "",
            "p_date" if "p_date" in refund_cols else "",
        ),
    )
    signals["metrics"]["refund_count"] = refund_count
    signals["metrics"]["refund_gmv"] = _scaled(amount_scale, refund_gmv)
    if refund_note:
        signals["errors"].append(refund_note)

    cb_cols = _table_columns(engine, "chargebacks")
    cb_count, cb_gmv, cb_note = _fetch_window_sum_and_count(
        engine,
        "chargebacks",
        merchant_col=_pick("merchant_id" if "merchant_id" in cb_cols else "", "mid" if "mid" in cb_cols else ""),
        amount_col=_pick("amount_rupees" if "amount_rupees" in cb_cols else "", "chargeback_amount" if "chargeback_amount" in cb_cols else ""),
        count_label="chargeback_count",
        sum_label="chargeback_gmv",
        merchant_id=mid,
        start_date=window.start_date,
        end_date=window.end_date,
        date_col=_pick(
            "chargeback_date" if "chargeback_date" in cb_cols else "",
            "created_at" if "created_at" in cb_cols else "",
            "opened_at" if "opened_at" in cb_cols else "",
            "p_date" if "p_date" in cb_cols else "",
        ),
    )
    signals["metrics"]["chargeback_count"] = cb_count
    signals["metrics"]["chargeback_gmv"] = _scaled(amount_scale, cb_gmv)
    if cb_note:
        signals["errors"].append(cb_note)

    if not settlement_provider.has("merchant_id"):
        signals["errors"].append("settlement source missing merchant scope; reconciliation metrics not computed")
        return signals
    if not settlement_provider.has("amount_rupees"):
        signals["errors"].append("settlement source missing amount fields; reconciliation metrics not computed")
        return signals

    where_parts = [f"{settlement_provider.value('merchant_id')} = :mid"]
    params: dict[str, Any] = {"mid": mid}
    scope_expr = settlement_provider.value("scope_date") if settlement_provider.has("scope_date") else ""
    if scope_expr:
        where_parts.extend([f"{scope_expr} >= :start_date", f"{scope_expr} < :end_date"])
        params["start_date"] = window.start_date
        params["end_date"] = window.end_date
    else:
        signals["errors"].append("settlement source missing date column; using all available rows for settlement evidence")

    order_expr = scope_expr
    if not order_expr and settlement_provider.has("settlement_id"):
        order_expr = settlement_provider.value("settlement_id")
    if not order_expr:
        order_expr = settlement_provider.value("amount_rupees")

    settlement_query = text(
        f"""
        SELECT
            {settlement_provider.select('settlement_id', alias='settlement_id', null_if_missing=True)},
            {settlement_provider.select('scope_date', alias='settlement_date', null_if_missing=True)},
            {settlement_provider.select('status', alias='settlement_status', null_if_missing=True)},
            {settlement_provider.select('hold_reason', alias='hold_reason', null_if_missing=True)},
            {settlement_provider.select('gross_amount', alias='gross_amount', null_if_missing=True)},
            {settlement_provider.select('net_settlement_amount', alias='net_settlement_amount', null_if_missing=True)},
            {settlement_provider.select('mdr_deducted', alias='mdr_deducted', null_if_missing=True)},
            {settlement_provider.select('gst_on_mdr', alias='gst_on_mdr', null_if_missing=True)},
            {settlement_provider.select('tds_deducted', alias='tds_deducted', null_if_missing=True)},
            {settlement_provider.select('chargeback_deductions', alias='chargeback_deductions', null_if_missing=True)},
            {settlement_provider.select('reserve_held', alias='reserve_held', null_if_missing=True)},
            {settlement_provider.select('adjustment_amount', alias='adjustment_amount', null_if_missing=True)}
        FROM {settlement_provider.source_table}
        WHERE {' AND '.join(where_parts)}
        ORDER BY {order_expr} DESC
        """
    )

    with engine.connect() as conn:
        settlement_rows = [dict(row) for row in conn.execute(settlement_query, params).mappings().all()]

    gross_total = 0.0
    net_total = 0.0
    known_total = 0.0
    unexplained_total = 0.0
    actual_mdr_base = 0.0
    actual_mdr_value = 0.0
    held_batches = 0
    delayed_batches = 0
    deduction_totals: dict[str, float] = defaultdict(float)
    settlement_status_breakdown: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "gross_amount": 0.0})
    hold_reason_breakdown: dict[str, int] = defaultdict(int)
    largest_shortfalls: list[dict[str, Any]] = []
    shortfall_aging: dict[str, int] = defaultdict(int)

    for row in settlement_rows:
        gross = _scaled(amount_scale, row.get("gross_amount"))
        net = _scaled(amount_scale, row.get("net_settlement_amount"))
        mdr = _scaled(amount_scale, row.get("mdr_deducted"))
        gst = _scaled(amount_scale, row.get("gst_on_mdr"))
        tds = _scaled(amount_scale, row.get("tds_deducted"))
        chargeback = _scaled(amount_scale, row.get("chargeback_deductions"))
        reserve = _scaled(amount_scale, row.get("reserve_held"))
        adjustment = _scaled(amount_scale, row.get("adjustment_amount"))
        known = round(mdr + gst + tds + chargeback + reserve + adjustment, 2)
        shortfall = round(max(gross - net, 0.0), 2)
        residual = round(abs(shortfall - known), 2)
        settlement_date = _to_date(row.get("settlement_date"))
        settlement_status = str(row.get("settlement_status") or "UNKNOWN").upper()
        hold_reason = str(row.get("hold_reason") or "").strip() or "UNKNOWN"

        gross_total += gross
        net_total += net
        known_total += known
        unexplained_total += residual
        actual_mdr_value += mdr
        actual_mdr_base += gross

        deduction_totals["MDR"] += mdr
        deduction_totals["GST on MDR"] += gst
        deduction_totals["TDS"] += tds
        deduction_totals["Chargeback deductions"] += chargeback
        deduction_totals["Reserve held"] += reserve
        deduction_totals["Adjustments"] += adjustment

        settlement_status_breakdown[settlement_status]["count"] += 1
        settlement_status_breakdown[settlement_status]["gross_amount"] += gross

        held_like = settlement_status in {"HELD", "ON_HOLD", "HOLD", "RESERVE_HOLD"} or hold_reason != "UNKNOWN"
        delayed_like = settlement_status in {"PENDING", "DELAYED", "HELD", "ON_HOLD"}
        if held_like:
            held_batches += 1
            hold_reason_breakdown[hold_reason] += 1
        if delayed_like:
            delayed_batches += 1

        if shortfall > 0:
            age_days = max(0, (window.end_date - (settlement_date or window.start_date)).days)
            if age_days <= 2:
                shortfall_aging["0-2 days"] += 1
            elif age_days <= 7:
                shortfall_aging["3-7 days"] += 1
            elif age_days <= 30:
                shortfall_aging["8-30 days"] += 1
            else:
                shortfall_aging["31+ days"] += 1
            largest_shortfalls.append(
                {
                    "settlement_id": str(row.get("settlement_id") or ""),
                    "settlement_date": str(settlement_date) if settlement_date else None,
                    "status": settlement_status,
                    "gross_amount": round(gross, 2),
                    "net_settlement_amount": round(net, 2),
                    "shortfall_amount": shortfall,
                    "known_deductions_total": round(known, 2),
                    "unexplained_residual": round(residual, 2),
                }
            )

    expected_mdr_pct, mdr_note = _compute_expected_mdr_pct(engine, mid, window.start_date, window.end_date)
    if mdr_note:
        signals["errors"].append(mdr_note)
    actual_mdr_pct = round((actual_mdr_value / actual_mdr_base) * 100.0, 4) if actual_mdr_base > 0 else None
    mdr_variance_pct = round(actual_mdr_pct - expected_mdr_pct, 4) if actual_mdr_pct is not None and expected_mdr_pct is not None else None

    signals["metrics"].update(
        {
            "settlement_batches": len(settlement_rows),
            "gross_settlement": round(gross_total, 2),
            "net_settlement": round(net_total, 2),
            "known_deductions_total": round(known_total, 2),
            "unexplained_residual": round(unexplained_total, 2),
            "expected_mdr_pct": expected_mdr_pct,
            "actual_mdr_pct": actual_mdr_pct,
            "mdr_variance_pct": mdr_variance_pct,
            "held_batches": held_batches,
            "delayed_batches": delayed_batches,
        }
    )
    signals["evidence"]["deduction_components"] = [
        {"component": component, "amount": round(amount, 2)}
        for component, amount in deduction_totals.items()
        if abs(amount) > 0
    ]
    signals["evidence"]["settlement_status_breakdown"] = [
        {"status": status, "count": data["count"], "gross_amount": round(data["gross_amount"], 2)}
        for status, data in sorted(settlement_status_breakdown.items(), key=lambda item: item[1]["count"], reverse=True)
    ]
    signals["evidence"]["hold_reason_breakdown"] = [
        {"hold_reason": reason, "count": count}
        for reason, count in sorted(hold_reason_breakdown.items(), key=lambda item: item[1], reverse=True)
    ]
    signals["evidence"]["largest_shortfalls"] = sorted(
        largest_shortfalls,
        key=lambda row: (float(row.get("shortfall_amount") or 0.0), float(row.get("unexplained_residual") or 0.0)),
        reverse=True,
    )[:5]
    signals["evidence"]["shortfall_aging"] = [
        {"bucket": bucket, "count": count}
        for bucket, count in (
            ("0-2 days", shortfall_aging.get("0-2 days", 0)),
            ("3-7 days", shortfall_aging.get("3-7 days", 0)),
            ("8-30 days", shortfall_aging.get("8-30 days", 0)),
            ("31+ days", shortfall_aging.get("31+ days", 0)),
        )
        if count > 0
    ]

    recon_cols = _table_columns(engine, "reconciliation_records")
    if recon_cols:
        recon_mid_col = _pick("merchant_id" if "merchant_id" in recon_cols else "", "mid" if "mid" in recon_cols else "")
        status_col = _pick("recon_status" if "recon_status" in recon_cols else "", "status" if "status" in recon_cols else "")
        reason_col = _pick("exception_reason" if "exception_reason" in recon_cols else "", "reason" if "reason" in recon_cols else "")
        recon_date_col = _pick(
            "created_at" if "created_at" in recon_cols else "",
            "p_date" if "p_date" in recon_cols else "",
            "record_date" if "record_date" in recon_cols else "",
        )
        if recon_mid_col and status_col:
            where_parts = [f"{recon_mid_col} = :mid"]
            params = {"mid": mid}
            if recon_date_col:
                where_parts.extend([f"{recon_date_col} >= :start_date", f"{recon_date_col} < :end_date"])
                params["start_date"] = window.start_date
                params["end_date"] = window.end_date
            else:
                signals["errors"].append("reconciliation_records missing date column; evidence is not window-filtered")
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT {status_col} AS recon_status, COUNT(*) AS count
                        FROM reconciliation_records
                        WHERE {' AND '.join(where_parts)}
                        GROUP BY {status_col}
                        ORDER BY count DESC
                        """
                    ),
                    params,
                ).mappings().all()
                signals["evidence"]["recon_status_breakdown"] = [
                    {"status": str(row.get("recon_status") or "UNKNOWN"), "count": int(row.get("count") or 0)}
                    for row in rows
                ]
                if reason_col:
                    rows = conn.execute(
                        text(
                            f"""
                            SELECT {reason_col} AS exception_reason, COUNT(*) AS count
                            FROM reconciliation_records
                            WHERE {' AND '.join(where_parts)}
                            GROUP BY {reason_col}
                            ORDER BY count DESC
                            LIMIT 5
                            """
                        ),
                        params,
                    ).mappings().all()
                    signals["evidence"]["top_recon_exceptions"] = [
                        {"reason": str(row.get("exception_reason") or "UNKNOWN"), "count": int(row.get("count") or 0)}
                        for row in rows
                    ]

    return signals

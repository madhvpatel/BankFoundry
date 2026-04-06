from __future__ import annotations

import datetime as dt
import logging
from collections import defaultdict
from typing import Any

from sqlalchemy import text

from ..money import get_amount_scale, scale_inr
from .operational_signals import resolve_window_from_data

logger = logging.getLogger("dispute_signals")

OPEN_STATUSES = {
    "OPEN",
    "CHARGEBACK",
    "PENDING",
    "UNDER_REVIEW",
    "REPRESENTMENT",
    "PRE_ARBITRATION",
    "INITIATED",
}
WON_STATUSES = {"WON", "CLOSED_WON", "REPRESENTMENT_WON", "REVERSED", "RESOLVED_WON"}
LOST_STATUSES = {"LOST", "CLOSED_LOST", "DEBITED", "EXPIRED", "WRITE_OFF", "RESOLVED_LOST"}


def _table_columns(engine: Any, table: str) -> set[str]:
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
                      AND table_name = :table_name
                    """
                ),
                {"table_name": table},
            ).fetchall()
        cols = {str(row[0]).lower() for row in rows if row and row[0]}
        if cols:
            return cols
    except Exception:
        pass
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {str(row[1]).lower() for row in rows if len(row) > 1 and row[1]}
    except Exception:
        return set()


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


def _classify_status(value: str) -> str:
    normalized = str(value or "UNKNOWN").strip().upper()
    if normalized in WON_STATUSES:
        return "won"
    if normalized in LOST_STATUSES:
        return "lost"
    if normalized in OPEN_STATUSES:
        return "open"
    return "open"


def _metric_defaults() -> dict[str, Any]:
    return {
        "chargeback_count": 0,
        "chargeback_gmv": 0.0,
        "open_count": 0,
        "open_gmv": 0.0,
        "overdue_count": 0,
        "overdue_gmv": 0.0,
        "won_count": 0,
        "won_gmv": 0.0,
        "lost_count": 0,
        "lost_gmv": 0.0,
        "resolution_rate_pct": 0.0,
    }


def collect_dispute_signals(engine, mid, window_days: int = 30):
    window = resolve_window_from_data(engine, mid=mid, table="transaction_features", window_days=window_days)
    amount_scale = get_amount_scale(engine)
    signals = {
        "engine": "dispute_signals",
        "merchant_id": mid,
        "window": {"start_date": str(window.start_date), "end_date": str(window.end_date)},
        "metrics": _metric_defaults(),
        "evidence": {
            "chargeback_stage_distribution": [],
            "top_chargeback_reasons_by_count": [],
            "top_chargeback_reasons_by_value": [],
            "chargebacks_by_network": [],
            "chargebacks_by_network_value": [],
            "aging_buckets": [],
            "oldest_open_cases": [],
        },
        "errors": [],
    }

    cols = _table_columns(engine, "chargebacks")
    if not cols:
        signals["errors"].append("chargebacks table not found")
        return signals

    merchant_col = _pick("merchant_id" if "merchant_id" in cols else "", "mid" if "mid" in cols else "")
    amount_col = _pick("amount_rupees" if "amount_rupees" in cols else "", "chargeback_amount" if "chargeback_amount" in cols else "")
    stage_col = _pick("status" if "status" in cols else "", "chargeback_stage" if "chargeback_stage" in cols else "")
    reason_code_col = _pick("reason_code" if "reason_code" in cols else "", "chargeback_reason_code" if "chargeback_reason_code" in cols else "")
    reason_desc_col = _pick("reason_desc" if "reason_desc" in cols else "", "chargeback_reason_desc" if "chargeback_reason_desc" in cols else "")
    network_col = _pick("network" if "network" in cols else "", "card_network" if "card_network" in cols else "")
    created_col = _pick(
        "opened_at" if "opened_at" in cols else "",
        "created_at" if "created_at" in cols else "",
        "chargeback_date" if "chargeback_date" in cols else "",
        "p_date" if "p_date" in cols else "",
    )
    due_col = _pick(
        "due_by" if "due_by" in cols else "",
        "response_due_date" if "response_due_date" in cols else "",
        "due_date" if "due_date" in cols else "",
    )
    chargeback_id_col = _pick("chargeback_id" if "chargeback_id" in cols else "", "id" if "id" in cols else "")
    outcome_col = _pick("resolution_outcome" if "resolution_outcome" in cols else "", "outcome" if "outcome" in cols else "")

    if not merchant_col or not amount_col:
        signals["errors"].append("chargebacks table missing merchant or amount column")
        return signals

    where_parts = [f"{merchant_col} = :mid"]
    params: dict[str, Any] = {"mid": mid}
    if created_col:
        where_parts.extend([f"{created_col} >= :start_date", f"{created_col} < :end_date"])
        params["start_date"] = window.start_date
        params["end_date"] = window.end_date
    else:
        signals["errors"].append("chargebacks missing created/open date column; using all available rows")

    query = text(
        f"""
        SELECT
            {chargeback_id_col if chargeback_id_col else 'NULL'} AS chargeback_id,
            {amount_col} AS chargeback_amount,
            {stage_col if stage_col else 'NULL'} AS stage_value,
            {outcome_col if outcome_col else 'NULL'} AS outcome_value,
            {reason_code_col if reason_code_col else 'NULL'} AS reason_code,
            {reason_desc_col if reason_desc_col else 'NULL'} AS reason_desc,
            {network_col if network_col else 'NULL'} AS network,
            {created_col if created_col else 'NULL'} AS opened_at,
            {due_col if due_col else 'NULL'} AS due_by
        FROM chargebacks
        WHERE {' AND '.join(where_parts)}
        """
    )

    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(query, params).mappings().all()]

    total = len(rows)
    total_gmv = 0.0
    open_count = open_gmv = 0.0
    overdue_count = overdue_gmv = 0.0
    won_count = won_gmv = 0.0
    lost_count = lost_gmv = 0.0

    stage_distribution: dict[str, int] = defaultdict(int)
    reasons_by_count: dict[tuple[str, str], int] = defaultdict(int)
    reasons_by_value: dict[tuple[str, str], float] = defaultdict(float)
    networks_by_count: dict[str, int] = defaultdict(int)
    networks_by_value: dict[str, float] = defaultdict(float)
    aging_buckets: dict[str, int] = defaultdict(int)
    open_cases: list[dict[str, Any]] = []

    if not due_col:
        signals["errors"].append("chargebacks missing due-date column; overdue metrics set to 0")

    for row in rows:
        amount = _scaled(amount_scale, row.get("chargeback_amount"))
        total_gmv += amount
        stage_value = str(row.get("stage_value") or row.get("outcome_value") or "UNKNOWN").strip().upper()
        status_group = _classify_status(stage_value)
        reason_code = str(row.get("reason_code") or "UNKNOWN").strip() or "UNKNOWN"
        reason_desc = str(row.get("reason_desc") or "UNKNOWN").strip() or "UNKNOWN"
        network = str(row.get("network") or "UNKNOWN").strip() or "UNKNOWN"
        opened_at = _to_date(row.get("opened_at"))
        due_by = _to_date(row.get("due_by"))

        stage_distribution[stage_value] += 1
        reasons_by_count[(reason_code, reason_desc)] += 1
        reasons_by_value[(reason_code, reason_desc)] += amount
        networks_by_count[network] += 1
        networks_by_value[network] += amount

        if status_group == "open":
            open_count += 1
            open_gmv += amount
            if opened_at:
                age_days = max(0, (window.end_date - opened_at).days)
                if age_days <= 7:
                    aging_buckets["0-7 days"] += 1
                elif age_days <= 30:
                    aging_buckets["8-30 days"] += 1
                else:
                    aging_buckets["31+ days"] += 1
            if due_by and due_by < window.end_date:
                overdue_count += 1
                overdue_gmv += amount
            if opened_at or due_by:
                open_cases.append(
                    {
                        "chargeback_id": str(row.get("chargeback_id") or ""),
                        "amount_rupees": round(amount, 2),
                        "stage": stage_value,
                        "reason_code": reason_code,
                        "reason_desc": reason_desc,
                        "opened_at": str(opened_at) if opened_at else None,
                        "due_by": str(due_by) if due_by else None,
                    }
                )
        elif status_group == "won":
            won_count += 1
            won_gmv += amount
        elif status_group == "lost":
            lost_count += 1
            lost_gmv += amount

    resolved_count = int(won_count + lost_count)
    resolution_rate_pct = round((won_count / resolved_count) * 100.0, 2) if resolved_count > 0 else 0.0

    signals["metrics"].update(
        {
            "chargeback_count": total,
            "chargeback_gmv": round(total_gmv, 2),
            "open_count": int(open_count),
            "open_gmv": round(open_gmv, 2),
            "overdue_count": int(overdue_count),
            "overdue_gmv": round(overdue_gmv, 2),
            "won_count": int(won_count),
            "won_gmv": round(won_gmv, 2),
            "lost_count": int(lost_count),
            "lost_gmv": round(lost_gmv, 2),
            "resolution_rate_pct": resolution_rate_pct,
        }
    )
    signals["evidence"]["chargeback_stage_distribution"] = [
        {"stage": stage, "count": count}
        for stage, count in sorted(stage_distribution.items(), key=lambda item: item[1], reverse=True)
    ]
    signals["evidence"]["top_chargeback_reasons_by_count"] = [
        {"code": code, "description": desc, "count": count}
        for (code, desc), count in sorted(reasons_by_count.items(), key=lambda item: item[1], reverse=True)[:10]
    ]
    signals["evidence"]["top_chargeback_reasons_by_value"] = [
        {"code": code, "description": desc, "amount_rupees": round(amount, 2)}
        for (code, desc), amount in sorted(reasons_by_value.items(), key=lambda item: item[1], reverse=True)[:10]
    ]
    signals["evidence"]["chargebacks_by_network"] = [
        {"network": network, "count": count}
        for network, count in sorted(networks_by_count.items(), key=lambda item: item[1], reverse=True)
    ]
    signals["evidence"]["chargebacks_by_network_value"] = [
        {"network": network, "amount_rupees": round(amount, 2)}
        for network, amount in sorted(networks_by_value.items(), key=lambda item: item[1], reverse=True)
    ]
    signals["evidence"]["aging_buckets"] = [
        {"bucket": bucket, "count": count}
        for bucket, count in (("0-7 days", aging_buckets.get("0-7 days", 0)), ("8-30 days", aging_buckets.get("8-30 days", 0)), ("31+ days", aging_buckets.get("31+ days", 0)))
        if count > 0
    ]
    signals["evidence"]["oldest_open_cases"] = sorted(
        open_cases,
        key=lambda row: (row.get("due_by") or "9999-12-31", row.get("opened_at") or "9999-12-31"),
    )[:5]
    return signals

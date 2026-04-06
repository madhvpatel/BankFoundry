from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

from .calibration import estimate_recoverable_value, estimate_recovery_rate
from .constants import FAILED_STATUS_SQL
from .money import get_amount_scale, scale_inr
from .source_adapters import resolve_transaction_source


@dataclass
class RecoveryBacktestResult:
    windows_evaluated: int
    details: list[dict[str, Any]] = field(default_factory=list)


def run_recovery_backtest(
    engine,
    mid: str,
    *,
    end_date: dt.date,
    window_days: int = 7,
    n_windows: int = 4,
) -> RecoveryBacktestResult:
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return RecoveryBacktestResult(windows_evaluated=0, details=[])

    amount_scale = get_amount_scale(engine)
    details: list[dict[str, Any]] = []
    current_end = end_date

    for index in range(max(0, int(n_windows or 0))):
        current_start = current_end - dt.timedelta(days=window_days)
        calibration = estimate_recovery_rate(engine, mid, current_start, current_end)
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT
                        COALESCE(SUM(CASE WHEN {provider.value('status')} IN {FAILED_STATUS_SQL} THEN {provider.value('amount_rupees')} ELSE 0 END), 0) AS failed_gmv,
                        SUM(CASE WHEN {provider.value('status')} IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS failed_txns
                    FROM {provider.source_table}
                    WHERE {provider.value('merchant_id')} = :mid
                      AND {provider.value('p_date')} >= :start_date
                      AND {provider.value('p_date')} < :end_date
                    """
                ),
                {"mid": mid, "start_date": current_start, "end_date": current_end},
            ).mappings().first() or {}
        failed_gmv = float(scale_inr(row.get("failed_gmv"), amount_scale) or 0.0)
        details.append(
            {
                "window_start": current_start,
                "window_end": current_end,
                "failed_txns": int(row.get("failed_txns") or 0),
                "failed_gmv": failed_gmv,
                "estimated_recovery_rate": float(calibration.get("rate") or 0.0),
                "estimated_recoverable_value": estimate_recoverable_value(failed_gmv, calibration),
                "retry_events": int(calibration.get("retry_events") or 0),
            }
        )
        current_end = current_start

    return RecoveryBacktestResult(windows_evaluated=len(details), details=details)

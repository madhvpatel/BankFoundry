# app/intelligence/engines/peak_hour.py
import datetime as dt
import logging
import uuid

from sqlalchemy import text

from ..money import get_amount_scale, scale_inr
from ..source_adapters import resolve_transaction_source
from ..type import Recommendation


def _peak_hour_sql(provider) -> str:
    return f"""
    SELECT {provider.value('hour_of_day')} AS hour_of_day,
           SUM({provider.value('amount_rupees')}) AS revenue
    FROM {provider.source_table}
    WHERE {provider.value('merchant_id')} = :mid
      AND {provider.value('status')} = 'SUCCESS'
      AND {provider.value('p_date')} >= :start_date
      AND {provider.value('p_date')} < :end_date
    GROUP BY 1
    ORDER BY revenue DESC
    LIMIT 1
    """


def _peak_hour_failure_sql(provider) -> str:
    return f"""
    SELECT
      COUNT(*) AS attempts,
      SUM(CASE WHEN {provider.value('status')} IN ('FAILURE','FAILED') THEN 1 ELSE 0 END) AS failed_txns,
      ROUND(100.0 * SUM(CASE WHEN {provider.value('status')} IN ('FAILURE','FAILED') THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS failure_rate_pct
    FROM {provider.source_table}
    WHERE {provider.value('merchant_id')} = :mid
      AND {provider.value('hour_of_day')} = :hour_of_day
      AND {provider.value('p_date')} >= :start_date
      AND {provider.value('p_date')} < :end_date
    """


def _deterministic_peak_hour_summary(peak_hour: int, revenue: float, failure_rate_pct: float, attempts: int) -> str:
    risk_tail = ""
    if attempts > 0:
        risk_tail = f" Failure rate in that hour is {failure_rate_pct:.2f}% across {attempts:,} attempts."
    return (
        f"The strongest revenue hour is {int(peak_hour):02d}:00 with Rs {revenue:,.0f} in successful payments.{risk_tail} "
        "Keep terminal connectivity and checkout staffing stable in that hour so peak demand does not turn into avoidable failures."
    )


def build_peak_hour_reco(engine, mid: str, window_days: int, start_date: dt.date, end_date: dt.date) -> Recommendation | None:
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "hour_of_day", "amount_rupees")
    if missing:
        logging.getLogger("acquiguru").warning("Peak hour reco skipped; missing canonical fields: %s", ", ".join(sorted(missing)))
        return None
    try:
        with engine.connect() as conn:
            res = conn.execute(
                text(_peak_hour_sql(provider)),
                {"mid": mid, "start_date": start_date, "end_date": end_date},
            ).fetchone()
            if not res:
                return None
            peak_hour, revenue = res
            if peak_hour is None or revenue is None:
                return None
            failure_row = conn.execute(
                text(_peak_hour_failure_sql(provider)),
                {
                    "mid": mid,
                    "hour_of_day": int(peak_hour),
                    "start_date": start_date,
                    "end_date": end_date,
                },
            ).fetchone()

        amount_scale = get_amount_scale(engine)
        revenue = scale_inr(revenue, amount_scale)
        attempts = int(failure_row[0] or 0) if failure_row else 0
        failed_txns = int(failure_row[1] or 0) if failure_row else 0
        failure_rate_pct = float(failure_row[2] or 0.0) if failure_row else 0.0
        insight_summary = _deterministic_peak_hour_summary(int(peak_hour), float(revenue), failure_rate_pct, attempts)

        return Recommendation(
            reco_id=f"reco_{uuid.uuid4().hex[:12]}",
            merchant_id=mid,
            window_days=window_days,
            category="growth",
            title=f"📈 Peak revenue at {int(peak_hour):02d}:00",
            summary=(
                f"Between {start_date} and {end_date - dt.timedelta(days=1)}, most successful payments occur between "
                f"{int(peak_hour):02d}:00–{(int(peak_hour) + 1) % 24:02d}:00.\n\n{insight_summary}"
            ),
            impact_rupees=float(revenue) * 0.05,
            confidence=0.85,
            priority_score=7.0,
            drivers=[
                {
                    "dimension": "hour_of_day",
                    "value": int(peak_hour),
                    "peak_hour_failure_rate_pct": failure_rate_pct,
                    "peak_hour_attempts": attempts,
                    "peak_hour_failed_txns": failed_txns,
                }
            ],
            actions=[
                {"who": "merchant", "text": "Ensure stable internet connection during peak hours."},
                {"who": "merchant", "text": "Prioritize checkout speed and staff readiness during this window."},
            ],
            evidence_ids=[],
            metadata={
                "peak_hour": int(peak_hour),
                "peak_hour_failure_rate_pct": failure_rate_pct,
                "peak_hour_attempts": attempts,
                "peak_hour_failed_txns": failed_txns,
                "source_table": provider.source_table,
                "source_notes": list(provider.notes),
            },
        )
    except Exception as exc:
        logging.getLogger("acquiguru").error("Peak hour reco failed: %s", exc)
        return None

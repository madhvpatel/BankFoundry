import math

from sqlalchemy import text

from .constants import FAILED_STATUS_SQL
from .source_adapters import resolve_transaction_source

DEFAULT_RECOVERY_RATE = 0.28
RECOVERY_PRIOR_STRENGTH = 25
RECOVERY_MIN_RATE = 0.05
RECOVERY_MAX_RATE = 0.70
RECOVERY_MIN_SAMPLE = 20


def _clip_rate(rate: float) -> float:
    return max(RECOVERY_MIN_RATE, min(RECOVERY_MAX_RATE, float(rate)))


def estimate_recovery_rate(engine, mid: str, start_date, end_date, default_rate: float = DEFAULT_RECOVERY_RATE) -> dict:
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "initiated_at")
    if missing or not (provider.has("invoice_nr") or provider.has("source_txn_id") or provider.has("tx_id")):
        return {
            "rate": round(_clip_rate(default_rate), 4),
            "empirical_rate": None,
            "retry_events": 0,
            "recovered_events": 0,
            "method": "fallback_default_no_retry_identity",
            "confidence": 0.35,
        }

    attempt_key_expr = []
    if provider.has("invoice_nr"):
        attempt_key_expr.append(f"NULLIF({provider.value('invoice_nr')}, '')")
    if provider.has("source_txn_id"):
        attempt_key_expr.append(f"NULLIF({provider.value('source_txn_id')}, '')")
    elif provider.has("tx_id"):
        attempt_key_expr.append(f"NULLIF({provider.value('tx_id')}, '')")
    coalesce_expr = ", ".join(attempt_key_expr)
    params = {"mid": mid, "start_date": start_date, "end_date": end_date}

    retry_recovery_sql = f"""
    WITH attempts AS (
      SELECT
        COALESCE({coalesce_expr}) AS attempt_key,
        {provider.value('initiated_at')} AS initiated_at,
        {provider.value('status')} AS status
      FROM {provider.source_table}
      WHERE {provider.value('merchant_id')} = :mid
        AND {provider.value('p_date')} >= :start_date
        AND {provider.value('p_date')} < :end_date
        AND COALESCE({coalesce_expr}) IS NOT NULL
    ),
    ordered AS (
      SELECT
        attempt_key,
        status,
        initiated_at,
        LAG(status) OVER (
          PARTITION BY attempt_key
          ORDER BY initiated_at
        ) AS prev_status
      FROM attempts
    )
    SELECT
      SUM(CASE WHEN prev_status IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS retry_events,
      SUM(CASE WHEN prev_status IN {FAILED_STATUS_SQL} AND status = 'SUCCESS' THEN 1 ELSE 0 END) AS recovered_events
    FROM ordered
    """

    with engine.connect() as conn:
        row = conn.execute(text(retry_recovery_sql), params).mappings().first()

    retry_events = int((row or {}).get("retry_events") or 0)
    recovered_events = int((row or {}).get("recovered_events") or 0)

    posterior_num = recovered_events + (RECOVERY_PRIOR_STRENGTH * default_rate)
    posterior_den = retry_events + RECOVERY_PRIOR_STRENGTH
    estimated_rate = _clip_rate(posterior_num / posterior_den if posterior_den else default_rate)

    empirical_rate = (recovered_events / retry_events) if retry_events else None
    method = "empirical_bayesian_retry_recovery" if retry_events >= RECOVERY_MIN_SAMPLE else "smoothed_retry_recovery_low_sample"
    confidence = min(0.95, 0.35 + (math.log10(retry_events + 1) / 3.0))

    return {
        "rate": round(estimated_rate, 4),
        "empirical_rate": round(empirical_rate, 4) if empirical_rate is not None else None,
        "retry_events": retry_events,
        "recovered_events": recovered_events,
        "method": method,
        "confidence": round(confidence, 4),
    }


def estimate_recoverable_value(failed_gmv: float, calibration: dict) -> float:
    rate = float((calibration or {}).get("rate") or DEFAULT_RECOVERY_RATE)
    return max(0.0, float(failed_gmv or 0.0) * rate)

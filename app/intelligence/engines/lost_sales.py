# app/intelligence/engines/lost_sales.py
import uuid

from sqlalchemy import text

from ..calibration import estimate_recovery_rate, estimate_recoverable_value
from ..constants import FAILED_STATUS_SQL
from ..money import get_amount_scale, scale_inr
from ..playbooks import actions_for_failure_codes
from ..source_adapters import resolve_transaction_source
from ..type import Recommendation
from .helpers import confidence_from_volume, priority_score


def build_lost_sales_reco(
    engine,
    mid: str,
    window_days: int,
    start_date,
    end_date,
    top_codes: list[dict] | list[str],
    evidence_ids: list[str],
) -> Recommendation | None:
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return None

    params = {"mid": mid, "start_date": start_date, "end_date": end_date}
    amount_scale = get_amount_scale(engine)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                  SUM(CASE WHEN {provider.value('status')} IN {FAILED_STATUS_SQL} THEN {provider.value('amount_rupees')} ELSE 0 END) AS failed_gmv,
                  SUM(CASE WHEN {provider.value('status')} IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS failed_txns
                FROM {provider.source_table}
                WHERE {provider.value('merchant_id')} = :mid
                  AND {provider.value('p_date')} >= :start_date
                  AND {provider.value('p_date')} < :end_date
                """
            ),
            params,
        ).mappings().first() or {}
    failed_gmv = scale_inr(row.get("failed_gmv"), amount_scale)
    failed_txns = int(row.get("failed_txns") or 0)

    if failed_gmv <= 0:
        return None

    calibration = estimate_recovery_rate(engine, mid, start_date, end_date)
    recovery_rate = float(calibration.get("rate") or 0)
    recoverable = estimate_recoverable_value(failed_gmv, calibration)

    confidence = confidence_from_volume(failed_txns)
    priority = priority_score(recoverable, confidence, urgency=1.2)

    codes = []
    for item in (top_codes or [])[:3]:
        code = item.get("response_code") if isinstance(item, dict) else item
        if code:
            codes.append(str(code))
    actions = actions_for_failure_codes(codes)

    calibration_meta = {
        "recovery_rate": round(recovery_rate, 4),
        "calibration_method": str(calibration.get("method") or "unknown"),
        "retry_events": int(calibration.get("retry_events") or 0),
        "failed_gmv": float(failed_gmv),
        "failed_txns": failed_txns,
        "source_table": provider.source_table,
        "source_notes": list(provider.notes),
    }

    return Recommendation(
        reco_id=f"reco_{uuid.uuid4().hex[:12]}",
        merchant_id=mid,
        window_days=window_days,
        category="performance",
        title=f"Recover ₹{recoverable:,.0f} (est.) from failed payments",
        summary=(
            f"Between {start_date} and {end_date}, failed GMV was ₹{failed_gmv:,.0f}. "
            f"Calibrated recovery estimate: {int(recovery_rate * 100)}% "
            f"(method: {calibration_meta['calibration_method']}, retries observed: {calibration_meta['retry_events']})."
        ),
        impact_rupees=recoverable,
        confidence=confidence,
        priority_score=priority,
        drivers=[{"dimension": "response_code", "value": code, "contribution_pct": None} for code in codes],
        actions=actions,
        evidence_ids=evidence_ids,
        metadata=calibration_meta,
    )

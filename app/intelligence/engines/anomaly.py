# app/intelligence/engines/anomaly.py
import datetime as dt
import uuid

from sqlalchemy import text

from ..constants import FAILED_STATUS_SQL
from ..money import get_amount_scale, scale_inr
from ..source_adapters import resolve_transaction_source
from ..type import Recommendation
from . import helpers


def _kpi_sql(provider) -> str:
    return f"""
    SELECT
        COUNT(*) AS total_txns,
        SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
        ROUND(100.0 * SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS success_rate_pct,
        AVG(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} END) AS avg_ticket,
        SUM(CASE WHEN {provider.value('status')} IN {FAILED_STATUS_SQL} THEN {provider.value('amount_rupees')} ELSE 0 END) AS failed_gmv
    FROM {provider.source_table}
    WHERE {provider.value('merchant_id')} = :mid
      AND {provider.value('p_date')} >= :start_date
      AND {provider.value('p_date')} < :end_date
    """


def _fetch_window_kpi(engine, mid: str, start_date: dt.date, end_date: dt.date, provider) -> dict:
    amount_scale = get_amount_scale(engine)
    with engine.connect() as conn:
        row = conn.execute(
            text(_kpi_sql(provider)),
            {"mid": mid, "start_date": start_date, "end_date": end_date},
        ).mappings().first()
    if not row:
        return {"kpi": {}}
    total = int(row.get("total_txns") or 0)
    success = int(row.get("success_txns") or 0)
    return {
        "kpi": {
            "total_txns": total,
            "success_txns": success,
            "success_rate_pct": float(row.get("success_rate_pct") or 0.0),
            "avg_ticket": scale_inr(row.get("avg_ticket"), amount_scale),
            "failed_gmv": scale_inr(row.get("failed_gmv"), amount_scale),
        }
    }


def _rolling_baseline(engine, mid: str, current_start: dt.date, window_days: int, provider, lookback_windows: int = 4) -> tuple[dict, int]:
    samples: list[dict] = []
    for idx in range(1, lookback_windows + 1):
        end_date = current_start - dt.timedelta(days=window_days * (idx - 1))
        start_date = end_date - dt.timedelta(days=window_days)
        sample = _fetch_window_kpi(engine, mid, start_date, end_date, provider)
        total_txns = int(sample.get("kpi", {}).get("total_txns") or 0)
        if total_txns > 0:
            samples.append(sample)
    if not samples:
        return {"kpi": {}}, 0

    total_txns = sum(int(sample["kpi"].get("total_txns") or 0) for sample in samples)
    success_txns = sum(int(sample["kpi"].get("success_txns") or 0) for sample in samples)
    failed_gmv = sum(float(sample["kpi"].get("failed_gmv") or 0.0) for sample in samples)
    weighted_avg_ticket_num = sum(
        float(sample["kpi"].get("avg_ticket") or 0.0) * int(sample["kpi"].get("success_txns") or 0)
        for sample in samples
    )
    avg_ticket = (weighted_avg_ticket_num / success_txns) if success_txns else 0.0
    success_rate_pct = (success_txns / total_txns * 100.0) if total_txns else 0.0
    return {
        "kpi": {
            "total_txns": total_txns,
            "success_txns": success_txns,
            "success_rate_pct": round(success_rate_pct, 2),
            "avg_ticket": round(avg_ticket, 2),
            "failed_gmv": round(failed_gmv, 2),
        }
    }, len(samples)


def build_anomaly_reco(
    engine,
    mid: str,
    window_days: int,
    current_start: dt.date,
    current_end: dt.date,
    drivers: list[dict] | None = None,
    evidence_ids: list[str] | None = None,
    *,
    min_success_rate_drop_pp: float = 2.0,
    min_impact_rupees: float = 25000.0,
) -> Recommendation | None:
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return None

    current = _fetch_window_kpi(engine, mid, current_start, current_end, provider)
    baseline, sample_count = _rolling_baseline(engine, mid, current_start, window_days, provider)

    if int(baseline.get("kpi", {}).get("total_txns") or 0) < 50 or sample_count == 0:
        return None

    cur_sr = float(current["kpi"].get("success_rate_pct") or 0.0)
    base_sr = float(baseline["kpi"].get("success_rate_pct") or 0.0)
    sr_drop = base_sr - cur_sr
    if sr_drop < float(min_success_rate_drop_pp):
        return None

    confidence = helpers.confidence_from_volume(int(current["kpi"].get("total_txns") or 0))
    impact = helpers.estimate_loss_from_sr_drop(current, sr_drop)
    if impact < float(min_impact_rupees):
        return None

    priority = helpers.priority_score(impact, confidence, urgency=1.3)
    driver_rows = list(drivers or [])
    driver_rows.insert(
        0,
        {
            "dimension": "success_rate",
            "value": "merchant_relative_baseline",
            "current_success_rate_pct": round(cur_sr, 2),
            "baseline_success_rate_pct": round(base_sr, 2),
            "success_rate_drop_pp": round(sr_drop, 2),
            "baseline_windows": sample_count,
        },
    )

    return Recommendation(
        reco_id=f"reco_{uuid.uuid4().hex[:12]}",
        merchant_id=mid,
        window_days=window_days,
        category="performance",
        title=f"Success rate dropped by {sr_drop:.1f}pp",
        summary=(
            f"Success rate is {cur_sr:.2f}% versus a merchant-relative baseline of {base_sr:.2f}% across the last {sample_count} comparable window(s). "
            f"Estimated revenue at risk is ₹{impact:,.0f}."
        ),
        impact_rupees=impact,
        confidence=confidence,
        priority_score=priority,
        drivers=driver_rows,
        actions=[{"who": "merchant", "text": "Review top failure codes and enable retry or fallback for technical failures."}],
        evidence_ids=list(evidence_ids or []),
        metadata={
            "current_success_rate_pct": round(cur_sr, 2),
            "baseline_success_rate_pct": round(base_sr, 2),
            "success_rate_drop_pp": round(sr_drop, 2),
            "baseline_windows": sample_count,
            "min_impact_rupees": float(min_impact_rupees),
            "source_table": provider.source_table,
            "source_notes": list(provider.notes),
        },
    )


def maybe_anomaly_reco(mid: str, window_days: int, current: dict, baseline: dict, drivers: list[dict], evidence_ids: list[str]) -> Recommendation | None:
    cur_sr = float(current.get("kpi", {}).get("success_rate_pct") or 0.0)
    base_sr = float(baseline.get("kpi", {}).get("success_rate_pct") or 0.0)
    if int(baseline.get("kpi", {}).get("total_txns") or 0) < 50:
        return None
    sr_drop = base_sr - cur_sr
    if sr_drop < 3.0:
        return None
    confidence = helpers.confidence_from_volume(int(current.get("kpi", {}).get("total_txns") or 0))
    impact = helpers.estimate_loss_from_sr_drop(current, sr_drop)
    priority = helpers.priority_score(impact, confidence, urgency=1.3)
    return Recommendation(
        reco_id=f"reco_{uuid.uuid4().hex[:12]}",
        merchant_id=mid,
        window_days=window_days,
        category="performance",
        title=f"Success rate dropped by {sr_drop:.1f}pp",
        summary=f"Success rate is {cur_sr:.2f}% vs {base_sr:.2f}% baseline. Top drivers attached.",
        impact_rupees=impact,
        confidence=confidence,
        priority_score=priority,
        drivers=drivers,
        actions=[{"who": "merchant", "text": "Review top failure codes and enable retry/fallback for technical failures."}],
        evidence_ids=evidence_ids,
        metadata={"legacy_baseline": True},
    )

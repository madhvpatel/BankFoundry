import datetime as dt
import logging
import uuid
from typing import Any

from sqlalchemy import text

from app.ontology.recommendations import Recommendation
from app.ontology.signals.anomaly import build_anomaly_reco
from app.ontology.signals.attribution import compute_attribution
from app.ontology.signals.kpi_delta import compute_kpi_delta
from app.ontology.signals.lost_sales import build_lost_sales_reco
from app.ontology.signals.peak_hour import build_peak_hour_reco
from app.ontology.signals.payment_mode import build_payment_mode_reco
from app.intelligence.evidence_aggragator import collect_phase2_evidence
from app.intelligence.agent_reasoning import generate_recommendations
from app.intelligence.action_center import create_action
from app.intelligence.health_engine import build_health_vector
from app.intelligence.impact_engine_v2 import build_impact_vector
from .kpi_engine import compute_kpis
from .impact_engine import lost_revenue_estimate
from .constants import FAILED_STATUS_SQL
from .quality_checks import run_data_quality_checks
from .drift_checks import run_drift_checks
from .money import get_amount_scale, scale_inr
from .response_codes import (
    canonical_response_category,
    canonical_response_desc,
    normalize_response_code,
)
from .source_adapters import normalized_text, resolve_transaction_max_date, resolve_transaction_source
from config import Config

logger = logging.getLogger("runner")


def _date_range(engine, mid: str, window_days: int):
    provider, max_date = resolve_transaction_max_date(engine, mid)
    if not max_date:
        max_date = dt.date.today()

    end_date = max_date + dt.timedelta(days=1)
    start_date = end_date - dt.timedelta(days=window_days)
    prev_end = start_date
    prev_start = prev_end - dt.timedelta(days=window_days)
    return start_date, end_date, prev_start, prev_end


def _get_kpis(engine, mid: str, start_date, end_date, amount_scale: float = 1.0):
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return {"kpi": {}, "notes": list(provider.notes), "errors": [f"missing canonical fields: {', '.join(sorted(missing))}"]}
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS total_txns,
                    SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
                    AVG(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} END) AS avg_ticket,
                    SUM(CASE WHEN {provider.value('status')} IN {FAILED_STATUS_SQL} THEN {provider.value('amount_rupees')} ELSE 0 END) AS failed_gmv
                FROM {provider.source_table}
                WHERE {provider.value('merchant_id')} = :mid
                  AND {provider.value('p_date')} >= :start_date
                  AND {provider.value('p_date')} < :end_date
                """
            ),
            {
                "mid": mid,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).mappings().first()

    if not row:
        return {"kpi": {}}

    total = row["total_txns"] or 0
    success = row["success_txns"] or 0
    success_rate = (success / total * 100) if total else 0
    return {
        "kpi": {
            "total_txns": total,
            "success_rate_pct": round(success_rate, 2),
            "avg_ticket": scale_inr(row["avg_ticket"], amount_scale),
            "failed_gmv": scale_inr(row["failed_gmv"], amount_scale),
        },
        "notes": list(provider.notes),
    }


def _get_top_fail_codes(engine, mid: str, start_date, end_date) -> list[dict[str, Any]]:
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status")
    if missing:
        return []
    payment_mode_text = normalized_text(provider.value("payment_mode"), uppercase=True) if provider.has("payment_mode") else "'UNKNOWN'"
    response_code_text = normalized_text(provider.value("response_code"), uppercase=True) if provider.has("response_code") else ""
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
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT
                    {response_code_case} AS response_code,
                    COUNT(*) as c
                FROM {provider.source_table}
                WHERE {provider.value('merchant_id')} = :mid
                  AND {provider.value('status')} IN {FAILED_STATUS_SQL}
                  AND {provider.value('p_date')} >= :start_date
                  AND {provider.value('p_date')} < :end_date
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 3
                """
            ),
            {
                "mid": mid,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).fetchall()

    output: list[dict[str, Any]] = []
    for row in rows:
        if not row[0]:
            continue
        code = normalize_response_code(row[0])
        output.append(
            {
                "response_code": code,
                "response_desc": canonical_response_desc(code),
                "response_category": canonical_response_category(code),
                "count": int(row[1]),
            }
        )
    return output


def _collect_phase2_recommendations(engine, signals: dict, mid: str, window_days: int) -> list[Recommendation]:
    if not signals:
        return []
    if getattr(engine.dialect, "name", "") == "sqlite":
        logger.info("Phase-2 reasoning skipped for sqlite test engine.")
        return []
    try:
        return generate_recommendations(signals, mid, window_days)
    except Exception as exc:
        logger.error("Phase-2 recommendation generation failed: %s", exc)
        return []


def _count_negative_phase2_signals(signals: dict[str, Any]) -> int:
    health = signals.get("health_vector", {}) if isinstance(signals, dict) else {}
    negative_drivers = health.get("drivers", {}).get("negative", []) if isinstance(health.get("drivers"), dict) else []
    flags = health.get("flags", []) if isinstance(health, dict) else []
    evidence_count = 0
    for section_name in ("operational", "reconciliation", "disputes"):
        section = signals.get(section_name, {}) if isinstance(signals, dict) else {}
        evidence = section.get("evidence", {}) if isinstance(section, dict) else {}
        if not isinstance(evidence, dict):
            continue
        for value in evidence.values():
            if isinstance(value, list) and value:
                evidence_count += 1
    return len(negative_drivers) + len(flags) + evidence_count


def _requires_phase2_human_explanation(signals: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(signals, dict) or not signals:
        return False, "no signals available"

    health = signals.get("health_vector", {}) if isinstance(signals.get("health_vector"), dict) else {}
    impact = signals.get("impact_vector", {}) if isinstance(signals.get("impact_vector"), dict) else {}
    max_impact = 0.0
    for value in impact.values():
        try:
            max_impact = max(max_impact, float(value or 0.0))
        except Exception:
            continue

    negative_signal_count = _count_negative_phase2_signals(signals)
    min_impact = float(getattr(Config, "INTELLIGENCE_PHASE2_MIN_IMPACT_RUPEES", 50000.0))
    min_negative = int(getattr(Config, "INTELLIGENCE_PHASE2_MIN_NEGATIVE_SIGNALS", 2))
    health_status = str(health.get("status") or "").strip().lower()

    if max_impact < min_impact:
        return False, f"impact below threshold ({max_impact:,.0f} < {min_impact:,.0f})"
    if negative_signal_count < min_negative and health_status not in {"watchlist", "at risk"}:
        return False, f"signal complexity below threshold ({negative_signal_count} < {min_negative})"
    return True, f"material impact {max_impact:,.0f} with {negative_signal_count} negative signal(s)"


def _with_evidence(reco: Recommendation | None, evidence_ids: list[str]) -> Recommendation | None:
    if reco is None:
        return None
    merged = list(reco.evidence_ids or [])
    for evidence_id in evidence_ids:
        if evidence_id and evidence_id not in merged:
            merged.append(evidence_id)
    reco.evidence_ids = merged
    return reco


def _build_shortfall_reco(mid: str, window_days: int, alert: dict[str, Any]) -> Recommendation | None:
    shortfall = alert.get("shortfall") if isinstance(alert.get("shortfall"), dict) else {}
    if not shortfall:
        return None
    settlement_id = str(shortfall.get("settlement_id") or "unknown")
    difference_amount = float(shortfall.get("difference_amount") or 0.0)
    if difference_amount <= 0:
        return None
    evidence_ids = [str(item) for item in (alert.get("evidence") or []) if str(item)]
    return Recommendation(
        reco_id=f"reco_{uuid.uuid4().hex[:12]}",
        merchant_id=mid,
        window_days=window_days,
        category="reconciliation",
        title=f"Payout shortfall detected in settlement {settlement_id}",
        summary=str(alert.get("deduction_explanation") or alert.get("summary") or "").strip(),
        impact_rupees=difference_amount,
        confidence=0.98 if alert.get("verified") else 0.72,
        priority_score=max(difference_amount, 1.0) * (0.98 if alert.get("verified") else 0.72),
        drivers=[
            {"dimension": "settlement_id", "value": settlement_id, "difference_amount": difference_amount},
        ],
        actions=[{"who": "merchant", "text": str(item)} for item in list(alert.get("recommended_actions") or [])[:3]],
        evidence_ids=evidence_ids,
        metadata={"engine": "payout_shortfall_monitor", "shortfall": shortfall},
    )


def _build_reconciliation_reco(mid: str, window_days: int, reconciliation: dict[str, Any], evidence_ids: list[str]) -> Recommendation | None:
    metrics = reconciliation.get("metrics", {}) if isinstance(reconciliation.get("metrics"), dict) else {}
    evidence = reconciliation.get("evidence", {}) if isinstance(reconciliation.get("evidence"), dict) else {}
    unexplained = float(metrics.get("unexplained_residual") or 0.0)
    known = float(metrics.get("known_deductions_total") or 0.0)
    held = int(metrics.get("held_batches") or 0)
    delayed = int(metrics.get("delayed_batches") or 0)
    if unexplained <= 0 and known <= 0 and held <= 0 and delayed <= 0:
        return None

    title = "Settlement deductions need review"
    if unexplained > 0:
        title = f"Unexplained settlement gap of ₹{unexplained:,.0f} needs review"
    elif held > 0 or delayed > 0:
        title = "Held or delayed settlement batches need follow-up"

    summary_parts = []
    if unexplained > 0:
        summary_parts.append(f"Unexplained residual is ₹{unexplained:,.2f}")
    if known > 0:
        summary_parts.append(f"Known deductions total ₹{known:,.2f}")
    if held > 0 or delayed > 0:
        summary_parts.append(f"Held batches: {held}, delayed batches: {delayed}")
    actual_mdr_pct = metrics.get("actual_mdr_pct")
    expected_mdr_pct = metrics.get("expected_mdr_pct")
    if actual_mdr_pct is not None and expected_mdr_pct is not None:
        summary_parts.append(
            f"Actual MDR is {float(actual_mdr_pct):.2f}% vs expected {float(expected_mdr_pct):.2f}%"
        )

    largest_shortfalls = list(evidence.get("largest_shortfalls") or [])[:3]
    drivers = [
        {
            "dimension": "settlement_id",
            "value": str(item.get("settlement_id") or ""),
            "shortfall_amount": float(item.get("shortfall_amount") or 0.0),
            "unexplained_residual": float(item.get("unexplained_residual") or 0.0),
        }
        for item in largest_shortfalls
    ]
    actions = [
        {"who": "merchant", "text": "Review the largest shortfall settlements and reconcile named deduction components."},
        {"who": "merchant", "text": "Escalate batches with unexplained residuals or MDR variance."},
    ]
    impact_value = max(unexplained, known, float(metrics.get("gross_settlement") or 0.0) - float(metrics.get("net_settlement") or 0.0), 0.0)
    confidence = 0.9 if unexplained > 0 else 0.8
    return Recommendation(
        reco_id=f"reco_{uuid.uuid4().hex[:12]}",
        merchant_id=mid,
        window_days=window_days,
        category="reconciliation",
        title=title,
        summary=". ".join(summary_parts) + ("." if summary_parts else ""),
        impact_rupees=impact_value,
        confidence=confidence,
        priority_score=impact_value * confidence,
        drivers=drivers,
        actions=actions,
        evidence_ids=evidence_ids,
        metadata={"engine": "reconciliation_signals", "metrics": metrics},
    )


def _build_dispute_reco(mid: str, window_days: int, disputes: dict[str, Any], evidence_ids: list[str]) -> Recommendation | None:
    metrics = disputes.get("metrics", {}) if isinstance(disputes.get("metrics"), dict) else {}
    evidence = disputes.get("evidence", {}) if isinstance(disputes.get("evidence"), dict) else {}
    open_count = int(metrics.get("open_count") or 0)
    overdue_count = int(metrics.get("overdue_count") or 0)
    chargeback_count = int(metrics.get("chargeback_count") or 0)
    if chargeback_count <= 0:
        return None

    overdue_gmv = float(metrics.get("overdue_gmv") or 0.0)
    open_gmv = float(metrics.get("open_gmv") or 0.0)
    impact_value = max(overdue_gmv, open_gmv, float(metrics.get("chargeback_gmv") or 0.0), 0.0)
    if overdue_count > 0:
        title = f"{overdue_count} chargeback(s) are overdue"
    elif open_count > 0:
        title = f"{open_count} chargeback(s) need attention"
    else:
        title = "Chargeback exposure needs review"

    top_reasons = list(evidence.get("top_chargeback_reasons_by_value") or [])[:2]
    summary_parts = [
        f"Open disputes: {open_count}",
        f"Overdue disputes: {overdue_count}",
        f"Resolution rate: {float(metrics.get('resolution_rate_pct') or 0.0):.2f}%",
    ]
    if top_reasons:
        reason = top_reasons[0]
        summary_parts.append(
            f"Largest reason code is {reason.get('code') or 'UNKNOWN'} at ₹{float(reason.get('amount_rupees') or 0.0):,.2f}"
        )
    drivers = [
        {
            "dimension": "chargeback_reason_code",
            "value": str(item.get("code") or "UNKNOWN"),
            "impact_rupees": float(item.get("amount_rupees") or 0.0),
        }
        for item in top_reasons
    ]
    actions = [
        {"who": "merchant", "text": "Prioritize overdue disputes and prepare representment evidence."},
        {"who": "merchant", "text": "Review top reason codes by value and tighten the affected flow."},
    ]
    confidence = 0.92 if overdue_count > 0 else 0.85
    return Recommendation(
        reco_id=f"reco_{uuid.uuid4().hex[:12]}",
        merchant_id=mid,
        window_days=window_days,
        category="disputes",
        title=title,
        summary=". ".join(summary_parts) + ".",
        impact_rupees=impact_value,
        confidence=confidence,
        priority_score=impact_value * confidence,
        drivers=drivers,
        actions=actions,
        evidence_ids=evidence_ids,
        metadata={"engine": "dispute_signals", "metrics": metrics},
    )


def _build_kpi_delta_reco(mid: str, window_days: int, delta_bundle: dict[str, Any], evidence_ids: list[str]) -> Recommendation | None:
    merchant_level = delta_bundle.get("merchant_level", {}) if isinstance(delta_bundle.get("merchant_level"), dict) else {}
    success_rate_delta = merchant_level.get("success_rate_pct", {}) if isinstance(merchant_level.get("success_rate_pct"), dict) else {}
    success_gmv_delta = merchant_level.get("success_gmv", {}) if isinstance(merchant_level.get("success_gmv"), dict) else {}
    sr_delta_abs = float(success_rate_delta.get("delta_abs") or 0.0)
    gmv_delta_abs = float(success_gmv_delta.get("delta_abs") or 0.0)
    if abs(sr_delta_abs) < 1.0 and abs(gmv_delta_abs) < 10000.0:
        return None

    mode_rows = list(delta_bundle.get("by_payment_mode") or [])
    top_mode = None
    if mode_rows:
        top_mode = max(mode_rows, key=lambda row: abs(float(((row.get("success_gmv") or {}).get("delta_abs") or 0.0))))
    summary = f"Success rate changed by {sr_delta_abs:.2f}pp and successful GMV changed by ₹{gmv_delta_abs:,.2f} versus the previous period."
    drivers = []
    if isinstance(top_mode, dict):
        drivers.append(
            {
                "dimension": "payment_mode",
                "value": str(top_mode.get("payment_mode") or "UNKNOWN"),
                "success_gmv_delta_abs": float(((top_mode.get("success_gmv") or {}).get("delta_abs") or 0.0)),
                "success_rate_delta_abs": float(((top_mode.get("success_rate_pct") or {}).get("delta_abs") or 0.0)),
            }
        )
        summary += f" Largest mode-level movement is in {top_mode.get('payment_mode') or 'UNKNOWN'}."
    return Recommendation(
        reco_id=f"reco_{uuid.uuid4().hex[:12]}",
        merchant_id=mid,
        window_days=window_days,
        category="performance" if sr_delta_abs < 0 else "growth",
        title="Merchant KPIs shifted versus the previous period",
        summary=summary,
        impact_rupees=abs(gmv_delta_abs),
        confidence=0.82,
        priority_score=abs(gmv_delta_abs) * 0.82,
        drivers=drivers,
        actions=[{"who": "merchant", "text": "Review the largest payment-mode change and decide whether it is intended or operational."}],
        evidence_ids=evidence_ids,
        metadata={"engine": "kpi_delta", "merchant_level": merchant_level},
    )


def _build_attribution_reco(mid: str, window_days: int, attribution: dict[str, Any], evidence_ids: list[str]) -> Recommendation | None:
    rows = list(attribution.get("attributions") or [])
    if not rows:
        return None
    top = rows[0]
    impact_value = float(top.get("impact_rupees") or 0.0)
    if impact_value <= 0:
        return None
    metric = str(attribution.get("metric") or "metric")
    value = str(top.get("value") or "UNKNOWN")
    dimension = str(top.get("dimension") or "dimension")
    summary = (
        f"The biggest contributor to the {metric} change is {dimension}={value}, with current {float(top.get('current_value') or 0.0):,.2f} "
        f"versus previous {float(top.get('previous_value') or 0.0):,.2f}."
    )
    return Recommendation(
        reco_id=f"reco_{uuid.uuid4().hex[:12]}",
        merchant_id=mid,
        window_days=window_days,
        category="performance",
        title=f"{metric.replace('_', ' ')} change is concentrated in {value}",
        summary=summary,
        impact_rupees=impact_value,
        confidence=0.8,
        priority_score=impact_value * 0.8,
        drivers=rows[:3],
        actions=[{"who": "merchant", "text": f"Inspect {dimension}={value} first; it is driving the largest change."}],
        evidence_ids=evidence_ids,
        metadata={"engine": "attribution", "metric": metric, "dimension": dimension},
    )


def _evidence_id(prefix: str, mid: str, start_date: dt.date, end_date: dt.date, extra: str | None = None) -> str:
    base = f"{prefix}:{mid}:{start_date}:{end_date}"
    return f"{base}:{extra}" if extra else base


def run_intelligence(
    engine,
    mid: str,
    window_days: int = 30,
    enable_phase2_reasoning: bool = True,
    persist_actions: bool = True,
) -> dict[str, Any]:
    from app.intelligence.payout_shortfall_monitor import generate_payout_shortfall_alerts

    provider = resolve_transaction_source(engine)
    start_date, end_date, prev_start, prev_end = _date_range(engine, mid, window_days)
    amount_scale = get_amount_scale(engine)
    recos: list[Recommendation] = []

    if Config.INTELLIGENCE_ENABLE_DQ_CHECKS:
        dq = run_data_quality_checks(engine, mid, start_date, end_date, table=provider.source_table or "transaction_features")
    else:
        dq = {"passed": True, "issues": []}

    if not dq["passed"]:
        recos.append(
            Recommendation(
                reco_id=f"reco_{uuid.uuid4().hex[:12]}",
                merchant_id=mid,
                window_days=window_days,
                category="risk",
                title="Data quality checks found issues in transaction data",
                summary="Automated data quality checks found: " + ", ".join(dq["issues"]),
                impact_rupees=0.0,
                confidence=0.95,
                priority_score=8.0,
                drivers=[{"dimension": "dq_issue", "value": item} for item in dq["issues"]],
                actions=[
                    {"who": "bank", "text": "Fix ingestion mappings"},
                    {"who": "bank", "text": "Backfill invalid rows"},
                ],
                evidence_ids=[_evidence_id("dq", mid, start_date, end_date)],
                metadata={"engine": "quality_checks"},
            )
        )

    top_codes = _get_top_fail_codes(engine, mid, start_date, end_date)
    shortfall_alerts = generate_payout_shortfall_alerts(
        engine,
        mid,
        window_from=str(start_date),
        window_to=str(end_date),
        limit=1,
        min_difference_rupees=None,
    )
    if shortfall_alerts:
        shortfall_reco = _build_shortfall_reco(mid, window_days, shortfall_alerts[0])
        if shortfall_reco:
            recos.append(shortfall_reco)

    lost_sales_reco = build_lost_sales_reco(
        engine,
        mid,
        window_days,
        start_date,
        end_date,
        top_codes,
        evidence_ids=[_evidence_id("lost_sales", mid, start_date, end_date)],
    )
    if lost_sales_reco:
        recos.append(lost_sales_reco)

    current_kpi = _get_kpis(engine, mid, start_date, end_date, amount_scale=amount_scale)
    kpis = compute_kpis(engine, mid, start_date, end_date, provider.source_table or "transaction_features")
    lost_revenue = lost_revenue_estimate(kpis)

    signals = collect_phase2_evidence(engine, mid, window_days=window_days)
    kpi_delta = compute_kpi_delta(engine, mid, start_date=start_date, end_date=end_date, window_days=window_days)
    attribution_failed = compute_attribution(
        engine,
        mid,
        metric="failed_gmv",
        dimension="response_code",
        start_date=start_date,
        end_date=end_date,
        window_days=window_days,
    )
    attribution_growth = compute_attribution(
        engine,
        mid,
        metric="success_gmv",
        dimension="payment_mode",
        start_date=start_date,
        end_date=end_date,
        window_days=window_days,
    )
    signals["kpi_delta"] = kpi_delta
    signals["attribution"] = {
        "failed_gmv_by_response_code": attribution_failed,
        "success_gmv_by_payment_mode": attribution_growth,
    }

    health = build_health_vector(signals)
    impact = build_impact_vector(signals)
    signals["health_vector"] = health
    signals["impact_vector"] = impact

    recon_reco = _build_reconciliation_reco(
        mid,
        window_days,
        signals.get("reconciliation", {}),
        [_evidence_id("reconciliation", mid, start_date, end_date)],
    )
    if recon_reco:
        recos.append(recon_reco)

    dispute_reco = _build_dispute_reco(
        mid,
        window_days,
        signals.get("disputes", {}),
        [_evidence_id("disputes", mid, start_date, end_date)],
    )
    if dispute_reco:
        recos.append(dispute_reco)

    anomaly_reco = build_anomaly_reco(
        engine,
        mid,
        window_days,
        start_date,
        end_date,
        drivers=attribution_failed.get("attributions", [])[:3],
        evidence_ids=[_evidence_id("anomaly", mid, start_date, end_date)],
    )
    if anomaly_reco:
        recos.append(anomaly_reco)

    if kpi_delta.get("merchant_level"):
        kpi_delta_reco = _build_kpi_delta_reco(
            mid,
            window_days,
            kpi_delta,
            [_evidence_id("kpi_delta", mid, start_date, end_date)],
        )
        if kpi_delta_reco:
            recos.append(kpi_delta_reco)

    attribution_reco = _build_attribution_reco(
        mid,
        window_days,
        attribution_failed,
        [_evidence_id("attribution", mid, start_date, end_date, "failed_gmv_response_code")],
    )
    if attribution_reco:
        recos.append(attribution_reco)

    if Config.INTELLIGENCE_ENABLE_DRIFT_CHECKS:
        drift = run_drift_checks(engine, mid, start_date, end_date, prev_start, prev_end)
        signals["drift"] = drift
        if drift["alerts"]:
            recos.append(
                Recommendation(
                    reco_id=f"reco_{uuid.uuid4().hex[:12]}",
                    merchant_id=mid,
                    window_days=window_days,
                    category="risk",
                    title="Behavior drift detected",
                    summary="Drift monitor detected: " + ", ".join(drift["alerts"]),
                    impact_rupees=lost_revenue,
                    confidence=0.8,
                    priority_score=max(lost_revenue, 1.0) * 0.8,
                    drivers=[],
                    actions=[
                        {"who": "merchant", "text": "Review product mix changes"},
                        {"who": "bank", "text": "Review issuer/network incidents"},
                    ],
                    evidence_ids=[_evidence_id("drift", mid, start_date, end_date)],
                    metadata={"engine": "drift_checks", "alerts": drift["alerts"]},
                )
            )

    peak_reco = build_peak_hour_reco(engine, mid, window_days, start_date, end_date)
    if peak_reco:
        recos.append(_with_evidence(peak_reco, [_evidence_id("peak_hour", mid, start_date, end_date)]))

    pm_reco = build_payment_mode_reco(engine, mid, window_days, start_date, end_date)
    if pm_reco:
        growth_attr_rows = attribution_growth.get("attributions", [])[:2]
        if growth_attr_rows:
            pm_reco.drivers.extend(growth_attr_rows)
        recos.append(_with_evidence(pm_reco, [_evidence_id("payment_mode", mid, start_date, end_date)]))

    phase2_recos: list[Recommendation] = []
    if enable_phase2_reasoning:
        should_run_phase2, phase2_reason = _requires_phase2_human_explanation(signals)
        if should_run_phase2:
            logger.info("Phase-2 LLM reasoning enabled: %s", phase2_reason)
            phase2_recos = _collect_phase2_recommendations(engine, signals, mid, window_days)
        else:
            logger.info("Phase-2 LLM reasoning skipped: %s", phase2_reason)
    else:
        logger.info("Phase-2 LLM reasoning disabled by global runtime mode.")
    if phase2_recos:
        recos.extend(phase2_recos)

    recos = sorted(recos, key=lambda reco: float(reco.priority_score or 0.0), reverse=True)
    recos = recos[:5]

    if persist_actions:
        phase2_reco_ids = {reco.reco_id for reco in phase2_recos}
        for reco in recos:
            action = {
                "category": reco.category,
                "title": reco.title,
                "description": reco.summary,
                "impact_rupees": reco.impact_rupees,
                "confidence": reco.confidence,
                "priority_score": reco.priority_score,
                "owner": "merchant_ops",
                "source": "phase2_reasoning" if reco.reco_id in phase2_reco_ids else "deterministic_engine",
                "evidence_ids": list(reco.evidence_ids or []),
                "workflow_steps": list(reco.actions or []),
                "evidence": {"signals": signals, "recommendation_metadata": dict(reco.metadata or {})},
            }
            create_action(engine, mid, action)

    return {
        "recommendations": recos,
        "recos": recos,
        "signals": signals,
        "phase2_recos": phase2_recos,
    }

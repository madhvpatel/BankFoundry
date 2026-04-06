import logging
from typing import Dict, Any

logger = logging.getLogger("health_engine")

WEIGHTS = {
    "performance": 0.40,
    "reconciliation": 0.25,
    "disputes": 0.20,
    "data_quality": 0.15,
}


def _score_success_rate(rate_pct: float) -> int:
    if rate_pct >= 97.0:
        return 95
    if rate_pct >= 95.0:
        return 85
    if rate_pct >= 92.0:
        return 70
    return 50


def _score_disputes(open_count: int, overdue_count: int, resolved_total: int, won_count: int) -> int:
    if overdue_count > 0:
        return 50
    if open_count > 5:
        return 60
    if resolved_total > 0:
        win_rate = won_count / resolved_total
        if win_rate >= 0.8:
            return 90
        if win_rate >= 0.5:
            return 75
    if open_count == 0:
        return 95
    return 80


def _score_reconciliation(unexplained_residual: float, held_batches: int, delayed_batches: int) -> int:
    if unexplained_residual <= 0 and held_batches == 0 and delayed_batches == 0:
        return 95
    if unexplained_residual <= 5000 and held_batches <= 1 and delayed_batches <= 1:
        return 80
    if unexplained_residual <= 25000 and held_batches <= 3 and delayed_batches <= 3:
        return 65
    return 50


def _score_data_quality(unknown_failures: int, total_failures: int) -> int:
    if total_failures == 0:
        return 95
    ratio = unknown_failures / total_failures
    if ratio < 0.05:
        return 95
    if ratio < 0.20:
        return 80
    if ratio < 0.40:
        return 65
    return 50


def build_health_vector(signals: Dict[str, Any]) -> Dict[str, Any]:
    operational = signals.get("operational", {}) if isinstance(signals.get("operational"), dict) else {}
    dispute = signals.get("disputes", {}) if isinstance(signals.get("disputes"), dict) else {}
    recon = signals.get("reconciliation", {}) if isinstance(signals.get("reconciliation"), dict) else {}

    op_metrics = operational.get("metrics", {}) if isinstance(operational.get("metrics"), dict) else {}
    op_evidence = operational.get("evidence", {}) if isinstance(operational.get("evidence"), dict) else {}
    dispute_metrics = dispute.get("metrics", {}) if isinstance(dispute.get("metrics"), dict) else {}
    recon_metrics = recon.get("metrics", {}) if isinstance(recon.get("metrics"), dict) else {}

    success_rate = float(op_metrics.get("success_rate_pct") or 0.0)
    success_txns = int(op_metrics.get("success_txns") or 0)
    fail_txns = int(op_metrics.get("fail_txns") or 0)
    failure_codes = op_evidence.get("top_failure_codes", []) if isinstance(op_evidence.get("top_failure_codes"), list) else []

    unknown_failures = 0
    for failure in failure_codes:
        code = str(failure.get("response_code") or "").upper()
        if code in {"UNKNOWN", "UPI_FAILURE", "UNMAPPED_FAILURE"}:
            unknown_failures += int(failure.get("fail_count") or 0)

    open_count = int(dispute_metrics.get("open_count") or 0)
    overdue_count = int(dispute_metrics.get("overdue_count") or 0)
    won_count = int(dispute_metrics.get("won_count") or 0)
    lost_count = int(dispute_metrics.get("lost_count") or 0)
    resolved_total = won_count + lost_count

    unexplained_residual = float(recon_metrics.get("unexplained_residual") or 0.0)
    held_batches = int(recon_metrics.get("held_batches") or 0)
    delayed_batches = int(recon_metrics.get("delayed_batches") or 0)

    sub_scores = {
        "performance": _score_success_rate(success_rate),
        "reconciliation": _score_reconciliation(unexplained_residual, held_batches, delayed_batches),
        "disputes": _score_disputes(open_count, overdue_count, resolved_total, won_count),
        "data_quality": _score_data_quality(unknown_failures, fail_txns),
    }

    health_score = int(sum(sub_scores[name] * weight for name, weight in WEIGHTS.items()))
    if health_score >= 90:
        status = "Excellent"
    elif health_score >= 80:
        status = "Healthy"
    elif health_score >= 65:
        status = "Watchlist"
    else:
        status = "At Risk"

    flags = []
    if fail_txns > 0 and unknown_failures > fail_txns * 0.30:
        flags.append("high_unknown_failure_codes")
    if unexplained_residual > 0:
        flags.append("unexplained_reconciliation_gap")
    if overdue_count > 0:
        flags.append("overdue_disputes")

    pm_split = op_evidence.get("by_payment_mode", []) if isinstance(op_evidence.get("by_payment_mode"), list) else []
    upi_rate = None
    card_rate = None
    for row in pm_split:
        mode = str(row.get("payment_mode") or "").upper()
        if mode == "UPI":
            upi_rate = float(row.get("success_rate_pct") or 0.0)
        elif mode == "CARD":
            card_rate = float(row.get("success_rate_pct") or 0.0)
    if upi_rate is not None and card_rate is not None and (upi_rate - card_rate) > 3.0:
        flags.append("card_success_rate_gap")

    drivers = {"positive": [], "negative": []}
    if sub_scores["performance"] >= 85:
        drivers["positive"].append("High overall payment success rate")
    else:
        drivers["negative"].append("Low overall payment success rate")

    if sub_scores["reconciliation"] >= 85:
        drivers["positive"].append("Settlement deductions are largely explained")
    else:
        if unexplained_residual > 0:
            drivers["negative"].append("High unexplained settlement residual")
        if held_batches > 0 or delayed_batches > 0:
            drivers["negative"].append("Held or delayed settlement batches need follow-up")

    if sub_scores["disputes"] >= 85:
        drivers["positive"].append("Dispute book is controlled")
    else:
        if overdue_count > 0:
            drivers["negative"].append("Overdue disputes need immediate action")
        elif open_count > 0:
            drivers["negative"].append("Open disputes are building up")

    if sub_scores["data_quality"] < 80:
        drivers["negative"].append("High share of UNKNOWN failure codes")
    if "card_success_rate_gap" in flags:
        drivers["negative"].append("Card success rate is materially lower than UPI")
    if success_txns > 0 and won_count == 0 and resolved_total == 0 and open_count == 0:
        drivers["positive"].append("No active dispute burden")

    return {
        "health_score": health_score,
        "status": status,
        "sub_scores": sub_scores,
        "weights": dict(WEIGHTS),
        "flags": flags,
        "drivers": drivers,
    }

from typing import Dict, Any


def compute_lost_sales(signals: Dict[str, Any]) -> float:
    op = signals.get("operational", {}) if isinstance(signals.get("operational"), dict) else {}
    metrics = op.get("metrics", {}) if isinstance(op.get("metrics"), dict) else {}
    fail_txns = float(metrics.get("fail_txns") or 0.0)
    avg_ticket = float(metrics.get("avg_ticket_success") or 0.0)
    return float(fail_txns * avg_ticket)


def compute_unknown_failure_impact(signals: Dict[str, Any]) -> float:
    op = signals.get("operational", {}) if isinstance(signals.get("operational"), dict) else {}
    evidence = op.get("evidence", {}) if isinstance(op.get("evidence"), dict) else {}
    failure_codes = evidence.get("top_failure_codes", []) if isinstance(evidence.get("top_failure_codes"), list) else []
    total_value = 0.0
    for failure in failure_codes:
        if str(failure.get("response_code") or "").upper() in {"UNKNOWN", "UPI_FAILURE", "UNMAPPED_FAILURE"}:
            total_value += float(failure.get("fail_amount") or 0.0)
    return total_value


def compute_chargeback_impact(signals: Dict[str, Any]) -> float:
    dispute = signals.get("disputes", {}) if isinstance(signals.get("disputes"), dict) else {}
    metrics = dispute.get("metrics", {}) if isinstance(dispute.get("metrics"), dict) else {}
    return float(metrics.get("chargeback_gmv") or 0.0)


def compute_overdue_chargeback_risk(signals: Dict[str, Any]) -> float:
    dispute = signals.get("disputes", {}) if isinstance(signals.get("disputes"), dict) else {}
    metrics = dispute.get("metrics", {}) if isinstance(dispute.get("metrics"), dict) else {}
    return float(metrics.get("overdue_gmv") or 0.0)


def compute_reconciliation_gap(signals: Dict[str, Any]) -> float:
    recon = signals.get("reconciliation", {}) if isinstance(signals.get("reconciliation"), dict) else {}
    metrics = recon.get("metrics", {}) if isinstance(recon.get("metrics"), dict) else {}
    gross = float(metrics.get("gross_settlement") or 0.0)
    net = float(metrics.get("net_settlement") or 0.0)
    return abs(gross - net)


def compute_reconciliation_gap_explained(signals: Dict[str, Any]) -> float:
    recon = signals.get("reconciliation", {}) if isinstance(signals.get("reconciliation"), dict) else {}
    metrics = recon.get("metrics", {}) if isinstance(recon.get("metrics"), dict) else {}
    return float(metrics.get("known_deductions_total") or 0.0)


def compute_reconciliation_gap_unexplained(signals: Dict[str, Any]) -> float:
    recon = signals.get("reconciliation", {}) if isinstance(signals.get("reconciliation"), dict) else {}
    metrics = recon.get("metrics", {}) if isinstance(recon.get("metrics"), dict) else {}
    return float(metrics.get("unexplained_residual") or 0.0)


def build_impact_vector(signals: Dict[str, Any]) -> Dict[str, float]:
    return {
        "lost_sales": compute_lost_sales(signals),
        "unknown_failure_value": compute_unknown_failure_impact(signals),
        "chargeback_risk": compute_chargeback_impact(signals),
        "overdue_chargeback_risk": compute_overdue_chargeback_risk(signals),
        "reconciliation_gap": compute_reconciliation_gap(signals),
        "reconciliation_gap_explained": compute_reconciliation_gap_explained(signals),
        "reconciliation_gap_unexplained": compute_reconciliation_gap_unexplained(signals),
    }

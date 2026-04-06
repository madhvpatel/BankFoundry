# app/intelligence/engines/helpers.py
import math

def confidence_from_volume(total_txns: int) -> float:
    if total_txns < 50: return 0.40
    if total_txns < 200: return 0.60
    return 0.75

def priority_score(impact_rupees: float, confidence: float, urgency: float = 1.0) -> float:
    return math.log10(impact_rupees + 1) * float(confidence) * float(urgency)

def estimate_loss_from_sr_drop(current: dict, sr_drop_pp: float) -> float:
    kpi = current.get("kpi", {})
    total = float(kpi.get("total_txns") or 0)
    avg_ticket = float(kpi.get("avg_ticket") or 0)
    failed_gmv = float(kpi.get("failed_gmv") or 0)

    extra_successes = total * (sr_drop_pp / 100.0)
    impact = extra_successes * avg_ticket

    # fallback if avg_ticket missing/0
    if impact <= 0 and failed_gmv > 0:
        impact = failed_gmv * 0.25

    return max(0.0, impact)
from __future__ import annotations

from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _mode_map(payment_modes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(m.get("mode") or "").upper(): m for m in payment_modes or []}


def simulate_success_rate(baseline, knobs):
    attempts = _safe_float(baseline.get("attempts"), 0.0)
    success_rate = _safe_float(baseline.get("success_rate"), 0.0)
    avg_ticket_success = _safe_float(baseline.get("avg_ticket_success"), 0.0)
    requested_delta_pp = max(0.0, _safe_float(knobs.get("delta_success_rate_pct"), 0.0))
    headroom_pp = max(0.0, 100.0 - success_rate)
    effective_delta_pp = min(requested_delta_pp, headroom_pp)

    recovered_txns = attempts * (effective_delta_pp / 100.0)
    recovered_revenue = recovered_txns * avg_ticket_success
    new_success_rate = max(0.0, min(100.0, success_rate + effective_delta_pp))

    return {
        "scenario_type": "SUCCESS_RATE_UPLIFT",
        "delta_success_rate_pct": round(effective_delta_pp, 2),
        "requested_delta_success_rate_pct": round(requested_delta_pp, 2),
        "recovered_txns": round(recovered_txns, 2),
        "recovered_revenue": round(recovered_revenue, 2),
        "new_success_rate": round(new_success_rate, 2),
    }


def simulate_mode_shift(baseline, knobs):
    attempts = _safe_float(baseline.get("attempts"), 0.0)
    avg_ticket_success = _safe_float(baseline.get("avg_ticket_success"), 0.0)
    base_success_rate = _safe_float(baseline.get("success_rate"), 0.0)
    payment_modes = _mode_map(baseline.get("payment_modes", []))

    shift_pct = max(0.0, min(100.0, _safe_float(knobs.get("shift_pct"), 0.0)))
    from_mode = str(knobs.get("from_mode") or "CARD").upper()
    to_mode = str(knobs.get("to_mode") or "UPI").upper()

    from_row = payment_modes.get(from_mode, {})
    to_row = payment_modes.get(to_mode, {})

    from_attempts = _safe_float(from_row.get("attempts"), 0.0)
    from_sr = _safe_float(from_row.get("success_rate"), base_success_rate)
    to_sr = _safe_float(to_row.get("success_rate"), base_success_rate)

    current_successes = attempts * (base_success_rate / 100.0)
    shifted_attempts = from_attempts * (shift_pct / 100.0)
    new_successes = current_successes - shifted_attempts * (from_sr / 100.0) + shifted_attempts * (to_sr / 100.0)
    recovered_txns = new_successes - current_successes
    recovered_revenue = recovered_txns * avg_ticket_success
    new_success_rate = (new_successes / attempts * 100.0) if attempts else 0.0

    return {
        "scenario_type": "MODE_SHIFT",
        "from_mode": from_mode,
        "to_mode": to_mode,
        "shift_pct": round(shift_pct, 2),
        "shifted_attempts": round(shifted_attempts, 2),
        "recovered_txns": round(recovered_txns, 2),
        "recovered_revenue": round(recovered_revenue, 2),
        "new_success_rate": round(new_success_rate, 2),
    }


def simulate_refund_reduction(baseline, knobs):
    refund_count = _safe_float(baseline.get("refund_count"), 0.0)
    refund_gmv = _safe_float(baseline.get("refund_gmv"), 0.0)
    reduction_pct = max(0.0, min(100.0, _safe_float(knobs.get("reduction_pct"), 0.0)))

    saved_revenue = refund_gmv * (reduction_pct / 100.0)
    reduced_refund_count = refund_count * (reduction_pct / 100.0)

    return {
        "scenario_type": "REFUND_REDUCTION",
        "reduction_pct": round(reduction_pct, 2),
        "reduced_refund_count": round(reduced_refund_count, 2),
        "saved_revenue": round(saved_revenue, 2),
    }


def simulate_chargeback_reduction(baseline, knobs):
    chargeback_count = _safe_float(baseline.get("chargeback_count"), 0.0)
    chargeback_gmv = _safe_float(baseline.get("chargeback_gmv"), 0.0)
    reduction_pct = max(0.0, min(100.0, _safe_float(knobs.get("reduction_pct"), 0.0)))

    saved_revenue = chargeback_gmv * (reduction_pct / 100.0)
    reduced_chargeback_count = chargeback_count * (reduction_pct / 100.0)

    return {
        "scenario_type": "CHARGEBACK_REDUCTION",
        "reduction_pct": round(reduction_pct, 2),
        "reduced_chargeback_count": round(reduced_chargeback_count, 2),
        "saved_revenue": round(saved_revenue, 2),
    }


def simulate_scenario(scenario_type: str, baseline: dict[str, Any], knobs: dict[str, Any]) -> dict[str, Any]:
    normalized = (scenario_type or "SUCCESS_RATE_UPLIFT").upper()
    if normalized == "MODE_SHIFT":
        return simulate_mode_shift(baseline, knobs)
    if normalized == "REFUND_REDUCTION":
        return simulate_refund_reduction(baseline, knobs)
    if normalized == "CHARGEBACK_REDUCTION":
        return simulate_chargeback_reduction(baseline, knobs)
    return simulate_success_rate(baseline, knobs)

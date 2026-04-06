from __future__ import annotations

from typing import Any

from .types import ResolvedScenario, ScenarioSpec

DEFAULT_MARGIN_PCT = 25.0
DEFAULT_PROMO_UPLIFT_PCT = 8.0
DEFAULT_MODE_SHIFT_PCT = 10.0
DEFAULT_REFUND_REDUCTION_PCT = 10.0
DEFAULT_CHARGEBACK_REDUCTION_PCT = 10.0


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def resolve_assumptions(spec: ScenarioSpec) -> ResolvedScenario:
    scenario_type = (spec.scenario_type or "SUCCESS_RATE_UPLIFT").upper()
    knobs = dict(spec.knobs or {})
    assumptions: list[dict[str, Any]] = []
    questions: list[dict[str, Any]] = []

    def ensure_numeric(key: str, default: float, reason: str):
        if key in knobs and knobs[key] not in (None, ""):
            value = _safe_float(knobs[key], default)
            knobs[key] = value
            assumptions.append({"key": key, "value": value, "source": "merchant_input", "reason": reason})
            return
        knobs[key] = default
        assumptions.append({"key": key, "value": default, "source": "default", "reason": reason})

    def ensure_text(key: str, default: str, reason: str):
        value = str(knobs.get(key) or "").strip().upper()
        if value:
            knobs[key] = value
            assumptions.append({"key": key, "value": value, "source": "merchant_input", "reason": reason})
            return
        knobs[key] = default
        assumptions.append({"key": key, "value": default, "source": "default", "reason": reason})

    if scenario_type == "MODE_SHIFT":
        ensure_numeric("shift_pct", DEFAULT_MODE_SHIFT_PCT, "Share of transactions moved between payment modes.")
        ensure_text("from_mode", "CARD", "Source payment mode for transaction shift.")
        ensure_text("to_mode", "UPI", "Target payment mode for transaction shift.")
    elif scenario_type == "REFUND_REDUCTION":
        ensure_numeric("reduction_pct", DEFAULT_REFUND_REDUCTION_PCT, "Expected reduction in refund value.")
    elif scenario_type == "CHARGEBACK_REDUCTION":
        ensure_numeric("reduction_pct", DEFAULT_CHARGEBACK_REDUCTION_PCT, "Expected reduction in chargeback value.")
    else:
        scenario_type = "SUCCESS_RATE_UPLIFT"
        ensure_numeric(
            "delta_success_rate_pct",
            2.0,
            "Expected uplift in success rate in percentage points.",
        )

    if "promo_uplift_pct" in knobs or "promo_uplift_pct" in (spec.missing or []):
        ensure_numeric("promo_uplift_pct", DEFAULT_PROMO_UPLIFT_PCT, "Promo uplift for scenario sensitivity.")

    if "gross_margin_pct" in knobs or "gross_margin_pct" in (spec.missing or []):
        ensure_numeric("gross_margin_pct", DEFAULT_MARGIN_PCT, "Margin assumption for net impact interpretation.")
        if "gross_margin_pct" in (spec.missing or []) and not questions:
            questions.append(
                {
                    "ask": "What gross margin % should I assume for the discounted items?",
                    "key": "gross_margin_pct",
                }
            )

    unresolved = [m for m in (spec.missing or []) if m not in knobs]
    if unresolved and not questions:
        key = unresolved[0]
        questions.append(
            {
                "ask": f"Please share an assumption for `{key}` to refine this scenario.",
                "key": key,
            }
        )

    return ResolvedScenario(
        scenario_type=scenario_type,
        knobs=knobs,
        assumptions=assumptions,
        questions=questions,
    )

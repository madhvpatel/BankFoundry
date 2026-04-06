from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

from app.intelligence.prompt_loader import load_prompt_section
from config import Config
from .types import ScenarioSpec

logger = logging.getLogger("scenario_engine")

ALLOWED_SCENARIOS = {
    "SUCCESS_RATE_UPLIFT",
    "MODE_SHIFT",
    "REFUND_REDUCTION",
    "CHARGEBACK_REDUCTION",
}

REQUIRED_KNOBS: dict[str, list[str]] = {
    "SUCCESS_RATE_UPLIFT": ["delta_success_rate_pct"],
    "MODE_SHIFT": ["shift_pct", "from_mode", "to_mode"],
    "REFUND_REDUCTION": ["reduction_pct"],
    "CHARGEBACK_REDUCTION": ["reduction_pct"],
}

ALLOWED_KNOBS: dict[str, set[str]] = {
    "SUCCESS_RATE_UPLIFT": {"delta_success_rate_pct"},
    "MODE_SHIFT": {"shift_pct", "from_mode", "to_mode"},
    "REFUND_REDUCTION": {"reduction_pct"},
    "CHARGEBACK_REDUCTION": {"reduction_pct"},
}

SYSTEM_PROMPT = """You are a payments business analyst.
Convert the merchant request into a structured scenario.
Return JSON only.

Allowed scenario types:
- SUCCESS_RATE_UPLIFT
- MODE_SHIFT
- REFUND_REDUCTION
- CHARGEBACK_REDUCTION

Output schema:
{
  "scenario_type": "SUCCESS_RATE_UPLIFT | MODE_SHIFT | REFUND_REDUCTION | CHARGEBACK_REDUCTION",
  "knobs": {},
  "missing": []
}
"""

AGENTS_MD_PATH = Path(__file__).with_name("AGENTS.md")


def _planner_system_prompt() -> str:
    return load_prompt_section(AGENTS_MD_PATH, "scenario_planner_system", SYSTEM_PROMPT)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_json_object(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}

    text = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = text.replace("```", "").strip()

    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        return {}
    try:
        obj = json.loads(match.group(0))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _first_percent(question: str) -> float | None:
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", question)
    if not m:
        return None
    return _safe_float(m.group(1))


def _heuristic_spec(question: str) -> ScenarioSpec:
    q = (question or "").lower()
    scenario_type = "SUCCESS_RATE_UPLIFT"
    knobs: dict[str, Any] = {}

    if "refund" in q:
        scenario_type = "REFUND_REDUCTION"
        pct = _first_percent(q)
        if pct is not None:
            knobs["reduction_pct"] = pct
    elif "chargeback" in q or "dispute" in q:
        scenario_type = "CHARGEBACK_REDUCTION"
        pct = _first_percent(q)
        if pct is not None:
            knobs["reduction_pct"] = pct
    elif "mode" in q or "upi" in q or "card" in q:
        scenario_type = "MODE_SHIFT"
        pct = _first_percent(q)
        if pct is not None:
            knobs["shift_pct"] = pct

        knobs["from_mode"] = "CARD"
        knobs["to_mode"] = "UPI"

        transition = re.search(r"from\s+(upi|card)\s+to\s+(upi|card)", q)
        if transition:
            knobs["from_mode"] = transition.group(1).upper()
            knobs["to_mode"] = transition.group(2).upper()
        elif "to card" in q or "more card" in q:
            knobs["from_mode"] = "UPI"
            knobs["to_mode"] = "CARD"
        elif "to upi" in q or "more upi" in q:
            knobs["from_mode"] = "CARD"
            knobs["to_mode"] = "UPI"
    else:
        pct = _first_percent(q)
        if pct is not None:
            knobs["delta_success_rate_pct"] = pct

    required = REQUIRED_KNOBS.get(scenario_type, [])
    missing = [k for k in required if k not in knobs or knobs[k] in (None, "")]
    return ScenarioSpec(scenario_type=scenario_type, knobs=knobs, missing=missing)


def _normalize_spec(raw: dict[str, Any], question: str) -> ScenarioSpec:
    fallback = _heuristic_spec(question)

    scenario_type = str(raw.get("scenario_type") or "").strip().upper()
    if scenario_type not in ALLOWED_SCENARIOS:
        scenario_type = fallback.scenario_type

    knobs = raw.get("knobs") if isinstance(raw.get("knobs"), dict) else {}
    knobs = dict(knobs or {})
    allowed = ALLOWED_KNOBS.get(scenario_type, set())
    knobs = {k: v for k, v in knobs.items() if k in allowed}

    missing_raw = raw.get("missing") if isinstance(raw.get("missing"), list) else []
    missing = [str(k).strip() for k in missing_raw if str(k).strip()]

    # Normalize knob values.
    if "delta_success_rate_pct" in knobs:
        v = _safe_float(knobs.get("delta_success_rate_pct"))
        knobs["delta_success_rate_pct"] = v if v is not None else knobs.get("delta_success_rate_pct")
    if "shift_pct" in knobs:
        v = _safe_float(knobs.get("shift_pct"))
        knobs["shift_pct"] = v if v is not None else knobs.get("shift_pct")
    if "reduction_pct" in knobs:
        v = _safe_float(knobs.get("reduction_pct"))
        knobs["reduction_pct"] = v if v is not None else knobs.get("reduction_pct")
    if "from_mode" in knobs:
        knobs["from_mode"] = str(knobs.get("from_mode") or "").upper() or "CARD"
    if "to_mode" in knobs:
        knobs["to_mode"] = str(knobs.get("to_mode") or "").upper() or "UPI"

    # Fill from heuristic only for missing required knobs.
    for k in REQUIRED_KNOBS.get(scenario_type, []):
        if k not in knobs or knobs[k] in (None, ""):
            if k in fallback.knobs:
                knobs[k] = fallback.knobs[k]

    for k in REQUIRED_KNOBS.get(scenario_type, []):
        if k not in knobs or knobs[k] in (None, ""):
            if k not in missing:
                missing.append(k)

    return ScenarioSpec(scenario_type=scenario_type, knobs=knobs, missing=missing)


def plan_scenario(question: str) -> ScenarioSpec:
    llm = ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=0.2,
    )

    try:
        response = llm.invoke(
            [
                SystemMessage(content=_planner_system_prompt()),
                HumanMessage(content=question or ""),
            ]
        )
        raw = response.content if hasattr(response, "content") else str(response)
        parsed = _extract_json_object(raw)
    except Exception as exc:
        logger.warning("Scenario planner LLM call failed: %s", exc)
        parsed = {}

    spec = _normalize_spec(parsed, question)
    logger.info(
        "Scenario planner: type=%s knobs=%s missing=%s",
        spec.scenario_type,
        spec.knobs,
        spec.missing,
    )
    return spec

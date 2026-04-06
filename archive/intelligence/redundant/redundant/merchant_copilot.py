from __future__ import annotations

import datetime
import json
import logging
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

from config import Config
from app.intelligence.agent_reasoning import generate_recommendations
from app.intelligence.evidence_aggragator import collect_phase2_evidence
from app.intelligence.health_engine import build_health_vector
from app.intelligence.impact_engine_v2 import build_impact_vector
from app.intelligence.prompt_loader import load_prompt_section
from app.intelligence.scenario_engine.service import run_scenario

logger = logging.getLogger("merchant_copilot")

SCENARIO_HINTS = ("what if", "simulate", "impact", "increase", "reduce")
RECOMMENDATION_HINTS = ("what should i do", "recommend", "action", "improve", "increase sales")

COPILOT_SYSTEM_PROMPT = """You are AcquiGuru Merchant Copilot.
You help merchants with practical payment performance decisions.

Rules:
- Use only the numbers provided in the context.
- Keep the answer simple and actionable.
- Include:
  1) what this means
  2) top actions today
  3) expected impact
"""

EXPERIMENTAL_REASONING_PROMPT = """You are an experimental payments copilot.
You are allowed to reason beyond deterministic rule chains and propose causal hypotheses.
Always separate evidence-backed links from hypotheses.

Respond in free text (not JSON). Use short sections or bullets if useful.

Rules:
- Use provided numbers as-is; do not invent new financial metrics.
- You may infer causes where evidence is incomplete, but mark them as "hypothesis".
- Keep language simple and actionable for merchant teams.
- No strict output schema.
"""

AGENTS_MD_PATH = Path(__file__).with_name("AGENTS.md")


def _copilot_system_prompt() -> str:
    return load_prompt_section(AGENTS_MD_PATH, "merchant_copilot_system", COPILOT_SYSTEM_PROMPT)


def _copilot_experimental_prompt() -> str:
    return load_prompt_section(AGENTS_MD_PATH, "merchant_copilot_experimental", EXPERIMENTAL_REASONING_PROMPT)

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_inr(value: Any) -> str:
    amount = _safe_float(value, 0.0)
    if amount >= 1e7:
        return f"₹{amount / 1e7:.2f} Cr"
    if amount >= 1e5:
        return f"₹{amount / 1e5:.2f} L"
    return f"₹{amount:,.0f}"


def _sanitize(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, tuple):
        return [_sanitize(v) for v in obj]
    return obj


def _detect_intent(question: str) -> str:
    q = (question or "").strip().lower()
    if any(h in q for h in SCENARIO_HINTS):
        return "scenario"
    if any(h in q for h in RECOMMENDATION_HINTS):
        return "recommendation"
    return "insight"


def _signal_snapshot(signals: dict[str, Any]) -> dict[str, Any]:
    op = (signals or {}).get("operational", {}) or {}
    op_metrics = op.get("metrics", {}) or {}
    return {
        "attempts": int(op_metrics.get("attempts") or 0),
        "success_rate_pct": _safe_float(op_metrics.get("success_rate_pct"), 0.0),
        "fail_txns": int(op_metrics.get("fail_txns") or 0),
        "success_revenue": _safe_float(op_metrics.get("success_revenue"), 0.0),
        "health_vector": (signals or {}).get("health_vector", {}) or {},
        "impact_vector": (signals or {}).get("impact_vector", {}) or {},
    }


def _llm_narrative(question: str, context: dict[str, Any]) -> str:
    llm = ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=0.2,
    )
    prompt = (
        f"Merchant question: {question}\n\n"
        "Context JSON:\n"
        f"{json.dumps(_sanitize(context), ensure_ascii=False, indent=2)}\n\n"
        "Answer now."
    )
    try:
        resp = llm.invoke(
            [
                SystemMessage(content=_copilot_system_prompt()),
                HumanMessage(content=prompt),
            ]
        )
        text = resp.content if hasattr(resp, "content") else str(resp)
    except Exception as exc:
        logger.warning("Copilot narrative LLM call failed: %s", exc)
        return ""

    if not text:
        return ""
    if re.search(r"\d[\d,.\s]*[\*\+\-/]\s*\d", text):
        return ""
    return text.strip()


def _llm_experimental_reasoning(question: str, context: dict[str, Any]) -> str:
    llm = ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=float(getattr(Config, "COPILOT_EXPERIMENT_TEMPERATURE", 0.35) or 0.35),
    )

    prompt = (
        f"Merchant question: {question}\n\n"
        "Context JSON:\n"
        f"{json.dumps(_sanitize(context), ensure_ascii=False, indent=2)}\n\n"
        "Generate response now."
    )
    try:
        resp = llm.invoke(
            [
                SystemMessage(content=_copilot_experimental_prompt()),
                HumanMessage(content=prompt),
            ]
        )
        raw = resp.content if hasattr(resp, "content") else str(resp)
    except Exception as exc:
        logger.warning("Experimental copilot call failed: %s", exc)
        return ""

    text = (raw or "").strip()
    logger.info("Experimental copilot raw output: %s", text[:1200])
    return text


def merchant_copilot(engine, mid, question: str) -> dict[str, Any]:
    intent = _detect_intent(question or "")
    experiment_mode = bool(getattr(Config, "COPILOT_EXPERIMENT_MODE", True))

    if intent == "scenario":
        scenario = run_scenario(engine, mid, question, window_days=30)
        projections = scenario.projections or {}
        impact = _safe_float(
            projections.get("recovered_revenue")
            or projections.get("saved_revenue")
            or 0.0
        )
        actions = [
            "Track this metric weekly against actuals.",
            "Prioritize fixes for top failure drivers from Phase-2 signals.",
            "Coordinate with bank ops on issuer/network-heavy decline codes.",
        ]
        insights = [
            f"Baseline success rate: {scenario.baseline.get('success_rate', 0):.2f}%",
            f"Baseline attempts: {int(scenario.baseline.get('attempts') or 0):,}",
            f"Projected impact: {_fmt_inr(impact)}",
        ]
        if projections.get("questions"):
            q = projections.get("questions")[0]
            insights.append(str(q.get("ask") or "More input needed for better precision."))

        answer = scenario.narrative
        causal_chain: list[dict[str, Any]] = []
        copilot_confidence = 0.0
        reasoning_mode = "guided_deterministic"
        experimental_raw_output = ""

        if experiment_mode:
            signals = collect_phase2_evidence(engine, mid)
            signals["health_vector"] = build_health_vector(signals)
            signals["impact_vector"] = build_impact_vector(signals)
            exp_context = {
                "intent": intent,
                "scenario": {
                    "baseline": scenario.baseline,
                    "projections": scenario.projections,
                    "assumptions": scenario.assumptions,
                },
                "signals": signals,
            }
            exp_text = _llm_experimental_reasoning(question, exp_context)
            if exp_text:
                reasoning_mode = "exploratory_model_only"
                answer = exp_text
                insights = []
                actions = []
                causal_chain = []
                copilot_confidence = 0.0
                experimental_raw_output = exp_text

        logger.info(
            "Merchant copilot scenario intent handled: impact=%s mode=%s",
            impact,
            reasoning_mode,
        )
        return {
            "intent": intent,
            "answer": answer,
            "insights": insights,
            "actions": actions,
            "impact_rupees": impact,
            "scenario_result": scenario,
            "reasoning_mode": reasoning_mode,
            "causal_chain": causal_chain,
            "copilot_confidence": copilot_confidence,
            "experimental_raw_output": experimental_raw_output,
        }

    signals = collect_phase2_evidence(engine, mid)
    signals["health_vector"] = build_health_vector(signals)
    signals["impact_vector"] = build_impact_vector(signals)
    recos = generate_recommendations(signals, mid, 30)
    recos = sorted(recos, key=lambda r: _safe_float(getattr(r, "priority_score", 0.0), 0.0), reverse=True)

    top_recos = recos[:3]
    actions: list[str] = []
    insights: list[str] = []
    for reco in top_recos:
        insights.append(str(getattr(reco, "title", "Recommendation")))
        for action in getattr(reco, "actions", []) or []:
            text = str(action.get("text") or "").strip()
            if text and text not in actions:
                actions.append(text)
            if len(actions) >= 5:
                break

    snapshot = _signal_snapshot(signals)
    impact_total = sum(_safe_float(getattr(r, "impact_rupees", 0.0), 0.0) for r in top_recos)

    context = {
        "intent": intent,
        "snapshot": snapshot,
        "signals": signals,
        "top_recommendations": [
            {
                "title": getattr(r, "title", ""),
                "summary": getattr(r, "summary", ""),
                "impact_rupees": _safe_float(getattr(r, "impact_rupees", 0.0), 0.0),
                "confidence": _safe_float(getattr(r, "confidence", 0.0), 0.0),
            }
            for r in top_recos
        ],
        "actions": actions,
    }

    answer = ""
    causal_chain: list[dict[str, Any]] = []
    copilot_confidence = 0.0
    reasoning_mode = "guided_deterministic"
    experimental_raw_output = ""

    if experiment_mode:
        exp_context = {
            "intent": intent,
            "snapshot": snapshot,
            "signals": signals,
            "top_recommendations": context["top_recommendations"],
            "actions": actions,
        }
        exp_text = _llm_experimental_reasoning(question, exp_context)
        if exp_text:
            reasoning_mode = "exploratory_model_only"
            answer = exp_text
            insights = []
            actions = []
            causal_chain = []
            copilot_confidence = 0.0
            experimental_raw_output = exp_text
    else:
        answer = _llm_narrative(question, context)

    if not answer:
        if experiment_mode:
            answer = "No experimental model output generated for this query."
        elif top_recos:
            answer = (
                f"Your current success rate is {snapshot['success_rate_pct']:.2f}% across "
                f"{snapshot['attempts']:,} attempts. Focus today on: "
                f"{'; '.join(insights[:3])}. Estimated tracked impact is {_fmt_inr(impact_total)}."
            )
        else:
            answer = (
                f"I could not generate recommendation cards for this run. "
                f"Baseline snapshot: {snapshot['success_rate_pct']:.2f}% success on {snapshot['attempts']:,} attempts."
            )

    logger.info(
        "Merchant copilot intent=%s recos=%s impact=%s mode=%s",
        intent,
        len(top_recos),
        impact_total,
        reasoning_mode,
    )
    return {
        "intent": intent,
        "answer": answer,
        "insights": insights,
        "actions": actions,
        "impact_rupees": impact_total,
        "recommendations": top_recos,
        "signals": signals,
        "reasoning_mode": reasoning_mode,
        "causal_chain": causal_chain,
        "copilot_confidence": copilot_confidence,
        "experimental_raw_output": experimental_raw_output,
    }

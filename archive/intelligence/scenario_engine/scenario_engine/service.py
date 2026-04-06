from __future__ import annotations

import json
import logging
from pathlib import Path

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

from app.intelligence.engines.operational_signals import resolve_window_from_data
from app.intelligence.prompt_loader import load_prompt_section
from config import Config

from .assumptions import resolve_assumptions
from .baseline import fetch_baseline
from .narrator import narrate_scenario
from .planner import plan_scenario
from .simulators import simulate_scenario
from .types import ScenarioResult

logger = logging.getLogger("scenario_engine")

EXPERIMENTAL_SCENARIO_PROMPT = """You are an experimental payments scenario analyst.
Your role is to extend deterministic what-if outputs with hypothesis-based reasoning for demo purposes.

Respond in free text. Use bullets/sections if helpful.

Rules:
- Keep baseline deterministic metrics unchanged.
- Do not invent baseline numbers that are absent.
- You may propose hypotheses, but mark them clearly with label="hypothesis".
- No strict output schema.
"""

AGENTS_MD_PATH = Path(__file__).with_name("AGENTS.md")


def _scenario_experimental_prompt() -> str:
    return load_prompt_section(AGENTS_MD_PATH, "scenario_experimental_system", EXPERIMENTAL_SCENARIO_PROMPT)


def _experimental_reasoning(
    question: str,
    baseline: dict[str, Any],
    projections: dict[str, Any],
    assumptions: list[dict[str, Any]],
) -> str:
    llm = ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=float(getattr(Config, "SCENARIO_EXPERIMENT_TEMPERATURE", 0.35) or 0.35),
    )

    prompt = (
        f"Merchant question: {question}\n\n"
        "Context JSON:\n"
        f"{json.dumps({'baseline': baseline, 'deterministic_projection': projections, 'assumptions': assumptions}, ensure_ascii=False, default=str, indent=2)}\n\n"
        "Generate exploratory scenario reasoning now."
    )

    try:
        resp = llm.invoke(
            [
                SystemMessage(content=_scenario_experimental_prompt()),
                HumanMessage(content=prompt),
            ]
        )
        raw = resp.content if hasattr(resp, "content") else str(resp)
    except Exception as exc:
        logger.warning("Scenario experimental reasoning call failed: %s", exc)
        return ""

    text = (raw or "").strip()
    logger.info("Scenario experimental raw output: %s", text[:1200])
    return text


def run_scenario(engine, mid, question: str, window_days: int = 30, experimental: bool | None = None) -> ScenarioResult:
    """
    Orchestrates scenario lifecycle:
    1) Interpret merchant prompt
    2) Resolve assumptions/defaults
    3) Pull deterministic baseline
    4) Simulate deterministic projection
    5) Narrate outcome for merchant
    """
    spec = plan_scenario(question or "")
    resolved = resolve_assumptions(spec)

    window = resolve_window_from_data(
        engine=engine,
        mid=mid,
        table="transaction_features",
        window_days=window_days,
    )
    baseline = fetch_baseline(engine, mid, window.start_date, window.end_date)
    projections = simulate_scenario(resolved.scenario_type, baseline, resolved.knobs)
    if resolved.questions:
        projections["questions"] = resolved.questions

    reasoning_mode = "guided_deterministic"
    assumptions_out = list(resolved.assumptions or [])
    if experimental is None:
        experimental = bool(getattr(Config, "SCENARIO_EXPERIMENT_MODE", True))
    if getattr(engine.dialect, "name", "") == "sqlite":
        experimental = False

    if experimental:
        exp_text = _experimental_reasoning(
            question=question or "",
            baseline=baseline,
            projections=projections,
            assumptions=assumptions_out,
        )
        if exp_text:
            reasoning_mode = "exploratory_model_only"
            projections["_experimental"] = {"raw_output": exp_text}

    narrative = narrate_scenario(
        baseline=baseline,
        projections=projections,
        assumptions=assumptions_out,
    )
    if experimental:
        exp_raw = ((projections.get("_experimental") or {}).get("raw_output") if isinstance(projections.get("_experimental"), dict) else "")
        if exp_raw:
            narrative = str(exp_raw)
    projections["reasoning_mode"] = reasoning_mode

    logger.info(
        "Scenario run completed: type=%s mode=%s assumptions=%s baseline_attempts=%s projection=%s",
        resolved.scenario_type,
        reasoning_mode,
        assumptions_out,
        baseline.get("attempts"),
        projections,
    )

    return ScenarioResult(
        baseline=baseline,
        projections=projections,
        assumptions=assumptions_out,
        narrative=narrative,
    )

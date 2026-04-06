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

logger = logging.getLogger("scenario_engine")

SYSTEM_PROMPT = """You are a merchant payments copilot.
Explain scenario simulation outputs in simple business language.

Rules:
- Use only the numbers provided in the input.
- Never invent metrics.
- Do not include formulas or symbolic math expressions.
- Keep the response concise and actionable.
- Include: summary, impact, and suggested actions.
"""

AGENTS_MD_PATH = Path(__file__).with_name("AGENTS.md")


def _narrator_system_prompt() -> str:
    return load_prompt_section(AGENTS_MD_PATH, "scenario_narrator_system", SYSTEM_PROMPT)


def _fmt_inr(value: Any) -> str:
    try:
        amount = float(value or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    if amount >= 1e7:
        return f"₹{amount / 1e7:.2f} Cr"
    if amount >= 1e5:
        return f"₹{amount / 1e5:.2f} L"
    return f"₹{amount:,.0f}"


def _contains_formula(text: str) -> bool:
    if not text:
        return False
    pattern = r"(\d[\d,.\s]*[\*\+\-/]\s*\(?\d)"
    return bool(re.search(pattern, text))


def _fallback_narrative(
    baseline: dict[str, Any],
    projections: dict[str, Any],
    assumptions: list[dict[str, Any]],
) -> str:
    attempts = int(float(baseline.get("attempts") or 0))
    success_rate = float(baseline.get("success_rate") or 0.0)
    avg_ticket = float(baseline.get("avg_ticket_success") or 0.0)

    impact = float(
        projections.get("recovered_revenue")
        or projections.get("saved_revenue")
        or 0.0
    )

    summary = "Projection computed from your recent baseline performance."
    if "new_success_rate" in projections:
        summary = (
            f"If current performance shifts as specified, success rate can move to "
            f"{float(projections.get('new_success_rate') or 0.0):.2f}%."
        )

    assumption_lines = []
    for item in assumptions[:3]:
        assumption_lines.append(f"- {item.get('key')}: {item.get('value')} ({item.get('source')})")
    assumptions_text = "\n".join(assumption_lines) if assumption_lines else "- No additional assumptions."

    return (
        f"Summary: {summary}\n\n"
        f"Impact: Estimated upside is {_fmt_inr(impact)} for this analysis window.\n\n"
        f"Suggested actions:\n"
        f"- Focus on the failure clusters highlighted in operational signals.\n"
        f"- Track this scenario weekly and compare actuals against projection.\n"
        f"- Align merchant and bank actions on top decline drivers.\n\n"
        f"Baseline used: {attempts:,} attempts, {success_rate:.2f}% success rate, average ticket {_fmt_inr(avg_ticket)}.\n"
        f"Assumptions:\n{assumptions_text}"
    )


def narrate_scenario(
    baseline: dict[str, Any],
    projections: dict[str, Any],
    assumptions: list[dict[str, Any]],
) -> str:
    llm = ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=0.2,
    )

    payload = {
        "baseline": baseline,
        "projection": projections,
        "assumptions": assumptions,
    }

    prompt = (
        "Convert this deterministic scenario output into a merchant explanation.\n"
        "Return plain text only.\n\n"
        f"{json.dumps(payload, ensure_ascii=False, default=str, indent=2)}"
    )

    try:
        resp = llm.invoke(
            [
                SystemMessage(content=_narrator_system_prompt()),
                HumanMessage(content=prompt),
            ]
        )
        text = resp.content if hasattr(resp, "content") else str(resp)
    except Exception as exc:
        logger.warning("Scenario narrator call failed: %s", exc)
        return _fallback_narrative(baseline, projections, assumptions)

    if not text or _contains_formula(text):
        return _fallback_narrative(baseline, projections, assumptions)
    return text.strip()

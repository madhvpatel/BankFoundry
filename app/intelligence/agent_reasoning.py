import datetime
import json
import logging
import re
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage

from config import Config
from .type import Recommendation
from .prompt_loader import load_prompt_section

logger = logging.getLogger("agent_reasoning")

ALLOWED_CATEGORIES = {"performance", "risk", "growth", "reconciliation", "disputes"}

SYSTEM_PROMPT = """You are a senior payments intelligence analyst working at an acquiring bank.

You analyze merchant payment data and operational signals to identify risks,
performance issues, and business opportunities.

Use only the evidence provided.
Do not invent numbers.
Generate actionable recommendations with clear ownership.
Focus on merchant value and operational improvements.
Look for patterns such as:
- Success rate drops or payment mode differences
- Concentration of failures in specific hours or banks
- High UPI failures with missing response codes indicating data quality issues
- Reconciliation mismatches or settlement issues
- Chargeback trends or dispute risks
- Opportunities to increase revenue or improve success rates

You will also receive:
health_vector
impact_vector

Use these values for reasoning.
Do NOT invent financial numbers.
Use the impact_vector values when estimating impact.

Return ONLY a JSON array. No markdown, no commentary.
Each object must follow:
{
  "title": "...",
  "summary": "...",
  "category": "performance | risk | growth | reconciliation | disputes",
  "impact_rupees": number,
  "confidence": number,
  "priority_score": number,
  "actions": [
    {"who":"merchant","text":"..."},
    {"who":"bank","text":"..."}
  ]
}
"""

JSON_REPAIR_PROMPT = """Convert the input into a strict JSON array of recommendation objects.

Return ONLY JSON. No prose, no markdown.
Use this exact object schema:
{
  "title": "...",
  "summary": "...",
  "category": "performance | risk | growth | reconciliation | disputes",
  "impact_rupees": number,
  "confidence": number,
  "priority_score": number,
  "actions": [
    {"who":"merchant","text":"..."},
    {"who":"bank","text":"..."}
  ]
}

If the input has no usable recommendations, return [].
"""

AGENTS_MD_PATH = Path(__file__).with_name("AGENTS.md")


def _system_prompt() -> str:
    return load_prompt_section(AGENTS_MD_PATH, "agent_reasoning_system", SYSTEM_PROMPT)


def _json_repair_prompt() -> str:
    return load_prompt_section(AGENTS_MD_PATH, "agent_reasoning_json_repair", JSON_REPAIR_PROMPT)


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


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_actions(actions_raw: Any) -> list[dict[str, str]]:
    if not isinstance(actions_raw, list):
        return []

    actions: list[dict[str, str]] = []
    for item in actions_raw:
        if not isinstance(item, dict):
            continue
        who = str(item.get("who") or "merchant").strip().lower()
        if who not in {"merchant", "bank"}:
            who = "merchant"
        text = str(item.get("text") or "").strip()
        if text:
            actions.append({"who": who, "text": text})
    return actions[:5]


def _extract_json_array(raw: str) -> list[dict[str, Any]]:
    text = (raw or "").strip()
    if not text:
        return []

    # Remove markdown code fences.
    text = re.sub(r"```.*?\n", "", text)
    text = text.replace("```", "")

    parsed = None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict) and "recommendations" in parsed:
            parsed = parsed.get("recommendations")
    except Exception:
        pass

    if parsed is None:
        match = re.search(r"\[[\s\S]*\]", text)
        if match:
            try:
                parsed = json.loads(match.group(0))
                if isinstance(parsed, dict) and "recommendations" in parsed:
                    parsed = parsed.get("recommendations")
            except Exception:
                parsed = None

    if not isinstance(parsed, list):
        return []

    out: list[dict[str, Any]] = []
    for item in parsed:
        if isinstance(item, dict):
            out.append(item)
    return out


def _build_user_prompt(signals: dict, merchant_id: str, window_days: int) -> str:
    clean = _sanitize(signals)
    evidence_json = json.dumps(clean, ensure_ascii=False, indent=2)
    return (
        f"Merchant ID: {merchant_id}\n"
        f"Analysis Window Days: {window_days}\n\n"
        "Evidence signals JSON:\n"
        f"{evidence_json}\n\n"
        "Generate recommendations now."
    )


def _repair_to_json_array(llm: ChatOllama, raw_text: str) -> list[dict[str, Any]]:
    if not raw_text:
        return []
    try:
        repair_response = llm.invoke(
            [
                SystemMessage(content=_json_repair_prompt()),
                HumanMessage(content=raw_text[:12000]),
            ]
        )
        repaired = repair_response.content if hasattr(repair_response, "content") else str(repair_response)
    except Exception as exc:
        logger.warning("Phase-2 JSON repair call failed: %s", exc)
        return []

    logger.info("LLM repaired JSON output: %s", repaired[:1500])
    return _extract_json_array(repaired)


def generate_recommendations(signals: dict, merchant_id: str, window_days: int) -> list[Recommendation]:
    llm = ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=0.2,
    )

    prompt = _build_user_prompt(signals, merchant_id, window_days)

    try:
        response = llm.invoke(
            [
                SystemMessage(content=_system_prompt()),
                HumanMessage(content=prompt),
            ]
        )
        raw = response.content if hasattr(response, "content") else str(response)
    except Exception as exc:
        logger.error("Phase-2 reasoning call failed: %s", exc)
        return []

    logger.info("LLM reasoning output: %s", raw[:2000])

    items = _extract_json_array(raw)
    if not items:
        logger.warning("Phase-2 reasoning output was not JSON; attempting structured repair.")
        items = _repair_to_json_array(llm, raw)
    if not items:
        logger.warning("Phase-2 reasoning produced no parseable recommendations.")
    recos: list[Recommendation] = []

    for item in items[:8]:
        category = str(item.get("category") or "risk").strip().lower()
        if category not in ALLOWED_CATEGORIES:
            category = "risk"

        title = str(item.get("title") or "").strip() or "Phase-2 intelligence recommendation"
        summary = str(item.get("summary") or "").strip() or "Signal-driven recommendation generated by reasoning agent."
        impact_rupees = max(0.0, _to_float(item.get("impact_rupees"), 0.0))
        confidence = _to_float(item.get("confidence"), 0.6)
        confidence = min(1.0, max(0.0, confidence))
        priority_score = max(0.0, _to_float(item.get("priority_score"), impact_rupees * confidence))
        actions = _normalize_actions(item.get("actions"))
        if impact_rupees == 0 and confidence < 0.4:
            continue

        recos.append(
            Recommendation(
                reco_id=f"reco_{uuid.uuid4().hex[:12]}",
                merchant_id=merchant_id,
                window_days=window_days,
                category=category,  # type: ignore[arg-type]
                title=title,
                summary=summary,
                impact_rupees=impact_rupees,
                confidence=confidence,
                priority_score=priority_score,
                actions=actions,
                drivers=[],
                evidence_ids=[],
            )
        )

    return recos

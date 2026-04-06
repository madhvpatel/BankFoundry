from __future__ import annotations

import datetime as dt
import json
import logging
import re
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

from config import Config
from .prompt_loader import load_prompt_section
try:
    from .intent_scoring import score_intent as _score_intent
    _INTENT_SCORING_AVAILABLE = True
except Exception:  # pragma: no cover
    _INTENT_SCORING_AVAILABLE = False
    _score_intent = None  # type: ignore[assignment]

logger = logging.getLogger("chat_reasoning")

AGENTS_MD_PATH = Path(__file__).with_name("AGENTS.md")

SYNTHESIZER_PROMPT = """You are Bank Foundry, a payments analyst speaking directly to a merchant.

You are given deterministic evidence bundles, ranked recommendations, and conversation context.
Your job is to investigate the user's question, explain what matters, and stay grounded.

Rules:
- Use only the supplied evidence.
- Do not invent numbers, evidence IDs, ranks, or actions.
- You may form hypotheses, but label them clearly.
- Prefer short, direct answers over report formatting.
- If the evidence is ambiguous, ask one short clarifying question instead of bluffing.
- Return ONLY JSON.

Return this schema:
{
  "raw_answer": "merchant-facing answer",
  "verified_points": ["..."],
  "hypotheses": ["..."],
  "next_best_action": "..." | null,
  "follow_ups": ["..."],
  "claims": [
    {
      "text": "...",
      "kind": "number|ranking|action|evidence|hypothesis|general",
      "status": "fact|hypothesis",
      "evidence_ids": ["..."]
    }
  ]
}
"""

CLARIFIER_PROMPT = """You are Bank Foundry, a merchant payments analyst.

The user's request is ambiguous or needs a small amount of context before a good answer is possible.
Ask one short clarifying question and provide 2-4 concise choices.
Return ONLY JSON.

Schema:
{
  "question": "...",
  "choices": ["...", "..."],
  "reason": "..."
}
"""

JSON_REPAIR_PROMPT = """Convert the input into strict JSON for the requested schema.
Return ONLY JSON. No markdown, no prose.
If the input is unusable, return {}.
"""

ROUTER_PROMPT = """You are a small routing model for Bank Foundry.

Classify the user's request into one route and one intent.
Prefer the smallest correct route.
Handle typos and natural merchant phrasing.

Routes:
- direct
- greeting
- social_ack
- out_of_scope
- risky
- deterministic
- analysis
- clarify

Intents:
- assistant_identity
- business_identity
- business_overview
- what_changed
- top_growth_opportunities
- operational_risks
- success_rate_drop
- why_payments_failing
- terminal_expansion
- recent_transactions
- recent_settlements
- recent_refunds
- recent_chargebacks
- settlement_total
- exact_shortfall
- social_challenge
- general

Rules:
- Use direct for assistant identity, brief conversational pushback, and simple non-analytical conversational replies.
- Use deterministic for lists, exact totals, and exact payout shortfalls.
- Use analysis for broad business, growth, operational, and change questions.
- Use clarify when the ask is materially ambiguous and needs one follow-up.
- Use social_ack for short continuations like \"interesting\", \"got it\", \"okay\".
- Use risky for abusive or unsafe language.
- Use out_of_scope for unrelated questions like geography, recipes, movies, or weather.

Few-shot examples:
- Input: "hey man"
  Output: {"route": "greeting", "intent": "general", "confidence": 0.95, "reason": "Casual greeting; respond warmly without asking a follow-up."}
- Input: "what can you do for me?"
  Output: {"route": "direct", "intent": "assistant_identity", "confidence": 0.94, "reason": "Capability question about the assistant; answer directly."}
- Input: "who are you?"
  Output: {"route": "direct", "intent": "assistant_identity", "confidence": 0.97, "reason": "Assistant identity question; no merchant analysis needed."}
- Input: "did I ask?"
  Output: {"route": "direct", "intent": "social_challenge", "confidence": 0.92, "reason": "Brief conversational pushback; respond directly, do not switch to merchant overview."}
- Input: "why?"
  Output: {"route": "social_ack", "intent": "social_ack", "confidence": 0.72, "reason": "Short follow-up that should continue the conversation lightly instead of forcing full clarification."}
- Input: "show my recent settlemetns"
  Output: {"route": "deterministic", "intent": "recent_settlements", "confidence": 0.93, "reason": "Typo in a clear settlement list request; route to deterministic retrieval."}
- Input: "why did sales drop?"
  Output: {"route": "analysis", "intent": "what_changed", "confidence": 0.90, "reason": "Specific business performance question that needs analysis, not clarification."}
- Input: "what is the weather like today?"
  Output: {"route": "out_of_scope", "intent": "out_of_scope", "confidence": 0.98, "reason": "Unrelated to merchant operations."}

Return ONLY JSON:
{
  "route": "...",
  "intent": "...",
  "confidence": 0.0,
  "reason": "..."
}
"""


def _prompt(section: str, fallback: str) -> str:
    return load_prompt_section(AGENTS_MD_PATH, section, fallback)


def _sanitize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _sanitize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize(v) for v in value]
    if is_dataclass(value):
        return _sanitize(asdict(value))
    return value


def _llm() -> ChatOllama:
    return ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=float(getattr(Config, "CHAT_REASONING_TEMPERATURE", 0.2)),
    )


def _router_llm() -> ChatOllama:
    return ChatOllama(
        model=getattr(Config, "CHAT_ROUTER_MODEL", Config.OLLAMA_MODEL),
        base_url=Config.OLLAMA_BASE_URL,
        temperature=float(getattr(Config, "CHAT_ROUTER_TEMPERATURE", 0.0)),
    )


def _extract_json(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).replace("```", "").strip()
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            try:
                parsed = json.loads(match.group(0))
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
    return {}


def _repair_json(llm: ChatOllama, raw: str) -> dict[str, Any]:
    try:
        repaired = llm.invoke(
            [
                SystemMessage(content=_prompt("chat_reasoning_json_repair", JSON_REPAIR_PROMPT)),
                HumanMessage(content=str(raw or "")[:12000]),
            ]
        )
        return _extract_json(getattr(repaired, "content", str(repaired)))
    except Exception as exc:
        logger.warning("Chat reasoning JSON repair failed: %s", exc)
        return {}


def synthesize_chat_answer(
    *,
    question: str,
    intent: str,
    scope: dict[str, Any],
    history: list[dict[str, Any]],
    evidence_package: dict[str, Any],
    deterministic_answer: str,
    follow_ups: list[str] | None = None,
) -> dict[str, Any] | None:
    if not getattr(Config, "CHAT_REASONING_ENABLED", True):
        return None

    payload = {
        "question": question,
        "intent": intent,
        "scope": _sanitize(scope),
        "history": _sanitize(history[-6:]),
        "deterministic_fallback": deterministic_answer,
        "suggested_follow_ups": list(follow_ups or []),
        "evidence_package": _sanitize(evidence_package),
    }
    llm = _llm()
    try:
        response = llm.invoke(
            [
                SystemMessage(content=_prompt("chat_reasoning_synthesizer_system", SYNTHESIZER_PROMPT)),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False, indent=2)),
            ]
        )
        parsed = _extract_json(getattr(response, "content", str(response)))
        if not parsed:
            parsed = _repair_json(llm, getattr(response, "content", str(response)))
    except Exception as exc:
        logger.warning("Chat reasoning synthesis failed: %s", exc)
        return None

    if not isinstance(parsed, dict) or not str(parsed.get("raw_answer") or "").strip():
        return None
    parsed.setdefault("verified_points", [])
    parsed.setdefault("hypotheses", [])
    parsed.setdefault("follow_ups", list(follow_ups or []))
    parsed.setdefault("claims", [])
    return parsed


def propose_clarifying_question(
    *,
    question: str,
    intent: str,
    scope: dict[str, Any],
    history: list[dict[str, Any]],
    evidence_package: dict[str, Any],
) -> dict[str, Any] | None:
    if not getattr(Config, "CHAT_REASONING_ENABLED", True):
        return None
    llm = _llm()
    payload = {
        "question": question,
        "intent": intent,
        "scope": _sanitize(scope),
        "history": _sanitize(history[-6:]),
        "evidence_package": _sanitize(evidence_package),
    }
    try:
        response = llm.invoke(
            [
                SystemMessage(content=_prompt("chat_reasoning_clarifier_system", CLARIFIER_PROMPT)),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False, indent=2)),
            ]
        )
        parsed = _extract_json(getattr(response, "content", str(response)))
        if not parsed:
            parsed = _repair_json(llm, getattr(response, "content", str(response)))
    except Exception as exc:
        logger.warning("Chat clarifier failed: %s", exc)
        return None

    question_text = str(parsed.get("question") or "").strip()
    choices = [str(item).strip() for item in list(parsed.get("choices") or []) if str(item).strip()][:4]
    if not question_text:
        return None
    return {
        "question": question_text,
        "choices": choices,
        "reason": str(parsed.get("reason") or "").strip() or "More context would improve the answer.",
    }


def _flatten_strings(value: Any, out: list[str]) -> None:
    if value is None:
        return
    if isinstance(value, dict):
        for item in value.values():
            _flatten_strings(item, out)
        return
    if isinstance(value, list):
        for item in value:
            _flatten_strings(item, out)
        return
    if isinstance(value, (str, int, float, Decimal)):
        out.append(str(value))


def _flatten_numbers(value: Any, out: list[float]) -> None:
    if value is None:
        return
    if isinstance(value, dict):
        for key, item in value.items():
            extracted_from_key = set(_extract_numbers(str(key)))
            for num in extracted_from_key:
                out.append(num)
            _flatten_numbers(item, out)
        return
    if isinstance(value, list):
        for item in value:
            _flatten_numbers(item, out)
        return
    if isinstance(value, (int, float, Decimal)):
        out.append(float(value))
        return
    if isinstance(value, str):
        extracted = set(_extract_numbers(value))
        for num in extracted:
            out.append(num)
        return


def _extract_numbers(text: str) -> list[float]:
    values = []
    for match in re.finditer(r"(?:₹|rs\.?\s*)?(-?\d[\d,]*(?:\.\d+)?)", str(text or ""), flags=re.IGNORECASE):
        raw = match.group(1)
        try:
            values.append(float(raw.replace(",", "")))
        except Exception:
            continue
    return values


def _contains_known_phrase(text: str, candidates: list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(candidate and candidate.lower() in lowered for candidate in candidates)


def validate_reasoning_output(
    *,
    answer_payload: dict[str, Any],
    evidence_package: dict[str, Any],
    sources: list[str],
    ranking_candidates: list[str] | None = None,
    action_candidates: list[str] | None = None,
) -> dict[str, Any]:
    claims = [item for item in list(answer_payload.get("claims") or []) if isinstance(item, dict)]
    evidence_strings: list[str] = []
    evidence_numbers: list[float] = []
    _flatten_strings(evidence_package, evidence_strings)
    _flatten_numbers(evidence_package, evidence_numbers)
    known_sources = {str(item) for item in list(sources or []) if str(item)}
    ranking_candidates = [str(item) for item in list(ranking_candidates or []) if str(item)]
    action_candidates = [str(item) for item in list(action_candidates or []) if str(item)]

    verified_claims: list[dict[str, Any]] = []
    invalid_claims: list[dict[str, Any]] = []
    validation_issues: list[dict[str, Any]] = []

    for claim in claims:
        text = str(claim.get("text") or "").strip()
        kind = str(claim.get("kind") or "general").strip().lower()
        status = str(claim.get("status") or "fact").strip().lower()
        evidence_ids = [str(item) for item in list(claim.get("evidence_ids") or []) if str(item)]
        claim_issues: list[dict[str, Any]] = []

        if status != "hypothesis":
            for number in _extract_numbers(text):
                if not any(abs(ev - number) <= 0.05 for ev in evidence_numbers):
                    claim_issues.append(
                        {
                            "type": "number_not_found_in_evidence",
                            "claim": text,
                            "number": number,
                        }
                    )

            if kind == "ranking" and re.search(r"\b(top|largest|biggest|highest|priority)\b", text, flags=re.IGNORECASE):
                if ranking_candidates and not _contains_known_phrase(text, ranking_candidates):
                    claim_issues.append(
                        {
                            "type": "unsupported_top_rank_claim",
                            "claim": text,
                            "expected_candidates": ranking_candidates[:5],
                        }
                    )

            if kind == "action" and action_candidates and not _contains_known_phrase(text, action_candidates):
                claim_issues.append(
                    {
                        "type": "unsupported_action_statement",
                        "claim": text,
                        "expected_candidates": action_candidates[:5],
                    }
                )

            for evidence_id in evidence_ids:
                if evidence_id not in known_sources:
                    claim_issues.append(
                        {
                            "type": "unknown_evidence_id",
                            "claim": text,
                            "evidence_id": evidence_id,
                        }
                    )

        if claim_issues:
            invalid_claims.append({"text": text, "kind": kind, "status": status, "issues": claim_issues})
            validation_issues.extend(claim_issues)
        else:
            verified_claims.append({"text": text, "kind": kind, "status": status, "evidence_ids": evidence_ids})

    if not claims:
        status = "unverified"
        summary = "No claim-level validation was available for this answer."
    elif invalid_claims and verified_claims:
        status = "partial"
        summary = f"{len(verified_claims)} claim(s) verified, {len(invalid_claims)} claim(s) need review."
    elif invalid_claims:
        status = "unverified"
        summary = f"{len(invalid_claims)} claim(s) need review."
    else:
        status = "clean"
        summary = f"{len(verified_claims)} claim(s) verified."

    display_notice = None
    if status == "partial":
        display_notice = {
            "title": "Some details are directional",
            "summary": "The main answer is grounded, but a few details still need review.",
            "validation_summary": summary,
            "severity": "warning",
            "recommended_next_step": "Review the supporting rows before acting on the less-certain details.",
            "issues": validation_issues,
        }
    elif status == "unverified":
        display_notice = {
            "title": "Review the supporting rows before acting on this",
            "summary": "I found directional support, but the main claim is not fully verified yet.",
            "validation_summary": summary,
            "severity": "warning",
            "recommended_next_step": "Open the exact rows behind this answer and confirm the key numbers first.",
            "issues": validation_issues,
        }

    return {
        "verification_summary": summary,
        "validation_status": status,
        "verified_claims": verified_claims,
        "invalid_claims": invalid_claims,
        "validation_issues": validation_issues,
        "display_notice": display_notice,
    }


def route_chat_intent(
    *,
    question: str,
    scope: dict[str, Any],
    history: list[dict[str, Any]],
) -> dict[str, Any] | None:
    # ── Step 1: Deterministic pre-filter (avoids LLM call for common intents) ──
    if _INTENT_SCORING_AVAILABLE and _score_intent is not None:
        # Build a lightweight memory context from session history
        memory_context: dict[str, Any] | None = None
        if history:
            last_turn = history[-1] if isinstance(history[-1], dict) else {}
            memory_context = {
                "last_intent": last_turn.get("intent"),
                "last_route": last_turn.get("route"),
            }
        try:
            pre_score = _score_intent(
                question=question,
                memory_context=memory_context,
            )
            if pre_score and not pre_score.needs_llm:
                logger.debug(
                    "route_chat_intent: pre-filter resolved route=%s intent=%s "
                    "confidence=%.2f source=%s (LLM skipped)",
                    pre_score.route, pre_score.intent,
                    pre_score.confidence, pre_score.source,
                )
                return {
                    "route": pre_score.route,
                    "intent": pre_score.intent,
                    "confidence": pre_score.confidence,
                    "reason": pre_score.reason,
                    "source": pre_score.source,
                }
        except Exception as pre_exc:
            logger.warning("route_chat_intent: pre-filter raised %s — falling through to LLM", pre_exc)

    # ── Step 2: LLM router (fallback for ambiguous / complex queries) ──────────
    payload = {
        "question": question,
        "scope": _sanitize(scope),
        "history": _sanitize(history[-6:]),
    }
    llm = _router_llm()
    try:
        response = llm.invoke(
            [
                SystemMessage(content=_prompt("chat_reasoning_router_system", ROUTER_PROMPT)),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False, indent=2)),
            ]
        )
        parsed = _extract_json(getattr(response, "content", str(response)))
        if not parsed:
            parsed = _repair_json(llm, getattr(response, "content", str(response)))
    except Exception as exc:
        logger.warning("Chat router failed: %s", exc)
        return None

    route = str(parsed.get("route") or "").strip().lower()
    intent = str(parsed.get("intent") or "").strip()
    if route not in {"direct", "greeting", "social_ack", "out_of_scope", "risky", "deterministic", "analysis", "clarify"}:
        return None
    if not intent:
        intent = "general"
    try:
        confidence = float(parsed.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    return {
        "route": route,
        "intent": intent,
        "confidence": max(0.0, min(confidence, 1.0)),
        "reason": str(parsed.get("reason") or "").strip(),
        "source": "llm",
    }

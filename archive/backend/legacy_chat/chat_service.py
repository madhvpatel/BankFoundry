from __future__ import annotations

import datetime as dt
from dataclasses import asdict, is_dataclass
import re
from typing import Any

from app.copilot.toolcalling import default_window_from_max_date
from app.copilot.tools import (
    ToolContext,
    compute_kpis,
    explain_settlement_shortfall,
    get_merchant_context,
    list_chargebacks,
    list_refunds,
    list_settlements,
    list_transactions,
    terminal_health_summary,
    terminal_performance,
    verify_failure_drivers,
)
from app.intelligence.chat_reasoning import (
    propose_clarifying_question,
    route_chat_intent,
    synthesize_chat_answer,
    validate_reasoning_output,
)
from app.intelligence.runner import run_intelligence
from config import Config

GREETINGS = {"hi", "hello", "hey", "thanks", "thank you"}
ACKNOWLEDGEMENTS = {"interesting", "got it", "okay", "ok", "makes sense", "understood", "thanks", "thank you"}
ABUSIVE_PATTERNS = [r"\bnigga\b", r"\bfuck you\b", r"\bbitch\b", r"\bcunt\b"]

LIST_COLUMNS = {
    "transactions": ["tx_id", "p_date", "initiated_at", "payment_mode", "status", "response_code", "amount_rupees"],
    "settlements": ["settlement_id", "status", "expected_date", "settled_at", "amount_rupees"],
    "chargebacks": ["chargeback_id", "status", "opened_at", "due_by", "amount_rupees", "reason_code"],
    "refunds": ["refund_id", "status", "created_at", "amount_rupees", "tx_id"],
}

SUBSTANTIVE_INTENTS = {
    "business_overview",
    "what_changed",
    "top_growth_opportunities",
    "operational_risks",
    "success_rate_drop",
    "why_payments_failing",
    "terminal_expansion",
}

DETERMINISTIC_INTENTS = {
    "recent_transactions",
    "recent_settlements",
    "recent_refunds",
    "recent_chargebacks",
    "settlement_total",
    "exact_shortfall",
}


def ask_chat(
    engine: Any,
    *,
    merchant_id: str,
    prompt: str,
    terminal_id: str | None = None,
    history: list[dict[str, Any]] | None = None,
    debug: bool = False,
) -> dict[str, Any]:
    prompt_text = str(prompt or "").strip()
    normalized = _normalize(prompt_text)
    ctx = ToolContext(engine=engine, merchant_id=merchant_id, terminal_id=terminal_id)
    history = list(history or [])[-6:]
    default_from, default_to = default_window_from_max_date(engine, merchant_id, days=_window_days(prompt_text), terminal_id=terminal_id)
    scope = _scope_payload(merchant_id, terminal_id, level="terminal" if terminal_id else "merchant")
    fallback_intent = _infer_intent(normalized)
    fallback_route = _fallback_route(fallback_intent, normalized)
    router: dict[str, Any] = {}
    if fallback_intent == "unknown":
        if fallback_route["route"] != "clarify":
            route = str(fallback_route["route"]).strip().lower()
            intent = str(fallback_route["intent"] or "general").strip() or "general"
        else:
            router = route_chat_intent(question=prompt_text, scope=scope, history=history) or {}
            route = str(router.get("route") or fallback_route["route"]).strip().lower()
            intent = str(router.get("intent") or "").strip() or fallback_route["intent"]
            if route not in {"direct", "greeting", "social_ack", "out_of_scope", "risky", "deterministic", "analysis", "clarify"}:
                route = fallback_route["route"]
            if route == "deterministic" and intent not in DETERMINISTIC_INTENTS:
                route = "clarify"
                intent = "general"
            if route in {"analysis", "clarify"} and intent in {"", "general"}:
                intent = "general"
            if not intent:
                intent = "general"
            route, intent = _apply_router_confidence(
                route=route,
                intent=intent,
                confidence=float(router.get("confidence") or 0.0),
                fallback_route=fallback_route,
            )
    else:
        intent = fallback_intent
        route = _regex_route(intent)

    if route == "greeting":
        return _response(
            answer="Hi. I can help with payments, settlements, chargebacks, growth opportunities, and payout shortfalls.",
            verification_status="Not applicable",
            sources=[],
            structured_result=None,
            follow_ups=[
                "How is my business doing?",
                "Show my recent settlements.",
                "What are my top growth opportunities in the last 30 days?",
            ],
            action_preview=None,
            scope=scope,
            intent="greeting",
            answer_source="direct",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="greeting", internal_lane="general", router=router or fallback_route),
        )

    if route == "direct" and intent == "assistant_identity":
        return _response(
            answer="I’m AcquiGuru. I help you understand payments performance, settlements, chargebacks, refunds, terminals, and growth opportunities for your merchant business.",
            verification_status="Not applicable",
            sources=[],
            structured_result=None,
            follow_ups=["How is my business doing?", "Show my recent settlements.", "What changed compared to the previous period?"],
            action_preview=None,
            scope=scope,
            intent=intent,
            answer_source="direct",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="direct", internal_lane="general", router=router),
        )

    if route == "direct" and intent == "social_challenge":
        return _response(
            answer="No. If you want, ask for a metric, a list, or an analysis and I’ll answer that directly.",
            verification_status="Not applicable",
            sources=[],
            structured_result=None,
            follow_ups=["Show my recent settlements.", "How is my business doing?", "What changed compared to the previous period?"],
            action_preview=None,
            scope=scope,
            intent=intent,
            answer_source="direct",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="direct", internal_lane="general", router=router),
        )

    if route == "risky":
        return _response(
            answer="I can help with your merchant operations and payments data, but I won’t engage with abusive language.",
            verification_status="Not applicable",
            sources=[],
            structured_result=None,
            follow_ups=["How is my business doing?", "Show my recent settlements."],
            action_preview=None,
            scope=scope,
            intent="abusive",
            answer_source="direct",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="risky", internal_lane="general", router=router or fallback_route),
        )

    if route == "social_ack":
        return _social_ack_response(scope, history, debug, router=router or fallback_route)

    if route == "out_of_scope":
        return _response(
            answer="I’m focused on your merchant operations and payments data. Ask about settlements, failures, chargebacks, refunds, payouts, terminals, or growth opportunities.",
            verification_status="Not applicable",
            sources=[],
            structured_result=None,
            follow_ups=[
                "What is my business?",
                "How is my business doing?",
                "Show my recent settlements.",
            ],
            action_preview=None,
            scope=scope,
            intent="out_of_scope",
            answer_source="direct",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="out_of_scope", internal_lane="general", router=router or fallback_route),
        )

    if route == "clarify":
        clarifying_question = propose_clarifying_question(
            question=prompt_text,
            intent=intent,
            scope=scope,
            history=history,
            evidence_package={"route_reason": str(router.get("reason") or ""), "scope": scope},
        )
        if clarifying_question:
            return _response(
                answer=clarifying_question["question"],
                verification_status="Not applicable",
                sources=[],
                structured_result=None,
                follow_ups=[],
                action_preview=None,
                scope=scope,
                intent=intent,
                answer_source="clarifying_question",
                verification_summary="No claim-level validation was needed.",
                validation_status="clean",
                validation_issues=[],
                display_notice=None,
                clarifying_question=clarifying_question,
                debug_payload=_debug(debug, route="clarify", internal_lane=_internal_lane(intent), router=router or fallback_route),
            )
        return _response(
            answer="I can answer that, but I need a bit more direction. Do you want a list, an exact number, or a business analysis?",
            verification_status="Not applicable",
            sources=[],
            structured_result=None,
            follow_ups=["Show my recent settlements.", "How is my business doing?", "What changed compared to the previous period?"],
            action_preview=None,
            scope=scope,
            intent=intent,
            answer_source="direct",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="clarify-fallback", internal_lane="general", router=router or {"route": route, "intent": intent}),
        )

    if route == "deterministic" and intent in DETERMINISTIC_INTENTS:
        result = _handle_tool_first_intent(ctx, prompt_text, intent, default_from, default_to)
        result["scope"] = result.get("scope") or scope
        result["intent"] = intent
        if debug:
            result["debug"] = {**(result.get("debug") or {}), "route": "tool-first", "internal_lane": _internal_lane(intent), "router": router or fallback_route}
        else:
            result.pop("debug", None)
        return result

    if route == "analysis" and terminal_id and intent in {"top_growth_opportunities", "why_payments_failing", "success_rate_drop"}:
        result = _handle_terminal_analytics_intent(ctx, prompt_text, intent, default_from, default_to, history=history, debug=debug)
        result["scope"] = result.get("scope") or scope
        result["intent"] = intent
        if debug:
            result["debug"] = {**(result.get("debug") or {}), "router": router or fallback_route}
        return result

    result = _handle_substantive_intent(engine, ctx, prompt_text, intent, history=history, debug=debug)
    result["scope"] = result.get("scope") or scope
    result["intent"] = intent
    if debug:
        result["debug"] = {**(result.get("debug") or {}), "router": router or fallback_route}
    return result


def _normalize(text: str) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "").strip().lower())
    typo_fixes = {
        "settlemetns": "settlements",
        "settlemetns": "settlements",
        "settelments": "settlements",
        "tranasctions": "transactions",
        "transacitons": "transactions",
        "chargbacks": "chargebacks",
        "refnds": "refunds",
    }
    for wrong, correct in typo_fixes.items():
        normalized = normalized.replace(wrong, correct)
    return normalized


def _debug(enabled: bool, **payload: Any) -> dict[str, Any] | None:
    return payload if enabled else None


def _response(
    *,
    answer: str,
    verification_status: str,
    sources: list[str],
    structured_result: dict[str, Any] | None,
    follow_ups: list[str] | None,
    action_preview: dict[str, Any] | None,
    scope: dict[str, Any],
    intent: str,
    answer_source: str,
    verification_summary: str,
    validation_status: str,
    validation_issues: list[dict[str, Any]],
    display_notice: dict[str, Any] | None,
    clarifying_question: dict[str, Any] | None,
    debug_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "answer": answer.strip(),
        "verification_status": verification_status,
        "sources": _dedupe([str(item) for item in sources if str(item)]),
        "structured_result": structured_result,
        "follow_ups": list(follow_ups or []),
        "action_preview": action_preview,
        "scope": scope,
        "intent": intent,
        "answer_source": answer_source,
        "verification_summary": verification_summary,
        "validation_status": validation_status,
        "validation_issues": list(validation_issues or []),
        "display_notice": display_notice,
        "clarifying_question": clarifying_question,
    }
    if debug_payload:
        payload["debug"] = debug_payload
    return payload


def _infer_intent(normalized_prompt: str) -> str:
    if _matches_assistant_capability(normalized_prompt):
        return "assistant_identity"
    if _matches_social_challenge(normalized_prompt):
        return "social_challenge"
    if re.search(r"\bwhat (?:is|'?s) my business\b|\btell me about my business\b|\bwho am i as a merchant\b", normalized_prompt):
        return "business_identity"
    if re.search(r"\bhow is my business doing\b|\bhealth snapshot\b|\boverview\b|\bsummary\b", normalized_prompt):
        return "business_overview"
    if re.search(r"\bwhat changed\b|\bcompared to (?:the )?previous\b|\bversus (?:the )?previous\b|\bprevious period\b", normalized_prompt):
        return "what_changed"
    if re.search(r"\btop growth opportunit|\bgrowth opportunit|\bincrease sales\b|\bgrow revenue\b", normalized_prompt):
        return "top_growth_opportunities"
    if re.search(r"\boperational risks?\b|\bmain operational risks?\b|\boperational issues\b", normalized_prompt):
        return "operational_risks"
    if re.search(r"\bsuccess rate drop\b|\bdid my success rate drop\b|\babnormal success rate\b", normalized_prompt):
        return "success_rate_drop"
    if re.search(r"\bwhy are my payments failing\b|\bwhy are so many payments failing\b|\bwhy did payments fail\b", normalized_prompt):
        return "why_payments_failing"
    if re.search(r"\b(add|get|need|more)\b.*\b(pos|terminal|terminals)\b|\banother pos\b|\banother terminal\b", normalized_prompt):
        return "terminal_expansion"
    if re.search(r"\b(last|recent)\s+\d*\s*transactions\b|\brecent transactions\b", normalized_prompt):
        return "recent_transactions"
    if re.search(r"\b(last|recent)\s+\d*\s*settlements\b|\brecent settlements\b|\bshow .*settlements\b|\bwhat are my settlements\b|\bmy settlements\b", normalized_prompt):
        return "recent_settlements"
    if re.search(r"\brecent refunds\b|\blist .*refunds\b|\bshow .*refunds\b", normalized_prompt):
        return "recent_refunds"
    if re.search(r"\brecent chargebacks\b|\blist .*chargebacks\b|\bshow .*chargebacks\b", normalized_prompt):
        return "recent_chargebacks"
    if _looks_like_shortfall_request(normalized_prompt):
        return "exact_shortfall"
    if re.search(r"\bsettlement amount\b|\btotal settlements?\b|\bhow much .*settlement\b", normalized_prompt):
        return "settlement_total"
    return "unknown"


def _matches_greeting(normalized_prompt: str) -> bool:
    return bool(re.match(r"^(?:hi|hello|hey|yo|sup|what'?s up)(?:\s+(?:man|bro|buddy|friend|there))?[!.?]*$", normalized_prompt))


def _matches_acknowledgement(normalized_prompt: str) -> bool:
    return bool(re.match(r"^(?:interesting|got it|okay|ok|makes sense|understood|cool|thanks|thank you)[!.?]*$", normalized_prompt))


def _matches_assistant_capability(normalized_prompt: str) -> bool:
    return bool(
        re.match(
            r"^(?:who are you|what can you do(?: for me)?|how can you help(?: me)?|what do you do|what are your capabilities)\??$",
            normalized_prompt,
        )
    )


def _matches_social_challenge(normalized_prompt: str) -> bool:
    return bool(re.match(r"^(?:did i ask|who asked|asked\?)\??$", normalized_prompt))


def _internal_lane(intent: str) -> str:
    if intent in {"top_growth_opportunities", "success_rate_drop", "why_payments_failing", "terminal_expansion"}:
        return "growth"
    if intent in {"recent_settlements", "recent_refunds", "recent_chargebacks", "settlement_total", "exact_shortfall", "operational_risks"}:
        return "operations"
    return "general"


def _scope_payload(merchant_id: str, terminal_id: str | None, level: str) -> dict[str, Any]:
    payload = {"merchant_id": merchant_id, "terminal_id": terminal_id, "level": level}
    if terminal_id and level != "terminal":
        payload["note"] = "This answer is merchant-wide because the underlying data path is not terminal-scoped."
    return payload


def _window_days(prompt: str, default: int = 30) -> int:
    match = re.search(r"last\s+(\d{1,3})\s+days", str(prompt or ""), re.IGNORECASE)
    if match:
        return max(7, min(int(match.group(1)), 90))
    if re.search(r"last\s+7\s+days|past\s+week", str(prompt or ""), re.IGNORECASE):
        return 7
    if re.search(r"last\s+90\s+days|past\s+quarter", str(prompt or ""), re.IGNORECASE):
        return 90
    return default


def _date_window(prompt: str, default_from: str, default_to: str) -> tuple[str, str]:
    today = dt.date.today()
    text = _normalize(prompt)
    if "today" in text:
        return today.isoformat(), (today + dt.timedelta(days=1)).isoformat()
    if "yesterday" in text:
        start = today - dt.timedelta(days=1)
        return start.isoformat(), today.isoformat()
    return default_from, default_to


def _looks_like_shortfall_request(normalized_prompt: str) -> bool:
    if "shortfall" in normalized_prompt:
        return True
    return bool(re.search(r"\bexpected\b.*\b(received|got)\b", normalized_prompt))


def _extract_amount_pair(prompt: str) -> tuple[float | None, float | None]:
    pattern = re.compile(
        r"expected\s*(?:rs\.?|₹)?\s*([\d,]+(?:\.\d+)?)"
        r".*?(?:received|got)\s*(?:rs\.?|₹)?\s*([\d,]+(?:\.\d+)?)",
        re.IGNORECASE,
    )
    match = pattern.search(str(prompt or ""))
    if match:
        return _parse_amount(match.group(1)), _parse_amount(match.group(2))

    amounts = re.findall(r"(?:rs\.?|₹)?\s*([\d,]+(?:\.\d+)?)", str(prompt or ""), re.IGNORECASE)
    if len(amounts) >= 2:
        return _parse_amount(amounts[0]), _parse_amount(amounts[1])
    return None, None


def _parse_amount(text: str | None) -> float | None:
    if not text:
        return None
    try:
        return float(str(text).replace(",", "").strip())
    except ValueError:
        return None


def _dedupe(values: list[str]) -> list[str]:
    output: list[str] = []
    seen = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            output.append(value)
    return output


def _normalize_recommendations(recos: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for reco in recos:
        if isinstance(reco, dict):
            normalized.append(reco)
        elif is_dataclass(reco):
            normalized.append(asdict(reco))
    normalized.sort(key=lambda item: float(item.get("priority_score") or 0.0), reverse=True)
    return normalized


def _recommendation_preview(reco: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(reco, dict):
        return None
    actions = list(reco.get("actions") or [])
    if not actions:
        return None
    return {
        "title": reco.get("title"),
        "category": reco.get("category"),
        "summary": reco.get("summary"),
        "impact_rupees": reco.get("impact_rupees"),
        "confidence": reco.get("confidence"),
        "actions": actions[:3],
        "evidence_ids": list(reco.get("evidence_ids") or []),
    }


def _is_out_of_scope(normalized_prompt: str) -> bool:
    return bool(
        re.search(
            r"\bcapital of\b|\bweather\b|\brecipe\b|\bmovie\b|\bfootball score\b|\bstock market news\b",
            normalized_prompt,
        )
    )


def _is_abusive(normalized_prompt: str) -> bool:
    return any(re.search(pattern, normalized_prompt, re.IGNORECASE) for pattern in ABUSIVE_PATTERNS)


def _social_ack_response(scope: dict[str, Any], history: list[dict[str, Any]], debug: bool, *, router: dict[str, Any] | None = None) -> dict[str, Any]:
    last_assistant = next((item for item in reversed(history) if str(item.get("role") or "") == "assistant"), {})
    last_text = str(last_assistant.get("text") or "").strip()
    if last_text:
        answer = "Understood. If you want, I can break that down further or compare it to the previous period."
    else:
        answer = "Understood. Ask the next question whenever you're ready."
    return _response(
        answer=answer,
        verification_status="Not applicable",
        sources=[],
        structured_result=None,
        follow_ups=["Why?", "What changed compared to the previous period?"],
        action_preview=None,
        scope=scope,
        intent="social_ack",
        answer_source="direct",
        verification_summary="No claim-level validation was needed.",
        validation_status="clean",
        validation_issues=[],
        display_notice=None,
        clarifying_question=None,
        debug_payload=_debug(debug, route="social_ack", internal_lane="general", router=router),
    )


def _fallback_route(intent: str, normalized_prompt: str) -> dict[str, Any]:
    if normalized_prompt in GREETINGS or _matches_greeting(normalized_prompt):
        return {"route": "greeting", "intent": "greeting", "confidence": 1.0, "reason": "Exact greeting match fallback."}
    if normalized_prompt in ACKNOWLEDGEMENTS or _matches_acknowledgement(normalized_prompt):
        return {"route": "social_ack", "intent": "social_ack", "confidence": 1.0, "reason": "Exact acknowledgement fallback."}
    if _matches_assistant_capability(normalized_prompt):
        return {"route": "direct", "intent": "assistant_identity", "confidence": 1.0, "reason": "Assistant capability/identity fallback."}
    if _matches_social_challenge(normalized_prompt):
        return {"route": "direct", "intent": "social_challenge", "confidence": 1.0, "reason": "Conversational pushback fallback."}
    if _is_abusive(normalized_prompt):
        return {"route": "risky", "intent": "abusive", "confidence": 1.0, "reason": "Abusive language fallback."}
    if _is_out_of_scope(normalized_prompt):
        return {"route": "out_of_scope", "intent": "out_of_scope", "confidence": 1.0, "reason": "Out-of-scope fallback."}
    if intent in DETERMINISTIC_INTENTS:
        return {"route": "deterministic", "intent": intent, "confidence": 1.0, "reason": "Deterministic merchant data retrieval fallback."}
    if intent == "terminal_expansion":
        return {"route": "clarify", "intent": intent, "confidence": 0.8, "reason": "Terminal expansion usually needs one follow-up."}
    if intent in SUBSTANTIVE_INTENTS | {"business_identity"}:
        return {"route": "analysis", "intent": intent, "confidence": 0.7, "reason": "Broad merchant analysis fallback."}
    return {"route": "clarify", "intent": "general", "confidence": 0.5, "reason": "Unknown prompt should be clarified, not coerced into overview."}


def _regex_route(intent: str) -> str:
    if intent == "unknown":
        return "clarify"
    if intent in {"assistant_identity", "social_challenge"}:
        return "direct"
    if intent in DETERMINISTIC_INTENTS:
        return "deterministic"
    if intent == "terminal_expansion":
        return "clarify"
    if intent in SUBSTANTIVE_INTENTS | {"business_identity"}:
        return "analysis"
    return "clarify"


def _apply_router_confidence(*, route: str, intent: str, confidence: float, fallback_route: dict[str, Any]) -> tuple[str, str]:
    hard_threshold = float(getattr(Config, "CHAT_ROUTER_MIN_CONFIDENCE", 0.65))
    soft_threshold = float(getattr(Config, "CHAT_ROUTER_SOFT_CONFIDENCE", 0.50))
    if route in {"direct", "greeting", "social_ack"} and confidence >= soft_threshold:
        return route, intent
    if confidence >= hard_threshold:
        return route, intent
    return str(fallback_route.get("route") or "clarify"), str(fallback_route.get("intent") or "general")


def _handle_substantive_intent(engine: Any, ctx: ToolContext, prompt: str, intent: str, *, history: list[dict[str, Any]], debug: bool) -> dict[str, Any]:
    if intent not in SUBSTANTIVE_INTENTS and intent != "business_identity":
        intent = "business_overview"

    merchant_context = get_merchant_context(ctx)
    window_days = _window_days(prompt)
    payload = run_intelligence(
        engine,
        ctx.merchant_id,
        window_days=window_days,
        enable_phase2_reasoning=False,
        persist_actions=False,
    )
    recommendations = _normalize_recommendations(list(payload.get("recommendations") or payload.get("recos") or []))
    signals = payload.get("signals") if isinstance(payload.get("signals"), dict) else {}
    filtered = _filter_recommendations(recommendations, intent)
    top = filtered[0] if filtered else (recommendations[0] if recommendations else None)
    sources = _collect_sources(filtered or recommendations)
    fallback_answer = _deterministic_fallback_answer(intent, ctx, prompt, merchant_context, signals, filtered or recommendations, window_days)
    follow_ups = _default_follow_ups(intent)
    scope = _scope_payload(ctx.merchant_id, ctx.terminal_id, level="merchant")

    evidence_package = _build_evidence_package(
        ctx=ctx,
        intent=intent,
        prompt=prompt,
        merchant_context=merchant_context,
        signals=signals,
        recommendations=filtered or recommendations,
        window_days=window_days,
    )

    if intent == "business_identity":
        return _response(
            answer=fallback_answer,
            verification_status=_verification_label(intent),
            sources=sources,
            structured_result=None,
            follow_ups=follow_ups,
            action_preview=None,
            scope=scope,
            intent=intent,
            answer_source="engine",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="engine-identity", internal_lane=_internal_lane(intent), window_days=window_days),
        )

    clarifying_question = None
    if _should_clarify(intent, prompt, evidence_package):
        clarifying_question = propose_clarifying_question(
            question=prompt,
            intent=intent,
            scope=scope,
            history=history,
            evidence_package=evidence_package,
        )
        if clarifying_question:
            return _response(
                answer=clarifying_question["question"],
                verification_status="Not applicable",
                sources=sources,
                structured_result=None,
                follow_ups=[],
                action_preview=None,
                scope=scope,
                intent=intent,
                answer_source="clarifying_question",
                verification_summary="No claim-level validation was needed.",
                validation_status="clean",
                validation_issues=[],
                display_notice=None,
                clarifying_question=clarifying_question,
                debug_payload=_debug(debug, route="clarifier", internal_lane=_internal_lane(intent), window_days=window_days),
            )

    synthesized = synthesize_chat_answer(
        question=prompt,
        intent=intent,
        scope=scope,
        history=history,
        evidence_package=evidence_package,
        deterministic_answer=fallback_answer,
        follow_ups=follow_ups,
    )

    action_preview = _recommendation_preview(top) if _should_include_action_preview(intent) else None
    if not synthesized:
        return _response(
            answer=fallback_answer,
            verification_status=_verification_label(intent),
            sources=sources,
            structured_result=None,
            follow_ups=follow_ups,
            action_preview=action_preview,
            scope=scope,
            intent=intent,
            answer_source="engine",
            verification_summary="No claim-level validation was available for this answer.",
            validation_status="unverified",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="engine-fallback", internal_lane=_internal_lane(intent), window_days=window_days),
        )

    synthesized_follow_ups = [str(item).strip() for item in list(synthesized.get("follow_ups") or follow_ups) if str(item).strip()][:4]
    next_best_action = str(synthesized.get("next_best_action") or "").strip()
    if next_best_action and not action_preview:
        action_preview = {
            "title": "Suggested next action",
            "category": "analysis",
            "summary": next_best_action,
            "actions": [{"who": "merchant", "text": next_best_action}],
            "evidence_ids": sources,
        }
        if not _should_include_action_preview(intent):
            action_preview = None

    ranking_candidates = _ranking_candidates(filtered or recommendations, signals)
    action_candidates = _action_candidates(filtered or recommendations)
    validation = validate_reasoning_output(
        answer_payload=synthesized,
        evidence_package=evidence_package,
        sources=sources,
        ranking_candidates=ranking_candidates,
        action_candidates=action_candidates,
    )

    return _response(
        answer=str(synthesized.get("raw_answer") or fallback_answer),
        verification_status=_verification_label(intent),
        sources=sources,
        structured_result=None,
        follow_ups=synthesized_follow_ups,
        action_preview=action_preview,
        scope=scope,
        intent=intent,
        answer_source="agentic_synthesis",
        verification_summary=str(validation.get("verification_summary") or ""),
        validation_status=str(validation.get("validation_status") or "unverified"),
        validation_issues=list(validation.get("validation_issues") or []),
        display_notice=validation.get("display_notice") if isinstance(validation.get("display_notice"), dict) else None,
        clarifying_question=None,
        debug_payload=_debug(
            debug,
            route="agentic-synthesis",
            internal_lane=_internal_lane(intent),
            window_days=window_days,
            ranking_candidates=ranking_candidates[:5],
            validation_status=validation.get("validation_status"),
        ),
    )


def _build_evidence_package(
    *,
    ctx: ToolContext,
    intent: str,
    prompt: str,
    merchant_context: dict[str, Any],
    signals: dict[str, Any],
    recommendations: list[dict[str, Any]],
    window_days: int,
) -> dict[str, Any]:
    operational = signals.get("operational") if isinstance(signals.get("operational"), dict) else {}
    reconciliation = signals.get("reconciliation") if isinstance(signals.get("reconciliation"), dict) else {}
    disputes = signals.get("disputes") if isinstance(signals.get("disputes"), dict) else {}
    kpi_delta = signals.get("kpi_delta") if isinstance(signals.get("kpi_delta"), dict) else {}
    attribution = signals.get("attribution") if isinstance(signals.get("attribution"), dict) else {}
    package = {
        "merchant_context": merchant_context,
        "window_days": window_days,
        "intent": intent,
        "prompt": prompt,
        "top_recommendations": recommendations[:3],
        "health": signals.get("health_vector") if isinstance(signals.get("health_vector"), dict) else {},
    }
    if intent in {"business_overview", "business_identity"}:
        package["operational"] = {
            "metrics": operational.get("metrics", {}),
            "evidence": {
                "by_payment_mode": list(((operational.get("evidence") or {}).get("by_payment_mode") or []))[:3],
                "top_failure_codes": list(((operational.get("evidence") or {}).get("top_failure_codes") or []))[:3],
            },
        }
        package["kpi_delta"] = {
            "merchant_level": kpi_delta.get("merchant_level", {}),
            "by_payment_mode": list(kpi_delta.get("by_payment_mode") or [])[:3],
        }
    elif intent == "what_changed":
        package["kpi_delta"] = {
            "merchant_level": kpi_delta.get("merchant_level", {}),
            "by_payment_mode": list(kpi_delta.get("by_payment_mode") or [])[:3],
        }
        package["attribution"] = {
            key: {"metric": value.get("metric"), "dimension": value.get("dimension"), "attributions": list(value.get("attributions") or [])[:3]}
            for key, value in attribution.items()
            if isinstance(value, dict)
        }
    elif intent == "top_growth_opportunities":
        package["operational"] = {
            "metrics": operational.get("metrics", {}),
            "evidence": {
                "by_payment_mode": list(((operational.get("evidence") or {}).get("by_payment_mode") or []))[:3],
                "top_failure_codes": list(((operational.get("evidence") or {}).get("top_failure_codes") or []))[:3],
                "top_payer_banks_in_failures": list(((operational.get("evidence") or {}).get("top_payer_banks_in_failures") or []))[:3],
            },
        }
        package["attribution"] = {
            key: {"metric": value.get("metric"), "dimension": value.get("dimension"), "attributions": list(value.get("attributions") or [])[:3]}
            for key, value in attribution.items()
            if isinstance(value, dict)
        }
    elif intent == "operational_risks":
        package["reconciliation"] = {
            "metrics": reconciliation.get("metrics", {}),
            "evidence": {
                "largest_shortfalls": list(((reconciliation.get("evidence") or {}).get("largest_shortfalls") or []))[:3],
                "settlement_status_breakdown": list(((reconciliation.get("evidence") or {}).get("settlement_status_breakdown") or []))[:3],
            },
        }
        package["disputes"] = {
            "metrics": disputes.get("metrics", {}),
            "evidence": {
                "top_chargeback_reasons_by_value": list(((disputes.get("evidence") or {}).get("top_chargeback_reasons_by_value") or []))[:3],
                "oldest_open_cases": list(((disputes.get("evidence") or {}).get("oldest_open_cases") or []))[:3],
            },
        }
    elif intent == "success_rate_drop":
        package["operational"] = {
            "metrics": operational.get("metrics", {}),
            "evidence": {
                "top_failure_codes": list(((operational.get("evidence") or {}).get("top_failure_codes") or []))[:3],
                "by_payment_mode": list(((operational.get("evidence") or {}).get("by_payment_mode") or []))[:3],
            },
        }
        package["attribution"] = {
            key: {"metric": value.get("metric"), "dimension": value.get("dimension"), "attributions": list(value.get("attributions") or [])[:3]}
            for key, value in attribution.items()
            if isinstance(value, dict)
        }
    elif intent == "why_payments_failing":
        package["operational"] = {
            "metrics": operational.get("metrics", {}),
            "evidence": {
                "top_failure_codes": list(((operational.get("evidence") or {}).get("top_failure_codes") or []))[:3],
                "top_payer_banks_in_failures": list(((operational.get("evidence") or {}).get("top_payer_banks_in_failures") or []))[:3],
                "by_payment_mode": list(((operational.get("evidence") or {}).get("by_payment_mode") or []))[:3],
            },
        }
        package["attribution"] = {
            key: {"metric": value.get("metric"), "dimension": value.get("dimension"), "attributions": list(value.get("attributions") or [])[:3]}
            for key, value in attribution.items()
            if isinstance(value, dict)
        }
    elif intent == "terminal_expansion":
        package["operational"] = {
            "metrics": operational.get("metrics", {}),
            "evidence": {
                "by_payment_mode": list(((operational.get("evidence") or {}).get("by_payment_mode") or []))[:3],
            },
        }
        package["terminal_context"] = {"terminal_id": ctx.terminal_id, "merchant_id": ctx.merchant_id}
    return package


def _should_clarify(intent: str, prompt: str, evidence_package: dict[str, Any]) -> bool:
    if intent == "terminal_expansion":
        return True
    if prompt.strip().lower() in {"what should i do", "what next", "why did this drop"}:
        return True
    return False


def _verification_label(intent: str) -> str:
    if intent in {"top_growth_opportunities", "business_overview", "what_changed", "operational_risks", "success_rate_drop", "why_payments_failing", "terminal_expansion", "business_identity"}:
        return "Verified - deterministic evidence with AI-led synthesis"
    return "Verified"


def _default_follow_ups(intent: str) -> list[str]:
    mapping = {
        "business_identity": ["How is my business doing?", "What changed compared to the previous period?"],
        "business_overview": ["What changed compared to the previous period?", "What are my top growth opportunities in the last 30 days?", "Show my recent settlements."],
        "what_changed": ["Why are my payments failing?", "What are my top growth opportunities in the last 30 days?"],
        "top_growth_opportunities": ["Why are my payments failing?", "What changed compared to the previous period?"],
        "operational_risks": ["Show my recent settlements.", "List my recent chargebacks."],
        "success_rate_drop": ["Why are my payments failing?", "What changed compared to the previous period?"],
        "why_payments_failing": ["What changed compared to the previous period?", "What are my top growth opportunities in the last 30 days?"],
        "terminal_expansion": ["How is my business doing?", "What are my top growth opportunities in the last 30 days?"],
    }
    return mapping.get(intent, ["How is my business doing?", "Show my recent settlements."])


def _should_include_action_preview(intent: str) -> bool:
    return intent in {"operational_risks", "why_payments_failing", "top_growth_opportunities", "success_rate_drop", "terminal_expansion", "exact_shortfall"}


def _deterministic_fallback_answer(
    intent: str,
    ctx: ToolContext,
    prompt: str,
    merchant_context: dict[str, Any],
    signals: dict[str, Any],
    recommendations: list[dict[str, Any]],
    window_days: int,
) -> str:
    top = recommendations[0] if recommendations else None
    health = signals.get("health_vector") if isinstance(signals.get("health_vector"), dict) else {}
    operational = signals.get("operational") if isinstance(signals.get("operational"), dict) else {}
    kpi_delta = signals.get("kpi_delta") if isinstance(signals.get("kpi_delta"), dict) else {}
    attribution_bundle = signals.get("attribution") if isinstance(signals.get("attribution"), dict) else {}

    if intent == "business_identity":
        merchant = merchant_context.get("merchant") if isinstance(merchant_context.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or ctx.merchant_id)
        business = str(merchant.get("nature_of_business") or "merchant business").strip()
        city = str(merchant.get("business_city") or "").strip()
        answer = f"You are {trade_name}, a {business} merchant"
        if city:
            answer += f" in {city}"
        answer += "."
        if top:
            answer += f" The biggest current theme is {top.get('title')}."
        return answer

    if intent == "what_changed":
        merchant_level = kpi_delta.get("merchant_level") if isinstance(kpi_delta.get("merchant_level"), dict) else {}
        success_rate_delta = merchant_level.get("success_rate_pct") if isinstance(merchant_level.get("success_rate_pct"), dict) else {}
        success_gmv_delta = merchant_level.get("success_gmv") if isinstance(merchant_level.get("success_gmv"), dict) else {}
        top_attr = _top_attribution(attribution_bundle)
        parts = []
        if success_rate_delta:
            parts.append(f"Success rate moved by {float(success_rate_delta.get('delta_abs') or 0.0):.2f}pp versus the previous equal-length period.")
        if success_gmv_delta:
            parts.append(f"Successful GMV moved by Rs {float(success_gmv_delta.get('delta_abs') or 0.0):,.2f}.")
        if top_attr:
            parts.append(f"The strongest contributor was {top_attr.get('dimension')}={top_attr.get('value')}, contributing about Rs {float(top_attr.get('impact_rupees') or 0.0):,.2f} of the shift.")
        if top:
            parts.append(f"Priority takeaway: {top.get('title')}.")
        return " ".join(parts) if parts else "I could not find a meaningful period-over-period change in the current data window."

    if intent == "top_growth_opportunities":
        return _ranked_recommendation_answer(recommendations[:3], fallback="I could not find a strong growth opportunity in the current window.")

    if intent == "operational_risks":
        return _ranked_recommendation_answer(recommendations[:3], fallback="I could not find a material operational risk in the current window.")

    if intent == "success_rate_drop":
        anomaly_reco = next((reco for reco in recommendations if str((reco.get("metadata") or {}).get("engine") or "") == "anomaly"), top)
        if anomaly_reco:
            return f"{anomaly_reco.get('summary')} Next best action: review the top failure drivers behind the drop before changing pricing or routing."
        return "I do not see an abnormal success-rate drop in the current comparison window."

    if intent == "why_payments_failing":
        op_evidence = operational.get("evidence") if isinstance(operational.get("evidence"), dict) else {}
        top_codes = list(op_evidence.get("top_failure_codes") or [])
        top_banks = list(op_evidence.get("top_payer_banks_in_failures") or [])
        parts = []
        if top_codes:
            top_code = top_codes[0]
            parts.append(f"The biggest failure driver is {top_code.get('response_code')} with {int(top_code.get('fail_count') or 0)} failed transactions and Rs {float(top_code.get('fail_amount') or 0.0):,.2f} in failed GMV.")
        mode_rows = list(op_evidence.get("by_payment_mode") or [])
        if mode_rows:
            worst_mode = min(mode_rows, key=lambda row: float(row.get("success_rate_pct") or 100.0))
            parts.append(f"The weakest payment mode right now is {str(worst_mode.get('payment_mode') or 'UNKNOWN').upper()} at {float(worst_mode.get('success_rate_pct') or 0.0):.2f}% success.")
        if top_banks:
            bank = top_banks[0]
            parts.append(f"Among UPI failures, bank concentration is highest at {bank.get('payer_bank_code')} with {int(bank.get('fail_count') or 0)} failures.")
        if top:
            parts.append(f"Priority fix: {top.get('title')}.")
        return " ".join(parts) if parts else "I could not find a clear failure concentration in the current window."

    if intent == "terminal_expansion":
        op_metrics = operational.get("metrics") if isinstance(operational.get("metrics"), dict) else {}
        terminal_count = int(op_metrics.get("distinct_terminals") or 0)
        attempts = int(op_metrics.get("attempts") or 0)
        if terminal_count <= 1 and attempts >= 500:
            return "You currently appear to rely on a very small terminal footprint for meaningful payment volume, so adding another POS terminal is worth evaluating for resilience and peak-hour coverage."
        if terminal_count >= 2:
            return "You already have multiple active terminals, so I would first check whether the issue is utilization or reliability rather than buying more hardware."
        return "I need a little more context about whether you are solving for peak-hour capacity or failure resilience before recommending another terminal."

    op_metrics = operational.get("metrics") if isinstance(operational.get("metrics"), dict) else {}
    parts = []
    if health:
        parts.append(f"Payment health is {health.get('status') or 'stable'} at {int(health.get('health_score') or 0)}/100.")
    attempts = int(op_metrics.get("attempts") or 0)
    success_rate = op_metrics.get("success_rate_pct")
    if attempts > 0 and success_rate is not None:
        parts.append(f"In the latest {window_days}-day window, you processed {attempts:,} attempts at a {float(success_rate):.2f}% success rate.")
    if top:
        parts.append(f"Top priority right now is {top.get('title')}. {top.get('summary')}")
    return " ".join(parts) if parts else "I could not build a useful operating summary from the current data."


def _filter_recommendations(recommendations: list[dict[str, Any]], intent: str) -> list[dict[str, Any]]:
    if intent == "top_growth_opportunities":
        preferred_titles = ["lost sales", "payment mode", "peak revenue", "peak hour"]
        growth = [reco for reco in recommendations if str(reco.get("category") or "") in {"growth", "performance"}]
        growth.sort(
            key=lambda reco: (
                0 if any(token in str(reco.get("title") or "").lower() for token in preferred_titles) else 1,
                -float(reco.get("priority_score") or 0.0),
            )
        )
        return growth or recommendations
    if intent in {"operational_risks", "exact_shortfall", "settlement_total"}:
        preferred = {"reconciliation", "disputes", "risk", "performance"}
    elif intent == "why_payments_failing":
        preferred = {"performance", "growth", "risk"}
    elif intent == "success_rate_drop":
        preferred = {"performance"}
    elif intent == "terminal_expansion":
        preferred = {"growth", "performance", "risk"}
    else:
        preferred = {"growth", "performance", "reconciliation", "disputes", "risk"}
    filtered = [reco for reco in recommendations if str(reco.get("category") or "") in preferred]
    return filtered or recommendations


def _collect_sources(recommendations: list[dict[str, Any]]) -> list[str]:
    return _dedupe([str(ev) for reco in recommendations for ev in (reco.get("evidence_ids") or []) if str(ev)])


def _top_attribution(bundle: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("failed_gmv_by_response_code", "success_gmv_by_payment_mode"):
        candidate = bundle.get(key) if isinstance(bundle.get(key), dict) else {}
        rows = list(candidate.get("attributions") or [])
        if rows:
            return rows[0]
    return None


def _ranked_recommendation_answer(recommendations: list[dict[str, Any]], *, fallback: str) -> str:
    if not recommendations:
        return fallback
    lines = []
    for idx, reco in enumerate(recommendations[:3], start=1):
        impact = reco.get("impact_rupees")
        impact_text = f" (impact Rs {float(impact or 0.0):,.0f})" if impact else ""
        lines.append(f"{idx}. {reco.get('title')}{impact_text}. {reco.get('summary')}")
    return " ".join(lines)


def _ranking_candidates(recommendations: list[dict[str, Any]], signals: dict[str, Any]) -> list[str]:
    candidates = [str(reco.get("title") or "").strip() for reco in recommendations if str(reco.get("title") or "").strip()]
    attribution = signals.get("attribution") if isinstance(signals.get("attribution"), dict) else {}
    top_attr = _top_attribution(attribution)
    if top_attr:
        candidates.append(str(top_attr.get("value") or ""))
        candidates.append(f"{top_attr.get('dimension')}={top_attr.get('value')}")
    operational = signals.get("operational") if isinstance(signals.get("operational"), dict) else {}
    op_evidence = operational.get("evidence") if isinstance(operational.get("evidence"), dict) else {}
    for row in list(op_evidence.get("top_failure_codes") or [])[:3]:
        candidates.append(str(row.get("response_code") or ""))
    return [item for item in candidates if item]


def _action_candidates(recommendations: list[dict[str, Any]]) -> list[str]:
    candidates: list[str] = []
    for reco in recommendations[:5]:
        for action in list(reco.get("actions") or []):
            if isinstance(action, dict):
                text = str(action.get("text") or "").strip()
                if text:
                    candidates.append(text)
    return candidates


def _handle_terminal_analytics_intent(ctx: ToolContext, prompt: str, intent: str, default_from: str, default_to: str, *, history: list[dict[str, Any]], debug: bool) -> dict[str, Any]:
    if intent == "top_growth_opportunities":
        faildrivers = verify_failure_drivers(ctx, from_date=default_from, to_date=default_to, by="response_code", limit=5)
        terminal = terminal_performance(ctx, from_date=default_from, to_date=default_to, limit=5)
        sources = _dedupe(list(faildrivers.get("evidence") or []) + list(terminal.get("evidence") or []))
        rows = list(faildrivers.get("rows") or [])
        term_rows = list(terminal.get("rows") or [])
        evidence_package = {"failure_drivers": rows, "terminal_performance": term_rows, "window": faildrivers.get("window") or terminal.get("window")}
        fallback_parts = []
        if term_rows:
            row = term_rows[0]
            fallback_parts.append(f"Terminal {row.get('terminal_id')} processed {int(row.get('attempts') or 0):,} attempts at a {float(row.get('success_rate_pct') or 0.0):.2f}% success rate.")
        if rows:
            top = rows[0]
            driver = str(top.get("driver") or "UNKNOWN")
            if driver.upper() == "UNKNOWN":
                fallback_parts.append(f"The biggest issue is {int(top.get('failed_txns') or 0)} failures in the UNKNOWN bucket, worth Rs {float(top.get('failed_gmv') or 0.0):,.2f}. Treat this as a diagnosis problem first: tighten response-code capture and map the failures before changing merchant-side flows.")
            else:
                fallback_parts.append(f"The top failure driver is {driver} with {int(top.get('failed_txns') or 0)} failed transactions and Rs {float(top.get('failed_gmv') or 0.0):,.2f} in failed GMV.")
        fallback_parts.append("Next best action: focus on the highest-value failure bucket on this terminal before broader acceptance changes.")
        synthesized = synthesize_chat_answer(
            question=prompt,
            intent=intent,
            scope=_scope_payload(ctx.merchant_id, ctx.terminal_id, level="terminal"),
            history=history,
            evidence_package=evidence_package,
            deterministic_answer=" ".join(fallback_parts),
            follow_ups=["Show recent settlements.", "Why are my payments failing?"],
        )
        if synthesized:
            validation = validate_reasoning_output(
                answer_payload=synthesized,
                evidence_package=evidence_package,
                sources=sources,
                ranking_candidates=[str(row.get("driver") or "") for row in rows],
                action_candidates=["focus on the highest-value failure bucket"],
            )
            return _response(
                answer=str(synthesized.get("raw_answer") or " ".join(fallback_parts)),
                verification_status="Verified - terminal-scoped analytics",
                sources=sources,
                structured_result=None,
                follow_ups=list(synthesized.get("follow_ups") or ["Show recent settlements.", "Why are my payments failing?"]),
                action_preview=None,
                scope=_scope_payload(ctx.merchant_id, ctx.terminal_id, level="terminal"),
                intent=intent,
                answer_source="agentic_synthesis",
                verification_summary=str(validation.get("verification_summary") or ""),
                validation_status=str(validation.get("validation_status") or "unverified"),
                validation_issues=list(validation.get("validation_issues") or []),
                display_notice=validation.get("display_notice") if isinstance(validation.get("display_notice"), dict) else None,
                clarifying_question=None,
                debug_payload=_debug(debug, route="agentic-terminal", internal_lane="growth", window={"from": default_from, "to": default_to}),
            )
        return _response(
            answer=" ".join(fallback_parts),
            verification_status="Verified - terminal-scoped analytics",
            sources=sources,
            structured_result=None,
            follow_ups=["Show recent settlements.", "Why are my payments failing?", "What changed compared to the previous period?"],
            action_preview=None,
            scope=_scope_payload(ctx.merchant_id, ctx.terminal_id, level="terminal"),
            intent=intent,
            answer_source="tool",
            verification_summary="No claim-level validation was available for this answer.",
            validation_status="unverified",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="tool-first-terminal", internal_lane="growth", window={"from": default_from, "to": default_to}),
        )

    if intent == "success_rate_drop":
        kpis = compute_kpis(ctx, from_date=default_from, to_date=default_to, group_by="none")
        sources = list(kpis.get("evidence") or [])
        rows = list(kpis.get("rows") or [])
        row = rows[0] if rows else {}
        answer = f"For the selected terminal, the current window shows {int(row.get('attempts') or 0):,} attempts at a {float(row.get('success_rate_pct') or 0.0):.2f}% success rate. If you want a deeper change analysis versus the previous period, switch back to merchant-wide scope for the comparison path."
        return _response(
            answer=answer,
            verification_status="Verified - terminal-scoped KPI snapshot",
            sources=sources,
            structured_result=None,
            follow_ups=["Why are my payments failing?", "What are my top growth opportunities in the last 30 days?"],
            action_preview=None,
            scope=_scope_payload(ctx.merchant_id, ctx.terminal_id, level="terminal"),
            intent=intent,
            answer_source="tool",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload=_debug(debug, route="tool-first-terminal", internal_lane="growth", window={"from": default_from, "to": default_to}),
        )

    health = terminal_health_summary(ctx, from_date=default_from, to_date=default_to, flag="low_network_stability", limit=5)
    faildrivers = verify_failure_drivers(ctx, from_date=default_from, to_date=default_to, by="response_code", limit=5)
    sources = _dedupe(list(health.get("evidence") or []) + list(faildrivers.get("evidence") or []))
    rows = list(faildrivers.get("rows") or [])
    health_rows = list(health.get("rows") or [])
    parts = []
    if rows:
        top = rows[0]
        driver = str(top.get("driver") or "UNKNOWN")
        if driver.upper() == "UNKNOWN":
            parts.append(f"Top failure concentration on this terminal sits in the UNKNOWN bucket with {int(top.get('failed_txns') or 0)} failed transactions and Rs {float(top.get('failed_gmv') or 0.0):,.2f} in failed GMV. Treat that as a diagnosis issue first and tighten response capture before changing checkout flows.")
        else:
            parts.append(f"Top failure driver on this terminal is {driver} with {int(top.get('failed_txns') or 0)} failed transactions and Rs {float(top.get('failed_gmv') or 0.0):,.2f} in failed GMV.")
    if health_rows:
        issue = health_rows[0]
        parts.append(f"Terminal health also shows {float(issue.get('flag_pct') or 0.0):.2f}% low network stability across {int(issue.get('snapshots') or 0)} snapshots.")
    if not parts:
        parts.append("I could not find a strong terminal-specific failure signal in the current window.")
    return _response(
        answer=" ".join(parts),
        verification_status="Verified - terminal-scoped analytics",
        sources=sources,
        structured_result=None,
        follow_ups=["Show my recent settlements.", "What changed compared to the previous period?"],
        action_preview=None,
        scope=_scope_payload(ctx.merchant_id, ctx.terminal_id, level="terminal"),
        intent=intent,
        answer_source="tool",
        verification_summary="No claim-level validation was needed.",
        validation_status="clean",
        validation_issues=[],
        display_notice=None,
        clarifying_question=None,
        debug_payload=_debug(debug, route="tool-first-terminal", internal_lane="growth", window={"from": default_from, "to": default_to}),
    )


def _handle_tool_first_intent(ctx: ToolContext, prompt: str, intent: str, default_from: str, default_to: str) -> dict[str, Any]:
    from_date, to_date = _date_window(prompt, default_from, default_to)
    if intent == "recent_transactions":
        result = list_transactions(ctx, from_date=from_date, to_date=to_date, limit=10)
        return _list_response("transactions", "Recent transactions", result, ctx, scoped_level="terminal" if ctx.terminal_id else "merchant")
    if intent == "recent_settlements":
        result = list_settlements(ctx, from_date=from_date, to_date=to_date, limit=10)
        return _list_response("settlements", "Recent settlements", result, ctx, scoped_level="merchant")
    if intent == "recent_refunds":
        result = list_refunds(ctx, from_date=from_date, to_date=to_date, limit=10)
        return _list_response("refunds", "Recent refunds", result, ctx, scoped_level="merchant")
    if intent == "recent_chargebacks":
        result = list_chargebacks(ctx, from_date=from_date, to_date=to_date, limit=10, status="all")
        return _list_response("chargebacks", "Recent chargebacks", result, ctx, scoped_level="merchant")
    if intent == "settlement_total":
        result = list_settlements(ctx, from_date=from_date, to_date=to_date, limit=200)
        rows = list(result.get("rows") or [])
        total = sum(float(row.get("amount_rupees") or 0.0) for row in rows)
        day_text = "the selected period"
        normalized = _normalize(prompt)
        if "today" in normalized or "yesterday" in normalized:
            day_text = f"{from_date}"
        answer = f"Settlement amount for {day_text} is Rs {total:,.2f}."
        if not rows:
            answer = f"There are no settlements in the selected period, so the total settlement amount is Rs 0.00."
        return _response(
            answer=answer,
            verification_status="Verified - deterministic settlement aggregation",
            sources=list(result.get("evidence") or []),
            structured_result=_build_structured_result("settlements", "Settlements used for total", rows, result.get("window")),
            follow_ups=["Show my recent settlements.", "Explain this exact settlement shortfall."],
            action_preview=None,
            scope=_scope_payload(ctx.merchant_id, ctx.terminal_id, level="merchant"),
            intent=intent,
            answer_source="tool",
            verification_summary="No claim-level validation was needed.",
            validation_status="clean",
            validation_issues=[],
            display_notice=None,
            clarifying_question=None,
            debug_payload={"tool_names": ["list_settlements"]},
        )

    expected_amount, received_amount = _extract_amount_pair(prompt)
    result = explain_settlement_shortfall(
        ctx,
        from_date=from_date,
        to_date=to_date,
        expected_amount=expected_amount,
        received_amount=received_amount,
    )
    shortfall = result.get("shortfall") if isinstance(result.get("shortfall"), dict) else {}
    action_preview = None
    recommended_actions = list(result.get("recommended_actions") or [])
    if recommended_actions:
        action_preview = {
            "title": f"Review settlement {shortfall.get('settlement_id') or ''} shortfall".strip(),
            "category": "reconciliation",
            "summary": result.get("deduction_explanation") or result.get("summary"),
            "actions": [{"who": "merchant", "text": item} for item in recommended_actions[:3]],
            "evidence_ids": list(result.get("evidence") or []),
        }
    verification_status = "Verified - deterministic payout shortfall attribution succeeded" if result.get("verified") else (
        "Unverified (supported) - closest settlement matched but payout did not fully reconcile" if result.get("directional_support") else "Insufficient evidence"
    )
    return _response(
        answer=str(result.get("deduction_explanation") or result.get("summary") or "I could not explain the payout shortfall from the available settlement fields."),
        verification_status=verification_status,
        sources=list(result.get("evidence") or []),
        structured_result=None,
        follow_ups=["Show my recent settlements.", "List my recent chargebacks."],
        action_preview=action_preview,
        scope=_scope_payload(ctx.merchant_id, ctx.terminal_id, level="merchant"),
        intent=intent,
        answer_source="tool",
        verification_summary="No claim-level validation was needed.",
        validation_status="clean",
        validation_issues=[],
        display_notice=None,
        clarifying_question=None,
        debug_payload={"tool_names": ["explain_settlement_shortfall"], "shortfall": shortfall},
    )


def _build_structured_result(kind: str, title: str, rows: list[dict[str, Any]], window: dict[str, Any] | None) -> dict[str, Any] | None:
    if not rows:
        return None
    configured = LIST_COLUMNS.get(kind, [])
    columns = [column for column in configured if any(column in row for row in rows)]
    if not columns and rows:
        columns = list(rows[0].keys())
    return {
        "kind": kind,
        "title": title,
        "columns": columns,
        "rows": [{column: row.get(column) for column in columns} for row in rows[:10]],
        "window": window or None,
    }


def _list_response(kind: str, title: str, result: dict[str, Any], ctx: ToolContext, *, scoped_level: str) -> dict[str, Any]:
    rows = list(result.get("rows") or [])
    window = result.get("window") if isinstance(result.get("window"), dict) else None
    if rows:
        answer = f"Here are your {title.lower()} for {window.get('from') if window else 'the selected window'} to {window.get('to') if window else ''}."
    else:
        answer = f"I did not find any {kind} in the selected window."
    return _response(
        answer=answer,
        verification_status="Verified - deterministic list retrieval",
        sources=list(result.get("evidence") or []),
        structured_result=_build_structured_result(kind, title, rows, window),
        follow_ups=["Show me the details of one item from this list.", "How is my business doing?"],
        action_preview=None,
        scope=_scope_payload(ctx.merchant_id, ctx.terminal_id, level=scoped_level),
        intent=f"recent_{kind}",
        answer_source="tool",
        verification_summary="No claim-level validation was needed.",
        validation_status="clean",
        validation_issues=[],
        display_notice=None,
        clarifying_question=None,
        debug_payload={"tool_names": [f"list_{kind}"]},
    )

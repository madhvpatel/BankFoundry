"""
intent_scoring.py — Multi-signal intent scoring pipeline.

Replaces the pure LLM call in route_chat_intent() with a 4-axis pipeline:

  1. Lexical trigger  — keyword/regex scan (deterministic, O(1))
  2. Entity signal    — entity ID present → strong prior on intent
  3. Session memory   — continuation phrase → inherit last intent
  4. Semantic score   — TF-IDF cosine similarity to intent exemplars
  5. LLM fallback     — route_chat_intent() called only if unresolved

For the most common intents (settlement queries, chargeback, recent transactions)
the LLM call is avoided entirely, saving 200-500ms per turn.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Intent + Route constants
# ---------------------------------------------------------------------------

ROUTE_DIRECT      = "direct"
ROUTE_DETERMINISTIC = "deterministic"
ROUTE_ANALYSIS    = "analysis"
ROUTE_CLARIFY     = "clarify"
ROUTE_GREETING    = "greeting"
ROUTE_SOCIAL_ACK  = "social_ack"
ROUTE_OUT_OF_SCOPE = "out_of_scope"
ROUTE_RISKY       = "risky"


# ---------------------------------------------------------------------------
# Lexical trigger map
# Each intent maps to a list of (pattern, route, base_confidence) tuples.
# Patterns are compiled once at import time.
# ---------------------------------------------------------------------------

@dataclass
class LexicalTrigger:
    pattern: re.Pattern[str]
    route: str
    intent: str
    confidence: float


def _t(pattern: str, route: str, intent: str, confidence: float) -> LexicalTrigger:
    return LexicalTrigger(
        pattern=re.compile(pattern, re.IGNORECASE),
        route=route,
        intent=intent,
        confidence=confidence,
    )


LEXICAL_TRIGGERS: list[LexicalTrigger] = [
    # ── Greetings / social ──────────────────────────────────────────────────
    _t(r"^\s*(hey|hi|hello|howdy|yo|sup)\b.*$", ROUTE_GREETING, "general", 0.95),
    _t(r"^\s*(thanks?|thank you|great|awesome|nice|cool|ok|okay|got it|noted|interesting|makes sense)\s*[.!]?\s*$",
       ROUTE_SOCIAL_ACK, "social_ack", 0.93),

    # ── Identity ────────────────────────────────────────────────────────────
    _t(r"\b(who are you|what are you|what can you do|your capabilities|how do you work)\b",
       ROUTE_DIRECT, "assistant_identity", 0.96),

    # ── Settlements (deterministic) ──────────────────────────────────────────
    _t(r"\b(show|list|get|fetch|my)\s+(recent\s+)?settlements?\b",
       ROUTE_DETERMINISTIC, "recent_settlements", 0.94),
    _t(r"\bsettlement\b.{0,40}\b(total|amount|sum|payout)\b",
       ROUTE_DETERMINISTIC, "settlement_total", 0.92),
    _t(r"\b(what|why|explain|how).{0,40}\bshortfall\b",
       ROUTE_ANALYSIS, "exact_shortfall", 0.91),
    _t(r"\b(shortfall|payout.*short|net.*payout|deduction)\b",
       ROUTE_DETERMINISTIC, "exact_shortfall", 0.89),

    # ── Transactions ─────────────────────────────────────────────────────────
    _t(r"\b(show|list|get|fetch|my)\s+(recent\s+)?transactions?\b",
       ROUTE_DETERMINISTIC, "recent_transactions", 0.93),
    _t(r"\b(failed|failed\s+transaction|tx\s+failure|payment\s+failure)\b",
       ROUTE_ANALYSIS, "why_payments_failing", 0.88),

    # ── Chargebacks / refunds ─────────────────────────────────────────────────
    _t(r"\b(show|list|get|my)\s+(recent\s+)?chargebacks?\b",
       ROUTE_DETERMINISTIC, "recent_chargebacks", 0.93),
    _t(r"\b(show|list|get|my)\s+(recent\s+)?refunds?\b",
       ROUTE_DETERMINISTIC, "recent_refunds", 0.93),

    # ── Success rate / failures ───────────────────────────────────────────────
    _t(r"\b(success\s+rate|approval\s+rate|decline\s+rate).{0,20}drop(ped)?\b",
       ROUTE_ANALYSIS, "success_rate_drop", 0.91),
    _t(r"\bwhy.{0,40}(fail|failing|payment\s+fail|decline)\b",
       ROUTE_ANALYSIS, "why_payments_failing", 0.90),

    # ── Business overview / growth ─────────────────────────────────────────────
    _t(r"\b(business\s+overview|how\s+(am\s+i|is\s+my\s+business)\s+doing|overall\s+performance)\b",
       ROUTE_ANALYSIS, "business_overview", 0.89),
    _t(r"\b(top|best|biggest)\s+(growth|opportunit|revenue)\b",
       ROUTE_ANALYSIS, "top_growth_opportunities", 0.87),
    _t(r"\b(what\s+changed|what\s+happened|sales\s+drop|revenue\s+drop|decline)\b",
       ROUTE_ANALYSIS, "what_changed", 0.88),
    _t(r"\b(operational\s+risk|risk|issues|problems|what.{0,20}wrong)\b",
       ROUTE_ANALYSIS, "operational_risks", 0.82),

    # ── Terminal ───────────────────────────────────────────────────────────────
    _t(r"\b(terminal|pos\s+device|machine).{0,30}(fail|down|issue|health|expand)\b",
       ROUTE_ANALYSIS, "terminal_expansion", 0.87),

    # ── Out of scope ───────────────────────────────────────────────────────────
    _t(r"\b(weather|recipe|sport|cricket|football|movie|news|politics|stock\s+market)\b",
       ROUTE_OUT_OF_SCOPE, "out_of_scope", 0.97),

    # ── Risky ──────────────────────────────────────────────────────────────────
    _t(r"\b(idiot|stupid|hate|kill|abuse|scam\s+me|cheating|liar)\b",
       ROUTE_RISKY, "general", 0.90),
]


# ── Entity ID patterns (mirrors bank_ops_agents.py extraction patterns) ───────

_SETTLEMENT_ID_RE = re.compile(r"\bsettlement\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)
_CHARGEBACK_ID_RE = re.compile(r"\bchargeback\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)
_REFUND_ID_RE     = re.compile(r"\brefund\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)
_TX_ID_RE         = re.compile(r"\b(?:tx|transaction)\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)
_TERMINAL_ID_RE   = re.compile(r"\btid\b(?:[:#\s-]+)([A-Za-z0-9][A-Za-z0-9_-]*)\b", re.IGNORECASE)

_CONTINUATION_PHRASES = (
    "same window", "same period", "same dates", "same range",
    "that window", "that period", "again", "show me that", "tell me more",
    "elaborate", "expand on", "what about that",
)


# ---------------------------------------------------------------------------
# IntentScore result
# ---------------------------------------------------------------------------

@dataclass
class IntentScore:
    route: str
    intent: str
    confidence: float
    source: str           # "lexical" | "entity" | "session" | "semantic" | "llm"
    reason: str = ""
    needs_llm: bool = False


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def _step_lexical(text: str) -> IntentScore | None:
    """Check deterministic keyword/regex triggers."""
    stripped = text.strip()
    for trigger in LEXICAL_TRIGGERS:
        if trigger.pattern.search(stripped):
            return IntentScore(
                route=trigger.route,
                intent=trigger.intent,
                confidence=trigger.confidence,
                source="lexical",
                reason=f"Matched lexical trigger pattern for {trigger.intent}",
            )
    return None


def _step_entity(text: str) -> IntentScore | None:
    """
    If the message contains a specific entity ID (settlement:XXX, chargeback:XXX…),
    return a high-confidence deterministic score.
    """
    if _SETTLEMENT_ID_RE.search(text):
        return IntentScore(
            route=ROUTE_DETERMINISTIC, intent="recent_settlements",
            confidence=0.92, source="entity",
            reason="Settlement ID detected in message",
        )
    if _CHARGEBACK_ID_RE.search(text):
        return IntentScore(
            route=ROUTE_DETERMINISTIC, intent="recent_chargebacks",
            confidence=0.92, source="entity",
            reason="Chargeback ID detected in message",
        )
    if _REFUND_ID_RE.search(text):
        return IntentScore(
            route=ROUTE_DETERMINISTIC, intent="recent_refunds",
            confidence=0.92, source="entity",
            reason="Refund ID detected in message",
        )
    if _TX_ID_RE.search(text):
        return IntentScore(
            route=ROUTE_DETERMINISTIC, intent="recent_transactions",
            confidence=0.91, source="entity",
            reason="Transaction ID detected in message",
        )
    if _TERMINAL_ID_RE.search(text):
        return IntentScore(
            route=ROUTE_ANALYSIS, intent="terminal_expansion",
            confidence=0.88, source="entity",
            reason="Terminal ID detected in message",
        )
    return None


def _step_session_memory(text: str, memory_context: dict[str, Any] | None) -> IntentScore | None:
    """
    If the message is a continuation phrase AND there is a pinned last_intent
    in memory, inherit that intent.
    """
    if not isinstance(memory_context, dict):
        return None
    last_intent = str(memory_context.get("last_intent") or "").strip()
    last_route = str(memory_context.get("last_route") or "").strip()
    if not last_intent or not last_route:
        return None

    lowered = text.strip().lower()
    if any(phrase in lowered for phrase in _CONTINUATION_PHRASES):
        return IntentScore(
            route=last_route,
            intent=last_intent,
            confidence=0.82,
            source="session",
            reason=f"Continuation phrase detected; inheriting intent '{last_intent}' from session memory",
        )
    return None


def _step_semantic(text: str) -> IntentScore | None:
    """
    TF-IDF similarity to a small set of intent exemplars.
    This is a lightweight fallback before hitting the LLM.
    Only fires if confidence is strong enough (>= 0.75).
    """
    try:
        import numpy as np
        from sklearn.feature_extraction.text import TfidfVectorizer
    except ImportError:
        return None

    exemplars = {
        "recent_settlements":     "show my settlements list payout",
        "recent_transactions":    "show recent transactions list",
        "recent_chargebacks":     "show chargebacks disputes list",
        "recent_refunds":         "show refunds list",
        "what_changed":           "what changed why did sales drop revenue fell",
        "top_growth_opportunities": "growth opportunities what can i improve revenue",
        "why_payments_failing":   "why are payments failing decline reason codes",
        "business_overview":      "how is my business doing overview performance",
        "exact_shortfall":        "shortfall payout deduction net gross shortfall explain",
        "operational_risks":      "what operational issues risks problems wrong",
    }

    labels = list(exemplars.keys())
    docs = [exemplars[l] for l in labels] + [text]

    vectorizer = TfidfVectorizer(lowercase=True, stop_words="english", ngram_range=(1, 2))
    try:
        matrix = vectorizer.fit_transform(docs)
    except Exception:
        return None

    query_vec = matrix[-1]
    scores = (matrix[:-1] @ query_vec.T).toarray().reshape(-1)
    best_idx = int(np.argmax(scores))
    best_score = float(scores[best_idx])

    if best_score < 0.15:
        return None

    intent = labels[best_idx]
    route = ROUTE_DETERMINISTIC if intent in {
        "recent_settlements", "recent_transactions", "recent_chargebacks",
        "recent_refunds", "exact_shortfall",
    } else ROUTE_ANALYSIS

    # Scale raw TF-IDF score to [0.6, 0.85] range
    confidence = min(0.85, 0.60 + best_score * 1.5)

    return IntentScore(
        route=route,
        intent=intent,
        confidence=confidence,
        source="semantic",
        reason=f"TF-IDF similarity {best_score:.3f} to exemplar '{intent}'",
    )


# ---------------------------------------------------------------------------
# Public pipeline entry point
# ---------------------------------------------------------------------------

CONFIDENCE_THRESHOLD = 0.80  # below this → fall through to LLM


def score_intent(
    *,
    question: str,
    memory_context: dict[str, Any] | None = None,
    confidence_threshold: float = CONFIDENCE_THRESHOLD,
) -> IntentScore:
    """
    Run the 4-axis pipeline and return the best IntentScore.

    If no axis achieves confidence >= threshold, returns an IntentScore
    with needs_llm=True so the caller knows to invoke route_chat_intent().

    Parameters
    ----------
    question            : raw user message
    memory_context      : session memory dict (may contain last_intent, last_route)
    confidence_threshold: minimum confidence to bypass the LLM fallback
    """
    text = str(question or "").strip()
    if not text:
        return IntentScore(
            route=ROUTE_SOCIAL_ACK, intent="general",
            confidence=0.70, source="lexical",
            reason="Empty message",
        )

    # Step 1: Lexical trigger
    score = _step_lexical(text)
    if score and score.confidence >= confidence_threshold:
        return score

    # Step 2: Entity signal
    entity_score = _step_entity(text)
    if entity_score and entity_score.confidence >= confidence_threshold:
        return entity_score

    # Step 3: Session memory
    session_score = _step_session_memory(text, memory_context)
    if session_score and session_score.confidence >= confidence_threshold:
        return session_score

    # Step 4: Semantic
    semantic_score = _step_semantic(text)
    if semantic_score and semantic_score.confidence >= confidence_threshold:
        return semantic_score

    # Best partial score — return with needs_llm=True
    best_partial = max(
        [s for s in [score, entity_score, session_score, semantic_score] if s],
        key=lambda s: s.confidence,
        default=None,
    )
    if best_partial:
        best_partial.needs_llm = True
        best_partial.reason += " (confidence below threshold — LLM fallback triggered)"
        return best_partial

    # No signal at all
    return IntentScore(
        route=ROUTE_ANALYSIS, intent="general",
        confidence=0.0, source="lexical",
        reason="No signal matched — LLM fallback required",
        needs_llm=True,
    )

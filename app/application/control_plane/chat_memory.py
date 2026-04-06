from __future__ import annotations

import datetime as dt
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

MAX_STORED_TURNS = 40
MAX_HISTORY_MESSAGES = 10
MAX_RECENT_TURNS = 4
MAX_RELEVANT_TURNS = 3
MAX_FACTS = 8
MAX_SUMMARIES = 10
MAX_FOLLOW_UPS = 4

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "me",
    "my",
    "of",
    "on",
    "or",
    "our",
    "please",
    "show",
    "that",
    "the",
    "this",
    "to",
    "what",
    "when",
    "where",
    "which",
    "why",
    "with",
    "you",
    "your",
}

ENTITY_PATTERNS = {
    "settlement_id": re.compile(r"^settlement:(.+)$", flags=re.IGNORECASE),
    "chargeback_id": re.compile(r"^chargeback:(.+)$", flags=re.IGNORECASE),
    "refund_id": re.compile(r"^refund:(.+)$", flags=re.IGNORECASE),
    "terminal_id": re.compile(r"^terminal:(.+)$", flags=re.IGNORECASE),
    "transaction_id": re.compile(r"^tx:(.+)$", flags=re.IGNORECASE),
    "action_id": re.compile(r"^action:(.+)$", flags=re.IGNORECASE),
}

PROMPT_ENTITY_PATTERNS = [
    ("settlement_id", re.compile(r"\bsettlement\s+([A-Za-z0-9_-]+)\b", flags=re.IGNORECASE)),
    ("chargeback_id", re.compile(r"\bchargeback\s+([A-Za-z0-9_-]+)\b", flags=re.IGNORECASE)),
    ("refund_id", re.compile(r"\brefund\s+([A-Za-z0-9_-]+)\b", flags=re.IGNORECASE)),
    ("terminal_id", re.compile(r"\bterminal\s+([A-Za-z0-9_-]+)\b", flags=re.IGNORECASE)),
    ("transaction_id", re.compile(r"\b(?:transaction|txn)\s+([A-Za-z0-9_-]+)\b", flags=re.IGNORECASE)),
]

TOPIC_KEYWORDS = {
    "settlements": {"settlement", "payout", "shortfall", "cashflow", "hold", "utr"},
    "transactions": {"transaction", "tx", "gmv", "amount", "ticket"},
    "chargebacks": {"chargeback", "dispute", "prearbitration"},
    "refunds": {"refund", "reversal"},
    "terminals": {"terminal", "device", "tid", "network", "printer", "battery"},
    "failures": {"failure", "failed", "decline", "response", "success", "acceptance"},
    "growth": {"growth", "opportunity", "revenue", "uplift", "credit"},
}

REFERENTIAL_PATTERNS = [
    re.compile(r"\bthat\b", flags=re.IGNORECASE),
    re.compile(r"\bthose\b", flags=re.IGNORECASE),
    re.compile(r"\bit\b", flags=re.IGNORECASE),
    re.compile(r"\bagain\b", flags=re.IGNORECASE),
    re.compile(r"\bsame\b", flags=re.IGNORECASE),
    re.compile(r"\bprevious\b", flags=re.IGNORECASE),
    re.compile(r"\bthe rows\b", flags=re.IGNORECASE),
]

WINDOW_REFERENTIAL_PATTERNS = [
    re.compile(r"\bsame (?:window|period|dates|range)\b", flags=re.IGNORECASE),
    re.compile(r"\bthat (?:window|period|range)\b", flags=re.IGNORECASE),
    re.compile(r"\bagain\b", flags=re.IGNORECASE),
]


def _iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _clean_text(value: Any, *, limit: int | None = None) -> str:
    text_value = str(value or "").strip()
    if not text_value:
        return ""
    if limit is not None and len(text_value) > limit:
        return text_value[: limit - 1].rstrip() + "…"
    return text_value


def _json_dump(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, default=str)


def _json_load_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_load_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _tokenize(text_value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9_]+", str(text_value or "").lower())
        if len(token) >= 3 and token not in STOPWORDS
    }


def _looks_referential(prompt: str) -> bool:
    return any(pattern.search(str(prompt or "")) for pattern in REFERENTIAL_PATTERNS)


def _wants_same_window(prompt: str) -> bool:
    return any(pattern.search(str(prompt or "")) for pattern in WINDOW_REFERENTIAL_PATTERNS)


def _extract_entities_from_sources(
    sources: list[str],
    *,
    terminal_id: str | None = None,
) -> dict[str, str]:
    entities: dict[str, str] = {}
    for source in sources:
        text_value = str(source or "").strip()
        if not text_value:
            continue
        for key, pattern in ENTITY_PATTERNS.items():
            match = pattern.match(text_value)
            if match:
                entities[key] = str(match.group(1)).strip()
    if terminal_id:
        entities.setdefault("terminal_id", str(terminal_id))
    return entities


def _extract_entities_from_trace(trace: dict[str, Any]) -> dict[str, str]:
    entities: dict[str, str] = {}
    for call in list(trace.get("tool_calls") or []):
        if not isinstance(call, dict):
            continue
        name = str(call.get("name") or "").strip()
        args = call.get("args") or {}
        if not isinstance(args, dict):
            args = {}
        if name == "get_settlement_detail" and args.get("settlement_id") is not None:
            entities["settlement_id"] = str(args.get("settlement_id"))
        elif name == "get_chargeback_detail" and args.get("chargeback_id") is not None:
            entities["chargeback_id"] = str(args.get("chargeback_id"))
        elif name == "get_transaction_detail" and args.get("tx_id") is not None:
            entities["transaction_id"] = str(args.get("tx_id"))
        elif name == "geo_drift_check" and args.get("tid") is not None:
            entities["terminal_id"] = str(args.get("tid"))
    return entities


def _extract_entities_from_prompt(prompt: str) -> dict[str, str]:
    entities: dict[str, str] = {}
    for key, pattern in PROMPT_ENTITY_PATTERNS:
        match = pattern.search(str(prompt or ""))
        if match:
            entities[key] = str(match.group(1)).strip()
    return entities


def _detect_topics(prompt: str, payload: dict[str, Any]) -> list[str]:
    text_parts = [str(prompt or "")]
    for source in list(payload.get("sources") or [])[:20]:
        text_parts.append(str(source))
    trace = payload.get("trace") or {}
    if isinstance(trace, dict):
        for call in list(trace.get("tool_calls") or [])[:10]:
            if isinstance(call, dict):
                text_parts.append(str(call.get("name") or ""))
    haystack = " ".join(text_parts).lower()
    topic_scores: list[tuple[int, str]] = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in haystack)
        if score > 0:
            topic_scores.append((score, topic))
    topic_scores.sort(reverse=True)
    return [topic for _, topic in topic_scores[:4]]


def _extract_active_window(payload: dict[str, Any], *, prior_window: dict[str, Any] | None = None) -> dict[str, Any]:
    trace = payload.get("trace") or {}
    if not isinstance(trace, dict):
        trace = {}

    normalized = trace.get("normalized_time_window")
    if isinstance(normalized, dict):
        from_date = _clean_text(normalized.get("from_date"))
        to_date = _clean_text(normalized.get("to_date"))
        if from_date or to_date:
            return {
                "from_date": from_date,
                "to_date": to_date,
                "label": _clean_text(normalized.get("label")),
                "reason": _clean_text(normalized.get("reason")) or "normalized_time_window",
                "source_phrase": _clean_text(normalized.get("source_phrase")),
            }

    structured_result = payload.get("structured_result")
    if isinstance(structured_result, dict):
        window = structured_result.get("window")
        if isinstance(window, dict):
            from_date = _clean_text(window.get("from") or window.get("from_date"))
            to_date = _clean_text(window.get("to") or window.get("to_date"))
            if from_date or to_date:
                return {
                    "from_date": from_date,
                    "to_date": to_date,
                    "label": _clean_text(window.get("label")),
                    "reason": "structured_result",
                }

    default_window = trace.get("default_window")
    if isinstance(default_window, dict):
        from_date = _clean_text(default_window.get("from_date"))
        to_date = _clean_text(default_window.get("to_date"))
        if from_date or to_date:
            return {
                "from_date": from_date,
                "to_date": to_date,
                "label": _clean_text(default_window.get("label")),
                "reason": "default_window",
            }

    return dict(prior_window or {})


def _build_turn_summary(prompt: str, payload: dict[str, Any]) -> str:
    sections = payload.get("answer_sections") or {}
    if not isinstance(sections, dict):
        sections = {}
    executive = _clean_text(sections.get("executive_summary"), limit=220)
    next_step = _clean_text(sections.get("next_best_action"), limit=120)
    if executive and next_step:
        return f"{executive} Next: {next_step}"
    if executive:
        return executive
    answer = _clean_text(payload.get("answer"), limit=220)
    if answer:
        return answer
    question = _clean_text(payload.get("clarifying_question", {}).get("question") if isinstance(payload.get("clarifying_question"), dict) else "", limit=180)
    if question:
        return question
    return _clean_text(prompt, limit=180)


def _fact_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not list(payload.get("sources") or []):
        return []
    sections = payload.get("answer_sections") or {}
    if not isinstance(sections, dict):
        sections = {}
    texts: list[str] = []
    executive = _clean_text(sections.get("executive_summary"), limit=180)
    if executive:
        texts.append(executive)
    for item in list(sections.get("key_findings") or [])[:2]:
        text_value = _clean_text(item, limit=180)
        if text_value:
            texts.append(text_value)
    facts: list[dict[str, Any]] = []
    for text_value in texts[:3]:
        facts.append(
            {
                "text": text_value,
                "evidence_ids": [str(item) for item in list(payload.get("sources") or [])[:6]],
                "validation_status": _clean_text(payload.get("validation_status")) or "clean",
            }
        )
    return facts


def _merge_entities(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, str]:
    merged: dict[str, str] = {}
    for source in (previous or {}, current or {}):
        for key, value in source.items():
            text_value = _clean_text(value)
            if text_value:
                merged[str(key)] = text_value
    return merged


def _merge_unique_text(left: list[Any] | None, right: list[Any] | None, *, limit: int) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for source in (left or []) + (right or []):
        text_value = _clean_text(source)
        if text_value and text_value not in seen:
            seen.add(text_value)
            merged.append(text_value)
        if len(merged) >= limit:
            break
    return merged


def _merge_facts(previous: list[dict[str, Any]] | None, current: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in (current or []) + (previous or []):
        if not isinstance(item, dict):
            continue
        text_value = _clean_text(item.get("text"), limit=180)
        if not text_value or text_value in seen:
            continue
        seen.add(text_value)
        merged.append(
            {
                "text": text_value,
                "evidence_ids": [str(value) for value in list(item.get("evidence_ids") or [])[:6]],
                "validation_status": _clean_text(item.get("validation_status")) or "clean",
            }
        )
        if len(merged) >= MAX_FACTS:
            break
    return merged


def _empty_session_state(
    *,
    session_key: str,
    merchant_id: str,
    terminal_id: str | None,
    thread_scope: str | None,
) -> dict[str, Any]:
    selected_entities: dict[str, str] = {}
    if terminal_id:
        selected_entities["terminal_id"] = str(terminal_id)
    return {
        "session_key": session_key,
        "merchant_id": merchant_id,
        "terminal_id": terminal_id,
        "thread_scope": _clean_text(thread_scope) or "default",
        "selected_entities": selected_entities,
        "active_window": {},
        "active_topics": [],
        "last_evidence_ids": [],
        "outstanding_follow_ups": [],
        "verified_facts": [],
        "recent_summaries": [],
        "turn_count": 0,
        "last_turn_id": None,
        "last_turn_at": None,
    }


def _normalize_session_state(
    value: Any,
    *,
    session_key: str,
    merchant_id: str,
    terminal_id: str | None,
    thread_scope: str | None,
) -> dict[str, Any]:
    payload = _json_load_dict(value)
    normalized = _empty_session_state(
        session_key=session_key,
        merchant_id=merchant_id,
        terminal_id=terminal_id,
        thread_scope=thread_scope,
    )
    normalized["selected_entities"] = _merge_entities(
        normalized["selected_entities"],
        payload.get("selected_entities") if isinstance(payload.get("selected_entities"), dict) else {},
    )
    normalized["active_window"] = payload.get("active_window") if isinstance(payload.get("active_window"), dict) else {}
    normalized["active_topics"] = _merge_unique_text([], payload.get("active_topics"), limit=6)
    normalized["last_evidence_ids"] = _merge_unique_text([], payload.get("last_evidence_ids"), limit=10)
    normalized["outstanding_follow_ups"] = _merge_unique_text([], payload.get("outstanding_follow_ups"), limit=MAX_FOLLOW_UPS)
    normalized["verified_facts"] = _merge_facts([], payload.get("verified_facts"))
    normalized["recent_summaries"] = _merge_unique_text([], payload.get("recent_summaries"), limit=MAX_SUMMARIES)
    normalized["turn_count"] = int(payload.get("turn_count") or 0)
    normalized["last_turn_id"] = _clean_text(payload.get("last_turn_id")) or None
    normalized["last_turn_at"] = _clean_text(payload.get("last_turn_at")) or None
    return normalized


def _normalize_turn_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "turn_id": _clean_text(row.get("turn_id")),
        "turn_index": int(row.get("turn_index") or 0),
        "prompt": _clean_text(row.get("prompt")),
        "answer": _clean_text(row.get("answer_text")),
        "summary": _clean_text(row.get("summary_text")),
        "sources": [str(item) for item in _json_load_list(row.get("sources_json")) if _clean_text(item)],
        "follow_ups": [str(item) for item in _json_load_list(row.get("follow_ups_json")) if _clean_text(item)],
        "selected_entities": _json_load_dict(row.get("selected_entities_json")),
        "active_window": _json_load_dict(row.get("active_window_json")),
        "topics": [str(item) for item in _json_load_list(row.get("topics_json")) if _clean_text(item)],
        "validation_status": _clean_text(row.get("validation_status")) or "clean",
        "payload": _json_load_dict(row.get("payload_json")),
        "created_at": _clean_text(row.get("created_at")),
    }


def ensure_chat_memory_schema(engine: Any) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS control_plane_sessions (
            session_key TEXT PRIMARY KEY,
            request_type TEXT NOT NULL,
            surface TEXT NOT NULL,
            merchant_id TEXT NOT NULL,
            terminal_id TEXT NULL,
            thread_scope TEXT NULL,
            state_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS control_plane_session_turns (
            turn_id TEXT PRIMARY KEY,
            session_key TEXT NOT NULL,
            turn_index INTEGER NOT NULL,
            prompt TEXT NOT NULL,
            answer_text TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            sources_json TEXT NOT NULL,
            follow_ups_json TEXT NOT NULL,
            selected_entities_json TEXT NOT NULL,
            active_window_json TEXT NOT NULL,
            topics_json TEXT NOT NULL,
            validation_status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """,
    ]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


@dataclass
class ChatMemoryService:
    engine: Any

    def load_session(
        self,
        *,
        session_key: str,
        merchant_id: str,
        terminal_id: str | None = None,
        thread_scope: str | None = None,
        request_type: str = "chat_turn",
        surface: str = "web_chat",
    ) -> dict[str, Any]:
        ensure_chat_memory_schema(self.engine)
        with self.engine.begin() as conn:
            session_row = conn.execute(
                text(
                    """
                    SELECT session_key, state_json, created_at, updated_at, thread_scope, merchant_id, terminal_id
                    FROM control_plane_sessions
                    WHERE session_key = :session_key
                    """
                ),
                {"session_key": session_key},
            ).mappings().first()

            if session_row is None:
                now = _iso_now()
                state = _empty_session_state(
                    session_key=session_key,
                    merchant_id=merchant_id,
                    terminal_id=terminal_id,
                    thread_scope=thread_scope,
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO control_plane_sessions (
                            session_key, request_type, surface, merchant_id, terminal_id, thread_scope,
                            state_json, created_at, updated_at
                        ) VALUES (
                            :session_key, :request_type, :surface, :merchant_id, :terminal_id, :thread_scope,
                            :state_json, :created_at, :updated_at
                        )
                        """
                    ),
                    {
                        "session_key": session_key,
                        "request_type": request_type,
                        "surface": surface,
                        "merchant_id": merchant_id,
                        "terminal_id": terminal_id,
                        "thread_scope": _clean_text(thread_scope) or "default",
                        "state_json": _json_dump(state),
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                session_row = {
                    "session_key": session_key,
                    "state_json": _json_dump(state),
                    "created_at": now,
                    "updated_at": now,
                    "thread_scope": _clean_text(thread_scope) or "default",
                    "merchant_id": merchant_id,
                    "terminal_id": terminal_id,
                }

            turns = [
                _normalize_turn_row(dict(row))
                for row in conn.execute(
                    text(
                        """
                        SELECT *
                        FROM control_plane_session_turns
                        WHERE session_key = :session_key
                        ORDER BY turn_index ASC
                        """
                    ),
                    {"session_key": session_key},
                ).mappings().all()
            ]

        session = _normalize_session_state(
            session_row.get("state_json"),
            session_key=session_key,
            merchant_id=merchant_id,
            terminal_id=terminal_id,
            thread_scope=thread_scope,
        )
        session["created_at"] = _clean_text(session_row.get("created_at")) or None
        session["updated_at"] = _clean_text(session_row.get("updated_at")) or None
        return {"session": session, "turns": turns}

    def merged_history(
        self,
        session_bundle: dict[str, Any],
        *,
        request_history: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, str]]:
        messages: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()

        for turn in list(session_bundle.get("turns") or [])[-MAX_RECENT_TURNS:]:
            if not isinstance(turn, dict):
                continue
            for item in (
                {"role": "user", "text": _clean_text(turn.get("prompt"), limit=320)},
                {"role": "assistant", "text": _clean_text(turn.get("answer") or turn.get("summary"), limit=420)},
            ):
                role = _clean_text(item.get("role"))
                text_value = _clean_text(item.get("text"))
                if not role or not text_value:
                    continue
                key = (role, text_value)
                if key in seen:
                    continue
                seen.add(key)
                messages.append({"role": role, "text": text_value})

        for item in list(request_history or [])[-MAX_HISTORY_MESSAGES:]:
            if not isinstance(item, dict):
                continue
            role = _clean_text(item.get("role"))
            text_value = _clean_text(item.get("text"), limit=420)
            if not role or not text_value:
                continue
            key = (role, text_value)
            if key in seen:
                continue
            seen.add(key)
            messages.append({"role": role, "text": text_value})

        return messages[-MAX_HISTORY_MESSAGES:]

    def agent_memory_context(
        self,
        session_bundle: dict[str, Any],
        *,
        prompt: str,
    ) -> dict[str, Any]:
        session = session_bundle.get("session") or {}
        turns = [turn for turn in list(session_bundle.get("turns") or []) if isinstance(turn, dict)]
        prompt_entities = _extract_entities_from_prompt(prompt)
        if not prompt_entities and _looks_referential(prompt):
            prompt_entities = dict(session.get("selected_entities") or {})
        prompt_tokens = _tokenize(prompt)
        prompt_topics = _detect_topics(prompt, {})
        if not prompt_topics and _looks_referential(prompt):
            prompt_topics = list(session.get("active_topics") or [])

        relevant_turns = self._rank_relevant_turns(
            turns=turns,
            prompt=prompt,
            prompt_tokens=prompt_tokens,
            prompt_entities=prompt_entities,
            prompt_topics=prompt_topics,
        )

        recent_turns = []
        for turn in turns[-MAX_RECENT_TURNS:]:
            recent_turns.append(
                {
                    "turn_id": turn.get("turn_id"),
                    "prompt": _clean_text(turn.get("prompt"), limit=160),
                    "summary": _clean_text(turn.get("summary"), limit=200),
                    "sources": list(turn.get("sources") or [])[:6],
                    "created_at": turn.get("created_at"),
                }
            )

        memory_context = {
            "session_key": session.get("session_key"),
            "thread_scope": session.get("thread_scope") or "default",
            "turn_count": int(session.get("turn_count") or 0),
            "selected_entities": dict(session.get("selected_entities") or {}),
            "active_window": dict(session.get("active_window") or {}),
            "active_topics": list(session.get("active_topics") or [])[:6],
            "last_evidence_ids": list(session.get("last_evidence_ids") or [])[:8],
            "outstanding_follow_ups": list(session.get("outstanding_follow_ups") or [])[:MAX_FOLLOW_UPS],
            "verified_facts": list(session.get("verified_facts") or [])[:MAX_FACTS],
            "recent_turns": recent_turns,
            "relevant_memories": relevant_turns,
        }

        if _wants_same_window(prompt) and session.get("active_window"):
            memory_context["preferred_window"] = dict(session.get("active_window") or {})

        return memory_context

    def remember_turn(
        self,
        *,
        session_key: str,
        merchant_id: str,
        terminal_id: str | None,
        thread_scope: str | None,
        prompt: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        existing = self.load_session(
            session_key=session_key,
            merchant_id=merchant_id,
            terminal_id=terminal_id,
            thread_scope=thread_scope,
        )
        session = dict(existing.get("session") or {})
        turns = [dict(turn) for turn in list(existing.get("turns") or []) if isinstance(turn, dict)]

        sources = [str(item) for item in list(payload.get("sources") or []) if _clean_text(item)]
        trace = payload.get("trace") or {}
        if not isinstance(trace, dict):
            trace = {}
        selected_entities = _merge_entities(
            session.get("selected_entities") if isinstance(session.get("selected_entities"), dict) else {},
            _extract_entities_from_sources(sources, terminal_id=terminal_id),
        )
        selected_entities = _merge_entities(selected_entities, _extract_entities_from_trace(trace))
        active_window = _extract_active_window(payload, prior_window=session.get("active_window") if isinstance(session.get("active_window"), dict) else {})
        topics = _detect_topics(prompt, payload)
        turn_id = _clean_text(trace.get("turn_id")) or f"turn_{uuid.uuid4().hex[:12]}"
        turn_index = int(session.get("turn_count") or 0) + 1
        summary_text = _build_turn_summary(prompt, payload)
        created_at = _iso_now()

        turn_record = {
            "turn_id": turn_id,
            "turn_index": turn_index,
            "prompt": _clean_text(prompt),
            "answer": _clean_text(payload.get("answer")),
            "summary": summary_text,
            "sources": sources[:10],
            "follow_ups": [str(item) for item in list(payload.get("follow_ups") or [])[:MAX_FOLLOW_UPS]],
            "selected_entities": selected_entities,
            "active_window": active_window,
            "topics": topics,
            "validation_status": _clean_text(payload.get("validation_status")) or "clean",
            "payload": dict(payload),
            "created_at": created_at,
        }

        updated_session = {
            "session_key": session_key,
            "merchant_id": merchant_id,
            "terminal_id": terminal_id,
            "thread_scope": _clean_text(thread_scope) or "default",
            "selected_entities": selected_entities,
            "active_window": active_window,
            "active_topics": _merge_unique_text(
                topics,
                session.get("active_topics") if isinstance(session.get("active_topics"), list) else [],
                limit=6,
            ),
            "last_evidence_ids": sources[:10],
            "outstanding_follow_ups": [str(item) for item in list(payload.get("follow_ups") or [])[:MAX_FOLLOW_UPS]],
            "verified_facts": _merge_facts(
                session.get("verified_facts") if isinstance(session.get("verified_facts"), list) else [],
                _fact_items(payload),
            ),
            "recent_summaries": _merge_unique_text(
                [summary_text],
                session.get("recent_summaries") if isinstance(session.get("recent_summaries"), list) else [],
                limit=MAX_SUMMARIES,
            ),
            "turn_count": turn_index,
            "last_turn_id": turn_id,
            "last_turn_at": created_at,
        }

        ensure_chat_memory_schema(self.engine)
        with self.engine.begin() as conn:
            collision = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM control_plane_session_turns
                    WHERE turn_id = :turn_id
                    """
                ),
                {"turn_id": turn_record["turn_id"]},
            ).first()
            if collision is not None:
                turn_record["turn_id"] = f"{turn_record['turn_id']}_{uuid.uuid4().hex[:6]}"
                updated_session["last_turn_id"] = turn_record["turn_id"]

            conn.execute(
                text(
                    """
                    UPDATE control_plane_sessions
                    SET merchant_id = :merchant_id,
                        terminal_id = :terminal_id,
                        thread_scope = :thread_scope,
                        state_json = :state_json,
                        updated_at = :updated_at
                    WHERE session_key = :session_key
                    """
                ),
                {
                    "session_key": session_key,
                    "merchant_id": merchant_id,
                    "terminal_id": terminal_id,
                    "thread_scope": _clean_text(thread_scope) or "default",
                    "state_json": _json_dump(updated_session),
                    "updated_at": created_at,
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO control_plane_session_turns (
                        turn_id, session_key, turn_index, prompt, answer_text, summary_text,
                        sources_json, follow_ups_json, selected_entities_json, active_window_json,
                        topics_json, validation_status, payload_json, created_at
                    ) VALUES (
                        :turn_id, :session_key, :turn_index, :prompt, :answer_text, :summary_text,
                        :sources_json, :follow_ups_json, :selected_entities_json, :active_window_json,
                        :topics_json, :validation_status, :payload_json, :created_at
                    )
                    """
                ),
                {
                    "turn_id": turn_record["turn_id"],
                    "session_key": session_key,
                    "turn_index": turn_record["turn_index"],
                    "prompt": turn_record["prompt"],
                    "answer_text": turn_record["answer"],
                    "summary_text": turn_record["summary"],
                    "sources_json": _json_dump(turn_record["sources"]),
                    "follow_ups_json": _json_dump(turn_record["follow_ups"]),
                    "selected_entities_json": _json_dump(turn_record["selected_entities"]),
                    "active_window_json": _json_dump(turn_record["active_window"]),
                    "topics_json": _json_dump(turn_record["topics"]),
                    "validation_status": turn_record["validation_status"],
                    "payload_json": _json_dump(turn_record["payload"]),
                    "created_at": turn_record["created_at"],
                },
            )

            stale_turn_ids = [
                row["turn_id"]
                for row in conn.execute(
                    text(
                        """
                        SELECT turn_id
                        FROM control_plane_session_turns
                        WHERE session_key = :session_key
                        ORDER BY turn_index DESC
                        """
                    ),
                    {"session_key": session_key},
                ).mappings().all()[MAX_STORED_TURNS:]
            ]
            for stale_turn_id in stale_turn_ids:
                conn.execute(
                    text("DELETE FROM control_plane_session_turns WHERE turn_id = :turn_id"),
                    {"turn_id": stale_turn_id},
                )

        turns.append(turn_record)
        turns = turns[-MAX_STORED_TURNS:]
        return {"session": updated_session, "turns": turns}

    def response_memory(self, session_bundle: dict[str, Any], *, prompt: str) -> dict[str, Any]:
        context = self.agent_memory_context(session_bundle, prompt=prompt)
        return {
            "thread_scope": context.get("thread_scope") or "default",
            "turn_count": context.get("turn_count") or 0,
            "selected_entities": context.get("selected_entities") or {},
            "active_window": context.get("active_window") or {},
            "active_topics": context.get("active_topics") or [],
            "last_evidence_ids": context.get("last_evidence_ids") or [],
            "outstanding_follow_ups": context.get("outstanding_follow_ups") or [],
            "verified_facts": context.get("verified_facts") or [],
            "recent_turns": context.get("recent_turns") or [],
            "relevant_memories": context.get("relevant_memories") or [],
        }

    def _rank_relevant_turns(
        self,
        *,
        turns: list[dict[str, Any]],
        prompt: str,
        prompt_tokens: set[str],
        prompt_entities: dict[str, str],
        prompt_topics: list[str],
    ) -> list[dict[str, Any]]:
        if not turns:
            return []

        scored: list[tuple[float, dict[str, Any], list[str]]] = []
        total_turns = len(turns)
        topic_set = {str(item) for item in prompt_topics}
        for idx, turn in enumerate(turns):
            summary = _clean_text(turn.get("summary"))
            if not summary:
                continue
            matches: list[str] = []
            score = 0.0

            turn_tokens = _tokenize(" ".join([turn.get("prompt") or "", summary]))
            token_overlap = len(prompt_tokens & turn_tokens)
            if token_overlap:
                score += token_overlap * 2.0
                matches.append("keyword overlap")

            turn_entities = turn.get("selected_entities") if isinstance(turn.get("selected_entities"), dict) else {}
            entity_overlap = [
                key
                for key, value in prompt_entities.items()
                if _clean_text(value) and _clean_text(turn_entities.get(key)) == _clean_text(value)
            ]
            if entity_overlap:
                score += len(entity_overlap) * 5.0
                matches.append("entity recall")

            turn_topics = {str(item) for item in list(turn.get("topics") or [])}
            if topic_set and topic_set & turn_topics:
                score += len(topic_set & turn_topics) * 2.5
                matches.append("topic overlap")

            if _looks_referential(prompt) and idx >= total_turns - MAX_RECENT_TURNS:
                score += 2.0
                matches.append("recent reference")

            recency_bonus = max(0.0, 3.0 - ((total_turns - 1 - idx) * 0.4))
            score += recency_bonus

            if score <= 0:
                continue

            scored.append((score, turn, matches))

        scored.sort(key=lambda item: item[0], reverse=True)
        relevant: list[dict[str, Any]] = []
        seen_turn_ids: set[str] = set()
        for score, turn, matches in scored:
            turn_id = _clean_text(turn.get("turn_id"))
            if not turn_id or turn_id in seen_turn_ids:
                continue
            seen_turn_ids.add(turn_id)
            relevant.append(
                {
                    "turn_id": turn_id,
                    "summary": _clean_text(turn.get("summary"), limit=200),
                    "selected_entities": dict(turn.get("selected_entities") or {}),
                    "sources": list(turn.get("sources") or [])[:6],
                    "matched_on": matches[:3],
                    "score": round(score, 2),
                }
            )
            if len(relevant) >= MAX_RELEVANT_TURNS:
                break
        return relevant

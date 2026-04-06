from __future__ import annotations

import datetime as dt
import json
import logging
import re
import uuid
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

from app.copilot.toolcalling import default_window_from_max_date, invoke_with_tools, make_tools
from app.copilot.tools import ToolContext
from app.intelligence.chat_reasoning import validate_reasoning_output
from app.intelligence.prompt_loader import load_prompt_section
from config import Config

logger = logging.getLogger("unified_agent")

AGENTS_MD_PATH = Path(__file__).with_name("AGENTS.md")

SYSTEM_PROMPT = """You are Bank Foundry, the single active merchant intelligence runtime.

You decide whether to answer directly, ask a clarifying question, or use tools.
Use tools before making merchant-specific claims about metrics, settlements, payouts,
refunds, chargebacks, terminals, failures, or growth opportunities.
Use memory_context only to resolve follow-up references, not as evidence.

Rules:
- Use only the active merchant scope.
- Prefer the shortest successful tool path.
- Do not invent numbers, dates, evidence IDs, or actions.
- Do not use write tools unless the user explicitly asks for a write operation.
- If the request is casual or out of scope, answer directly without tools.
"""

COMPOSER_PROMPT = """You are the final response composer for Bank Foundry.

Use only the supplied evidence for merchant-specific claims.
Use session memory only to resolve references like "that settlement" or "same period", never as proof.
If evidence is missing or ambiguous, ask one short clarifying question.
When you want a table to be shown, select the exact supporting evidence IDs.
The runtime will hydrate the rows from tool outputs; do not invent table rows.
Never mention internal query mechanics, SQL, table names, or prompt workflow unless the user explicitly asks a technical question.
Lead with the direct merchant-facing conclusion, then the clearest next step.
If support is partial, use caveated language like "appears", "looks", or "based on the current evidence".
Return only JSON with the requested schema.
"""

JSON_REPAIR_PROMPT = """Convert the input into strict JSON for the requested schema.
Return only JSON. No prose. No markdown.
If the input is unusable, return {}.
"""

TABLE_TITLES = {
    "list_transactions": "Transactions",
    "list_settlements": "Settlements",
    "list_chargebacks": "Chargebacks",
    "list_refunds": "Refunds",
    "terminal_performance": "Terminal performance",
    "terminal_health_summary": "Terminal health summary",
}

INTERNAL_MECHANICS_PATTERNS = [
    re.compile(r"\bthe query was adjusted\b", flags=re.IGNORECASE),
    re.compile(r"\busing the correct table\b", flags=re.IGNORECASE),
    re.compile(r"\bcorrect table\b", flags=re.IGNORECASE),
    re.compile(r"\bcorrect(?:ed)? the date range\b", flags=re.IGNORECASE),
    re.compile(r"\bi noticed an issue with the date format\b", flags=re.IGNORECASE),
    re.compile(r"\bquery\b.*\b(table|column|sql|adjusted|rewritten|fixed)\b", flags=re.IGNORECASE),
]

GENERIC_FOLLOW_UP_PATTERNS = [
    re.compile(r"\bshow the evidence\b", flags=re.IGNORECASE),
    re.compile(r"\bshow the exact rows behind this\b", flags=re.IGNORECASE),
    re.compile(r"\bwhat should i do next\b", flags=re.IGNORECASE),
    re.compile(r"\bwhat changed compared to the previous period\b", flags=re.IGNORECASE),
]

SETTLEMENT_TOOL_NAMES = {"list_settlements", "get_settlement_detail", "explain_settlement_shortfall", "cashflow_snapshot"}
TRANSACTION_TOOL_NAMES = {"list_transactions", "get_transaction_detail"}
FAILURE_TOOL_NAMES = {"verify_failure_drivers", "compute_kpis", "compare_kpis"}
TERMINAL_TOOL_NAMES = {"terminal_performance", "terminal_health_summary", "geo_drift_check", "terminal_issue_correlator"}
DISPUTE_TOOL_NAMES = {"list_chargebacks", "get_chargeback_detail", "list_refunds"}

MONTH_NAME_TO_NUMBER = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _prompt(section: str, fallback: str) -> str:
    return load_prompt_section(AGENTS_MD_PATH, section, fallback)


def _composer_llm() -> ChatOllama:
    return ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=float(getattr(Config, "UNIFIED_AGENT_COMPOSER_TEMPERATURE", 0.1)),
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
        if not match:
            return {}
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}


def _repair_json(raw: str) -> dict[str, Any]:
    llm = _composer_llm()
    try:
        repaired = llm.invoke(
            [
                SystemMessage(content=_prompt("unified_agent_json_repair", JSON_REPAIR_PROMPT)),
                HumanMessage(content=str(raw or "")[:12000]),
            ]
        )
    except Exception as exc:
        logger.warning("Unified agent JSON repair failed: %s", exc)
        return {}
    return _extract_json(getattr(repaired, "content", str(repaired)))


def _trim_for_prompt(value: Any, *, depth: int = 0) -> Any:
    if depth >= 4:
        return str(value)
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for idx, (key, item) in enumerate(value.items()):
            if idx >= 20:
                out["__truncated__"] = True
                break
            out[str(key)] = _trim_for_prompt(item, depth=depth + 1)
        return out
    if isinstance(value, list):
        items = [_trim_for_prompt(item, depth=depth + 1) for item in value[:20]]
        if len(value) > 20:
            items.append({"__truncated__": len(value) - 20})
        return items
    if isinstance(value, tuple):
        return _trim_for_prompt(list(value), depth=depth + 1)
    if isinstance(value, str):
        return value[:2000]
    return value


def _scope_payload(merchant_id: str, terminal_id: str | None) -> dict[str, Any]:
    return {
        "merchant_id": merchant_id,
        "terminal_id": terminal_id,
        "level": "terminal" if terminal_id else "merchant",
    }


def _anchor_date_from_window_end(to_date: str) -> dt.date:
    try:
        return dt.date.fromisoformat(str(to_date)) - dt.timedelta(days=1)
    except Exception:
        return dt.date.today()


def _month_window(year: int, month: int) -> tuple[str, str]:
    start = dt.date(year, month, 1)
    if month == 12:
        end = dt.date(year + 1, 1, 1)
    else:
        end = dt.date(year, month + 1, 1)
    return start.isoformat(), end.isoformat()


def _deterministic_time_window(prompt_text: str, *, anchor_to: str) -> dict[str, Any] | None:
    text = str(prompt_text or "").strip()
    if not text:
        return None

    lowered = text.lower()
    anchor = _anchor_date_from_window_end(anchor_to)

    explicit_month_year = re.search(
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\s+(\d{4})\b",
        lowered,
    )
    if explicit_month_year:
        month = MONTH_NAME_TO_NUMBER[explicit_month_year.group(1)]
        year = int(explicit_month_year.group(2))
        start, end = _month_window(year, month)
        return {
            "from_date": start,
            "to_date": end,
            "label": f"{dt.date(year, month, 1):%B %Y}",
            "reason": "explicit_month_year",
            "source_phrase": explicit_month_year.group(0),
        }

    if "this month" in lowered:
        start, end = _month_window(anchor.year, anchor.month)
        return {
            "from_date": start,
            "to_date": end,
            "label": f"{dt.date(anchor.year, anchor.month, 1):%B %Y}",
            "reason": "this_month",
            "source_phrase": "this month",
        }

    if "last month" in lowered:
        last_month_anchor = dt.date(anchor.year, anchor.month, 1) - dt.timedelta(days=1)
        start, end = _month_window(last_month_anchor.year, last_month_anchor.month)
        return {
            "from_date": start,
            "to_date": end,
            "label": f"{dt.date(last_month_anchor.year, last_month_anchor.month, 1):%B %Y}",
            "reason": "last_month",
            "source_phrase": "last month",
        }

    if "today" in lowered:
        start = anchor.isoformat()
        end = (anchor + dt.timedelta(days=1)).isoformat()
        return {
            "from_date": start,
            "to_date": end,
            "label": start,
            "reason": "today",
            "source_phrase": "today",
        }

    if "yesterday" in lowered:
        day = anchor - dt.timedelta(days=1)
        return {
            "from_date": day.isoformat(),
            "to_date": anchor.isoformat(),
            "label": day.isoformat(),
            "reason": "yesterday",
            "source_phrase": "yesterday",
        }

    rolling_days = re.search(r"\blast\s+(\d{1,3})\s+days\b", lowered)
    if rolling_days:
        days = max(1, min(int(rolling_days.group(1)), 365))
        end_date = anchor + dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=days)
        return {
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "label": f"last {days} days",
            "reason": "rolling_days",
            "source_phrase": rolling_days.group(0),
        }

    if "this week" in lowered:
        week_start = anchor - dt.timedelta(days=anchor.weekday())
        return {
            "from_date": week_start.isoformat(),
            "to_date": (anchor + dt.timedelta(days=1)).isoformat(),
            "label": "this week",
            "reason": "this_week",
            "source_phrase": "this week",
        }

    if "last week" in lowered:
        this_week_start = anchor - dt.timedelta(days=anchor.weekday())
        last_week_start = this_week_start - dt.timedelta(days=7)
        return {
            "from_date": last_week_start.isoformat(),
            "to_date": this_week_start.isoformat(),
            "label": "last week",
            "reason": "last_week",
            "source_phrase": "last week",
        }

    month_only = re.search(
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\b",
        lowered,
    )
    if month_only:
        month = MONTH_NAME_TO_NUMBER[month_only.group(1)]
        year = anchor.year if month <= anchor.month else anchor.year - 1
        start, end = _month_window(year, month)
        return {
            "from_date": start,
            "to_date": end,
            "label": f"{dt.date(year, month, 1):%B %Y}",
            "reason": "named_month_inferred_year",
            "source_phrase": month_only.group(0),
        }

    return None


def _window_from_memory_context(prompt_text: str, memory_context: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(memory_context, dict):
        return None
    preferred_window = memory_context.get("preferred_window")
    if not isinstance(preferred_window, dict):
        return None
    from_date = str(preferred_window.get("from_date") or "").strip()
    to_date = str(preferred_window.get("to_date") or "").strip()
    if not from_date or not to_date:
        return None

    lowered = str(prompt_text or "").lower()
    continuation_phrases = (
        "same window",
        "same period",
        "same dates",
        "same range",
        "that window",
        "that period",
        "again",
    )
    if not any(phrase in lowered for phrase in continuation_phrases):
        return None

    return {
        "from_date": from_date,
        "to_date": to_date,
        "label": str(preferred_window.get("label") or "").strip() or "session_memory_window",
        "reason": "session_memory_window",
        "source_phrase": "session memory",
    }


def _collect_sources(tool_results: list[dict[str, Any]]) -> list[str]:
    sources: list[str] = []
    for result in tool_results:
        output = result.get("output")
        if not isinstance(output, dict):
            continue
        for item in list(output.get("evidence") or [])[:80]:
            source = str(item).strip()
            if source and source not in sources:
                sources.append(source)
    return sources


def _tool_results_for_prompt(tool_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for result in tool_results:
        payload.append(
            {
                "tool": str(result.get("tool") or ""),
                "ok": bool(result.get("ok")),
                "error": str(result.get("error") or "") or None,
                "output": _trim_for_prompt(result.get("output")),
            }
        )
    return payload


def _trace_tool_result(result: dict[str, Any]) -> dict[str, Any]:
    output = result.get("output")
    trace_output: dict[str, Any] | None = None
    if isinstance(output, dict):
        trace_output = {}
        for key in ("verified", "row_count", "window", "error_code", "summary", "shortfall"):
            if key in output:
                trace_output[key] = _trim_for_prompt(output.get(key))
        evidence = list(output.get("evidence") or [])[:10]
        if evidence:
            trace_output["evidence"] = evidence
        rows = output.get("rows")
        if isinstance(rows, list):
            trace_output["rows_preview"] = _trim_for_prompt(rows[:5])
            trace_output["row_count"] = int(output.get("row_count") or len(rows))
        row = output.get("row")
        if isinstance(row, dict):
            trace_output["row"] = _trim_for_prompt(row)
        if not trace_output:
            trace_output = _trim_for_prompt(output)
    else:
        trace_output = _trim_for_prompt(output)
    return {
        "tool": str(result.get("tool") or ""),
        "ok": bool(result.get("ok")),
        "error": str(result.get("error") or "") or None,
        "output": trace_output,
    }


def _candidate_structured_results(tool_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for result in tool_results:
        output = result.get("output")
        if not result.get("ok") or not isinstance(output, dict):
            continue

        rows = output.get("rows")
        row = output.get("row")
        normalized_rows: list[dict[str, Any]] = []
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            normalized_rows = [dict(item) for item in rows[:20] if isinstance(item, dict)]
        elif isinstance(row, dict) and row:
            normalized_rows = [dict(row)]

        if not normalized_rows:
            continue

        columns = list(output.get("columns") or normalized_rows[0].keys())[:8]
        candidates.append(
            {
                "title": TABLE_TITLES.get(str(result.get("tool") or ""), "Result"),
                "kind": str(result.get("tool") or "result"),
                "columns": [str(column) for column in columns],
                "rows": normalized_rows,
                "window": output.get("window") if isinstance(output.get("window"), dict) else {},
                "evidence_ids": [str(item).strip() for item in list(output.get("evidence") or []) if str(item).strip()],
            }
        )
    return candidates


def _structured_result_from_tools(
    tool_results: list[dict[str, Any]],
    *,
    evidence_ids: list[str] | None = None,
    preferred_kind: str | None = None,
    fallback_sources: list[str] | None = None,
) -> dict[str, Any] | None:
    candidates = _candidate_structured_results(tool_results)
    if not candidates:
        return None

    selected_evidence = [str(item).strip() for item in list(evidence_ids or []) if str(item).strip()]
    selected_set = set(selected_evidence)
    if not selected_set and fallback_sources and len(candidates) == 1:
        selected_set = {str(item).strip() for item in list(fallback_sources or []) if str(item).strip()}

    if not selected_set:
        return None

    best: tuple[int, int, int, dict[str, Any]] | None = None
    normalized_kind = str(preferred_kind or "").strip().lower()
    for candidate in candidates:
        candidate_evidence = set(candidate.get("evidence_ids") or [])
        overlap = len(selected_set & candidate_evidence)
        if overlap <= 0:
            continue
        kind_bonus = 1 if normalized_kind and str(candidate.get("kind") or "").strip().lower() == normalized_kind else 0
        score = (overlap, kind_bonus, len(candidate.get("rows") or []), candidate)
        if best is None or score[:3] > best[:3]:
            best = score

    if best is None:
        return None

    chosen = dict(best[3])
    chosen["evidence_ids"] = list(chosen.get("evidence_ids") or [])
    return chosen


def _selected_structured_evidence_ids(
    *,
    claims: list[dict[str, Any]],
    action_preview: dict[str, Any] | None,
    structured_hint: dict[str, Any] | None,
) -> list[str]:
    selected: list[str] = []

    for item in list(structured_hint.get("evidence_ids") or []) if isinstance(structured_hint, dict) else []:
        evidence_id = str(item).strip()
        if evidence_id and evidence_id not in selected:
            selected.append(evidence_id)

    for claim in claims:
        for item in list(claim.get("evidence_ids") or []):
            evidence_id = str(item).strip()
            if evidence_id and evidence_id not in selected:
                selected.append(evidence_id)

    for item in list(action_preview.get("evidence_ids") or []) if isinstance(action_preview, dict) else []:
        evidence_id = str(item).strip()
        if evidence_id and evidence_id not in selected:
            selected.append(evidence_id)

    return selected


def _default_follow_ups() -> list[str]:
    return [
        "What changed compared to the previous period?",
        "Show the exact records behind this answer.",
        "What should I do next?",
    ]


def _sanitize_answer_text(answer: str) -> str:
    text = str(answer or "").strip()
    if not text:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    filtered = [
        sentence
        for sentence in sentences
        if sentence and not any(pattern.search(sentence) for pattern in INTERNAL_MECHANICS_PATTERNS)
    ]
    cleaned = " ".join(filtered).strip() if filtered else text
    cleaned = re.sub(r"\s+([,.;!?])", r"\1", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned or text


def _starts_with_caveat(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    return lowered.startswith("based on the current evidence") or lowered.startswith("from the current evidence")


def _with_validation_guardrails(answer: str, validation_status: str) -> str:
    text = str(answer or "").strip()
    if not text or validation_status == "clean":
        return text
    if validation_status == "partial":
        guarded = text if _starts_with_caveat(text) else f"Based on the current evidence, {text[:1].lower() + text[1:] if text[:1].isupper() else text}"
        if "need review" not in guarded.lower():
            guarded = guarded.rstrip(".") + ". Some details still need review."
        return guarded
    guarded = text if _starts_with_caveat(text) else f"Based on the current evidence, {text[:1].lower() + text[1:] if text[:1].isupper() else text}"
    if "directional" not in guarded.lower():
        guarded = guarded.rstrip(".") + ". This is directional and should be checked against the supporting rows."
    return guarded


def _validation_caveats(validation: dict[str, Any]) -> list[str]:
    status = str(validation.get("validation_status") or "clean")
    if status == "clean":
        return []
    caveats: list[str] = []
    if status == "partial":
        caveats.append("Some parts of this answer are directional because not every claim could be fully verified.")
    elif status == "unverified":
        caveats.append("The main conclusion is not fully verified against the available evidence yet.")

    issue_map = {
        "number_not_found_in_evidence": "One numeric detail still needs row-level verification.",
        "unsupported_top_rank_claim": "The ranking language is stronger than the available evidence supports.",
        "unsupported_action_statement": "The recommended action needs stronger supporting evidence.",
        "unknown_evidence_id": "One supporting evidence link could not be matched cleanly.",
    }
    for issue in list(validation.get("validation_issues") or []):
        issue_type = str(issue.get("type") or "").strip()
        caveat = issue_map.get(issue_type)
        if caveat and caveat not in caveats:
            caveats.append(caveat)
        if len(caveats) >= 3:
            break
    return caveats[:3]


def _claim_findings(claims: list[dict[str, Any]], validation: dict[str, Any], answer: str) -> list[str]:
    findings: list[str] = []
    answer_lower = str(answer or "").strip().lower()
    verified_claims = [item for item in list(validation.get("verified_claims") or []) if isinstance(item, dict)]
    candidates = verified_claims or claims
    for claim in candidates:
        text = str(claim.get("text") or "").strip()
        kind = str(claim.get("kind") or "general").strip().lower()
        if not text or text.lower() == answer_lower:
            continue
        if kind in {"action", "general"}:
            continue
        if text not in findings:
            findings.append(text)
        if len(findings) >= 3:
            break
    return findings


def _next_best_action(action_preview: dict[str, Any] | None, validation: dict[str, Any]) -> str | None:
    if isinstance(action_preview, dict):
        for action in list(action_preview.get("actions") or []):
            if isinstance(action, dict):
                text = str(action.get("text") or "").strip()
                if text:
                    return text
        summary = str(action_preview.get("summary") or "").strip()
        if summary:
            return summary
    if str(validation.get("validation_status") or "clean") != "clean":
        return "Review the supporting rows before acting on this conclusion."
    return None


def _build_answer_sections(
    *,
    answer: str,
    claims: list[dict[str, Any]],
    action_preview: dict[str, Any] | None,
    validation: dict[str, Any],
) -> dict[str, Any]:
    return {
        "executive_summary": str(answer or "").strip(),
        "key_findings": _claim_findings(claims, validation, answer),
        "next_best_action": _next_best_action(action_preview, validation),
        "caveats": _validation_caveats(validation),
    }


def _is_generic_follow_up(item: str) -> bool:
    return any(pattern.search(str(item or "")) for pattern in GENERIC_FOLLOW_UP_PATTERNS)


def _intent_follow_up_defaults(*, tool_names: set[str], validation_status: str) -> list[str]:
    follow_ups: list[str] = []
    if validation_status != "clean":
        follow_ups.append("Show the exact rows behind this.")

    if tool_names & SETTLEMENT_TOOL_NAMES:
        follow_ups.extend(
            [
                "Check why any held or pending settlements are delayed.",
                "Show the exact settlement row behind this answer.",
                "Compare the gross amount, deductions, and net payout.",
            ]
        )
    elif tool_names & TRANSACTION_TOOL_NAMES:
        follow_ups.extend(
            [
                "Show the failed transactions in this list.",
                "Break these transactions down by payment mode.",
                "Check whether these large transactions cluster on the same dates.",
            ]
        )
    elif tool_names & FAILURE_TOOL_NAMES:
        follow_ups.extend(
            [
                "Break this down by response code.",
                "Show the terminal-wise contribution to this issue.",
                "Compare this with the previous period.",
            ]
        )
    elif tool_names & TERMINAL_TOOL_NAMES:
        follow_ups.extend(
            [
                "Show the weakest terminals first.",
                "Check whether failures rise when terminal health worsens.",
                "Compare terminal performance by payment mode.",
            ]
        )
    elif tool_names & DISPUTE_TOOL_NAMES:
        follow_ups.extend(
            [
                "Show the highest-value open cases first.",
                "Check which cases are closest to their response deadline.",
                "Compare disputes and refunds over the same window.",
            ]
        )
    else:
        follow_ups.extend(_default_follow_ups())
    return follow_ups


def _select_follow_ups(
    *,
    composed_follow_ups: list[str],
    tool_calls: list[dict[str, Any]],
    validation_status: str,
) -> list[str]:
    tool_names = {str(call.get("name") or "").strip() for call in tool_calls if str(call.get("name") or "").strip()}
    specific = [item for item in composed_follow_ups if not _is_generic_follow_up(item)]
    generic = [item for item in composed_follow_ups if _is_generic_follow_up(item)]
    defaults = _intent_follow_up_defaults(tool_names=tool_names, validation_status=validation_status)

    selected: list[str] = []
    for group in (specific, defaults, generic, _default_follow_ups()):
        for item in group:
            text = str(item or "").strip()
            if text and text not in selected:
                selected.append(text)
            if len(selected) >= 4:
                return selected
    return selected[:4]


def _compose_final_response(
    *,
    prompt: str,
    scope: dict[str, Any],
    history: list[dict[str, Any]],
    tool_calls: list[dict[str, Any]],
    tool_results: list[dict[str, Any]],
    final_text: str,
) -> dict[str, Any]:
    llm = _composer_llm()
    payload = {
        "question": prompt,
        "scope": scope,
        "history": history[-6:],
        "tool_calls": tool_calls,
        "tool_results": _tool_results_for_prompt(tool_results),
        "agent_draft": str(final_text or ""),
        "response_requirements": {
            "follow_ups_max": 4,
            "must_ground_business_claims": True,
            "structured_result_allowed": True,
        },
    }
    try:
        response = llm.invoke(
            [
                SystemMessage(content=_prompt("unified_agent_composer", COMPOSER_PROMPT)),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False, indent=2, default=str)),
            ]
        )
    except Exception as exc:
        logger.warning("Unified agent composer failed: %s", exc)
        return {}

    parsed = _extract_json(getattr(response, "content", str(response)))
    if not parsed:
        parsed = _repair_json(getattr(response, "content", str(response)))
    return parsed if isinstance(parsed, dict) else {}


def _verification_status(tool_calls: list[dict[str, Any]], tool_results: list[dict[str, Any]], sources: list[str], has_clarification: bool) -> str:
    if has_clarification:
        return "Not applicable"
    if not tool_calls:
        return "Not applicable"
    if sources and tool_results and all(bool(result.get("ok")) for result in tool_results):
        return "Verified - grounded in tool evidence"
    if sources:
        return "Partially verified - grounded in available tool evidence"
    if any(bool(result.get("ok")) for result in tool_results):
        return "Insufficient evidence IDs returned by tool outputs"
    return "Tool execution failed"


def run_agent_turn(
    engine: Any,
    *,
    merchant_id: str,
    prompt: str,
    terminal_id: str | None = None,
    history: list[dict[str, Any]] | None = None,
    memory_context: dict[str, Any] | None = None,
    debug: bool = False,
) -> dict[str, Any]:
    prompt_text = str(prompt or "").strip()
    conversation_history = list(history or [])[-6:]
    scope = _scope_payload(merchant_id, terminal_id)
    ctx = ToolContext(engine=engine, merchant_id=merchant_id, terminal_id=terminal_id)
    anchor_from, anchor_to = default_window_from_max_date(engine, merchant_id, days=30, terminal_id=terminal_id)
    normalized_time_window = _deterministic_time_window(prompt_text, anchor_to=anchor_to)
    if normalized_time_window is None:
        normalized_time_window = _window_from_memory_context(prompt_text, memory_context)
    effective_from = str(normalized_time_window.get("from_date") or anchor_from) if normalized_time_window else anchor_from
    effective_to = str(normalized_time_window.get("to_date") or anchor_to) if normalized_time_window else anchor_to
    tools = make_tools(ctx=ctx, default_from=effective_from, default_to=effective_to)

    user_payload = {
        "merchant_id": merchant_id,
        "terminal_id": terminal_id,
        "scope": scope,
        "prompt": prompt_text,
        "history": conversation_history,
        "memory_context": memory_context or {},
        "default_window": {"from_date": effective_from, "to_date": effective_to},
        "data_anchor_window": {"from_date": anchor_from, "to_date": anchor_to},
        "normalized_time_window": normalized_time_window,
        "response_policy": {
            "use_tools_for_data_claims": True,
            "keep_answers_direct": True,
            "ask_for_clarification_if_needed": True,
            "avoid_calendar_clarification_when_window_is_resolved": True,
        },
    }
    max_steps = max(1, min(int(getattr(Config, "UNIFIED_AGENT_MAX_STEPS", 4) or 4), 8))

    # Extract intent early to guide tool selection
    from app.intelligence.chat_reasoning import route_chat_intent
    routed = route_chat_intent(question=prompt_text, scope=scope, history=conversation_history)
    resolved_intent = (routed.get("intent") if routed else "agent_turn") or "agent_turn"

    # Force tool selection for critical paths like lending
    if resolved_intent == "lending_eligibility":
        tools = [t for t in tools if t.name == "get_merchant_lending_offers"]

    tool_calls, tool_results, final_text = invoke_with_tools(
        system=_prompt("unified_agent_system", SYSTEM_PROMPT),
        user=user_payload,
        tools=tools,
        temperature=float(getattr(Config, "UNIFIED_AGENT_TOOL_TEMPERATURE", 0.1)),
        max_steps=max_steps,
    )

    composed = _compose_final_response(
        prompt=prompt_text,
        scope=scope,
        history=conversation_history,
        tool_calls=tool_calls,
        tool_results=tool_results,
        final_text=final_text,
    )

    sources = _collect_sources(tool_results)
    answer = _sanitize_answer_text(str(composed.get("answer") or final_text or "").strip())
    clarifying_question = composed.get("clarifying_question") if isinstance(composed.get("clarifying_question"), dict) else None
    if clarifying_question:
        clarifying_question = {
            "question": str(clarifying_question.get("question") or "").strip(),
            "choices": [str(item).strip() for item in list(clarifying_question.get("choices") or []) if str(item).strip()][:4],
            "reason": str(clarifying_question.get("reason") or "").strip() or "More context would improve the answer.",
        }
        if not clarifying_question["question"]:
            clarifying_question = None
    if clarifying_question and not answer:
        answer = clarifying_question["question"]
    if not answer:
        answer = "I could not ground a reliable answer from the available evidence yet."

    action_preview = composed.get("action_preview") if isinstance(composed.get("action_preview"), dict) else None
    structured_hint = composed.get("structured_result") if isinstance(composed.get("structured_result"), dict) else None
    claims = [item for item in list(composed.get("claims") or []) if isinstance(item, dict)]
    structured_evidence_ids = _selected_structured_evidence_ids(
        claims=claims,
        action_preview=action_preview,
        structured_hint=structured_hint,
    )
    structured_result = _structured_result_from_tools(
        tool_results,
        evidence_ids=structured_evidence_ids,
        preferred_kind=str(structured_hint.get("kind") or "") if isinstance(structured_hint, dict) else None,
        fallback_sources=sources,
    )
    if clarifying_question:
        validation = {
            "verification_summary": "No claim-level validation was needed.",
            "validation_status": "clean",
            "validation_issues": [],
            "display_notice": None,
        }
    else:
        validation = validate_reasoning_output(
            answer_payload={"claims": claims},
            evidence_package={"tool_results": [result.get("output") for result in tool_results if result.get("ok")], "scope": scope},
            sources=sources,
            ranking_candidates=[],
            action_candidates=[str(item.get("text") or "") for item in list(action_preview.get("actions") or [])] if isinstance(action_preview, dict) else [],
        )
        if not claims and not sources:
            validation = {
                "verification_summary": "No claim-level validation was needed.",
                "validation_status": "clean",
                "validation_issues": [],
                "display_notice": None,
            }

    answer = _with_validation_guardrails(answer, str(validation.get("validation_status") or "clean"))
    follow_ups = (
        []
        if clarifying_question
        else _select_follow_ups(
            composed_follow_ups=[str(item).strip() for item in list(composed.get("follow_ups") or []) if str(item).strip()],
            tool_calls=tool_calls,
            validation_status=str(validation.get("validation_status") or "clean"),
        )
    )
    answer_sections = _build_answer_sections(
        answer=answer,
        claims=claims,
        action_preview=action_preview,
        validation=validation,
    )

    trace = {
        "turn_id": f"turn_{uuid.uuid4().hex[:12]}",
        "scope": scope,
        "memory_context": _trim_for_prompt(memory_context or {}),
        "default_window": {"from_date": effective_from, "to_date": effective_to},
        "data_anchor_window": {"from_date": anchor_from, "to_date": anchor_to},
        "normalized_time_window": normalized_time_window,
        "max_steps": max_steps,
        "tool_calls": [{"name": str(call.get("name") or ""), "args": call.get("args") or {}} for call in tool_calls],
        "tool_results": [_trace_tool_result(result) for result in tool_results],
        "evidence_ids": sources,
        "plan_summary": str(composed.get("plan_summary") or "").strip() or None,
        "agent_draft": str(final_text or "").strip() or None,
        "final_state": "clarify" if clarifying_question else ("completed" if answer else "incomplete"),
    }

    # Intent was routed before tool execution
    payload = {
        "answer": answer,
        "verification_status": _verification_status(tool_calls, tool_results, sources, clarifying_question is not None),
        "verification_summary": str(validation.get("verification_summary") or "No claim-level validation was needed."),
        "validation_status": str(validation.get("validation_status") or "clean"),
        "validation_issues": list(validation.get("validation_issues") or []),
        "display_notice": validation.get("display_notice") if isinstance(validation.get("display_notice"), dict) else None,
        "sources": sources,
        "structured_result": structured_result,
        "answer_sections": answer_sections,
        "follow_ups": follow_ups,
        "action_preview": action_preview,
        "clarifying_question": clarifying_question,
        "scope": scope,
        "answer_source": "clarifying_question" if clarifying_question else "agent",
        "intent": resolved_intent,
        "trace": trace,
    }
    if debug:
        payload["debug"] = trace
    return payload

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
from pathlib import Path
import re
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

from app.intelligence.prompt_loader import load_prompt_section
from config import Config
from .md import load_agent_docs
from .sql_graph_agent import run_sql_langgraph
from .types import CopilotTurn, ToolCall, ToolResult
from app.copilot.toolcalling import default_window_from_max_date, invoke_with_tools, make_tools
from app.copilot.tools import (
    ToolContext,
    compute_kpis,
    explain_settlement_shortfall,
    get_chargeback_detail,
    get_merchant_context,
    get_settlement_detail,
    get_transaction_detail,
    list_chargebacks,
    list_refunds,
    list_settlements,
    list_transactions,
    propose_and_create_merchant_action,
    compare_kpis,
    sql_database,
    terminal_performance,
    verify_failure_drivers,
    end_to_end_analysis,
)

logger = logging.getLogger("copilot_runtime")

ROOT_AGENTS_MD_PATH = Path(__file__).resolve().parents[3] / "AGENTS.md"

GLOBAL_EXPERIMENTAL_SYSTEM_FALLBACK = """You are AcquiGuru running in global experimental mode.
You are the only active reasoning agent.

You can decide tool usage freely and in multiple steps:
- sql_database: query merchant transaction data
- knowledge_base: retrieve payment domain knowledge and external intelligence
- merchant_profile: read merchant context
- startup_kpis: read one-time startup KPI snapshot and bootstrap brief

Rules:
- Choose tools based on need; do not force tool use for simple greetings.
- Never invent numbers.
- Cite concrete values when available.
- Keep answers concise, practical, and merchant-friendly.
- When uncertain, run a tool call instead of guessing.
"""

GLOBAL_BOOTSTRAP_FALLBACK = """You are AcquiGuru startup analyst.
Summarize the merchant profile and KPI snapshot into a short operating brief.

Rules:
- Use only provided numbers.
- No invented metrics.
- Keep to 5-8 concise bullets.
- Highlight risk, growth, and immediate priorities.
"""

OPERATIONS_LANE_FALLBACK = """You are the Operations lane.
Focus on settlement deductions, disputes, chargebacks, and operational remediation.

Rules:
- Use evidence only.
- Do not claim verified without verified tool output.
- Include verification status and evidence IDs.
"""

GROWTH_LANE_FALLBACK = """You are the Growth lane.
Focus on acceptance lift, revenue opportunities, and actionable nudges.

Rules:
- Use evidence only.
- Do not claim verified without verified tool output.
- Include verification status and evidence IDs.
"""

NO_OP_SUMMARY = "No operational action was requested in this turn."
NO_OP_DEDUCTION = "No payout shortfall analysis requested yet."
NO_GROWTH_SUMMARY = "No growth optimization ask was requested in this turn."

LANE_TOOL_ALLOWLIST: dict[str, set[str]] = {
    "operations": {
        "startup_kpis",
        "compute_kpis",
        "compare_kpis",
        "verify_failure_drivers",
        "cashflow_snapshot",
        "list_settlements",
        "get_settlement_detail",
        "explain_settlement_shortfall",
        "list_chargebacks",
        "get_chargeback_detail",
        "list_refunds",
        "sql_database",
        "propose_and_create_merchant_action",
        "knowledge_base",
        "kb_search",
    },
    "growth": {
        "startup_kpis",
        "compute_kpis",
        "compare_kpis",
        "verify_failure_drivers",
        "terminal_performance",
        "terminal_health_summary",
        "terminal_issue_correlator",
        "end_to_end_analysis",
        "assess_credit_fit",
        "knowledge_base",
        "kb_search",
        "sql_database",
    },
}


def _iso(d: dt.date) -> str:
    return d.isoformat()


def _default_window(engine: Any, merchant_id: str, days: int = 30) -> tuple[str, str]:
    """Backward-compatible shim.

    Kept because other code may import it, but the tool-calling path now uses
    default_window_from_max_date() from toolcalling.py.
    """
    return default_window_from_max_date(engine, merchant_id, days=days)


def _global_experimental_system_prompt() -> str:
    return load_prompt_section(
        ROOT_AGENTS_MD_PATH,
        "global_experimental_system",
        GLOBAL_EXPERIMENTAL_SYSTEM_FALLBACK,
    )


def _global_bootstrap_prompt() -> str:
    return load_prompt_section(
        ROOT_AGENTS_MD_PATH,
        "global_experimental_bootstrap",
        GLOBAL_BOOTSTRAP_FALLBACK,
    )


def _operations_lane_prompt() -> str:
    return load_prompt_section(
        ROOT_AGENTS_MD_PATH,
        "global_operations_lane_system",
        OPERATIONS_LANE_FALLBACK,
    )


def _growth_lane_prompt() -> str:
    return load_prompt_section(
        ROOT_AGENTS_MD_PATH,
        "global_growth_lane_system",
        GROWTH_LANE_FALLBACK,
    )


def _sanitize_answer_text(text: str) -> str:
    clean = (text or "").strip()
    if not clean:
        return ""
    # Strip all internal reasoning/knowledge tags
    clean = re.sub(r"<[^>]+>[\s\S]*?</[^>]+>", "", clean).strip()
    clean = re.sub(r"<[^>]+>", "", clean).strip()
    
    banned = re.compile(r"\b(fraud_rules|block_list)\b", re.IGNORECASE)
    lines = [ln for ln in clean.splitlines() if not banned.search(ln)]
    return "\n".join(lines).strip()


def _extract_allowed_evidence(tool_results: list[ToolResult]) -> list[str]:
    allowed: list[str] = []
    for r in tool_results:
        if r.ok and isinstance(r.output, dict):
            ev = r.output.get("evidence")
            if isinstance(ev, list):
                for x in ev:
                    s = str(x)
                    if s and s not in allowed:
                        allowed.append(s)
    return allowed


def _has_terminal_failed_gmv(tool_results: list[ToolResult]) -> bool:
    """Only allow terminal mentions if a tool computed failed GMV by terminal."""
    for r in tool_results:
        if not (r.ok and isinstance(r.output, dict)):
            continue
        out = r.output
        # direct keys
        for k in ("by_terminal", "terminal_failures", "terminal_failed_gmv", "failed_gmv_by_terminal"):
            if k in out:
                return True
        # nested dicts (e.g., end_to_end_analysis output sections)
        for v in out.values():
            if isinstance(v, dict):
                for k in ("by_terminal", "terminal_failures", "terminal_failed_gmv", "failed_gmv_by_terminal"):
                    if k in v:
                        return True
    return False


def _strip_terminal_lines(text: str) -> str:
    lines: list[str] = []
    for ln in (text or "").splitlines():
        if "terminal" in ln.lower():
            continue
        lines.append(ln)
    return "\n".join(lines).strip()


def _replace_evidence_section(text: str, allowed_evidence: list[str]) -> str:
    if not text:
        return text

    # Locate an "Evidence" header line.
    lines = text.splitlines()
    idx = None
    for i, ln in enumerate(lines):
        if re.match(r"^\s*evidence\s*$", ln.strip(), flags=re.IGNORECASE):
            idx = i
            break

    ev_lines = [f"- {e}" for e in allowed_evidence] if allowed_evidence else ["- (no evidence IDs returned)"]

    if idx is None:
        return (text + "\n\nEvidence\n" + "\n".join(ev_lines)).strip()

    # End evidence section at the next header-ish line.
    end = len(lines)
    for j in range(idx + 1, len(lines)):
        if re.match(r"^\s*(next actions|next steps|tool trace|what i checked)\b", lines[j].strip(), flags=re.IGNORECASE):
            end = j
            break

    new_lines: list[str] = []
    new_lines.extend(lines[: idx + 1])
    new_lines.extend(ev_lines)
    new_lines.extend(lines[end:])
    return "\n".join(new_lines).strip()


def _postprocess_answer_text(text: str, tool_results: list[ToolResult]) -> str:
    out = _sanitize_answer_text(text)

    # Deterministic evidence handling: only show evidence IDs returned by tools.
    allowed_evidence = _extract_allowed_evidence(tool_results)
    out = _replace_evidence_section(out, allowed_evidence)

    # Deterministic relevance: drop terminal mentions unless terminal failed GMV is present.
    if not _has_terminal_failed_gmv(tool_results):
        out = _strip_terminal_lines(out)

    return out


def _is_driver_ranking_query(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False
    direct_patterns = (
        "top failure driver",
        "top drivers",
        "failure driver",
        "root cause",
        "verify failures",
        "verification",
        "what failed most",
        "top reason",
        "rank failures",
    )
    if any(p in q for p in direct_patterns):
        return True
    return bool(re.search(r"\b(top|main|primary)\b.*\b(driver|reason|failure)\b", q))


def _extract_verification_payload(tool_results: list[ToolResult]) -> dict[str, Any] | None:
    for r in tool_results:
        if not (r.ok and r.name == "verify_failure_drivers" and isinstance(r.output, dict)):
            continue
        return r.output
    return None


def _has_directional_failure_support(tool_results: list[ToolResult]) -> bool:
    for r in tool_results:
        if not (r.ok and isinstance(r.output, dict)):
            continue
        out = r.output
        if r.name == "sql_langgraph_agent":
            if bool(out.get("directional_failure_support")) or bool(out.get("directional_support")):
                return True
        if r.name == "startup_kpis":
            snap = out.get("kpi_snapshot") if isinstance(out.get("kpi_snapshot"), dict) else {}
            if int(snap.get("fail_txns") or 0) > 0 or float(snap.get("failed_gmv") or 0.0) > 0:
                return True
            modes = out.get("kpi_by_mode")
            if isinstance(modes, list):
                for row in modes:
                    if not isinstance(row, dict):
                        continue
                    if int(row.get("fail_txns") or 0) > 0 or float(row.get("failed_gmv") or 0.0) > 0:
                        return True
        if r.name == "compute_kpis":
            rows = out.get("rows")
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    if int(row.get("fail_txns") or 0) > 0 or float(row.get("failed_gmv") or 0.0) > 0:
                        return True
    return False


def _resolve_verification_state(question: str, tool_results: list[ToolResult]) -> tuple[str | None, str]:
    verification_relevant = _is_driver_ranking_query(question) or _extract_verification_payload(tool_results) is not None
    if not verification_relevant:
        return None, ""

    payload = _extract_verification_payload(tool_results)
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if bool(payload.get("verified")) and isinstance(rows, list) and len(rows) > 0:
            return "VERIFIED", "deterministic failure driver ranking succeeded"

    if _has_directional_failure_support(tool_results):
        reason = "verification unavailable; directional support from KPI aggregates"
        if isinstance(payload, dict) and payload.get("error"):
            reason = f"verification failed ({str(payload.get('error'))[:100]})"
        return "UNVERIFIED_SUPPORTED", reason

    if isinstance(payload, dict) and payload.get("error"):
        return "INSUFFICIENT_EVIDENCE", f"verification failed ({str(payload.get('error'))[:100]})"
    return "INSUFFICIENT_EVIDENCE", "no verification or directional failure evidence was returned"


def _upsert_status_line(text: str, prefix: str, value: str) -> str:
    line = f"{prefix}: {value}".strip()
    lines = (text or "").splitlines()
    for i, ln in enumerate(lines):
        if ln.strip().lower().startswith(prefix.lower() + ":"):
            lines[i] = line
            return "\n".join(lines).strip()
    return ((text or "").rstrip() + ("\n\n" if (text or "").strip() else "") + line).strip()


def _downgrade_unverified_claims(text: str) -> str:
    out = text or ""
    replacements = [
        (r"(?i)\btop failure drivers?\b", "likely failure concentration"),
        (r"(?i)\btop drivers?\b", "likely concentration"),
        (r"(?i)\bverified\b", "unverified"),
        (r"(?i)\broot cause\b", "likely contributor"),
    ]
    for pattern, repl in replacements:
        out = re.sub(pattern, repl, out)
    return out


def _collect_evidence_ids(tool_results: list[ToolResult]) -> list[str]:
    primary: list[str] = []
    secondary: list[str] = []

    for r in tool_results:
        if not (r.ok and isinstance(r.output, dict)):
            continue
        ev = r.output.get("evidence")
        if not isinstance(ev, list):
            continue
        bucket = primary if r.name == "verify_failure_drivers" else secondary
        for x in ev:
            s = str(x)
            if s and s not in bucket:
                bucket.append(s)

    out: list[str] = []
    for e in primary + secondary:
        if e not in out:
            out.append(e)
    return out


def _route_lanes(question: str) -> tuple[str, str]:
    q = (question or "").lower()
    if _is_driver_ranking_query(question):
        return "growth", ""
    ops_terms = (
        "settlement",
        "payout",
        "deduct",
        "shortfall",
        "chargeback",
        "dispute",
        "refund",
        "reconciliation",
        "expected",
        "got",
    )
    growth_terms = (
        "growth",
        "increase",
        "uplift",
        "revenue",
        "acceptance",
        "dcc",
        "international",
        "terminal",
        "card",
        "pos",
    )
    ops_score = sum(1 for t in ops_terms if t in q)
    growth_score = sum(1 for t in growth_terms if t in q)
    if ops_score == 0 and growth_score == 0:
        return "operations", ""
    if ops_score > 0 and growth_score == 0:
        return "operations", ""
    if growth_score > 0 and ops_score == 0:
        return "growth", ""
    primary = "operations" if ops_score >= growth_score else "growth"
    secondary = "growth" if primary == "operations" else "operations"
    return primary, secondary


def _normalize_brief_prompt(question: str) -> str:
    text = re.sub(r"[^\w\s]", " ", str(question or "").lower())
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_smalltalk_prompt(question: str) -> bool:
    return _normalize_brief_prompt(question) in {"hi", "hello", "hey", "thanks", "thank you"}


def _smalltalk_answer(*, lane: str | None) -> str:
    if lane == "growth":
        return "Hi. I can help with acceptance, failure reduction, terminals, and growth opportunities."
    if lane == "operations":
        return "Hi. I can help with settlements, payouts, chargebacks, refunds, and reconciliation."
    return "Hi. I can help with your merchant payments, settlements, failures, and growth opportunities."


def _is_out_of_scope_prompt(question: str) -> bool:
    q = _normalize_brief_prompt(question)
    if not q:
        return False

    merchant_terms = (
        "merchant",
        "business",
        "payment",
        "payments",
        "transaction",
        "transactions",
        "settlement",
        "settlements",
        "payout",
        "payouts",
        "chargeback",
        "chargebacks",
        "refund",
        "refunds",
        "reconciliation",
        "gmv",
        "revenue",
        "success rate",
        "failure",
        "failures",
        "terminal",
        "pos",
        "upi",
        "card",
        "dcc",
        "acceptance",
        "peak hours",
        "ticket size",
        "average ticket",
        "qr",
        "growth",
        "my business",
    )
    if any(term in q for term in merchant_terms):
        return False

    unrelated_terms = (
        "weather",
        "temperature",
        "rain",
        "cricket",
        "football",
        "match",
        "score",
        "capital of",
        "president",
        "prime minister",
        "recipe",
        "cook",
        "poem",
        "story",
        "joke",
        "movie",
        "song",
        "lyrics",
        "translate",
        "translation",
        "python",
        "javascript",
        "leetcode",
        "bug in my code",
        "programming",
        "sports",
    )
    if any(term in q for term in unrelated_terms):
        return True

    broad_general_patterns = (
        "what is ",
        "who is ",
        "tell me about ",
        "write ",
        "explain ",
    )
    return any(q.startswith(pattern) for pattern in broad_general_patterns)


def _out_of_scope_answer(*, lane: str | None) -> str:
    if lane == "growth":
        return "I'm focused on merchant growth and acceptance data here. Ask about failures, terminals, payment modes, or revenue opportunities."
    if lane == "operations":
        return "I'm focused on merchant operations data here. Ask about settlements, payouts, chargebacks, refunds, or reconciliation."
    return "I'm focused on your merchant data here. Ask about transactions, settlements, chargebacks, refunds, failures, terminals, or growth opportunities."


def _is_broad_overview_question(question: str, lane: str, terminal_id: str | None = None) -> bool:
    q = (question or "").strip().lower()
    if not q or terminal_id:
        return False
    narrow_terms = (
        "expected",
        "got",
        "shortfall",
        "settlement",
        "chargeback",
        "refund",
        "reconciliation",
        "transaction",
        "terminal",
        "this terminal",
        "detail",
        "settlement_id",
        "chargeback_id",
        "tx_id",
    )
    if any(term in q for term in narrow_terms):
        return False
    broad_terms = (
        "overview",
        "summary",
        "operating brief",
        "what should i focus on",
        "review performance",
        "top growth opportunities",
        "growth opportunities",
        "how is my business doing",
        "what is my business",
        "who am i",
    )
    if any(term in q for term in broad_terms):
        return True
    return lane == "growth" and ("growth" in q or "revenue" in q)


def _is_shortfall_question(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False
    payout_terms = ("settlement", "payout", "shortfall", "deduct", "received", "got")
    return ("expected" in q or "should have received" in q or "supposed to get" in q) and any(term in q for term in payout_terms)


_MONEY_TOKEN_RE = re.compile(r"(?i)(?:rs\.?|₹)?\s*([0-9][0-9,]*(?:\.\d+)?(?:\s*(?:k|l|lac|lakh|cr|crore))?)")


def _parse_money_token(token: str) -> float | None:
    raw = str(token or "").strip().lower().replace(",", "")
    if not raw:
        return None
    multiplier = 1.0
    for suffix, value in (("crore", 10000000.0), ("cr", 10000000.0), ("lakh", 100000.0), ("lac", 100000.0), ("l", 100000.0), ("k", 1000.0)):
        if raw.endswith(suffix):
            raw = raw[: -len(suffix)].strip()
            multiplier = value
            break
    try:
        return float(raw) * multiplier
    except Exception:
        return None


def _extract_shortfall_amounts(question: str) -> tuple[float | None, float | None]:
    tokens = [_parse_money_token(m.group(1)) for m in _MONEY_TOKEN_RE.finditer(question or "")]
    amounts = [amount for amount in tokens if amount is not None]
    if len(amounts) >= 2:
        return amounts[0], amounts[1]
    return None, None


def _lane_step_budget(question: str, lane: str, cap: int) -> int:
    q = (question or "").strip().lower()
    budget = 2
    deep_terms = (
        "compare",
        " vs ",
        " versus ",
        "previous",
        "end to end",
        "drilldown",
        "deep analysis",
        "full analysis",
        "impact on net settlement",
    )
    if any(term in q for term in deep_terms):
        budget = 4
    elif lane == "operations" and any(term in q for term in ("expected", "got", "shortfall", "deduct", "settlement")):
        budget = 3
    elif lane == "growth" and any(term in q for term in ("top growth opportunities", "acceptance", "card", "upi", "terminal")):
        budget = 2
    return max(1, min(cap, budget))


def _filter_tools_for_lane(tools: list[Any], lane: str) -> list[Any]:
    allow = LANE_TOOL_ALLOWLIST.get(lane, set())
    filtered = [t for t in tools if getattr(t, "name", "") in allow]
    return filtered or tools


def _flatten_text_values(value: Any) -> list[str]:
    out: list[str] = []
    if value is None:
        return out
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        for v in value.values():
            out.extend(_flatten_text_values(v))
        return out
    if isinstance(value, (list, tuple)):
        for v in value:
            out.extend(_flatten_text_values(v))
        return out
    return [str(value)]


def _contains_terms(value: Any, terms: tuple[str, ...]) -> bool:
    texts = _flatten_text_values(value)
    for t in texts:
        tl = t.lower()
        if any(term in tl for term in terms):
            return True
    return False


def _has_nonempty_rows(output: dict[str, Any], key: str = "rows") -> bool:
    rows = output.get(key)
    return isinstance(rows, list) and len(rows) > 0


def _has_operations_support(tool_results: list[ToolResult]) -> tuple[bool, bool]:
    verified = False
    directional = False
    ops_tools = {
        "list_settlements",
        "get_settlement_detail",
        "list_chargebacks",
        "get_chargeback_detail",
        "list_refunds",
        "cashflow_snapshot",
        "propose_and_create_merchant_action",
    }
    for r in tool_results:
        if not (r.ok and isinstance(r.output, dict)):
            continue
        out = r.output
        if r.name == "sql_langgraph_agent" and str(out.get("lane") or "") == "operations":
            if bool(out.get("verified")) and int(out.get("row_count") or 0) > 0:
                verified = True
            if bool(out.get("directional_support")) or int(out.get("row_count") or 0) > 0:
                directional = True
        if r.name == "explain_settlement_shortfall":
            if bool(out.get("verified")) and isinstance(out.get("shortfall"), dict):
                verified = True
            if bool(out.get("directional_support")) or isinstance(out.get("shortfall"), dict):
                directional = True
        if r.name in ops_tools:
            if _has_nonempty_rows(out, "rows") or out.get("row") or out.get("reconciliation") or out.get("past_expected"):
                verified = True
        if r.name in {"startup_kpis", "compute_kpis"} and _has_directional_failure_support([r]):
            directional = True
        if r.name == "cashflow_snapshot":
            directional = True
    return verified, (directional or verified)


def _resolve_growth_state(question: str, tool_results: list[ToolResult]) -> tuple[str, str]:
    for r in tool_results:
        if r.name == "sql_langgraph_agent" and r.ok and isinstance(r.output, dict):
            out = r.output
            if str(out.get("lane") or "") == "growth":
                if bool(out.get("verified")) and int(out.get("row_count") or 0) > 0:
                    return "VERIFIED", "langgraph SQL pipeline succeeded"
                if bool(out.get("directional_support")):
                    return "UNVERIFIED_SUPPORTED", "directional support from langgraph SQL evidence"

    payload = _extract_verification_payload(tool_results)
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if bool(payload.get("verified")) and isinstance(rows, list) and len(rows) > 0:
            return "VERIFIED", "deterministic failure driver ranking succeeded"

    if _has_directional_failure_support(tool_results):
        reason = "verification unavailable; directional support from KPI aggregates"
        if isinstance(payload, dict) and payload.get("error"):
            reason = f"verification failed ({str(payload.get('error'))[:100]})"
        return "UNVERIFIED_SUPPORTED", reason

    if _is_driver_ranking_query(question):
        if isinstance(payload, dict) and payload.get("error"):
            return "INSUFFICIENT_EVIDENCE", f"verification failed ({str(payload.get('error'))[:100]})"
        return "INSUFFICIENT_EVIDENCE", "no verification or directional failure evidence was returned"

    return "INSUFFICIENT_EVIDENCE", "no growth evidence was returned"


def _resolve_operations_state(tool_results: list[ToolResult]) -> tuple[str, str]:
    for r in tool_results:
        if r.name == "explain_settlement_shortfall" and r.ok and isinstance(r.output, dict):
            out = r.output
            if bool(out.get("verified")) and isinstance(out.get("shortfall"), dict):
                return "VERIFIED", "deterministic payout shortfall attribution succeeded"
            if bool(out.get("directional_support")) and isinstance(out.get("shortfall"), dict):
                return "UNVERIFIED_SUPPORTED", "shortfall was detected but not fully reconciled to named deductions"
    verified, directional = _has_operations_support(tool_results)
    if verified:
        return "VERIFIED", "deterministic settlement/dispute evidence was returned"
    if directional:
        return "UNVERIFIED_SUPPORTED", "partial support available from KPI/cashflow aggregates"
    return "INSUFFICIENT_EVIDENCE", "no settlement/dispute evidence was returned"


def _human_status(state: str, reason: str) -> str:
    if state == "VERIFIED":
        return f"Verified - {reason}"
    if state == "UNVERIFIED_SUPPORTED":
        return f"Unverified (supported) - {reason}"
    return f"Insufficient evidence - {reason}"


def _extract_action_preview_token(tool_results: list[ToolResult]) -> str | None:
    for r in tool_results:
        if r.name != "propose_and_create_merchant_action":
            continue
        if r.ok and isinstance(r.output, dict):
            token = r.output.get("confirmation_token")
            if token:
                return str(token)
    return None


def _find_first_output(tool_results: list[ToolResult], tool_name: str) -> dict[str, Any] | None:
    for r in tool_results:
        if r.name == tool_name and r.ok and isinstance(r.output, dict):
            return r.output
    return None


def _operations_deduction_explanation(tool_results: list[ToolResult]) -> str:
    shortfall = _find_first_output(tool_results, "explain_settlement_shortfall") or {}
    if isinstance(shortfall.get("deduction_explanation"), str) and shortfall.get("deduction_explanation"):
        return str(shortfall.get("deduction_explanation"))

    cash = _find_first_output(tool_results, "cashflow_snapshot") or {}
    past = cash.get("past_expected") if isinstance(cash.get("past_expected"), dict) else {}
    by_status = cash.get("by_status") if isinstance(cash.get("by_status"), list) else []
    if past and int(past.get("past_expected_count") or 0) > 0:
        return (
            f"{int(past.get('past_expected_count') or 0)} settlement(s) are past expected date, "
            f"amount {past.get('past_expected_amount') or 0}; this can explain payout shortfall timing."
        )
    if by_status:
        top = by_status[0] if isinstance(by_status[0], dict) else {}
        status = str(top.get("status") or "UNKNOWN")
        count = int(top.get("count") or 0)
        amount = top.get("amount") or 0
        return f"Top settlement bucket is {status} ({count} record(s), amount {amount}); shortfall may be tied to this bucket."

    settlements = _find_first_output(tool_results, "list_settlements") or {}
    rows = settlements.get("rows") if isinstance(settlements.get("rows"), list) else []
    if rows:
        first = rows[0] if isinstance(rows[0], dict) else {}
        sid = first.get("settlement_id") or "(unknown)"
        status = first.get("status") or "UNKNOWN"
        amount = first.get("amount_rupees") or 0
        return f"Latest settlement {sid} is in status {status} for amount {amount}; review this record for deduction details."

    return "Detailed deduction breakdown was not computed from current tools; run settlement detail drilldown for the impacted payout."


def _extract_lines(text: str, limit: int = 3) -> list[str]:
    out: list[str] = []
    for ln in (text or "").splitlines():
        s = ln.strip().lstrip("-").strip()
        if not s:
            continue
        if len(s) < 8:
            continue
        if s.lower().startswith(("verification status", "evidence ids", "operations", "growth")):
            continue
        out.append(s)
        if len(out) >= limit:
            break
    return out


def _operations_recommended_actions(question: str, tool_results: list[ToolResult], summary: str) -> list[str]:
    shortfall = _find_first_output(tool_results, "explain_settlement_shortfall") or {}
    if isinstance(shortfall.get("recommended_actions"), list) and shortfall.get("recommended_actions"):
        return [str(x) for x in shortfall.get("recommended_actions")[:3]]

    actions = _extract_lines(summary, limit=3)
    if actions:
        return actions

    out: list[str] = []
    chargebacks = _find_first_output(tool_results, "list_chargebacks") or {}
    cb_rows = chargebacks.get("rows") if isinstance(chargebacks.get("rows"), list) else []
    if cb_rows:
        out.append("Review open chargebacks and prepare representment packet for high-value cases.")
    cash = _find_first_output(tool_results, "cashflow_snapshot") or {}
    past = cash.get("past_expected") if isinstance(cash.get("past_expected"), dict) else {}
    if int(past.get("past_expected_count") or 0) > 0:
        out.append("Raise a settlement investigation request for past-expected payouts.")
    if "got" in (question or "").lower() and "expected" in (question or "").lower():
        out.append("Use get_settlement_detail on the impacted settlement_id to isolate fee, hold, or dispute deductions.")
    if not out:
        out.append("Run cashflow_snapshot and list_settlements for the impacted date to isolate deduction components.")
    return out[:3]


def _growth_nudges(question: str, tool_results: list[ToolResult], summary: str) -> list[str]:
    nudges = _extract_lines(summary, limit=3)

    has_card_failure = False
    for r in tool_results:
        if not (r.ok and isinstance(r.output, dict)):
            continue
        out = r.output
        if r.name == "startup_kpis":
            for row in out.get("kpi_by_mode") or []:
                if isinstance(row, dict) and str(row.get("bucket") or "").upper() == "CARD" and int(row.get("fail_txns") or 0) > 0:
                    has_card_failure = True
        if r.name == "verify_failure_drivers":
            for row in out.get("rows") or []:
                if isinstance(row, dict) and str(row.get("driver") or "").upper() == "CARD" and int(row.get("failed_txns") or 0) > 0:
                    has_card_failure = True

    has_dcc_or_international_evidence = any(
        _contains_terms(r.output, ("dcc", "international")) for r in tool_results if r.ok
    )

    filtered: list[str] = []
    for n in nudges:
        lower = n.lower()
        if "dcc" in lower and not (has_card_failure and has_dcc_or_international_evidence):
            continue
        filtered.append(n)

    if not filtered:
        filtered = [
            "Prioritize CARD decline reduction and retry/routing optimization for high-failure buckets.",
            "Track terminal-level acceptance variance and consider adding a backup POS if throughput is concentrated.",
        ]

    if has_card_failure and has_dcc_or_international_evidence:
        if not any("dcc" in n.lower() for n in filtered):
            filtered.append("Card failures plus international evidence detected; evaluate enabling DCC on eligible POS devices.")

    return filtered[:3]


def _section_summary(text: str, fallback: str) -> str:
    clean = _sanitize_answer_text(text or "").strip()
    return clean if clean else fallback


def _as_markdown_list(items: list[str]) -> str:
    if not items:
        return "- (none)"
    return "\n".join(f"- {x}" for x in items[:3])


def _strip_markdown_inline(text: str) -> str:
    clean = re.sub(r"[*_`#]+", "", str(text or ""))
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip(" :;-")


def _ensure_sentence(text: str) -> str:
    clean = _strip_markdown_inline(text)
    if not clean:
        return ""
    return clean if clean[-1] in ".!?" else f"{clean}."


def _lowercase_lead(text: str) -> str:
    clean = _strip_markdown_inline(text)
    if len(clean) >= 2 and clean[0].isalpha() and clean[1].islower():
        return clean[0].lower() + clean[1:]
    return clean


def _dedupe_strings(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        clean = _strip_markdown_inline(value)
        if clean and clean not in out:
            out.append(clean)
    return out


def _operations_chat_body(section: dict[str, Any]) -> str:
    return _strip_markdown_inline(section.get("summary") or "")

def _growth_chat_body(section: dict[str, Any]) -> str:
    return _strip_markdown_inline(section.get("summary") or "")


def _merge_evidence_ids(*sections: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for section in sections:
        for item in section.get("evidence_ids") or []:
            evidence_id = str(item or "").strip()
            if evidence_id and evidence_id not in out:
                out.append(evidence_id)
    return out


def _format_chat_answer(
    *,
    operations_section: dict[str, Any],
    growth_section: dict[str, Any],
    active_lane: str | None,
) -> str:
    op_body = _operations_chat_body(operations_section)
    gr_body = _growth_chat_body(growth_section)

    if active_lane == "operations":
        body = op_body or _ensure_sentence(operations_section.get("summary") or NO_OP_SUMMARY)
        verification_line = f"Verification status: {operations_section.get('verification_status')}"
        evidence_ids = _merge_evidence_ids(operations_section)
    elif active_lane == "growth":
        body = gr_body or _ensure_sentence(growth_section.get("summary") or NO_GROWTH_SUMMARY)
        verification_line = f"Verification status: {growth_section.get('verification_status')}"
        evidence_ids = _merge_evidence_ids(growth_section)
    else:
        segments: list[str] = []
        if op_body:
            segments.append(f"On operations, {_lowercase_lead(op_body)}")
        if gr_body:
            segments.append(f"On growth, {_lowercase_lead(gr_body)}")
        normalized_segments = [
            segment if segment.endswith((".", "!", "?")) else f"{segment}."
            for segment in segments
            if segment
        ]
        body = "\n\n".join(normalized_segments)
        verification_line = (
            "Verification status: "
            f"Operations — {operations_section.get('verification_status')} | "
            f"Growth — {growth_section.get('verification_status')}"
        )
        evidence_ids = _merge_evidence_ids(operations_section, growth_section)

    evidence_line = f"Evidence IDs: {', '.join(evidence_ids) if evidence_ids else '(none)'}"
    lines = [body.strip()]
    if verification_line.strip():
        lines.append(verification_line)
    if evidence_line.strip():
        lines.append(evidence_line)
    return "\n\n".join(line for line in lines if line)


def _window_from_results(default_from: str, default_to: str, tool_results: list[ToolResult]) -> tuple[str, str]:
    for r in tool_results:
        if not (r.ok and isinstance(r.output, dict)):
            continue
        w = r.output.get("window")
        if isinstance(w, dict):
            wf = str(w.get("from") or default_from)
            wt = str(w.get("to") or default_to)
            return wf, wt
    return default_from, default_to


def _persist_proactive_card(
    *,
    ctx: ToolContext,
    lane: str,
    section: dict[str, Any],
    window_from: str,
    window_to: str,
) -> dict[str, Any]:
    evidence_ids = [str(x) for x in (section.get("evidence_ids") or []) if str(x)]
    if getattr(ctx, "terminal_id", None):
        terminal_evidence = f"terminal:{ctx.terminal_id}"
        if terminal_evidence not in evidence_ids:
            evidence_ids.append(terminal_evidence)
    evidence_hash = hashlib.sha1("|".join(sorted(evidence_ids)).encode("utf-8")).hexdigest()[:16]
    scope_key = str(getattr(ctx, "terminal_id", "") or "all")
    dedupe_key = f"{ctx.merchant_id}:{scope_key}:{lane}:{evidence_hash}:{window_from}:{window_to}"
    payload = {
        "lane": lane,
        "terminal_id": getattr(ctx, "terminal_id", None),
        "summary": section.get("summary"),
        "verification_status": section.get("verification_status"),
        "evidence_ids": evidence_ids,
        "deduction_explanation": section.get("deduction_explanation"),
        "recommended_actions": section.get("recommended_actions"),
        "growth_nudges": section.get("growth_nudges"),
        "action_preview_token": section.get("action_preview_token"),
    }

    try:
        from sqlalchemy import text

        create_sql = text(
            """
            CREATE TABLE IF NOT EXISTS proactive_cards (
              dedupe_key TEXT PRIMARY KEY,
              merchant_id TEXT NOT NULL,
              lane TEXT NOT NULL,
              verification_status TEXT NOT NULL,
              evidence_ids TEXT NOT NULL,
              action_preview_token TEXT NULL,
              payload_json TEXT NOT NULL,
              window_from TEXT NOT NULL,
              window_to TEXT NOT NULL,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        dialect = str(getattr(getattr(ctx.engine, "dialect", None), "name", "")).lower()
        if "sqlite" in dialect:
            insert_sql = text(
                """
                INSERT OR IGNORE INTO proactive_cards
                (dedupe_key, merchant_id, lane, verification_status, evidence_ids, action_preview_token, payload_json, window_from, window_to)
                VALUES (:dedupe_key, :mid, :lane, :vs, :evidence_ids, :token, :payload, :wf, :wt)
                """
            )
        else:
            insert_sql = text(
                """
                INSERT INTO proactive_cards
                (dedupe_key, merchant_id, lane, verification_status, evidence_ids, action_preview_token, payload_json, window_from, window_to)
                VALUES (:dedupe_key, :mid, :lane, :vs, :evidence_ids, :token, :payload, :wf, :wt)
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            )

        params = {
            "dedupe_key": dedupe_key,
            "mid": ctx.merchant_id,
            "lane": lane,
            "vs": str(section.get("verification_status") or ""),
            "evidence_ids": json.dumps(evidence_ids, ensure_ascii=False),
            "token": section.get("action_preview_token"),
            "payload": json.dumps(payload, ensure_ascii=False, default=str),
            "wf": window_from,
            "wt": window_to,
        }

        with ctx.engine.begin() as conn:
            conn.execute(create_sql)
            result = conn.execute(insert_sql, params)
            inserted = bool(getattr(result, "rowcount", 0))
        return {"lane": lane, "dedupe_key": dedupe_key, "inserted": inserted}
    except Exception as exc:
        logger.debug("Proactive card persistence skipped: %s", exc)
        return {"lane": lane, "dedupe_key": dedupe_key, "inserted": False, "error": str(exc)}


def _apply_reliability_guard(question: str, text: str, tool_results: list[ToolResult]) -> str:
    out = _postprocess_answer_text(text, tool_results)
    state, reason = _resolve_verification_state(question, tool_results)

    if state == "UNVERIFIED_SUPPORTED":
        out = _downgrade_unverified_claims(out)
        out = _upsert_status_line(out, "Verification status", f"Unverified (supported) - {reason}")
    elif state == "INSUFFICIENT_EVIDENCE":
        out = _downgrade_unverified_claims(out)
        out = _upsert_status_line(out, "Verification status", f"Insufficient evidence - {reason}")
    elif state == "VERIFIED":
        out = _upsert_status_line(out, "Verification status", f"Verified - {reason}")

    evidence_ids = _collect_evidence_ids(tool_results)
    evidence_value = ", ".join(evidence_ids) if evidence_ids else "(none)"
    out = _upsert_status_line(out, "Evidence IDs", evidence_value)
    return out.strip()


def _tool_dispatch(ctx: ToolContext, name: str, args: dict[str, Any]) -> ToolResult:
    try:
        if name == "get_merchant_context":
            return ToolResult(name=name, ok=True, output=get_merchant_context(ctx))
        if name == "list_transactions":
            return ToolResult(name=name, ok=True, output=list_transactions(ctx, **args))
        if name == "get_transaction_detail":
            return ToolResult(name=name, ok=True, output=get_transaction_detail(ctx, **args))
        if name == "compute_kpis":
            return ToolResult(name=name, ok=True, output=compute_kpis(ctx, **args))
        if name == "list_settlements":
            return ToolResult(name=name, ok=True, output=list_settlements(ctx, **args))
        if name == "get_settlement_detail":
            return ToolResult(name=name, ok=True, output=get_settlement_detail(ctx, **args))
        if name == "explain_settlement_shortfall":
            return ToolResult(name=name, ok=True, output=explain_settlement_shortfall(ctx, **args))
        if name == "list_chargebacks":
            return ToolResult(name=name, ok=True, output=list_chargebacks(ctx, **args))
        if name == "get_chargeback_detail":
            return ToolResult(name=name, ok=True, output=get_chargeback_detail(ctx, **args))
        if name == "list_refunds":
            return ToolResult(name=name, ok=True, output=list_refunds(ctx, **args))
        if name == "compare_kpis":
            return ToolResult(name=name, ok=True, output=compare_kpis(ctx, **args))
        if name == "terminal_performance":
            return ToolResult(name=name, ok=True, output=terminal_performance(ctx, **args))
        if name == "end_to_end_analysis":
            return ToolResult(name=name, ok=True, output=end_to_end_analysis(ctx, **args))
        if name == "propose_and_create_merchant_action":
            return ToolResult(name=name, ok=True, output=propose_and_create_merchant_action(ctx, **args))
        if name == "sql_database":
            return ToolResult(name=name, ok=True, output=sql_database(ctx, **args))
        if name == "verify_failure_drivers":
            return ToolResult(name=name, ok=True, output=verify_failure_drivers(ctx, **args))
        if name == "startup_kpis":
            d1 = str(args.get("from_date") or "")
            d2 = str(args.get("to_date") or "")
            profile = get_merchant_context(ctx)
            kpi_none = compute_kpis(ctx, from_date=d1, to_date=d2, group_by="none")
            kpi_mode = compute_kpis(ctx, from_date=d1, to_date=d2, group_by="payment_mode")
            summary_row = {}
            rows_none = kpi_none.get("rows") if isinstance(kpi_none, dict) else []
            if isinstance(rows_none, list) and rows_none and isinstance(rows_none[0], dict):
                summary_row = rows_none[0]
            evidence: list[str] = [f"startup_kpis:{ctx.merchant_id}:{d1}:{d2}"]
            for payload in (kpi_none, kpi_mode):
                if isinstance(payload, dict):
                    for ev in payload.get("evidence") or []:
                        sev = str(ev)
                        if sev and sev not in evidence:
                            evidence.append(sev)
            return ToolResult(
                name=name,
                ok=True,
                output={
                    "merchant_profile": profile,
                    "window": {"from": d1, "to": d2},
                    "kpi_snapshot": summary_row,
                    "kpi_by_mode": (kpi_mode.get("rows") if isinstance(kpi_mode, dict) else []) or [],
                    "evidence": evidence[:80],
                },
            )
        return ToolResult(name=name, ok=False, output=None, error=f"Unknown tool: {name}")
    except Exception as exc:
        return ToolResult(name=name, ok=False, output=None, error=str(exc))


def _render_answer(
    *,
    agent_dir: Path,
    merchant_id: str,
    question: str,
    plan_intent: str,
    tool_results: list[ToolResult],
) -> str:
    docs = load_agent_docs(agent_dir)
    system = "\n\n".join([docs.root, docs.tone]).strip()

    llm = ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=0.2,
    )

    tool_names = [r.name for r in tool_results]
    ctx_json = {
        "merchant_id": merchant_id,
        "intent": plan_intent,
        "question": question,
        "humor_level": str(getattr(Config, "COPILOT_HUMOR_LEVEL", "dry") or "dry"),
        "tool_results": [
            {"tool": r.name, "ok": r.ok, "output": r.output if r.ok else None, "error": r.error}
            for r in tool_results
        ],
        "hints": {
            "has_cashflow_snapshot": "cashflow_snapshot" in tool_names,
            "has_credit_fit": "assess_credit_fit" in tool_names,
            "has_kb": "kb_search" in tool_names,
        },
        "rules": {
            "no_invented_numbers": True,
            "cite_ids": True,
            "max_next_actions": 3,
        },
    }

    user = (
        "Answer the merchant question using ONLY tool_results for numbers and facts.\n"
        "Keep it concise and merchant-friendly.\n\n"
        "Rules:\n"
        "- Do not invent numbers or tool names.\n"
        "- Do not include internal reasoning or <think> blocks.\n"
        "- Cite evidence IDs exactly as returned.\n"
        "- If evidence is missing, say what is missing and suggest the next tool.\n\n"
        f"Context JSON:\n{json.dumps(ctx_json, ensure_ascii=False, indent=2, default=str)}"
    )

    try:
        resp = llm.invoke(
            [
                SystemMessage(content=system + "\n\nIMPORTANT: Do not include internal reasoning, scratchpads, or <think> blocks in your answer."),
                HumanMessage(content=user),
            ]
        )
        text = resp.content if hasattr(resp, "content") else str(resp)
        text = _sanitize_answer_text(str(text or ""))
        return text or "I couldn't generate a response for this question."
    except Exception as exc:
        logger.warning("Narrator LLM failed: %s", exc)
        return "I hit an error generating the narrative."


def _base_system_prompt(*, global_mode: bool, agent_dir: Path) -> str:
    if global_mode:
        return (
            _global_experimental_system_prompt().strip()
            + "\n\n"
            + "Operating brief rubric (apply when startup_kpis data is available):\n"
            + _global_bootstrap_prompt().strip()
            + "\n\nVerification rules:\n"
            + "- Prefer verify_failure_drivers for driver-ranking questions.\n"
            + "- Do not claim verified/top driver unless a tool returns verified=true.\n"
            + "- If verification fails, provide supported evidence and mark status unverified."
        ).strip()

    docs = load_agent_docs(agent_dir)
    return (
        "\n\n".join([docs.root, docs.tools, docs.rubrics]).strip()
        + "\n\nYou may call tools when needed. If you call a tool without from_date/to_date, "
        + "the system will use default_from_date/default_to_date from the context. "
        + "Do not invent tool names. Operate strictly within merchant_id."
    ).strip()


def _lane_prompt(lane: str) -> str:
    return _operations_lane_prompt().strip() if lane == "operations" else _growth_lane_prompt().strip()


def _format_operations_answer(section: dict[str, Any]) -> str:
    token = section.get("action_preview_token")
    token_line = f"\nAction preview token: {token}" if token else ""
    evidence_text = ", ".join(section.get("evidence_ids") or []) or "(none)"
    return (
        "Operations\n"
        f"Summary: {section.get('summary')}\n"
        f"Deduction explanation: {section.get('deduction_explanation')}\n"
        f"Recommended actions:\n{_as_markdown_list(list(section.get('recommended_actions') or []))}\n"
        f"Verification status: {section.get('verification_status')}\n"
        f"Evidence IDs: {evidence_text}"
        f"{token_line}"
    ).strip()


def _format_growth_answer(section: dict[str, Any]) -> str:
    evidence_text = ", ".join(section.get("evidence_ids") or []) or "(none)"
    return (
        "Growth\n"
        f"Summary: {section.get('summary')}\n"
        f"Growth nudges:\n{_as_markdown_list(list(section.get('growth_nudges') or []))}\n"
        f"Verification status: {section.get('verification_status')}\n"
        f"Evidence IDs: {evidence_text}"
    ).strip()


def run_turn(
    *,
    engine: Any,
    agent_dir: Path,
    merchant_id: str,
    question: str,
    forced_lane: str | None = None,
    terminal_id: str | None = None,
) -> CopilotTurn:
    scoped_terminal_id = str(terminal_id or "").strip() or None
    ctx = ToolContext(engine=engine, merchant_id=merchant_id, terminal_id=scoped_terminal_id)

    tool_calls: list[ToolCall] = []
    tool_results: list[ToolResult] = []
    global_mode = bool(getattr(Config, "GLOBAL_EXPERIMENTAL_MODE", False))
    plan_intent = "general"
    q = (question or "").strip().lower()
    normalized_forced_lane = str(forced_lane or "").strip().lower()
    if normalized_forced_lane not in {"operations", "growth"}:
        normalized_forced_lane = ""
    primary_lane, secondary_lane = _route_lanes(question)
    if normalized_forced_lane:
        primary_lane = normalized_forced_lane
        secondary_lane = ""

    if _is_smalltalk_prompt(question):
        answer_lane = normalized_forced_lane or None
        return CopilotTurn(
            answer=_smalltalk_answer(lane=answer_lane),
            tool_calls=[],
            tool_results=[],
            intent=plan_intent,
            evidence=[],
            operations_section={},
            growth_section={},
            primary_lane=primary_lane,
            secondary_lane=secondary_lane,
            active_lane=answer_lane,
            proactive_cards=[],
            terminal_focus=scoped_terminal_id,
        )

    if _is_out_of_scope_prompt(question):
        answer_lane = normalized_forced_lane or None
        return CopilotTurn(
            answer=_out_of_scope_answer(lane=answer_lane),
            tool_calls=[],
            tool_results=[],
            intent=plan_intent,
            evidence=[],
            operations_section={},
            growth_section={},
            primary_lane=primary_lane,
            secondary_lane=secondary_lane,
            active_lane=answer_lane,
            proactive_cards=[],
            terminal_focus=scoped_terminal_id,
        )
    proactive_cards: list[dict[str, Any]] = []

    operations_section: dict[str, Any] = {
        "summary": NO_OP_SUMMARY,
        "deduction_explanation": NO_OP_DEDUCTION,
        "recommended_actions": ["Share a payout or deduction issue to trigger an operations drilldown."],
        "verification_status": "Insufficient evidence - no operational data was requested",
        "evidence_ids": [],
        "action_preview_token": None,
    }
    growth_section: dict[str, Any] = {
        "summary": NO_GROWTH_SUMMARY,
        "growth_nudges": ["Ask for growth analysis to get acceptance/revenue opportunities."],
        "verification_status": "Insufficient evidence - no growth data was requested",
        "evidence_ids": [],
    }

    f = ""
    t = ""
    f, t = default_window_from_max_date(engine, merchant_id, days=30, terminal_id=scoped_terminal_id)
    system_base = _base_system_prompt(global_mode=global_mode, agent_dir=agent_dir)
    tools = make_tools(ctx=ctx, default_from=f, default_to=t)
    lane_runs: dict[str, dict[str, Any]] = {}
    budget_cap = int(getattr(Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 4) or 4) if global_mode else 2
    budget_cap = max(2, min(budget_cap, 12))
    primary_budget = _lane_step_budget(question, primary_lane, budget_cap)
    secondary_budget = 1 if primary_budget <= 2 else min(2, max(1, primary_budget - 1))
    lanes_to_run = [primary_lane] if normalized_forced_lane else [primary_lane, secondary_lane]

    for lane in lanes_to_run:
        if not lane:
            continue
        lane_tools = _filter_tools_for_lane(tools, lane)
        lane_calls: list[ToolCall] = []
        lane_results: list[ToolResult] = []
        deterministic_summary = ""

        if lane == "operations" and _is_shortfall_question(question):
            expected_amount, received_amount = _extract_shortfall_amounts(question)
            shortfall_args: dict[str, Any] = {"from_date": f, "to_date": t, "limit": 20}
            if expected_amount is not None:
                shortfall_args["expected_amount"] = expected_amount
            if received_amount is not None:
                shortfall_args["received_amount"] = received_amount
            shortfall_call = ToolCall(name="explain_settlement_shortfall", args=shortfall_args)
            shortfall_result = _tool_dispatch(ctx, shortfall_call.name, shortfall_call.args)
            lane_calls.append(shortfall_call)
            lane_results.append(shortfall_result)
            tool_calls.append(shortfall_call)
            tool_results.append(shortfall_result)

            if shortfall_result.ok and isinstance(shortfall_result.output, dict):
                out = shortfall_result.output
                if isinstance(out.get("shortfall"), dict) and (
                    bool(out.get("verified")) or bool(out.get("directional_support"))
                ):
                    deterministic_summary = str(out.get("summary") or "")

        lane_user = {
            "merchant_id": merchant_id,
            "terminal_focus": scoped_terminal_id,
            "question": question,
            "lane": lane,
            "default_from_date": f,
            "default_to_date": t,
            "limits": {"limit": 200},
            "tooling_hint": {
                "sql_database_requires_mid_placeholder": True,
                "preferred_verification_tool": "verify_failure_drivers",
                "terminal_scope_note": (
                    f"Selected terminal {scoped_terminal_id}. Supported analytics tools are automatically terminal-scoped."
                    if scoped_terminal_id
                    else "No terminal focus selected."
                ),
                "sql_database_scope_note": (
                    "sql_database remains merchant-scoped unless the query explicitly filters terminal_id."
                    if scoped_terminal_id
                    else "sql_database is merchant-scoped."
                ),
            },
        }
        lane_system = f"{system_base}\n\n{_lane_prompt(lane)}"
        if deterministic_summary:
            raw_calls = []
            raw_results = []
            final_text = deterministic_summary
        elif bool(getattr(Config, "SQL_LANGGRAPH_ENABLED", False)):
            graph_out = run_sql_langgraph(
                engine=engine,
                merchant_id=merchant_id,
                question=question,
                lane=lane,
                from_date=f,
                to_date=t,
                terminal_id=scoped_terminal_id,
            )
            raw_calls = [
                {
                    "name": "sql_langgraph_agent",
                    "args": {"lane": lane, "from_date": f, "to_date": t},
                }
            ]
            raw_results = [{"tool": "sql_langgraph_agent", "ok": True, "output": graph_out, "error": None}]
            final_text = str(graph_out.get("summary") or "")
        else:
            raw_calls, raw_results, final_text = invoke_with_tools(
                system=lane_system,
                user=lane_user,
                tools=lane_tools,
                temperature=0.1,
                max_steps=primary_budget if lane == primary_lane else secondary_budget,
            )

        for c in raw_calls:
            call = ToolCall(name=str(c.get("name") or "").strip(), args=dict(c.get("args") or {}))
            lane_calls.append(call)
            tool_calls.append(call)

        for r in raw_results:
            tr = ToolResult(
                name=str(r.get("tool") or ""),
                ok=bool(r.get("ok")),
                output=r.get("output"),
                error=r.get("error"),
            )
            lane_results.append(tr)
            tool_results.append(tr)

        # Only use startup_kpis as a fallback for broad merchant-overview asks.
        if not lane_results and _is_broad_overview_question(question, lane, scoped_terminal_id):
            bootstrap_call = ToolCall(name="startup_kpis", args={"from_date": f, "to_date": t})
            bootstrap_result = _tool_dispatch(ctx, bootstrap_call.name, bootstrap_call.args)
            lane_calls.append(bootstrap_call)
            lane_results.append(bootstrap_result)
            tool_calls.append(bootstrap_call)
            tool_results.append(bootstrap_result)

            # Immediately narrate the bootstrap result
            final_text = _render_answer(
                agent_dir=agent_dir,
                merchant_id=merchant_id,
                question=question,
                plan_intent=plan_intent,
                tool_results=[bootstrap_result]
            )

        lane_final_text = _sanitize_answer_text(final_text)
        if not lane_final_text and lane_results:
            lane_final_text = _render_answer(
                agent_dir=agent_dir,
                merchant_id=merchant_id,
                question=question,
                plan_intent=plan_intent,
                tool_results=lane_results,
            )

        lane_runs[lane] = {
            "final_text": lane_final_text,
            "tool_calls": lane_calls,
            "tool_results": lane_results,
        }

    op_run = lane_runs.get("operations", {"final_text": "", "tool_results": []})
    op_results = op_run.get("tool_results") or []
    op_summary = _section_summary(op_run.get("final_text") or "", "Operational review completed for the selected window.")
    op_state, op_reason = _resolve_operations_state(op_results)
    if op_state != "VERIFIED":
        op_summary = _downgrade_unverified_claims(op_summary)
    op_verification = _human_status(op_state, op_reason)
    op_evidence = _collect_evidence_ids(op_results)
    op_actions = _operations_recommended_actions(question, op_results, op_run.get("final_text") or "")
    if op_state != "VERIFIED":
        op_actions = [_downgrade_unverified_claims(x) for x in op_actions]
    if op_state == "INSUFFICIENT_EVIDENCE":
        op_actions = ["Run cashflow_snapshot or list_settlements for the target payout date to compute deduction causes."]
    operations_section = {
        "summary": op_summary,
        "deduction_explanation": _operations_deduction_explanation(op_results),
        "recommended_actions": op_actions,
        "verification_status": op_verification,
        "evidence_ids": op_evidence,
        "action_preview_token": _extract_action_preview_token(op_results),
    }

    gr_run = lane_runs.get("growth", {"final_text": "", "tool_results": []})
    gr_results = gr_run.get("tool_results") or []
    gr_summary = _section_summary(gr_run.get("final_text") or "", "Growth review completed for the selected window.")
    gr_state, gr_reason = _resolve_growth_state(question, gr_results)
    if gr_state != "VERIFIED":
        gr_summary = _downgrade_unverified_claims(gr_summary)
    gr_verification = _human_status(gr_state, gr_reason)
    gr_evidence = _collect_evidence_ids(gr_results)
    gr_nudges = _growth_nudges(question, gr_results, gr_run.get("final_text") or "")
    if gr_state != "VERIFIED":
        gr_nudges = [_downgrade_unverified_claims(x) for x in gr_nudges]
    if gr_state == "INSUFFICIENT_EVIDENCE":
        gr_nudges = ["Run verify_failure_drivers and terminal_performance for the same window to rank acceptance opportunities."]
    growth_section = {
        "summary": gr_summary,
        "growth_nudges": gr_nudges,
        "verification_status": gr_verification,
        "evidence_ids": gr_evidence,
    }

    if not normalized_forced_lane or normalized_forced_lane == "operations":
        op_from, op_to = _window_from_results(f, t, op_results)
        proactive_cards.append(
            _persist_proactive_card(
                ctx=ctx,
                lane="operations",
                section=operations_section,
                window_from=op_from,
                window_to=op_to,
            )
        )
    if not normalized_forced_lane or normalized_forced_lane == "growth":
        gr_from, gr_to = _window_from_results(f, t, gr_results)
        proactive_cards.append(
            _persist_proactive_card(
                ctx=ctx,
                lane="growth",
                section=growth_section,
                window_from=gr_from,
                window_to=gr_to,
            )
        )

    answer_lane = normalized_forced_lane or (primary_lane if not secondary_lane else None)
    answer = _format_chat_answer(
        operations_section=operations_section,
        growth_section=growth_section,
        active_lane=answer_lane,
    )

    # Collect de-duplicated evidence strings returned by tools.
    evidence: list[str] = []
    for r in tool_results:
        if r.ok and isinstance(r.output, dict):
            ev = r.output.get("evidence")
            if isinstance(ev, list):
                for x in ev[:50]:
                    sx = str(x)
                    if sx and sx not in evidence:
                        evidence.append(sx)

    return CopilotTurn(
        answer=answer,
        tool_calls=tool_calls,
        tool_results=tool_results,
        intent=plan_intent,
        evidence=evidence[:80],
        operations_section=operations_section,
        growth_section=growth_section,
        primary_lane=primary_lane,
        secondary_lane=secondary_lane,
        active_lane=answer_lane,
        proactive_cards=proactive_cards,
        terminal_focus=scoped_terminal_id,
    )

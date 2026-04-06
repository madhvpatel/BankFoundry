from __future__ import annotations

import hashlib
import json
import logging
import re
from functools import lru_cache
from typing import Any, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama
from langgraph.graph import END, START, StateGraph
from sqlalchemy import text

from config import Config
from .sql_catalog import load_catalog, render_catalog_for_prompt

logger = logging.getLogger("copilot_sql_graph")

_BLOCKED_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|merge|call|execute|vacuum)\b",
    re.IGNORECASE,
)


class SQLGraphState(TypedDict, total=False):
    engine: Any
    merchant_id: str
    terminal_id: str | None
    lane: str
    question: str
    from_date: str
    to_date: str
    plan: dict[str, Any]
    catalog: dict[str, Any]
    selected_views: list[str]
    sql_query: str
    sql_errors: list[str]
    sql_retry_count: int
    requires_human_review: bool
    review_reason: str
    review_token: str
    rows: list[dict[str, Any]]
    row_count: int
    analysis: str
    assumptions: list[str]
    caveats: list[str]
    next_actions: list[str]
    error: str | None
    result: dict[str, Any]


def _sanitize_sql(raw: str) -> str:
    s = str(raw or "").strip()
    s = re.sub(r"^```(sql)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    s = s.strip()
    if s.endswith(";"):
        s = s[:-1].strip()
    return s


def _safe_json_obj(raw: str) -> dict[str, Any] | None:
    txt = str(raw or "").strip()
    if not txt:
        return None
    txt = re.sub(r"^```(json)?\s*", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\s*```$", "", txt)
    try:
        parsed = json.loads(txt)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    m = re.search(r"\{[\s\S]*\}", txt)
    if not m:
        return None
    try:
        parsed = json.loads(m.group(0))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _llm() -> ChatOllama:
    return ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=0.0,
    )


def _ask_json(system: str, user: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        resp = _llm().invoke(
            [
                SystemMessage(content=system + "\n\nReturn JSON only."),
                HumanMessage(content=json.dumps(user, ensure_ascii=False, indent=2, default=str)),
            ]
        )
        obj = _safe_json_obj(resp.content if hasattr(resp, "content") else str(resp))
        return obj if isinstance(obj, dict) else fallback
    except Exception:
        return fallback


def _ask_text(system: str, user: dict[str, Any], fallback: str) -> str:
    try:
        resp = _llm().invoke(
            [
                SystemMessage(content=system),
                HumanMessage(content=json.dumps(user, ensure_ascii=False, indent=2, default=str)),
            ]
        )
        txt = str(resp.content if hasattr(resp, "content") else resp).strip()
        return txt or fallback
    except Exception:
        return fallback


def _heuristic_plan(question: str, lane: str, from_date: str, to_date: str) -> dict[str, Any]:
    q = (question or "").lower()
    metric = "success_rate"
    if "gmv" in q or "revenue" in q:
        metric = "gmv"
    if "failure" in q or "decline" in q:
        metric = "failure_rate"
    dimensions: list[str] = []
    for k in ("payment_mode", "response_code", "terminal_id", "card_network"):
        if k.replace("_", " ") in q or k in q:
            dimensions.append(k)
    if "card" in q and "payment_mode" not in dimensions:
        dimensions.append("payment_mode")
    return {
        "objective": "operations" if lane == "operations" else "growth",
        "metric": metric,
        "dimensions": dimensions or ["payment_mode"],
        "window": {"from": from_date, "to": to_date},
    }


def _node_plan(state: SQLGraphState) -> dict[str, Any]:
    question = str(state.get("question") or "")
    lane = str(state.get("lane") or "growth")
    from_date = str(state.get("from_date") or "")
    to_date = str(state.get("to_date") or "")
    fallback = _heuristic_plan(question, lane, from_date, to_date)
    system = (
        "You are a payments semantic planner. Convert the business question into a compact JSON plan.\n"
        "Do not write SQL. Focus on business objective, metric, dimensions, and window."
    )
    user = {"question": question, "lane": lane, "window": {"from": from_date, "to": to_date}}
    plan = _ask_json(system, user, fallback)
    return {"plan": plan, "sql_retry_count": 0}


def _node_metadata(state: SQLGraphState) -> dict[str, Any]:
    catalog = load_catalog(state["engine"])
    return {"catalog": catalog}


def _heuristic_view_selection(question: str, lane: str, catalog: dict[str, Any]) -> list[str]:
    q = (question or "").lower()
    available = {
        str(t.get("name") or "").strip()
        for t in (catalog.get("tables") or [])
        if isinstance(t, dict) and str(t.get("name") or "").strip()
    }
    selected: list[str] = []

    def _pick(name: str) -> None:
        if name in available and name not in selected:
            selected.append(name)

    _pick("transaction_features")
    if lane == "operations":
        for n in ("settlements", "chargebacks", "refunds"):
            if n in q or n[:-1] in q:
                _pick(n)
    else:
        if "terminal" in q:
            _pick("terminal_features")

    if "merchant" in q or "profile" in q:
        _pick("merchants")

    if not selected:
        rec = [str(x) for x in (catalog.get("recommended_views") or []) if str(x)]
        selected = [x for x in rec if x in available][:3] or sorted(available)[:2]
    return selected[:4]


def _node_select_views(state: SQLGraphState) -> dict[str, Any]:
    question = str(state.get("question") or "")
    lane = str(state.get("lane") or "growth")
    catalog = state.get("catalog") or {}
    fallback = {"selected_views": _heuristic_view_selection(question, lane, catalog)}
    system = (
        "Select the best tables/views for this analytics question.\n"
        "Use only names present in catalog.tables."
    )
    user = {
        "lane": lane,
        "question": question,
        "catalog_tables": [t.get("name") for t in (catalog.get("tables") or []) if isinstance(t, dict)],
    }
    picked = _ask_json(system, user, fallback)
    selected = picked.get("selected_views") if isinstance(picked, dict) else None
    if not isinstance(selected, list) or not selected:
        selected = fallback["selected_views"]
    selected = [str(x) for x in selected if str(x).strip()]
    if not selected:
        selected = fallback["selected_views"]
    return {"selected_views": selected[:4]}


def _default_sql(state: SQLGraphState) -> str:
    lane = str(state.get("lane") or "growth")
    q = str(state.get("question") or "").lower()
    table = "transaction_features"
    available = {str(t.get("name") or "") for t in (state.get("catalog", {}).get("tables") or []) if isinstance(t, dict)}
    if table not in available and available:
        table = sorted(available)[0]

    if lane == "growth" and ("driver" in q or "response" in q or "failed" in q):
        return (
            f"SELECT COALESCE(NULLIF(TRIM(response_code), ''), 'UNKNOWN') AS bucket, "
            f"COUNT(*) AS fail_txns, COALESCE(SUM(amount_rupees),0) AS failed_gmv "
            f"FROM {table} "
            "WHERE merchant_id = :mid AND p_date >= :d1 AND p_date < :d2 "
            "AND status IN ('FAILED','FAILURE') "
            "GROUP BY 1 ORDER BY fail_txns DESC, failed_gmv DESC LIMIT 20"
        )

    if lane == "operations" and "settlement" in q and "settlements" in available:
        return (
            "SELECT COALESCE(status, settlement_status, 'UNKNOWN') AS status_bucket, "
            "COUNT(*) AS settlement_count "
            "FROM settlements "
            "WHERE COALESCE(merchant_id, mid) = :mid "
            "GROUP BY 1 ORDER BY settlement_count DESC LIMIT 20"
        )

    return (
        f"SELECT COALESCE(payment_mode, 'UNKNOWN') AS bucket, "
        "COUNT(*) AS attempts, "
        "SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS success_txns, "
        "SUM(CASE WHEN status IN ('FAILED','FAILURE') THEN 1 ELSE 0 END) AS fail_txns, "
        "COALESCE(SUM(CASE WHEN status IN ('FAILED','FAILURE') THEN amount_rupees ELSE 0 END),0) AS failed_gmv "
        f"FROM {table} "
        "WHERE merchant_id = :mid AND p_date >= :d1 AND p_date < :d2 "
        "GROUP BY 1 ORDER BY attempts DESC LIMIT 20"
    )


def _node_generate_sql(state: SQLGraphState) -> dict[str, Any]:
    catalog = state.get("catalog") or {}
    selected = [str(x) for x in (state.get("selected_views") or []) if str(x)]
    catalog_txt = render_catalog_for_prompt(catalog, max_tables=30, max_columns=30)
    fallback_sql = _default_sql(state)
    system = (
        "Generate one SQL query for analytics.\n"
        "Rules:\n"
        "- SELECT/CTE only\n"
        "- Use :mid for merchant scope\n"
        "- Use :d1 and :d2 for window where applicable\n"
        "- Use only selected views/tables\n"
        "- Include LIMIT <= 200\n"
        "Output SQL only."
    )
    user = {
        "lane": state.get("lane"),
        "question": state.get("question"),
        "plan": state.get("plan"),
        "selected_views": selected,
        "catalog": catalog_txt,
    }
    sql_text = _ask_text(system, user, fallback_sql)
    return {"sql_query": _sanitize_sql(sql_text)}


def _extract_table_refs(query: str) -> set[str]:
    refs = set(re.findall(r"\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)", query, flags=re.IGNORECASE))
    refs.update(re.findall(r"\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)", query, flags=re.IGNORECASE))
    return {r.lower() for r in refs}


def _check_sql(query: str, selected_views: list[str]) -> list[str]:
    errors: list[str] = []
    q = str(query or "").strip()
    if not q:
        return ["empty_sql"]
    q_lower = q.lower()
    if ";" in q:
        errors.append("single_statement_only")
    if not (q_lower.startswith("select") or q_lower.startswith("with ")):
        errors.append("read_only_select_required")
    if _BLOCKED_SQL.search(q):
        errors.append("blocked_keyword_detected")
    if ":mid" not in q:
        errors.append("missing_mid_scope")
    refs = _extract_table_refs(q)
    allow = {v.lower() for v in selected_views}
    if refs and allow and not refs.issubset(allow):
        errors.append("query_references_unselected_table")
    return errors


def _node_check_sql(state: SQLGraphState) -> dict[str, Any]:
    sql_query = str(state.get("sql_query") or "")
    selected_views = [str(v) for v in (state.get("selected_views") or []) if str(v)]
    errors = _check_sql(sql_query, selected_views)
    retry_count = int(state.get("sql_retry_count") or 0)
    updates: dict[str, Any] = {"sql_errors": errors}
    if errors:
        updates["sql_retry_count"] = retry_count + 1
    return updates


def _route_after_check(state: SQLGraphState) -> str:
    errors = state.get("sql_errors") or []
    retries = int(state.get("sql_retry_count") or 0)
    if errors and retries <= int(getattr(Config, "SQL_GRAPH_MAX_SQL_RETRIES", 1) or 1):
        return "n_sql_generator"
    return "n_policy_gate"


def _node_policy_gate(state: SQLGraphState) -> dict[str, Any]:
    q = str(state.get("sql_query") or "")
    joins = len(re.findall(r"\bjoin\b", q, flags=re.IGNORECASE))
    has_mid = ":mid" in q
    has_window = ":d1" in q and ":d2" in q
    risky = joins >= 2 or not has_mid
    if str(state.get("lane") or "") == "operations" and not has_window:
        risky = True

    require_review = bool(getattr(Config, "SQL_GRAPH_REQUIRE_HUMAN_REVIEW", False)) and risky
    reason = ""
    if require_review:
        reason = "query flagged as high-risk/ambiguous; review before execution"
    return {"requires_human_review": require_review, "review_reason": reason}


def _route_after_policy(state: SQLGraphState) -> str:
    return "n_human_review" if bool(state.get("requires_human_review")) else "n_sql_executor"


def _node_human_review(state: SQLGraphState) -> dict[str, Any]:
    q = str(state.get("sql_query") or "")
    token = hashlib.sha1(q.encode("utf-8")).hexdigest()[:16]
    return {
        "review_token": token,
        "analysis": "Execution paused pending human approval.",
        "rows": [],
        "row_count": 0,
    }


def _node_execute_sql(state: SQLGraphState) -> dict[str, Any]:
    sql_query = str(state.get("sql_query") or "").strip()
    if not sql_query:
        return {"rows": [], "row_count": 0, "error": "sql_query_missing"}
    if state.get("sql_errors"):
        return {"rows": [], "row_count": 0, "error": "sql_validation_failed"}

    limit = max(1, min(int(getattr(Config, "SQL_GRAPH_MAX_ROWS", 200) or 200), 500))
    wrapped = text(f"SELECT * FROM ({sql_query}) AS scoped_query LIMIT :_copilot_limit")
    params = {
        "mid": str(state.get("merchant_id") or ""),
        "d1": str(state.get("from_date") or ""),
        "d2": str(state.get("to_date") or ""),
        "from_date": str(state.get("from_date") or ""),
        "to_date": str(state.get("to_date") or ""),
        "_copilot_limit": limit,
    }

    try:
        with state["engine"].connect() as conn:
            rows = conn.execute(wrapped, params).mappings().all()
        out = [dict(r) for r in rows]
        return {"rows": out, "row_count": len(out), "error": None}
    except Exception as exc:
        return {"rows": [], "row_count": 0, "error": str(exc)}


def _node_analyze(state: SQLGraphState) -> dict[str, Any]:
    if bool(state.get("requires_human_review")):
        return {
            "assumptions": ["Human approval required before execution."],
            "caveats": [str(state.get("review_reason") or "Execution paused by policy gate.")],
            "next_actions": ["Approve and rerun SQL execution with review token."],
        }

    if state.get("error"):
        return {
            "analysis": "SQL execution did not complete.",
            "assumptions": [],
            "caveats": [str(state.get("error"))],
            "next_actions": ["Review SQL and table metadata, then retry."],
        }

    rows = state.get("rows") or []
    row_count = int(state.get("row_count") or 0)
    if row_count == 0:
        return {
            "analysis": "No rows returned for the selected window and filters.",
            "assumptions": ["Window and merchant scope were applied as requested."],
            "caveats": ["Result set is empty; trend conclusions are limited."],
            "next_actions": ["Try a wider date window or alternate dimension breakdown."],
        }

    sample = rows[:5]
    fallback = f"Retrieved {row_count} rows from approved analytics views."
    analysis = _ask_text(
        (
            "You are a merchant analytics explainer.\n"
            "Return a concise business summary in 2-4 lines.\n"
            "Do not invent numbers."
        ),
        {
            "lane": state.get("lane"),
            "question": state.get("question"),
            "row_count": row_count,
            "sample_rows": sample,
        },
        fallback,
    )
    return {
        "analysis": analysis,
        "assumptions": ["The query used merchant-scoped parameters and approved tables."],
        "caveats": ["This analysis reflects available structured data only."],
        "next_actions": ["Run deeper drilldowns by response_code, terminal, or settlement status if needed."],
    }


def _node_finalize(state: SQLGraphState) -> dict[str, Any]:
    lane = str(state.get("lane") or "growth")
    d1 = str(state.get("from_date") or "")
    d2 = str(state.get("to_date") or "")
    sql_query = str(state.get("sql_query") or "")
    row_count = int(state.get("row_count") or 0)
    error = state.get("error")
    requires_review = bool(state.get("requires_human_review"))
    sql_errors = [str(x) for x in (state.get("sql_errors") or []) if str(x)]
    selected_views = [str(x) for x in (state.get("selected_views") or []) if str(x)]

    evidence: list[str] = [f"sqlgraph:{lane}:{d1}:{d2}"]
    for v in selected_views:
        eid = f"sqlgraph:view:{v}"
        if eid not in evidence:
            evidence.append(eid)
    if sql_query:
        evidence.append(f"sqlgraph:query:{hashlib.sha1(sql_query.encode('utf-8')).hexdigest()[:12]}")

    verified = bool(row_count > 0 and not sql_errors and not error and not requires_review)
    directional_failure_support = bool(
        row_count > 0
        and (
            "fail_txns" in sql_query.lower()
            or "status in ('failed','failure')" in sql_query.lower().replace(" ", "")
            or "response_code" in sql_query.lower()
        )
    )

    result = {
        "verified": verified,
        "verification_type": "langgraph_sql_pipeline",
        "lane": lane,
        "window": {"from": d1, "to": d2},
        "selected_views": selected_views,
        "sql_query": sql_query,
        "rows": state.get("rows") or [],
        "row_count": row_count,
        "summary": str(state.get("analysis") or ""),
        "assumptions": [str(x) for x in (state.get("assumptions") or []) if str(x)],
        "caveats": [str(x) for x in (state.get("caveats") or []) if str(x)],
        "next_actions": [str(x) for x in (state.get("next_actions") or []) if str(x)],
        "requires_human_review": requires_review,
        "review_reason": str(state.get("review_reason") or ""),
        "review_token": str(state.get("review_token") or ""),
        "directional_failure_support": directional_failure_support,
        "directional_support": bool(row_count > 0),
        "evidence": evidence[:80],
        "error": str(error) if error else (", ".join(sql_errors) if sql_errors else None),
    }
    return {"result": result}


@lru_cache(maxsize=1)
def _compiled_graph():
    graph = StateGraph(SQLGraphState)
    graph.add_node("n_planner", _node_plan)
    graph.add_node("n_metadata", _node_metadata)
    graph.add_node("n_view_selector", _node_select_views)
    graph.add_node("n_sql_generator", _node_generate_sql)
    graph.add_node("n_sql_checker", _node_check_sql)
    graph.add_node("n_policy_gate", _node_policy_gate)
    graph.add_node("n_human_review", _node_human_review)
    graph.add_node("n_sql_executor", _node_execute_sql)
    graph.add_node("n_analysis", _node_analyze)
    graph.add_node("n_finalize", _node_finalize)

    graph.add_edge(START, "n_planner")
    graph.add_edge("n_planner", "n_metadata")
    graph.add_edge("n_metadata", "n_view_selector")
    graph.add_edge("n_view_selector", "n_sql_generator")
    graph.add_edge("n_sql_generator", "n_sql_checker")
    graph.add_conditional_edges(
        "n_sql_checker",
        _route_after_check,
        {"n_sql_generator": "n_sql_generator", "n_policy_gate": "n_policy_gate"},
    )
    graph.add_conditional_edges(
        "n_policy_gate",
        _route_after_policy,
        {"n_human_review": "n_human_review", "n_sql_executor": "n_sql_executor"},
    )
    graph.add_edge("n_human_review", "n_analysis")
    graph.add_edge("n_sql_executor", "n_analysis")
    graph.add_edge("n_analysis", "n_finalize")
    graph.add_edge("n_finalize", END)
    return graph.compile()


def run_sql_langgraph(
    *,
    engine: Any,
    merchant_id: str,
    question: str,
    lane: str,
    from_date: str,
    to_date: str,
    terminal_id: str | None = None,
) -> dict[str, Any]:
    initial: SQLGraphState = {
        "engine": engine,
        "merchant_id": str(merchant_id or ""),
        "terminal_id": str(terminal_id or "") or None,
        "lane": "operations" if str(lane or "").lower() == "operations" else "growth",
        "question": str(question or ""),
        "from_date": str(from_date or ""),
        "to_date": str(to_date or ""),
    }
    try:
        state = _compiled_graph().invoke(initial)
        result = state.get("result") if isinstance(state, dict) else None
        if isinstance(result, dict):
            return result
    except Exception as exc:
        logger.warning("LangGraph SQL pipeline failed: %s", exc)
        return {
            "verified": False,
            "verification_type": "langgraph_sql_pipeline",
            "lane": lane,
            "window": {"from": from_date, "to": to_date},
            "rows": [],
            "row_count": 0,
            "summary": "SQL graph execution failed.",
            "assumptions": [],
            "caveats": [str(exc)],
            "next_actions": ["Retry with fallback tool-based path."],
            "requires_human_review": False,
            "directional_failure_support": False,
            "directional_support": False,
            "evidence": [f"sqlgraph:{lane}:{from_date}:{to_date}"],
            "error": str(exc),
        }

    return {
        "verified": False,
        "verification_type": "langgraph_sql_pipeline",
        "lane": lane,
        "window": {"from": from_date, "to": to_date},
        "rows": [],
        "row_count": 0,
        "summary": "SQL graph returned no result.",
        "assumptions": [],
        "caveats": ["Graph completed without result payload."],
        "next_actions": ["Retry with fallback tool-based path."],
        "requires_human_review": False,
        "directional_failure_support": False,
        "directional_support": False,
        "evidence": [f"sqlgraph:{lane}:{from_date}:{to_date}"],
        "error": "missing_result",
    }

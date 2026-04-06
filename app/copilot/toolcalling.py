from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool, tool
from langchain_ollama import ChatOllama

from config import Config
from .kb import reindex_kb, search_kb
from .tools import (
    ToolContext,
    assess_credit_fit,
    cashflow_snapshot,
    compute_kpis,
    compare_kpis,
    end_to_end_analysis,
    explain_settlement_shortfall,
    geo_drift_check,
    get_chargeback_detail,
    get_merchant_context,
    get_merchant_lending_offers,
    get_settlement_detail,
    get_transaction_detail,
    intelligence_probe,
    list_chargebacks,
    list_refunds,
    list_settlements,
    list_transactions,
    propose_and_create_merchant_action,
    sql_database,
    terminal_health_summary,
    terminal_issue_correlator,
    terminal_performance,
    verify_failure_drivers,
)

logger = logging.getLogger("copilot_toolcalling")


def _iso(d: dt.date) -> str:
    return d.isoformat()


def default_window_from_max_date(engine: Any, merchant_id: str, days: int = 30, terminal_id: str | None = None) -> tuple[str, str]:
    """Default window anchored to latest available p_date for this merchant.

    Mirrors runtime._default_window but kept here to avoid import cycles.
    """
    try:
        from sqlalchemy import text

        max_date = None
        with engine.connect() as conn:
            if terminal_id:
                try:
                    max_date = conn.execute(
                        text(
                            f"""
                            SELECT MAX(p_date)
                            FROM {Config.QUERY_SOURCE_TABLE}
                            WHERE merchant_id = :mid
                              AND terminal_id = :tid
                            """
                        ),
                        {"mid": merchant_id, "tid": str(terminal_id)},
                    ).scalar()
                except Exception:
                    max_date = None
            if max_date is None:
                max_date = conn.execute(
                    text(f"SELECT MAX(p_date) FROM {Config.QUERY_SOURCE_TABLE} WHERE merchant_id = :mid"),
                    {"mid": merchant_id},
                ).scalar()

        if max_date:
            end = max_date + dt.timedelta(days=1)
            start = end - dt.timedelta(days=days)
            return _iso(start), _iso(end)
    except Exception:
        pass

    today = dt.date.today()
    return _iso(today - dt.timedelta(days=days)), _iso(today + dt.timedelta(days=1))


def make_tools(*, ctx: ToolContext, default_from: str, default_to: str) -> list[BaseTool]:
    """Create LangChain tool wrappers.

    Note: We keep args optional where reasonable and fill from default window,
    so the model can call tools without always supplying dates.
    """

    @tool("get_merchant_context")
    def get_merchant_context_tool() -> dict:
        """Fetch merchant profile + risk + KYC snapshot (merchant-scoped)."""
        return get_merchant_context(ctx)

    @tool("get_merchant_lending_offers")
    def get_merchant_lending_offers_tool() -> dict:
        """Call this tool EXCLUSIVELY if the user asks ANY question about:
        - funding options
        - loans or lending
        - borrowing money
        - overdraft or cash advances
        - eligibility for credit

        DO NOT call merchant_profile instead. ALWAYS use this tool for anything related to funding/loans.
        """
        return get_merchant_lending_offers(ctx)

    @tool("merchant_profile")
    def merchant_profile_tool() -> dict:
        """Alias: merchant profile snapshot (same output as get_merchant_context)."""
        return get_merchant_context(ctx)

    @tool("compute_kpis")
    def compute_kpis_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        group_by: str = "payment_mode",
    ) -> dict:
        """Compute KPI aggregates over a date range (merchant-scoped)."""
        return compute_kpis(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            group_by=group_by,
        )

    @tool("list_transactions")
    def list_transactions_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 50,
    ) -> dict:
        """List transactions in a date range (merchant-scoped, paginated by limit)."""
        return list_transactions(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            limit=limit,
        )

    @tool("get_transaction_detail")
    def get_transaction_detail_tool(tx_id: str | int) -> dict:
        """Fetch a single transaction record by tx_id (merchant-scoped)."""
        return get_transaction_detail(ctx, tx_id=str(tx_id))

    @tool("list_settlements")
    def list_settlements_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 50,
    ) -> dict:
        """List settlements for the merchant in a date range."""
        return list_settlements(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            limit=limit,
        )

    @tool("get_settlement_detail")
    def get_settlement_detail_tool(settlement_id: str | int) -> dict:
        """Fetch one settlement and (if available) reconciliation breakdown."""
        return get_settlement_detail(ctx, settlement_id=str(settlement_id))

    @tool("explain_settlement_shortfall")
    def explain_settlement_shortfall_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        expected_amount: float | None = None,
        received_amount: float | None = None,
        limit: int = 20,
    ) -> dict:
        """Explain a payout shortfall using settlement deduction fields when available."""
        return explain_settlement_shortfall(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            expected_amount=expected_amount,
            received_amount=received_amount,
            limit=limit,
        )

    @tool("list_chargebacks")
    def list_chargebacks_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        status: str = "open",
        limit: int = 50,
    ) -> dict:
        """List chargebacks (optionally date-filtered) for the merchant."""
        return list_chargebacks(
            ctx,
            status=status,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            limit=limit,
        )

    @tool("get_chargeback_detail")
    def get_chargeback_detail_tool(chargeback_id: str | int) -> dict:
        """Fetch a single chargeback record by chargeback_id (merchant-scoped)."""
        return get_chargeback_detail(ctx, chargeback_id=str(chargeback_id))

    @tool("list_refunds")
    def list_refunds_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 50,
    ) -> dict:
        """List refunds in a date range (merchant-scoped)."""
        return list_refunds(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            limit=limit,
        )

    @tool("compare_kpis")
    def compare_kpis_tool(
        from_date_a: str | None = None,
        to_date_a: str | None = None,
        from_date_b: str | None = None,
        to_date_b: str | None = None,
        window_a: dict | None = None,
        window_b: dict | None = None,
        group_by: str = "payment_mode",
    ) -> dict:
        """Compare KPI windows A vs B for the merchant.

        Accepts either explicit dates (from_date_*/to_date_*) or window dicts
        ({from_date,to_date} or {from,to}).
        """

        def _from_window(win: dict[str, Any] | None, key_a: str, key_b: str) -> str | None:
            if not isinstance(win, dict):
                return None
            return str(win.get(key_a) or win.get(key_b) or "").strip() or None

        a_from = from_date_a or _from_window(window_a, "from_date", "from") or default_from
        a_to = to_date_a or _from_window(window_a, "to_date", "to") or default_to
        b_from = from_date_b or _from_window(window_b, "from_date", "from")
        b_to = to_date_b or _from_window(window_b, "to_date", "to")

        # If B window is omitted, auto-use previous period matching A's span.
        if not b_from or not b_to:
            try:
                a1 = dt.date.fromisoformat(a_from)
                a2 = dt.date.fromisoformat(a_to)
                span = max(1, (a2 - a1).days)
                b_to_date = a1
                b_from_date = a1 - dt.timedelta(days=span)
                b_from = b_from or b_from_date.isoformat()
                b_to = b_to or b_to_date.isoformat()
            except Exception:
                b_from = b_from or default_from
                b_to = b_to or default_to

        return compare_kpis(
            ctx,
            from_date_a=a_from,
            to_date_a=a_to,
            from_date_b=b_from,
            to_date_b=b_to,
            group_by=group_by,
        )

    @tool("terminal_performance")
    def terminal_performance_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 20,
    ) -> dict:
        """Rank terminals/devices by attempts and success rate (merchant-scoped)."""
        return terminal_performance(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            limit=limit,
        )

    @tool("assess_credit_fit")
    def assess_credit_fit_tool(from_date: str | None = None, to_date: str | None = None) -> dict:
        """Compute an indicative fit assessment for volume-based working-capital credit."""
        return assess_credit_fit(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
        )

    @tool("cashflow_snapshot")
    def cashflow_snapshot_tool(from_date: str | None = None, to_date: str | None = None) -> dict:
        """Summarize settlement timing and cashflow signals (counts/amounts/past-expected)."""
        return cashflow_snapshot(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
        )

    @tool("terminal_health_summary")
    def terminal_health_summary_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        group_by: str = "tid_hour",
        limit: int = 50,
    ) -> dict:
        """Summarize terminal health snapshots (network/battery/printer) for RCA."""
        return terminal_health_summary(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            group_by=group_by,
            limit=limit,
        )

    @tool("geo_drift_check")
    def geo_drift_check_tool(from_date: str | None = None, to_date: str | None = None, tid: str | None = None) -> dict:
        """Check whether terminals show location drift/deviation (geo factor evidence)."""
        return geo_drift_check(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            tid=tid,
        )

    @tool("terminal_issue_correlator")
    def terminal_issue_correlator_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        flag: str = "low_network_strength",
        limit: int = 20,
    ) -> dict:
        """Correlate terminal health flags with elevated payment failure rates."""
        return terminal_issue_correlator(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            flag=flag,
            limit=limit,
        )

    @tool("end_to_end_analysis")
    def end_to_end_analysis_tool(from_date: str | None = None, to_date: str | None = None) -> dict:
        """Run a bundled merchant health check (KPIs, failures, terminal health, etc.)."""
        return end_to_end_analysis(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
        )

    @tool("intelligence_probe")
    def intelligence_probe_tool(window_days: int = 30, enable_reasoning: bool = False) -> dict:
        """Run the intelligence runner (signals + recommendations) for the merchant."""
        return intelligence_probe(ctx, window_days=window_days, enable_reasoning=enable_reasoning)

    @tool("propose_and_create_merchant_action")
    def propose_and_create_merchant_action_tool(**kwargs) -> dict:
        """Propose (and optionally create) a merchant action; supports confirmation token."""
        # Two-step write confirmation is handled inside the underlying function.
        return propose_and_create_merchant_action(ctx, **kwargs)

    @tool("kb_search")
    def kb_search_tool(query: str, top_k: int = 5) -> dict:
        """Search the bank knowledge base (bank_kb/*.md) for relevant snippets."""
        return search_kb(query=query, top_k=top_k)

    @tool("knowledge_base")
    def knowledge_base_tool(query: str, top_k: int = 5) -> dict:
        """Alias: search bank knowledge base snippets and citations."""
        return search_kb(query=query, top_k=top_k)

    @tool("kb_reindex")
    def kb_reindex_tool() -> dict:
        """Rebuild the bank knowledge base index from bank_kb/*.md."""
        return reindex_kb()

    @tool("sql_database")
    def sql_database_tool(query: str, parameters: dict | None = None, limit: int = 100) -> dict:
        """Run a read-only SQL query scoped to active merchant via :mid placeholder."""
        return sql_database(ctx, query=query, parameters=parameters, limit=limit)

    @tool("verify_failure_drivers")
    def verify_failure_drivers_tool(
        from_date: str | None = None,
        to_date: str | None = None,
        by: str = "response_code",
        limit: int = 5,
    ) -> dict:
        """Deterministically rank failure drivers by response_code or payment_mode."""
        return verify_failure_drivers(
            ctx,
            from_date=from_date or default_from,
            to_date=to_date or default_to,
            by="payment_mode" if str(by).strip().lower() == "payment_mode" else "response_code",
            limit=limit,
        )

    @tool("startup_kpis")
    def startup_kpis_tool(from_date: str | None = None, to_date: str | None = None) -> dict:
        """Return one-shot merchant profile + KPI snapshot for operating brief generation."""
        d1 = from_date or default_from
        d2 = to_date or default_to

        profile = get_merchant_context(ctx)
        kpis_overall = compute_kpis(
            ctx,
            from_date=d1,
            to_date=d2,
            group_by="none",
        )
        kpis_mode = compute_kpis(
            ctx,
            from_date=d1,
            to_date=d2,
            group_by="payment_mode",
        )

        summary_row = {}
        if isinstance(kpis_overall, dict):
            rows = kpis_overall.get("rows")
            if isinstance(rows, list) and rows:
                summary_row = rows[0] if isinstance(rows[0], dict) else {}

        evidence: list[str] = [f"startup_kpis:{ctx.merchant_id}:{d1}:{d2}"]
        if getattr(ctx, "terminal_id", None):
            evidence.append(f"terminal:{ctx.terminal_id}")
        for payload in (kpis_overall, kpis_mode):
            if isinstance(payload, dict):
                for ev in payload.get("evidence") or []:
                    sev = str(ev)
                    if sev and sev not in evidence:
                        evidence.append(sev)

        return {
            "merchant_profile": profile,
            "window": {"from": d1, "to": d2},
            "kpi_snapshot": summary_row,
            "kpi_by_mode": (kpis_mode.get("rows") if isinstance(kpis_mode, dict) else []) or [],
            "evidence": evidence[:80],
        }

    return [
        merchant_profile_tool,
        get_merchant_context_tool,
        get_merchant_lending_offers_tool,
        compute_kpis_tool,
        list_transactions_tool,
        get_transaction_detail_tool,
        list_settlements_tool,
        get_settlement_detail_tool,
        explain_settlement_shortfall_tool,
        list_chargebacks_tool,
        get_chargeback_detail_tool,
        list_refunds_tool,
        compare_kpis_tool,
        terminal_performance_tool,
        terminal_health_summary_tool,
        geo_drift_check_tool,
        terminal_issue_correlator_tool,
        end_to_end_analysis_tool,
        intelligence_probe_tool,
        propose_and_create_merchant_action_tool,
        startup_kpis_tool,
        sql_database_tool,
        verify_failure_drivers_tool,
        knowledge_base_tool,
        kb_search_tool,
        kb_reindex_tool,
        assess_credit_fit_tool,
        cashflow_snapshot_tool,
    ]


def invoke_with_tools(
    *,
    system: str,
    user: dict[str, Any],
    tools: list[BaseTool],
    temperature: float = 0.1,
    max_steps: int = 1,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    """Invoke the model with tools bound, optionally allowing multi-step tool loops.

    Returns:
    - tool_calls: flat list of tool call dicts across all steps
    - tool_results: tool execution results across all steps
    - final_text: last assistant text content from the loop (may be empty)
    """
    max_steps = max(1, min(int(max_steps or 1), 12))

    llm = ChatOllama(
        model=Config.OLLAMA_MODEL,
        base_url=Config.OLLAMA_BASE_URL,
        temperature=temperature,
    ).bind_tools(tools)

    messages = [
        SystemMessage(content=system),
        HumanMessage(content=json.dumps(user, ensure_ascii=False, indent=2, default=str)),
    ]

    all_tool_calls: list[dict[str, Any]] = []
    all_results: list[dict[str, Any]] = []
    final_text = ""

    for _ in range(max_steps):
        ai = llm.invoke(messages)
        messages.append(ai)

        ai_text = getattr(ai, "content", "")
        if isinstance(ai_text, str):
            final_text = ai_text.strip() or final_text
        elif ai_text is not None:
            text_fallback = str(ai_text).strip()
            if text_fallback:
                final_text = text_fallback

        tool_calls = getattr(ai, "tool_calls", None) or []
        if not tool_calls:
            break

        for tc in tool_calls:
            name = str(tc.get("name") or "")
            args = tc.get("args") or {}
            call_id = str(tc.get("id") or f"call_{len(all_tool_calls) + 1}")
            tool_fn: BaseTool | None = next((t for t in tools if t.name == name), None)
            if not tool_fn:
                result = {"tool": name, "ok": False, "output": None, "error": f"Unknown tool: {name}"}
                all_results.append(result)
                messages.append(
                    ToolMessage(
                        tool_call_id=call_id,
                        name=name,
                        content=json.dumps({"error": result["error"]}, ensure_ascii=False),
                    )
                )
                continue

            try:
                out = tool_fn.invoke(args)
                result = {"tool": name, "ok": True, "output": out, "error": None}
                tool_content = json.dumps(out, ensure_ascii=False, default=str)
            except Exception as exc:
                result = {"tool": name, "ok": False, "output": None, "error": str(exc)}
                tool_content = json.dumps({"error": str(exc)}, ensure_ascii=False, default=str)

            all_results.append(result)
            messages.append(
                ToolMessage(
                    tool_call_id=call_id,
                    name=name,
                    content=tool_content[:12000],
                )
            )

        all_tool_calls.extend(tool_calls)

    return all_tool_calls, all_results, final_text

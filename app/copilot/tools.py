from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass, is_dataclass
import hashlib
import re
from typing import Any, Literal

import numpy as np
from sqlalchemy import text

from config import Config

import json

from app.data.actions import repository as action_repository
from app.data.disputes import repository as disputes_repository
from app.data.merchant_ops import repository as merchant_ops_repository
from app.data.merchants import repository as merchants_repository
from app.data.settlements import repository as settlements_repository
from app.data.terminals import repository as terminals_repository
from app.data.transactions import repository as transactions_repository
from app.intelligence.runner import run_intelligence


def _table_columns(ctx: ToolContext, table: str) -> set[str]:
    return merchant_ops_repository.table_columns(ctx.engine, table)


@dataclass(frozen=True)
class ToolContext:
    engine: Any
    merchant_id: str
    terminal_id: str | None = None


def _scoped_terminal_id(ctx: "ToolContext", *, table: str | None = None, column: str = "terminal_id") -> str | None:
    terminal_id = str(getattr(ctx, "terminal_id", "") or "").strip() or None
    if terminal_id is None:
        return None
    if table:
        cols = _table_columns(ctx, table)
        if str(column or "").strip().lower() not in cols:
            return None
    return terminal_id


def _parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _clamp_window(from_date: dt.date, to_date: dt.date, max_days: int = 90) -> tuple[dt.date, dt.date]:
    if to_date < from_date:
        from_date, to_date = to_date, from_date
    if (to_date - from_date).days > max_days:
        from_date = to_date - dt.timedelta(days=max_days)
    return from_date, to_date


def sql_database(
    ctx: ToolContext,
    *,
    query: str,
    parameters: dict[str, Any] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Execute raw SQL against transaction data. 
    
    CRITICAL: You MUST include ':mid' (merchant_id placeholder) in your WHERE clause to scope to the active merchant. 
    Example: SELECT avg(amount_rupees) FROM transaction_features WHERE merchant_id = :mid
    Use this for complex calculations like average ticket size or specific filters not covered by other tools.
    """
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 100), 500))
    q = str(query or "").strip()

    def _schema_hint() -> dict[str, Any]:
        table = str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features"))
        cols = sorted(_table_columns(ctx, table))
        preferred = [c for c in ("merchant_id", "p_date", "status", "response_code", "payment_mode", "amount_rupees") if c in cols]
        return {
            "preferred_table": table,
            "preferred_columns": preferred or cols[:12],
            "scope_clause": "merchant_id = :mid",
        }

    def _error_code(exc_text: str) -> str:
        s = (exc_text or "").lower()
        if "no such table" in s or ("does not exist" in s and "relation" in s):
            return "undefined_table"
        if "no such column" in s or ("column" in s and "does not exist" in s):
            return "undefined_column"
        if "syntax error" in s:
            return "syntax"
        return "other"

    q = str(query or "").strip()
    if q.endswith(";"):
        q = q[:-1].strip()
    q_lower = q.lower()
    if not (q_lower.startswith("select") or q_lower.startswith("with ")):
        return {
            "verified": False,
            "rows": [],
            "row_count": 0,
            "evidence": [f"sql:{mid}:blocked"],
            "error_code": "other",
            "schema_hint": _schema_hint(),
            "error": "only read-only SELECT queries are allowed",
        }

    forbidden = re.compile(
        r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|merge|call|execute|vacuum)\b",
        re.IGNORECASE,
    )
    if forbidden.search(q):
        return {
            "verified": False,
            "rows": [],
            "row_count": 0,
            "evidence": [f"sql:{mid}:blocked"],
            "error_code": "other",
            "schema_hint": _schema_hint(),
            "error": "query contains non-read-only keywords",
        }

    if ":mid" not in q:
        return {
            "verified": False,
            "rows": [],
            "row_count": 0,
            "evidence": [f"sql:{mid}:blocked_scope"],
            "error_code": "scope_violation",
            "schema_hint": _schema_hint(),
            "error": "query must include ':mid' placeholder for merchant scoping",
        }

    wrapped = text(f"SELECT * FROM ({q}) AS scoped_query LIMIT :_copilot_limit")
    sql_params: dict[str, Any] = dict(parameters or {}) if isinstance(parameters, dict) else {}
    sql_params["mid"] = mid
    sql_params["_copilot_limit"] = limit

    try:
        with ctx.engine.connect() as conn:
            rows = conn.execute(wrapped, sql_params).mappings().all()
    except Exception as exc:
        err = str(exc)
        return {
            "verified": False,
            "rows": [],
            "row_count": 0,
            "evidence": [f"sql:{mid}:error"],
            "error_code": _error_code(err),
            "schema_hint": _schema_hint(),
            "error": err,
        }

    out = [dict(r) for r in rows]
    q_hash = hashlib.sha1(q.encode("utf-8")).hexdigest()[:12]
    return {
        "verified": True,
        "rows": out,
        "row_count": len(out),
        "evidence": [f"sql:{q_hash}"],
        "scope": {"merchant_id": mid},
        "error": None,
    }


def verify_failure_drivers(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    by: Literal["response_code", "payment_mode"] = "response_code",
    limit: int = 5,
) -> dict[str, Any]:
    """Deterministically verify top failure drivers for the active merchant."""
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 5), 20))
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=365)
    dimension = "payment_mode" if by == "payment_mode" else "response_code"
    terminal_id = _scoped_terminal_id(ctx, table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")), column="terminal_id")

    return transactions_repository.verify_failure_drivers(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        by=dimension,
        limit=limit,
        terminal_id=terminal_id,
        source_table=str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features")),
    )


def get_merchant_context(ctx: ToolContext) -> dict[str, Any]:
    """Return a compact merchant context snapshot."""
    return merchants_repository.fetch_merchant_context(ctx.engine, ctx.merchant_id)


def list_transactions(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    status: Literal["SUCCESS", "FAILURE", "ALL"] = "ALL",
    payment_mode: Literal["UPI", "CARD", "ALL"] = "ALL",
    limit: int = 50,
) -> dict[str, Any]:
    """List transactions from the configured fact table.

    This tool is intentionally narrow: scoped to merchant_id and limited rows.
    """
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 50), 200))

    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=90)
    terminal_id = _scoped_terminal_id(ctx, table=Config.QUERY_SOURCE_TABLE, column="terminal_id")

    return transactions_repository.list_transactions(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        status=status,
        payment_mode=payment_mode,
        limit=limit,
        terminal_id=terminal_id,
        source_table=Config.QUERY_SOURCE_TABLE,
    )


def compute_kpis(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    group_by: Literal["none", "day", "hour", "payment_mode", "status", "response_code"] = "none",
) -> dict[str, Any]:
    """Compute aggregate KPIs (GMV, success rate) with flexible grouping.
    
    Use group_by='hour' to identify PEAK HOURS or transaction distribution by time of day.
    Use group_by='day' for revenue trends.
    Use group_by='payment_mode' to compare UPI vs CARD performance.
    """
    mid = ctx.merchant_id
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=180)
    table = str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features"))
    terminal_id = _scoped_terminal_id(ctx, table=table, column="terminal_id")

    return transactions_repository.compute_kpis(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        group_by=group_by,
        terminal_id=terminal_id,
        source_table=table,
    )


def list_settlements(ctx: ToolContext, *, from_date: str, to_date: str, limit: int = 50) -> dict[str, Any]:
    """List settlements for the merchant.

    Supports multiple demo schemas:
    - Newer schema: settlements(merchant_id, status, expected_date, settled_at, amount_rupees)
    - Legacy schema: settlements(mid, settlement_status, settlement_date, net_settlement_amount, created_at, ...)
    """
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 50), 200))
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=365)

    return settlements_repository.list_settlements(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        limit=limit,
    )


def cashflow_snapshot(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    """Summarize settlement timing and cashflow signals in a window.

    Works across settlements table variants by detecting columns.

    Output is designed for a cashflow assistant:
    - counts of settlements by status
    - how many are past expected date (best-effort)
    - total amount pending vs settled
    - recent settlements list (top 10)

    Read-only diagnostic; no deterministic actions.
    """
    mid = ctx.merchant_id
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=365)

    return settlements_repository.cashflow_snapshot(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
    )


def explain_settlement_shortfall(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    expected_amount: float | None = None,
    received_amount: float | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Explain a payout shortfall using settlement-level deduction fields when available.

    The legacy demo schema exposes the most useful columns for this flow:
    gross_amount, mdr_deducted, gst_on_mdr, tds_deducted, chargeback_deductions,
    reserve_held, adjustment_amount, net_settlement_amount.

    If an exact payout cannot be matched, the tool returns the closest settlement in-window
    and marks the result unverified.
    """
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 20), 100))
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=365)
    expected_amount = _to_float(expected_amount)
    received_amount = _to_float(received_amount)
    return settlements_repository.explain_settlement_shortfall(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        expected_amount=expected_amount,
        received_amount=received_amount,
        limit=limit,
    )


def list_chargebacks(
    ctx: ToolContext,
    *,
    status: Literal["open", "closed", "all"] = "open",
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 50), 200))

    normalized_from = None
    normalized_to = None
    if from_date and to_date:
        d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=365)
        normalized_from = str(d1)
        normalized_to = str(d2)
    return disputes_repository.list_chargebacks(
        ctx.engine,
        merchant_id=mid,
        status=status,
        from_date=normalized_from,
        to_date=normalized_to,
        limit=limit,
    )


def terminal_health_summary(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    group_by: Literal["tid", "hour", "tid_hour"] = "tid_hour",
    limit: int = 50,
) -> dict[str, Any]:
    """Summarize terminal health signals (network/battery/printer/etc.) for RCA.

    Uses terminal_health_snapshots joined with terminals to enforce merchant scoping.
    """
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 50), 200))
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=90)
    terminal_id = _scoped_terminal_id(ctx)

    return terminals_repository.terminal_health_summary(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        group_by=group_by,
        limit=limit,
        terminal_id=terminal_id,
    )


def geo_drift_check(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    tid: str | None = None,
) -> dict[str, Any]:
    """Check whether terminals show location drift / deviation flags.

    Uses terminal_health_snapshots latitude/longitude + deviation boolean when available.
    """
    mid = ctx.merchant_id
    scoped_tid = tid or _scoped_terminal_id(ctx)
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=90)
    return terminals_repository.geo_drift_check(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        terminal_id=str(scoped_tid) if scoped_tid else None,
    )


def terminal_issue_correlator(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    flag: Literal["low_network_strength", "quick_battery_drainage", "geo_deviation"] = "low_network_strength",
    limit: int = 20,
) -> dict[str, Any]:
    """Correlate terminal health flags with payment failure rate by terminal.

    Returns terminals where health issues co-occur with elevated failure rates.
    """
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 20), 50))
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=90)
    terminal_id = _scoped_terminal_id(ctx, table=Config.QUERY_SOURCE_TABLE, column="terminal_id")

    return terminals_repository.terminal_issue_correlator(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        flag=flag,
        limit=limit,
        terminal_id=terminal_id,
        source_table=Config.QUERY_SOURCE_TABLE,
    )


def intelligence_probe(
    ctx: ToolContext,
    *,
    window_days: int = 30,
    enable_reasoning: bool = False,
) -> dict[str, Any]:
    """Run the intelligence pipeline to generate signals + recommendations (evidence-first).

    - Default is phase-1 only (deterministic engines) to avoid nested LLM calls.
    - Set enable_reasoning=true to allow phase-2 reasoning (slower, uses LLM).

    Returns a compact summary suitable for RCA and proactive insights.
    """
    mid = ctx.merchant_id
    window_days = max(7, min(int(window_days or 30), 90))

    payload = run_intelligence(
        ctx.engine,
        mid,
        window_days=window_days,
        enable_phase2_reasoning=bool(enable_reasoning),
        persist_actions=False,
    )

    recos = payload.get("recommendations") or payload.get("recos") or []
    recos_normalized: list[dict[str, Any]] = []
    for r in recos:
        if isinstance(r, dict):
            recos_normalized.append(r)
        elif is_dataclass(r):
            recos_normalized.append(asdict(r))
    # Keep output small: top 8 by priority if present.
    try:
        recos_sorted = sorted(recos_normalized, key=lambda r: float(r.get("priority_score") or 0), reverse=True)
    except Exception:
        recos_sorted = recos_normalized

    recos_out = []
    for r in recos_sorted[:8]:
        recos_out.append(
            {
                "category": r.get("category"),
                "title": r.get("title"),
                "summary": r.get("summary"),
                "impact_rupees": r.get("impact_rupees"),
                "confidence": r.get("confidence"),
                "priority_score": r.get("priority_score"),
                "drivers": r.get("drivers"),
                "actions": r.get("actions"),
                "evidence_ids": r.get("evidence_ids") or [],
                "metadata": r.get("metadata") or {},
            }
        )

    evidence: list[str] = [f"intel:{mid}:{window_days}d"]
    for r in recos_out:
        for ev in r.get("evidence_ids") or []:
            if ev and ev not in evidence:
                evidence.append(str(ev))

    return {
        "merchant_id": mid,
        "window_days": window_days,
        "recommendations": recos_out,
        "evidence": evidence[:120],
        "raw_keys": sorted([k for k in payload.keys()]) if isinstance(payload, dict) else [],
    }


def assess_credit_fit(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    """Assess whether the merchant appears to fit a volume-based bank working-capital product.

    This is **not** an underwriting decision. It returns a transparent feature summary and
    a configurable fit band based on policy knobs in Config.

    Returns evidence IDs for traceability.
    """
    mid = ctx.merchant_id
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=180)

    # Core KPI snapshot over the window
    kpis = compute_kpis(ctx, from_date=str(d1), to_date=str(d2), group_by="none")
    row0 = (kpis.get("rows") or [{}])[0] if isinstance(kpis, dict) else {}

    attempts = int(row0.get("attempts") or 0)
    success_txns = int(row0.get("success_txns") or 0)
    success_rate = float(row0.get("success_rate_pct") or 0.0)
    success_gmv = float(row0.get("success_gmv") or 0.0)

    # Daily success GMV stability (coefficient of variation)
    cv = None
    daily = transactions_repository.daily_success_gmv(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        source_table=Config.QUERY_SOURCE_TABLE,
    )
    try:
        vals = np.array([float(row.get("success_gmv") or 0.0) for row in daily], dtype=float)
        if len(vals) >= 3 and float(vals.mean()) > 0:
            cv = float(vals.std(ddof=0) / (vals.mean() + 1e-9))
    except Exception:
        cv = None

    # Refund rate (by count and value) in window
    refund_summary = disputes_repository.refund_summary(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
    )
    refunds_count = int(refund_summary.get("refunds_count") or 0)
    refunds_amount = float(refund_summary.get("refunds_amount") or 0.0)
    refunds_rate_pct = None
    if success_txns > 0:
        refunds_rate_pct = round(100.0 * refunds_count / success_txns, 2)

    # Chargebacks count in window
    chargebacks_count = disputes_repository.chargeback_count(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
    )

    # Configurable fit checks (policy knobs)
    policy = {
        "min_success_gmv": float(getattr(Config, "CREDIT_MIN_SUCCESS_GMV_30D", 0.0)),
        "min_success_rate_pct": float(getattr(Config, "CREDIT_MIN_SUCCESS_RATE_PCT_30D", 0.0)),
        "max_chargebacks": int(getattr(Config, "CREDIT_MAX_CHARGEBACKS_30D", 999999)),
        "max_refunds_rate_pct": float(getattr(Config, "CREDIT_MAX_REFUNDS_RATE_PCT_30D", 100.0)),
        "max_daily_gmv_cv": float(getattr(Config, "CREDIT_MAX_DAILY_GMV_CV_30D", 999999.0)),
    }

    checks = {
        "volume_ok": success_gmv >= policy["min_success_gmv"],
        "success_rate_ok": success_rate >= policy["min_success_rate_pct"],
        "refunds_ok": (refunds_rate_pct is None) or (refunds_rate_pct <= policy["max_refunds_rate_pct"]),
        "chargebacks_ok": (chargebacks_count is None) or (chargebacks_count <= policy["max_chargebacks"]),
        "stability_ok": (cv is None) or (cv <= policy["max_daily_gmv_cv"]),
    }

    passed = sum(1 for v in checks.values() if v)
    # Soft banding; not an approval.
    if passed >= 4:
        fit_band = "likely_fit"
    elif passed >= 2:
        fit_band = "possible_fit"
    else:
        fit_band = "unlikely_fit"

    evidence: list[str] = []
    if isinstance(kpis, dict) and isinstance(kpis.get("evidence"), list):
        evidence.extend([str(x) for x in kpis.get("evidence")])
    evidence.append(f"credit_fit:{str(d1)}:{str(d2)}")

    return {
        "window": {"from": str(d1), "to": str(d2)},
        "kpis": {
            "attempts": attempts,
            "success_txns": success_txns,
            "success_rate_pct": success_rate,
            "success_gmv": success_gmv,
        },
        "stability": {
            "daily_success_gmv_cv": cv,
        },
        "risk_proxies": {
            "refunds_count": refunds_count,
            "refunds_amount": refunds_amount,
            "refunds_rate_pct_of_success_txns": refunds_rate_pct,
            "chargebacks_count": chargebacks_count,
        },
        "policy": policy,
        "checks": checks,
        "fit_band": fit_band,
        "evidence": evidence[:80],
        "disclaimer": "This is an indicative fit assessment for demo purposes, not a credit decision.",
    }


def list_refunds(ctx: ToolContext, *, from_date: str, to_date: str, limit: int = 50) -> dict[str, Any]:
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 50), 200))
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=365)
    return disputes_repository.list_refunds(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        limit=limit,
    )


def get_transaction_detail(ctx: ToolContext, *, tx_id: str) -> dict[str, Any]:
    """Fetch a single transaction, merchant-scoped."""
    mid = ctx.merchant_id
    tx_id = str(tx_id or "").strip()
    if not tx_id:
        return {"row": None, "evidence": [], "error": "tx_id is required"}

    return transactions_repository.get_transaction_detail(
        ctx.engine,
        merchant_id=mid,
        tx_id=tx_id,
        source_table=Config.QUERY_SOURCE_TABLE,
    )


def get_settlement_detail(ctx: ToolContext, *, settlement_id: str) -> dict[str, Any]:
    """Fetch a single settlement with optional reconciliation context.

    Supports newer and legacy settlements schemas (see list_settlements).
    """
    mid = ctx.merchant_id
    settlement_id = str(settlement_id or "").strip()
    if not settlement_id:
        return {"row": None, "evidence": [], "error": "settlement_id is required"}

    return settlements_repository.get_settlement_detail(
        ctx.engine,
        merchant_id=mid,
        settlement_id=settlement_id,
    )


def get_chargeback_detail(ctx: ToolContext, *, chargeback_id: str) -> dict[str, Any]:
    mid = ctx.merchant_id
    chargeback_id = str(chargeback_id or "").strip()
    if not chargeback_id:
        return {"row": None, "evidence": [], "error": "chargeback_id is required"}

    return disputes_repository.get_chargeback_detail(
        ctx.engine,
        merchant_id=mid,
        chargeback_id=chargeback_id,
    )


def compare_kpis(
    ctx: ToolContext,
    *,
    from_date_a: str,
    to_date_a: str,
    from_date_b: str,
    to_date_b: str,
    group_by: Literal["none", "day", "hour", "payment_mode", "status", "response_code"] = "payment_mode",
) -> dict[str, Any]:
    """Compare KPIs between two windows (A vs B)."""
    a = compute_kpis(ctx, from_date=from_date_a, to_date=to_date_a, group_by=group_by)
    b = compute_kpis(ctx, from_date=from_date_b, to_date=to_date_b, group_by=group_by)
    evidence = []
    for src in (a, b):
        if isinstance(src, dict) and isinstance(src.get("evidence"), list):
            evidence.extend([str(x) for x in src.get("evidence")])
    return {"a": a, "b": b, "evidence": evidence[:50], "group_by": group_by}


def terminal_performance(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    limit: int = 10,
) -> dict[str, Any]:
    """Rank terminals by attempts and success rate (if terminal_id exists in source table)."""
    mid = ctx.merchant_id
    limit = max(1, min(int(limit or 10), 50))
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=180)
    terminal_id = _scoped_terminal_id(ctx, table=Config.QUERY_SOURCE_TABLE, column="terminal_id")

    return transactions_repository.terminal_performance(
        ctx.engine,
        merchant_id=mid,
        from_date=str(d1),
        to_date=str(d2),
        limit=limit,
        terminal_id=terminal_id,
        source_table=Config.QUERY_SOURCE_TABLE,
    )


def end_to_end_analysis(
    ctx: ToolContext,
    *,
    from_date: str,
    to_date: str,
    limit: int = 10,
) -> dict[str, Any]:
    """Run an end-to-end performance/health analysis bundle.

    This is a merchant-friendly, proactive "health check" that covers:
    - overall KPIs
    - payment mode performance
    - top failure response codes
    - terminal health (if terminal_id present)
    - card network / device / POS slices (best-effort if columns exist)

    It is intentionally best-effort: missing columns/tables return null sections.
    """
    mid = ctx.merchant_id
    d1, d2 = _clamp_window(_parse_date(from_date), _parse_date(to_date), max_days=180)
    limit = max(1, min(int(limit or 10), 50))

    bundle: dict[str, Any] = {
        "window": {"from": str(d1), "to": str(d2)},
        "overall": None,
        "by_payment_mode": None,
        "top_failure_codes": None,
        "terminal_health": None,
        "by_card_network": None,
        "by_pos_type": None,
        "by_device_type": None,
        "evidence": [],
    }

    # Core KPIs
    try:
        overall = compute_kpis(ctx, from_date=str(d1), to_date=str(d2), group_by="none")
        by_pm = compute_kpis(ctx, from_date=str(d1), to_date=str(d2), group_by="payment_mode")
        bundle["overall"] = overall
        bundle["by_payment_mode"] = by_pm
        for src in (overall, by_pm):
            if isinstance(src, dict) and isinstance(src.get("evidence"), list):
                bundle["evidence"].extend([str(x) for x in src["evidence"]])
    except Exception as exc:
        bundle["overall_error"] = str(exc)

    try:
        bundle["top_failure_codes"] = transactions_repository.top_failure_codes(
            ctx.engine,
            merchant_id=mid,
            from_date=str(d1),
            to_date=str(d2),
            limit=limit,
            source_table=Config.QUERY_SOURCE_TABLE,
        )
        bundle["evidence"].append(f"failcodes:{str(d1)}:{str(d2)}")
    except Exception:
        bundle["top_failure_codes"] = None

    # Terminal health (reuse tool)
    try:
        bundle["terminal_health"] = terminal_performance(ctx, from_date=str(d1), to_date=str(d2), limit=limit)
        if isinstance(bundle["terminal_health"], dict) and isinstance(bundle["terminal_health"].get("evidence"), list):
            bundle["evidence"].extend([str(x) for x in bundle["terminal_health"]["evidence"]])
    except Exception:
        bundle["terminal_health"] = None

    # Best-effort slices: card_network / pos_type / device_type
    def _slice(col: str) -> list[dict[str, Any]] | None:
        try:
            return transactions_repository.slice_performance_by_column(
                ctx.engine,
                merchant_id=mid,
                from_date=str(d1),
                to_date=str(d2),
                column=col,
                limit=limit,
                source_table=Config.QUERY_SOURCE_TABLE,
            )
        except Exception:
            return None

    bundle["by_card_network"] = _slice("card_network")
    bundle["by_pos_type"] = _slice("pos_type")
    bundle["by_device_type"] = _slice("device_type")

    # Dedup evidence
    ev = []
    seen = set()
    for x in bundle.get("evidence") or []:
        if x and x not in seen:
            seen.add(x)
            ev.append(x)
    bundle["evidence"] = ev[:120]

    return bundle


def propose_and_create_merchant_action(
    ctx: ToolContext,
    *,
    action_type: str,
    payload: dict[str, Any] | None = None,
    confirmation_token: str | None = None,
) -> dict[str, Any]:
    """Two-step action creator.

    Step 1 (no token): returns preview + token
    Step 2 (with token): writes to merchant_actions (if table exists) and returns action_id

    Not production-secure: token is just a JSON echo.
    """
    mid = ctx.merchant_id
    payload = payload or {}

    preview = {
        "merchant_id": mid,
        "action_type": str(action_type or "").strip().upper(),
        "payload": payload,
    }

    if not confirmation_token:
        token = json.dumps(preview, ensure_ascii=False, default=str)
        return {"preview": preview, "confirmation_token": token, "evidence": []}

    # Confirm path: best-effort parse and insert.
    try:
        requested = json.loads(str(confirmation_token))
        if isinstance(requested, dict):
            preview = requested
    except Exception:
        pass

    return action_repository.create_merchant_action(
        ctx.engine,
        merchant_id=mid,
        preview=preview,
    )


def get_merchant_lending_offers(ctx: ToolContext) -> dict[str, Any]:
    """Retrieve scoring and dynamic eligibility limits for LACR and Overdraft loans.

    This tool evaluates 30-day processed volumes, chargebacks, holds, and MCC risk
    profile to return a 0-100 merchant score, an eligibility tier, and pre-approved or 
    subject-to-review limits for LACR (Loan Against Credit Receivables) and Overdraft. 
    """
    from app.growth.lending_engine import get_lending_offers
    try:
        return get_lending_offers(ctx.engine, ctx.merchant_id)
    except Exception as exc:
        return {"error": f"Failed to retrieve lending offers: {exc}", "evidence": []}


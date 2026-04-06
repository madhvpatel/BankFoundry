from __future__ import annotations

from typing import Any

from sqlalchemy import text

from app.copilot.tools import ToolContext, explain_settlement_shortfall
from config import Config

from .source_adapters import resolve_settlement_source


def _table_columns(engine: Any, table: str) -> set[str]:
    table = str(table or "").strip()
    if not table:
        return set()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :t
                    """
                ),
                {"t": table},
            ).fetchall()
        cols = {str(r[0]).lower() for r in rows if r and r[0]}
        if cols:
            return cols
    except Exception:
        pass

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {str(r[1]).lower() for r in rows if len(r) > 1 and r[1]}
    except Exception:
        return set()


def _fmt_inr(value: Any) -> str:
    try:
        return f"Rs {float(value or 0.0):,.2f}"
    except Exception:
        return "Rs 0.00"


def _resolve_min_difference_rupees(engine: Any, merchant_id: str, requested: float | None = None) -> float:
    if requested is not None:
        return max(float(requested), 0.0)

    cols = _table_columns(engine, "merchants")
    threshold_col = ""
    for candidate in (
        "payout_shortfall_threshold_rupees",
        "shortfall_alert_threshold_rupees",
        "proactive_shortfall_threshold_rupees",
    ):
        if candidate in cols:
            threshold_col = candidate
            break
    merchant_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if threshold_col and merchant_col:
        try:
            with engine.connect() as conn:
                value = conn.execute(
                    text(
                        f"""
                        SELECT {threshold_col}
                        FROM merchants
                        WHERE {merchant_col} = :mid
                        LIMIT 1
                        """
                    ),
                    {"mid": merchant_id},
                ).scalar()
            if value is not None:
                return max(float(value), 0.0)
        except Exception:
            pass
    return max(float(getattr(Config, "PAYOUT_SHORTFALL_MIN_DIFFERENCE_RUPEES", 1000.0) or 1000.0), 0.0)


def _build_card(alert: dict[str, Any]) -> dict[str, Any]:
    shortfall = alert.get("shortfall") if isinstance(alert.get("shortfall"), dict) else {}
    settlement_id = shortfall.get("settlement_id") or "unknown"
    difference_amount = float(shortfall.get("difference_amount") or 0.0)
    matched = bool(shortfall.get("matched_user_amounts"))
    verified = bool(alert.get("verified"))
    status_label = "Verified payout shortfall" if verified else "Payout shortfall detected"
    title = f"{status_label}: settlement {settlement_id}"
    card_type = "warning" if verified else "info"
    confidence = 0.98 if verified else 0.72
    verification_status = (
        "Verified - deterministic payout shortfall attribution succeeded"
        if verified
        else "Unverified (supported) - payout shortfall matched a settlement but not all deductions were reconciled"
    )
    return {
        "id": f"payout_shortfall_{settlement_id}",
        "lane": "operations",
        "icon": "💸",
        "title": title,
        "type": card_type,
        "confidence": confidence,
        "impact_rupees": difference_amount,
        "actions": list(alert.get("recommended_actions") or []),
        "drivers": [
            f"Expected payout: {_fmt_inr(shortfall.get('expected_amount'))}",
            f"Received payout: {_fmt_inr(shortfall.get('received_amount'))}",
            f"Difference: {_fmt_inr(difference_amount)}",
        ],
        "body": str(alert.get("deduction_explanation") or alert.get("summary") or "").strip(),
        "verification_status": verification_status,
        "source": "payout_shortfall_monitor",
        "payload": {
            "monitor_type": "payout_shortfall",
            "shortfall": shortfall,
            "matched_user_amounts": matched,
        },
    }


def _build_action(alert: dict[str, Any]) -> dict[str, Any]:
    shortfall = alert.get("shortfall") if isinstance(alert.get("shortfall"), dict) else {}
    settlement_id = shortfall.get("settlement_id") or "unknown"
    difference_amount = float(shortfall.get("difference_amount") or 0.0)
    verified = bool(alert.get("verified"))
    description = str(alert.get("deduction_explanation") or alert.get("summary") or "").strip()
    evidence_payload = {
        "source": "payout_shortfall_monitor",
        "shortfall": shortfall,
        "summary": str(alert.get("summary") or "").strip(),
        "deduction_explanation": description,
        "recommended_actions": list(alert.get("recommended_actions") or []),
        "evidence_ids": list(alert.get("evidence") or []),
    }
    return {
        "category": "reconciliation",
        "title": f"Investigate payout shortfall for settlement {settlement_id}",
        "description": description,
        "impact_rupees": difference_amount,
        "confidence": 0.95 if verified else 0.7,
        "priority_score": max(difference_amount, 1.0) * (0.95 if verified else 0.7),
        "owner": "merchant_ops",
        "source": "payout_shortfall_monitor",
        "evidence_ids": list(alert.get("evidence") or []),
        "workflow_steps": [{"who": "merchant", "text": str(item)} for item in list(alert.get("recommended_actions") or [])[:3]],
        "evidence": evidence_payload,
    }


def generate_payout_shortfall_alerts(
    engine: Any,
    merchant_id: str,
    *,
    window_from: str,
    window_to: str,
    limit: int = 3,
    min_difference_rupees: float = 1000.0,
) -> list[dict[str, Any]]:
    threshold = _resolve_min_difference_rupees(engine, merchant_id, min_difference_rupees)
    provider = resolve_settlement_source(engine)
    if not provider.has("merchant_id") or not provider.has("scope_date") or not provider.has("gross_amount") or not provider.has("net_settlement_amount"):
        return []

    sql = text(
        f"""
        SELECT
          {provider.select('settlement_id', alias='settlement_id', null_if_missing=True)},
          {provider.select('scope_date', alias='settlement_date', null_if_missing=True)},
          {provider.select('gross_amount', alias='gross_amount')},
          {provider.select('net_settlement_amount', alias='net_settlement_amount')}
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND DATE({provider.value('scope_date')}) >= :d1
          AND DATE({provider.value('scope_date')}) < :d2
          AND {provider.value('gross_amount')} IS NOT NULL
          AND {provider.value('net_settlement_amount')} IS NOT NULL
          AND ({provider.value('gross_amount')} - {provider.value('net_settlement_amount')}) >= :min_diff
        ORDER BY ({provider.value('gross_amount')} - {provider.value('net_settlement_amount')}) DESC, DATE({provider.value('scope_date')}) DESC
        LIMIT :limit
        """
    )

    with engine.connect() as conn:
        candidates = [dict(r) for r in conn.execute(
            sql,
            {
                "mid": merchant_id,
                "d1": window_from,
                "d2": window_to,
                "min_diff": float(threshold),
                "limit": max(1, min(int(limit or 3), 10)),
            },
        ).mappings().all()]

    if not candidates:
        return []

    ctx = ToolContext(engine=engine, merchant_id=merchant_id)
    alerts: list[dict[str, Any]] = []
    for row in candidates:
        explained = explain_settlement_shortfall(
            ctx,
            from_date=window_from,
            to_date=window_to,
            expected_amount=float(row.get("gross_amount") or 0.0),
            received_amount=float(row.get("net_settlement_amount") or 0.0),
            limit=20,
        )
        shortfall = explained.get("shortfall") if isinstance(explained.get("shortfall"), dict) else {}
        if not shortfall:
            continue
        alert = dict(explained)
        alert["card"] = _build_card(alert)
        alert["action"] = _build_action(alert)
        alerts.append(alert)
    return alerts

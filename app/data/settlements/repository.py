from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import text

from app.data.merchant_ops import repository as merchant_ops_repository
from app.data.providers import ResolvedSource, resolve_settlement_provider


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _parse_date(value: Any) -> dt.date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return dt.date.fromisoformat(raw[:10])
    except Exception:
        return None


def _pending_status_clause(column: str) -> str:
    return (
        f"UPPER(COALESCE({column}, '')) LIKE 'PENDING%' OR "
        f"UPPER(COALESCE({column}, '')) LIKE 'HELD%' OR "
        f"UPPER(COALESCE({column}, '')) LIKE 'ON_HOLD%' OR "
        f"UPPER(COALESCE({column}, '')) LIKE 'HOLD%' OR "
        f"UPPER(COALESCE({column}, '')) LIKE 'PROCESSING%'"
    )


def _settled_status_clause(column: str) -> str:
    return (
        f"UPPER(COALESCE({column}, '')) LIKE 'SETTLED%' OR "
        f"UPPER(COALESCE({column}, '')) LIKE 'PAID%' OR "
        f"UPPER(COALESCE({column}, '')) LIKE 'SUCCESS%' OR "
        f"UPPER(COALESCE({column}, '')) LIKE 'PROCESSED%'"
    )


def _settlement_provider(engine: Any) -> ResolvedSource:
    return resolve_settlement_provider(engine)


def _settlement_select_exprs(provider: ResolvedSource) -> list[str]:
    return [
        provider.select("settlement_id", alias="settlement_id"),
        provider.select("merchant_id", alias="merchant_id"),
        provider.select("status", alias="status", null_if_missing=True),
        provider.select("expected_date", alias="expected_date", null_if_missing=True),
        provider.select("settled_at", alias="settled_at", null_if_missing=True),
        provider.select("amount_rupees", alias="amount_rupees", null_if_missing=True),
        provider.select("currency", alias="currency", null_if_missing=True),
        provider.select("reference", alias="reference", null_if_missing=True),
        provider.select("gross_amount", alias="gross_amount", null_if_missing=True),
        provider.select("net_settlement_amount", alias="net_settlement_amount", null_if_missing=True),
        provider.select("mdr_deducted", alias="mdr_deducted", null_if_missing=True),
        provider.select("gst_on_mdr", alias="gst_on_mdr", null_if_missing=True),
        provider.select("tds_deducted", alias="tds_deducted", null_if_missing=True),
        provider.select("chargeback_deductions", alias="chargeback_deductions", null_if_missing=True),
        provider.select("reserve_held", alias="reserve_held", null_if_missing=True),
        provider.select("adjustment_amount", alias="adjustment_amount", null_if_missing=True),
        provider.select("hold_reason", alias="hold_reason", null_if_missing=True),
        provider.select("payment_mode", alias="payment_mode", null_if_missing=True),
        provider.select("txn_count", alias="txn_count", null_if_missing=True),
        provider.select("refund_count", alias="refund_count", null_if_missing=True),
    ]


def _fetch_settlement_row(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
) -> dict[str, Any] | None:
    provider = _settlement_provider(engine)
    if provider.missing("merchant_id", "settlement_id"):
        return None
    query = text(
        f"""
        SELECT
          {", ".join(_settlement_select_exprs(provider))}
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('settlement_id')} = :sid
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"mid": merchant_id, "sid": settlement_id}).mappings().first()
    return dict(row) if row else None


def _fetch_reconciliation_rows(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    cols = merchant_ops_repository.table_columns(engine, "reconciliation_records")
    merchant_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    settlement_col = "settlement_id" if "settlement_id" in cols else ""
    status_col = "status" if "status" in cols else ("recon_status" if "recon_status" in cols else "")
    reason_col = "reason" if "reason" in cols else ("exception_reason" if "exception_reason" in cols else "")
    if not merchant_col or not settlement_col:
        return None, "reconciliation_records does not expose merchant_id and settlement_id columns"
    if not status_col and not reason_col:
        return None, "reconciliation_records does not expose status or reason columns"

    select_status = f"{status_col} AS status" if status_col else "NULL AS status"
    select_reason = f"{reason_col} AS reason" if reason_col else "NULL AS reason"
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT
                      {select_status},
                      {select_reason},
                      CAST(COUNT(*) AS INTEGER) AS count
                    FROM reconciliation_records
                    WHERE {merchant_col} = :mid
                      AND {settlement_col} = :sid
                    GROUP BY 1, 2
                    ORDER BY count DESC
                    LIMIT 10
                    """
                ),
                {"mid": merchant_id, "sid": settlement_id},
            ).mappings().all()
    except Exception as exc:
        return None, str(exc)
    return [dict(row) for row in rows], None


def _deduction_components(row: dict[str, Any]) -> tuple[list[dict[str, Any]], float]:
    component_defs = [
        ("MDR", _to_float(row.get("mdr_deducted"))),
        ("GST on MDR", _to_float(row.get("gst_on_mdr"))),
        ("TDS", _to_float(row.get("tds_deducted"))),
        ("Chargeback deductions", _to_float(row.get("chargeback_deductions"))),
        ("Reserve held", _to_float(row.get("reserve_held"))),
        ("Adjustments", _to_float(row.get("adjustment_amount"))),
    ]
    components: list[dict[str, Any]] = []
    explained_amount = 0.0
    for label, raw_value in component_defs:
        if raw_value is None or abs(raw_value) < 0.005:
            continue
        kind = "credit" if raw_value < 0 else "deduction"
        components.append(
            {
                "label": ("Adjustment credit" if label == "Adjustments" and raw_value < 0 else label),
                "kind": kind,
                "amount_rupees": round(abs(raw_value), 2),
                "signed_amount_rupees": round(raw_value, 2),
            }
        )
        explained_amount += raw_value
    return components, round(explained_amount, 2)


def _dedupe_evidence(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text_value = str(value or "").strip()
        if text_value and text_value not in seen:
            seen.add(text_value)
            out.append(text_value)
    return out


def list_settlements(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    limit: int = 50,
) -> dict[str, Any]:
    provider = _settlement_provider(engine)
    missing = provider.missing("merchant_id", "settlement_id", "scope_date")
    if missing:
        return {
            "rows": [],
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "error": f"{provider.source_table or 'settlement source'} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }
    params = {
        "mid": merchant_id,
        "d1": from_date,
        "d2": to_date,
        "limit": max(1, min(int(limit or 50), 200)),
    }
    query = text(
        f"""
        SELECT
          {provider.select('settlement_id', alias='settlement_id')},
          {provider.select('status', alias='status', null_if_missing=True)},
          {provider.select('expected_date', alias='expected_date', null_if_missing=True)},
          {provider.select('settled_at', alias='settled_at', null_if_missing=True)},
          {provider.select('amount_rupees', alias='amount_rupees', null_if_missing=True)}
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('scope_date')} >= :d1
          AND {provider.value('scope_date')} < :d2
        ORDER BY {provider.value('scope_date')} DESC
        LIMIT :limit
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    out = [dict(row) for row in rows]
    evidence = [f"settlement:{row.get('settlement_id')}" for row in out if row.get("settlement_id")]
    return {
        "rows": out,
        "evidence": evidence,
        "window": {"from": from_date, "to": to_date},
        "notes": list(provider.notes),
    }


def cashflow_snapshot(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    provider = _settlement_provider(engine)

    snapshot: dict[str, Any] = {
        "window": {"from": from_date, "to": to_date},
        "by_status": None,
        "past_expected": None,
        "amounts": None,
        "recent": None,
        "evidence": [f"cashflow:{from_date}:{to_date}"],
        "notes": list(provider.notes),
    }
    missing = provider.missing("merchant_id", "scope_date", "status", "amount_rupees")
    if missing:
        snapshot["by_status_error"] = f"{provider.source_table or 'settlement source'} is missing canonical fields: {', '.join(sorted(missing))}"
        return snapshot

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT
                      COALESCE({provider.value('status')}, 'UNKNOWN') AS status,
                      CAST(COUNT(*) AS INTEGER) AS count,
                      COALESCE(SUM({provider.value('amount_rupees')}),0) AS amount
                    FROM {provider.source_table}
                    WHERE {provider.value('merchant_id')} = :mid
                      AND {provider.value('scope_date')} >= :d1
                      AND {provider.value('scope_date')} < :d2
                    GROUP BY 1
                    ORDER BY count DESC
                    """
                ),
                {"mid": merchant_id, "d1": from_date, "d2": to_date},
            ).mappings().all()
        snapshot["by_status"] = [dict(row) for row in rows]
    except Exception as exc:
        snapshot["by_status_error"] = str(exc)

    try:
        with engine.connect() as conn:
            if provider.has("expected_date") and provider.has("settled_at"):
                row = conn.execute(
                    text(
                        f"""
                        SELECT
                          CAST(COUNT(*) AS INTEGER) AS past_expected_count,
                          COALESCE(SUM({provider.value('amount_rupees')}),0) AS past_expected_amount
                        FROM {provider.source_table}
                        WHERE {provider.value('merchant_id')} = :mid
                          AND {provider.value('expected_date')} IS NOT NULL
                          AND {provider.value('expected_date')} < CURRENT_DATE
                          AND {provider.value('settled_at')} IS NULL
                        """
                    ),
                    {"mid": merchant_id},
                ).mappings().first()
            else:
                row = conn.execute(
                    text(
                        f"""
                        SELECT
                          CAST(COUNT(*) AS INTEGER) AS past_expected_count,
                          COALESCE(SUM({provider.value('amount_rupees')}),0) AS past_expected_amount
                        FROM {provider.source_table}
                        WHERE {provider.value('merchant_id')} = :mid
                          AND {provider.value('scope_date')} IS NOT NULL
                          AND {provider.value('scope_date')} < CURRENT_DATE
                          AND ({_pending_status_clause(provider.value('status'))})
                        """
                    ),
                    {"mid": merchant_id},
                ).mappings().first()
        snapshot["past_expected"] = dict(row) if row else None
    except Exception as exc:
        snapshot["past_expected_error"] = str(exc)

    try:
        with engine.connect() as conn:
            if provider.has("settled_at"):
                row = conn.execute(
                    text(
                        f"""
                        SELECT
                          COALESCE(SUM(CASE WHEN {provider.value('settled_at')} IS NULL THEN {provider.value('amount_rupees')} ELSE 0 END),0) AS pending_amount,
                          COALESCE(SUM(CASE WHEN {provider.value('settled_at')} IS NOT NULL THEN {provider.value('amount_rupees')} ELSE 0 END),0) AS settled_amount
                        FROM {provider.source_table}
                        WHERE {provider.value('merchant_id')} = :mid
                          AND {provider.value('scope_date')} >= :d1
                          AND {provider.value('scope_date')} < :d2
                        """
                    ),
                    {"mid": merchant_id, "d1": from_date, "d2": to_date},
                ).mappings().first()
            else:
                row = conn.execute(
                    text(
                        f"""
                        SELECT
                          COALESCE(SUM(CASE WHEN ({_pending_status_clause(provider.value('status'))}) THEN {provider.value('amount_rupees')} ELSE 0 END),0) AS pending_amount,
                          COALESCE(SUM(CASE WHEN ({_settled_status_clause(provider.value('status'))}) THEN {provider.value('amount_rupees')} ELSE 0 END),0) AS settled_amount
                        FROM {provider.source_table}
                        WHERE {provider.value('merchant_id')} = :mid
                          AND {provider.value('scope_date')} >= :d1
                          AND {provider.value('scope_date')} < :d2
                        """
                    ),
                    {"mid": merchant_id, "d1": from_date, "d2": to_date},
                ).mappings().first()
        snapshot["amounts"] = dict(row) if row else None
    except Exception as exc:
        snapshot["amounts_error"] = str(exc)

    try:
        with engine.connect() as conn:
            recent = conn.execute(
                text(
                    f"""
                    SELECT
                      {provider.select('settlement_id', alias='settlement_id')},
                      {provider.select('status', alias='status', null_if_missing=True)},
                      {provider.select('expected_date', alias='expected_date', null_if_missing=True)},
                      {provider.select('settled_at', alias='settled_at', null_if_missing=True)},
                      {provider.select('amount_rupees', alias='amount_rupees', null_if_missing=True)}
                    FROM {provider.source_table}
                    WHERE {provider.value('merchant_id')} = :mid
                    ORDER BY {provider.value('scope_date')} DESC
                    LIMIT 10
                    """
                ),
                {"mid": merchant_id},
            ).mappings().all()
        snapshot["recent"] = [dict(row) for row in recent]
        snapshot["evidence"].extend(
            [f"settlement:{row.get('settlement_id')}" for row in snapshot["recent"] if row.get("settlement_id")]
        )
    except Exception as exc:
        snapshot["recent_error"] = str(exc)

    seen: set[str] = set()
    deduped: list[str] = []
    for item in snapshot.get("evidence") or []:
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    snapshot["evidence"] = deduped[:80]
    return snapshot


def explain_settlement_shortfall(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    expected_amount: float | None = None,
    received_amount: float | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    provider = _settlement_provider(engine)
    missing = provider.missing("merchant_id", "settlement_id", "scope_date")
    if missing:
        return {
            "verified": False,
            "directional_support": False,
            "window": {"from": from_date, "to": to_date},
            "shortfall": None,
            "summary": "Settlement payout fields are not available in the current schema.",
            "deduction_explanation": "I could not compute a payout shortfall from the current settlement schema.",
            "recommended_actions": ["Check whether settlements expose gross/net payout fields before attempting payout attribution."],
            "evidence": [],
            "error": f"{provider.source_table or 'settlement source'} is missing canonical fields: {', '.join(sorted(missing))}",
            "notes": list(provider.notes),
        }

    select_exprs = [
        provider.select("settlement_id", alias="settlement_id"),
        provider.select("status", alias="status", null_if_missing=True),
        provider.select("scope_date", alias="settlement_date"),
        provider.select("gross_amount", alias="gross_amount", null_if_missing=True),
        provider.select("net_settlement_amount", alias="net_settlement_amount", null_if_missing=True),
        provider.select("mdr_deducted", alias="mdr_deducted", null_if_missing=True),
        provider.select("gst_on_mdr", alias="gst_on_mdr", null_if_missing=True),
        provider.select("tds_deducted", alias="tds_deducted", null_if_missing=True),
        provider.select("chargeback_deductions", alias="chargeback_deductions", null_if_missing=True),
        provider.select("reserve_held", alias="reserve_held", null_if_missing=True),
        provider.select("adjustment_amount", alias="adjustment_amount", null_if_missing=True),
        provider.select("hold_reason", alias="hold_reason", null_if_missing=True),
        provider.select("reference", alias="settlement_utr", null_if_missing=True),
        provider.select("payment_mode", alias="payment_mode", null_if_missing=True),
        provider.select("txn_count", alias="txn_count", null_if_missing=True),
        provider.select("refund_count", alias="refund_count", null_if_missing=True),
    ]

    query = text(
        f"""
        SELECT
          {", ".join(select_exprs)}
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('scope_date')} >= :d1
          AND {provider.value('scope_date')} < :d2
        ORDER BY {provider.value('scope_date')} DESC, {provider.value('settlement_id')} DESC
        LIMIT :limit
        """
    )

    try:
        with engine.connect() as conn:
            rows = [dict(row) for row in conn.execute(
                query,
                {"mid": merchant_id, "d1": from_date, "d2": to_date, "limit": max(1, min(int(limit or 20), 100))},
            ).mappings().all()]
    except Exception as exc:
        return {
            "verified": False,
            "directional_support": False,
            "window": {"from": from_date, "to": to_date},
            "shortfall": None,
            "summary": "I could not read settlement rows for payout attribution.",
            "deduction_explanation": "The settlement shortfall query failed before a payout breakdown could be computed.",
            "recommended_actions": ["Check settlement schema access or retry the payout attribution query."],
            "evidence": [],
            "error": str(exc),
            "notes": list(provider.notes),
        }

    if not rows:
        return {
            "verified": False,
            "directional_support": False,
            "window": {"from": from_date, "to": to_date},
            "shortfall": None,
            "summary": "No settlements were found in the selected window.",
            "deduction_explanation": "There is no settlement data in the selected window to explain a payout shortfall.",
            "recommended_actions": ["Expand the settlement window or confirm the merchant had payouts in that period."],
            "evidence": [],
            "error": None,
            "notes": list(provider.notes),
        }

    candidate = rows[0]
    best_score = None
    match_inputs = expected_amount is not None or received_amount is not None
    for idx, row in enumerate(rows):
        gross = _to_float(row.get("gross_amount"))
        net = _to_float(row.get("net_settlement_amount"))
        if match_inputs:
            score = 0.0
            comparisons = 0
            if expected_amount is not None and gross is not None:
                score += abs(gross - expected_amount)
                comparisons += 1
            if received_amount is not None and net is not None:
                score += abs(net - received_amount)
                comparisons += 1
            if comparisons == 0:
                continue
            score += idx * 0.01
        else:
            score = float(idx)
        if best_score is None or score < best_score:
            best_score = score
            candidate = row

    candidate_expected = _to_float(candidate.get("gross_amount"))
    candidate_received = _to_float(candidate.get("net_settlement_amount"))
    user_match = False
    if match_inputs and best_score is not None:
        basis = max(expected_amount or 0.0, received_amount or 0.0, 1.0)
        tolerance = max(500.0, basis * 0.05)
        user_match = best_score <= tolerance

    effective_expected = expected_amount if user_match and expected_amount is not None else candidate_expected
    effective_received = received_amount if user_match and received_amount is not None else candidate_received

    difference_amount = None
    if effective_expected is not None and effective_received is not None:
        difference_amount = round(effective_expected - effective_received, 2)

    component_defs = [
        ("MDR", _to_float(candidate.get("mdr_deducted"))),
        ("GST on MDR", _to_float(candidate.get("gst_on_mdr"))),
        ("TDS", _to_float(candidate.get("tds_deducted"))),
        ("Chargeback deductions", _to_float(candidate.get("chargeback_deductions"))),
        ("Reserve held", _to_float(candidate.get("reserve_held"))),
        ("Adjustments", _to_float(candidate.get("adjustment_amount"))),
    ]
    components: list[dict[str, Any]] = []
    explained_amount = 0.0
    for label, raw_value in component_defs:
        if raw_value is None or abs(raw_value) < 0.005:
            continue
        kind = "credit" if raw_value < 0 else "deduction"
        components.append(
            {
                "label": ("Adjustment credit" if label == "Adjustments" and raw_value < 0 else label),
                "kind": kind,
                "amount_rupees": round(abs(raw_value), 2),
                "signed_amount_rupees": round(raw_value, 2),
            }
        )
        explained_amount += raw_value
    explained_amount = round(explained_amount, 2)
    unexplained_amount = round((difference_amount or 0.0) - explained_amount, 2) if difference_amount is not None else None

    matched_settlement_id = candidate.get("settlement_id")
    evidence = []
    if matched_settlement_id:
        evidence.extend([f"settlement:{matched_settlement_id}", f"shortfall:settlement:{matched_settlement_id}"])

    hold_reason = str(candidate.get("hold_reason") or "").strip() or None
    if user_match and difference_amount is not None:
        lead = (
            f"Settlement {matched_settlement_id or '(unknown)'} expected Rs {effective_expected:,.2f} "
            f"and received Rs {effective_received:,.2f}, so the payout is short by Rs {difference_amount:,.2f}."
        )
    elif match_inputs and difference_amount is not None:
        lead = (
            f"I could not match the exact Rs {expected_amount or 0:,.2f} vs Rs {received_amount or 0:,.2f} payout "
            f"to a single settlement in this window. The closest settlement {matched_settlement_id or '(unknown)'} "
            f"shows gross Rs {candidate_expected or 0:,.2f} and net Rs {candidate_received or 0:,.2f}."
        )
    elif difference_amount is not None:
        lead = (
            f"Settlement {matched_settlement_id or '(unknown)'} shows gross Rs {effective_expected or 0:,.2f} "
            f"and net Rs {effective_received or 0:,.2f}, for a payout delta of Rs {difference_amount:,.2f}."
        )
    else:
        lead = (
            f"Settlement {matched_settlement_id or '(unknown)'} is available, but the current schema does not expose "
            "both expected and received payout amounts."
        )

    component_bits: list[str] = []
    for component in components[:4]:
        component_bits.append(f"{component['label']} Rs {float(component['amount_rupees']):,.2f}")
    if hold_reason:
        component_bits.append(f"hold reason '{hold_reason}'")

    deduction_explanation = lead
    if component_bits:
        deduction_explanation += " Known components: " + ", ".join(component_bits) + "."
    if unexplained_amount is not None and abs(unexplained_amount) > 1.0:
        deduction_explanation += f" Rs {abs(unexplained_amount):,.2f} remains unexplained from the current fields."

    recommended_actions: list[str] = []
    if any(component["label"] == "Chargeback deductions" for component in components):
        recommended_actions.append("Review linked chargebacks before escalating the payout shortfall.")
    if any(component["label"] == "Reserve held" for component in components) or hold_reason:
        recommended_actions.append("Check settlement hold reasons and reserve settings for the impacted payout.")
    if any(component["label"] in {"MDR", "GST on MDR", "TDS"} for component in components):
        recommended_actions.append("Confirm fee deductions against the merchant pricing and tax configuration.")
    if unexplained_amount is not None and abs(unexplained_amount) > 1.0:
        recommended_actions.append("Escalate the unexplained residual after reviewing settlement and reconciliation records.")
    if not recommended_actions:
        recommended_actions.append("Review settlement detail and reconciliation records for the impacted payout.")

    verified = bool(user_match and difference_amount is not None and abs(unexplained_amount or 0.0) <= 1.0)
    directional_support = bool(difference_amount is not None or candidate_expected is not None or candidate_received is not None)
    if not user_match and match_inputs:
        verified = False

    return {
        "verified": verified,
        "directional_support": directional_support,
        "window": {"from": from_date, "to": to_date},
        "shortfall": {
            "settlement_id": matched_settlement_id,
            "status": candidate.get("status"),
            "settlement_date": candidate.get("settlement_date"),
            "expected_amount": effective_expected,
            "received_amount": effective_received,
            "difference_amount": difference_amount,
            "explained_amount": explained_amount,
            "unexplained_amount": unexplained_amount,
            "matched_user_amounts": user_match,
            "settlement_reference": candidate.get("settlement_utr"),
            "payment_mode": candidate.get("payment_mode"),
            "txn_count": candidate.get("txn_count"),
            "refund_count": candidate.get("refund_count"),
            "hold_reason": hold_reason,
            "components": components,
        },
        "summary": deduction_explanation,
        "deduction_explanation": deduction_explanation,
        "recommended_actions": recommended_actions[:3],
        "scope": {"merchant_id": merchant_id, "terminal_id": None},
        "evidence": evidence,
        "error": None,
        "notes": list(provider.notes),
    }


def get_settlement_detail(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
) -> dict[str, Any]:
    settlement = _fetch_settlement_row(engine, merchant_id=merchant_id, settlement_id=settlement_id)
    reconciliation, _error = _fetch_reconciliation_rows(engine, merchant_id=merchant_id, settlement_id=settlement_id)
    evidence = [f"settlement:{settlement_id}"] if settlement else []
    return {
        "row": settlement,
        "reconciliation": reconciliation,
        "evidence": evidence,
    }


def get_settlement_reconciliation(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
) -> dict[str, Any]:
    rows, error = _fetch_reconciliation_rows(engine, merchant_id=merchant_id, settlement_id=settlement_id)
    normalized_rows = rows or []
    total_rows = sum(int(row.get("count") or 0) for row in normalized_rows)
    open_row_count = sum(
        int(row.get("count") or 0)
        for row in normalized_rows
        if str(row.get("status") or "").strip().upper() not in {"SETTLED", "CLOSED", "RESOLVED", "MATCHED"}
    )
    top_reason = normalized_rows[0] if normalized_rows else None
    evidence = [f"settlement:{settlement_id}"]
    if normalized_rows:
        evidence.append(f"reconciliation:settlement:{settlement_id}")
    return {
        "settlement_id": settlement_id,
        "rows": normalized_rows,
        "total_rows": total_rows,
        "open_row_count": open_row_count,
        "top_reason": top_reason,
        "evidence": evidence,
        "error": error,
    }


def get_settlement_timeline(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
) -> dict[str, Any]:
    row = _fetch_settlement_row(engine, merchant_id=merchant_id, settlement_id=settlement_id)
    if row is None:
        return {
            "settlement_id": settlement_id,
            "status": None,
            "current_stage": None,
            "summary": "Settlement detail was not found for the requested timeline review.",
            "events": [],
            "evidence": [],
            "error": "settlement not found",
        }

    reconciliation = get_settlement_reconciliation(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    delay = get_payout_delay_context(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )

    status = str(row.get("status") or "UNKNOWN").upper()
    expected_date = str(row.get("expected_date") or "").strip()
    settled_at = str(row.get("settled_at") or "").strip()
    hold_reason = str(row.get("hold_reason") or "").strip() or None
    events: list[dict[str, Any]] = []

    if expected_date:
        events.append(
            {
                "event_type": "expected_date",
                "event_at": expected_date,
                "stage": "expected",
                "label": f"Expected settlement date is {expected_date}.",
            }
        )

    events.append(
        {
            "event_type": "status_snapshot",
            "event_at": settled_at or expected_date or None,
            "stage": status.lower(),
            "label": f"Current settlement status is {status}.",
            "details": {
                "reference": row.get("reference"),
                "payment_mode": row.get("payment_mode"),
                "hold_reason": hold_reason,
            },
        }
    )

    delay_state = str(delay.get("delay_state") or "").strip()
    delay_days = delay.get("delay_days")
    if delay_state:
        if settled_at and delay_state == "settled_late":
            label = (
                f"Settlement was completed {int(delay_days or 0)} day(s) after the expected date."
            )
        elif settled_at and delay_state == "settled_on_time":
            label = "Settlement was completed on or before the expected date."
        elif delay_state == "delayed_unsettled":
            label = (
                f"Settlement is still unsettled {int(delay_days or 0)} day(s) after the expected date."
            )
        elif delay_state == "not_yet_due":
            label = "Settlement has not yet crossed its expected date."
        else:
            label = "Settlement delay state is incomplete because the expected date is missing."
        events.append(
            {
                "event_type": "delay_state",
                "event_at": expected_date or settled_at or None,
                "stage": delay_state,
                "label": label,
            }
        )

    reconciliation_rows = reconciliation.get("rows") if isinstance(reconciliation.get("rows"), list) else []
    if reconciliation_rows:
        top_reason = (
            reconciliation.get("top_reason")
            if isinstance(reconciliation.get("top_reason"), dict)
            else reconciliation_rows[0]
        )
        reason = str(top_reason.get("reason") or top_reason.get("status") or "unknown").strip()
        events.append(
            {
                "event_type": "reconciliation_snapshot",
                "event_at": settled_at or expected_date or None,
                "stage": "reconciliation",
                "label": (
                    f"{int(reconciliation.get('open_row_count') or 0)} reconciliation row(s) remain open, "
                    f"led by {reason}."
                ),
                "details": {
                    "open_row_count": int(reconciliation.get("open_row_count") or 0),
                    "total_rows": int(reconciliation.get("total_rows") or 0),
                    "top_reason": top_reason,
                },
            }
        )

    if settled_at:
        events.append(
            {
                "event_type": "settled_at",
                "event_at": settled_at,
                "stage": "settled",
                "label": f"Settlement shows a settled timestamp at {settled_at}.",
            }
        )

    current_stage = delay_state or ("settled" if settled_at else status.lower())
    summary_bits = [f"Settlement {settlement_id} is {status}"]
    if expected_date:
        summary_bits.append(f"expected on {expected_date}")
    if settled_at:
        summary_bits.append(f"settled at {settled_at}")
    elif delay_state == "delayed_unsettled":
        summary_bits.append(f"and remains delayed by {int(delay_days or 0)} day(s)")
    if hold_reason:
        summary_bits.append(f"with hold reason {hold_reason}")
    summary = ", ".join(summary_bits).rstrip(".") + "."

    evidence = _dedupe_evidence(
        [f"settlement:{settlement_id}", f"timeline:settlement:{settlement_id}"]
        + [str(item) for item in (delay.get("evidence") or [])]
        + [str(item) for item in (reconciliation.get("evidence") or [])]
    )
    return {
        "settlement_id": settlement_id,
        "status": status,
        "current_stage": current_stage,
        "summary": summary,
        "expected_date": row.get("expected_date"),
        "settled_at": row.get("settled_at"),
        "delay_state": delay.get("delay_state"),
        "delay_days": delay.get("delay_days"),
        "open_reconciliation_rows": int(reconciliation.get("open_row_count") or 0),
        "events": events,
        "evidence": evidence,
        "error": None,
    }


def get_reconciliation_breaks(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
) -> dict[str, Any]:
    row = _fetch_settlement_row(engine, merchant_id=merchant_id, settlement_id=settlement_id)
    reconciliation = get_settlement_reconciliation(
        engine,
        merchant_id=merchant_id,
        settlement_id=settlement_id,
    )
    rows = reconciliation.get("rows") if isinstance(reconciliation.get("rows"), list) else []
    resolved_statuses = {"SETTLED", "CLOSED", "RESOLVED", "MATCHED"}
    breaks: list[dict[str, Any]] = []
    for item in rows:
        status = str(item.get("status") or "").strip().upper()
        if status in resolved_statuses:
            continue
        break_type = "pending_review" if status in {"PENDING", "UNDER_REVIEW", "REVIEW"} else "open_break"
        breaks.append(
            {
                "status": item.get("status"),
                "reason": item.get("reason"),
                "count": int(item.get("count") or 0),
                "break_type": break_type,
            }
        )

    total_break_rows = sum(int(item.get("count") or 0) for item in breaks)
    top_break = breaks[0] if breaks else None
    if top_break:
        summary = (
            f"Settlement {settlement_id} has {total_break_rows} reconciliation break row(s) "
            f"across {len(breaks)} bucket(s), led by {top_break.get('reason') or top_break.get('status')}."
        )
    else:
        summary = f"Settlement {settlement_id} has no unresolved reconciliation breaks in the current records."

    evidence = _dedupe_evidence(
        [f"settlement:{settlement_id}"]
        + [str(item) for item in (reconciliation.get("evidence") or [])]
        + ([f"reconciliation_breaks:settlement:{settlement_id}"] if breaks else [])
    )
    return {
        "settlement_id": settlement_id,
        "settlement_status": row.get("status") if row else None,
        "expected_date": row.get("expected_date") if row else None,
        "breaks": breaks,
        "total_break_rows": total_break_rows,
        "distinct_break_count": len(breaks),
        "top_break": top_break,
        "summary": summary,
        "evidence": evidence,
        "error": reconciliation.get("error"),
    }


def get_hold_reason(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
) -> dict[str, Any]:
    row = _fetch_settlement_row(engine, merchant_id=merchant_id, settlement_id=settlement_id)
    hold_reason = str((row or {}).get("hold_reason") or "").strip() or None
    return {
        "settlement_id": settlement_id,
        "status": (row or {}).get("status"),
        "expected_date": (row or {}).get("expected_date"),
        "settled_at": (row or {}).get("settled_at"),
        "reference": (row or {}).get("reference"),
        "payment_mode": (row or {}).get("payment_mode"),
        "hold_reason": hold_reason,
        "evidence": [f"settlement:{settlement_id}"] if row else [],
    }


def get_payout_delay_context(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
    anchor_date: dt.date | None = None,
) -> dict[str, Any]:
    row = _fetch_settlement_row(engine, merchant_id=merchant_id, settlement_id=settlement_id)
    if row is None:
        return {
            "settlement_id": settlement_id,
            "delay_state": None,
            "delay_days": None,
            "is_delayed": False,
            "expected_date": None,
            "settled_at": None,
            "status": None,
            "hold_reason": None,
            "reference": None,
            "evidence": [],
            "error": "settlement not found",
        }

    expected_date = _parse_date(row.get("expected_date"))
    settled_date = _parse_date(row.get("settled_at"))
    anchor = anchor_date or dt.date.today()
    delay_state = "missing_expected_date"
    delay_days: int | None = None
    is_delayed = False
    if expected_date is not None and settled_date is not None:
        delay_days = max((settled_date - expected_date).days, 0)
        is_delayed = delay_days > 0
        delay_state = "settled_late" if is_delayed else "settled_on_time"
    elif expected_date is not None:
        delay_days = max((anchor - expected_date).days, 0)
        is_delayed = anchor > expected_date
        delay_state = "delayed_unsettled" if is_delayed else "not_yet_due"

    return {
        "settlement_id": settlement_id,
        "status": row.get("status"),
        "expected_date": row.get("expected_date"),
        "settled_at": row.get("settled_at"),
        "delay_state": delay_state,
        "delay_days": delay_days,
        "is_delayed": is_delayed,
        "hold_reason": str(row.get("hold_reason") or "").strip() or None,
        "reference": row.get("reference"),
        "evidence": [f"settlement:{settlement_id}", f"payout_delay:settlement:{settlement_id}"],
        "error": None if expected_date is not None else "expected_date is not available for this settlement",
    }


def get_deduction_breakdown(
    engine: Any,
    *,
    merchant_id: str,
    settlement_id: str,
) -> dict[str, Any]:
    row = _fetch_settlement_row(engine, merchant_id=merchant_id, settlement_id=settlement_id)
    if row is None:
        return {
            "settlement_id": settlement_id,
            "summary": "The settlement row could not be found for deduction review.",
            "recommended_actions": ["Confirm the settlement id before reviewing deductions."],
            "evidence": [],
            "error": "settlement not found",
        }

    gross_amount = _to_float(row.get("gross_amount"))
    net_amount = _to_float(row.get("net_settlement_amount"))
    difference_amount = round(gross_amount - net_amount, 2) if gross_amount is not None and net_amount is not None else None
    components, explained_amount = _deduction_components(row)
    unexplained_amount = round((difference_amount or 0.0) - explained_amount, 2) if difference_amount is not None else None
    hold_reason = str(row.get("hold_reason") or "").strip() or None

    component_bits = [f"{item['label']} Rs {float(item['amount_rupees']):,.2f}" for item in components[:4]]
    if hold_reason:
        component_bits.append(f"hold reason '{hold_reason}'")

    if difference_amount is not None:
        summary = (
            f"Settlement {settlement_id} shows gross Rs {gross_amount or 0:,.2f}, "
            f"net Rs {net_amount or 0:,.2f}, and a payout delta of Rs {difference_amount:,.2f}."
        )
    else:
        summary = f"Settlement {settlement_id} is present, but gross and net payout fields are incomplete."
    if component_bits:
        summary += " Known components: " + ", ".join(component_bits) + "."
    if unexplained_amount is not None and abs(unexplained_amount) > 1.0:
        summary += f" Rs {abs(unexplained_amount):,.2f} remains unexplained from the current settlement fields."

    recommended_actions: list[str] = []
    if any(component["label"] == "Chargeback deductions" for component in components):
        recommended_actions.append("Review linked chargebacks before escalating the payout delta.")
    if any(component["label"] == "Reserve held" for component in components) or hold_reason:
        recommended_actions.append("Review hold reasons and reserve settings for the impacted settlement.")
    if any(component["label"] in {"MDR", "GST on MDR", "TDS"} for component in components):
        recommended_actions.append("Confirm fee deductions against the merchant pricing and tax setup.")
    if unexplained_amount is not None and abs(unexplained_amount) > 1.0:
        recommended_actions.append("Escalate the unexplained residual after checking reconciliation records.")
    if not recommended_actions:
        recommended_actions.append("Review the settlement row and reconciliation records before escalation.")

    return {
        "settlement_id": settlement_id,
        "status": row.get("status"),
        "expected_date": row.get("expected_date"),
        "settled_at": row.get("settled_at"),
        "gross_amount": gross_amount,
        "net_settlement_amount": net_amount,
        "difference_amount": difference_amount,
        "explained_amount": explained_amount,
        "unexplained_amount": unexplained_amount,
        "hold_reason": hold_reason,
        "payment_mode": row.get("payment_mode"),
        "txn_count": row.get("txn_count"),
        "refund_count": row.get("refund_count"),
        "components": components,
        "summary": summary,
        "recommended_actions": recommended_actions[:3],
        "evidence": [f"settlement:{settlement_id}", f"shortfall:settlement:{settlement_id}"],
        "error": None,
    }

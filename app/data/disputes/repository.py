from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text

from app.data.merchant_ops import repository as merchant_ops_repository


def _chargeback_scope_and_fields(engine: Any) -> dict[str, str]:
    cols = merchant_ops_repository.table_columns(engine, "chargebacks")
    return {
        "merchant_col": "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else ""),
        "status_col": "status" if "status" in cols else ("chargeback_stage" if "chargeback_stage" in cols else ""),
        "opened_col": next((col for col in ("opened_at", "chargeback_date", "created_at", "p_date") if col in cols), ""),
        "due_col": "due_by" if "due_by" in cols else ("response_due_date" if "response_due_date" in cols else ""),
        "amount_col": "amount_rupees" if "amount_rupees" in cols else ("chargeback_amount" if "chargeback_amount" in cols else ""),
        "reason_col": "reason_code" if "reason_code" in cols else ("chargeback_reason_code" if "chargeback_reason_code" in cols else ""),
        "network_col": "network" if "network" in cols else ("card_network" if "card_network" in cols else ""),
        "tx_id_col": "tx_id" if "tx_id" in cols else ("transaction_id" if "transaction_id" in cols else ("transaction_fact_id" if "transaction_fact_id" in cols else "")),
    }


def list_chargebacks(
    engine: Any,
    *,
    merchant_id: str,
    status: str = "open",
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    fields = _chargeback_scope_and_fields(engine)
    mid_col = fields["merchant_col"]
    if not mid_col:
        return {"rows": [], "evidence": [], "error": "chargebacks table is missing merchant scope column"}
    status_col = fields["status_col"]
    opened_col = fields["opened_col"]
    due_col = fields["due_col"]

    where = [f"{mid_col} = :mid"]
    params: dict[str, Any] = {"mid": merchant_id, "limit": max(1, min(int(limit or 50), 200))}

    if status_col:
        if status == "open":
            where.append(f"{status_col} NOT IN ('CLOSED','RESOLVED')")
        elif status == "closed":
            where.append(f"{status_col} IN ('CLOSED','RESOLVED')")

    if from_date and to_date and opened_col:
        params["d1"] = from_date
        params["d2"] = to_date
        where.append(f"DATE({opened_col}) >= :d1")
        where.append(f"DATE({opened_col}) < :d2")

    order_expr = "chargeback_id"
    if due_col and opened_col:
        order_expr = f"COALESCE({due_col}, {opened_col})"
    elif due_col:
        order_expr = due_col
    elif opened_col:
        order_expr = opened_col

    select_status = status_col or "NULL"
    select_opened = opened_col or "NULL"
    select_due = due_col or "NULL"
    select_amount = fields["amount_col"] or "NULL"
    select_reason = fields["reason_col"] or "NULL"

    query = text(
        f"""
        SELECT
          chargeback_id,
          {select_status} AS status,
          {select_opened} AS opened_at,
          {select_due} AS due_by,
          {select_amount} AS amount_rupees,
          {select_reason} AS reason_code
        FROM chargebacks
        WHERE {' AND '.join(where)}
        ORDER BY {order_expr} ASC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    out = [dict(row) for row in rows]
    evidence = [f"chargeback:{row.get('chargeback_id')}" for row in out if row.get("chargeback_id")]
    return {"rows": out, "evidence": evidence}


def get_chargeback_detail(
    engine: Any,
    *,
    merchant_id: str,
    chargeback_id: str,
) -> dict[str, Any]:
    fields = _chargeback_scope_and_fields(engine)
    mid_col = fields["merchant_col"]
    if not mid_col:
        return {"row": None, "evidence": [], "error": "chargebacks table is missing merchant scope column"}

    status_expr = f"{fields['status_col']} AS status" if fields["status_col"] else "'UNKNOWN' AS status"
    opened_expr = f"{fields['opened_col']} AS opened_at" if fields["opened_col"] else "NULL AS opened_at"
    due_expr = f"{fields['due_col']} AS due_by" if fields["due_col"] else "NULL AS due_by"
    amount_expr = (
        f"{fields['amount_col']} AS amount_rupees"
        if fields["amount_col"]
        else "NULL AS amount_rupees"
    )
    reason_expr = (
        f"{fields['reason_col']} AS reason_code"
        if fields["reason_col"]
        else "NULL AS reason_code"
    )
    network_expr = f"{fields['network_col']} AS network" if fields["network_col"] else "NULL AS network"
    tx_id_expr = f"{fields['tx_id_col']} AS tx_id" if fields["tx_id_col"] else "NULL AS tx_id"

    with engine.connect() as conn:
        chargeback = conn.execute(
            text(
                f"""
                SELECT
                  chargeback_id,
                  {mid_col} AS merchant_id,
                  {status_expr},
                  {opened_expr},
                  {due_expr},
                  {amount_expr},
                  {reason_expr},
                  {network_expr},
                  {tx_id_expr}
                FROM chargebacks
                WHERE {mid_col} = :mid
                  AND chargeback_id = :cid
                LIMIT 1
                """
            ),
            {"mid": merchant_id, "cid": chargeback_id},
        ).mappings().first()
    evidence = [f"chargeback:{chargeback_id}"] if chargeback else []
    return {"row": dict(chargeback) if chargeback else None, "evidence": evidence}


def chargeback_summary(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    payload = list_chargebacks(
        engine,
        merchant_id=merchant_id,
        status="all",
        from_date=from_date,
        to_date=to_date,
        limit=200,
    )
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    today = date.today().isoformat()
    open_count = 0
    overdue_count = 0
    due_soon_count = 0
    total_amount = 0.0
    stage_distribution: dict[str, int] = {}
    reason_counts: dict[str, int] = {}

    for row in rows:
        amount = row.get("amount_rupees")
        try:
            total_amount += float(amount or 0.0)
        except Exception:
            pass
        status_value = str(row.get("status") or "UNKNOWN").strip().upper()
        stage_distribution[status_value] = stage_distribution.get(status_value, 0) + 1
        if status_value not in {"CLOSED", "RESOLVED"}:
            open_count += 1
            due_by = str(row.get("due_by") or "").strip()
            if due_by:
                if due_by < today:
                    overdue_count += 1
                elif due_by <= to_date:
                    due_soon_count += 1
        reason_value = str(row.get("reason_code") or "UNKNOWN").strip().upper()
        if reason_value:
            reason_counts[reason_value] = reason_counts.get(reason_value, 0) + 1

    top_reason = None
    if reason_counts:
        top_reason = max(
            (
                {"reason_code": key, "count": count}
                for key, count in reason_counts.items()
            ),
            key=lambda item: (int(item.get("count") or 0), str(item.get("reason_code") or "")),
        )

    return {
        "chargebacks_count": len(rows),
        "open_chargebacks_count": open_count,
        "overdue_chargebacks_count": overdue_count,
        "due_soon_chargebacks_count": due_soon_count,
        "chargebacks_amount": round(total_amount, 2),
        "stage_distribution": [
            {"status": status, "count": count}
            for status, count in sorted(stage_distribution.items(), key=lambda item: (-item[1], item[0]))
        ],
        "top_reason": top_reason,
        "evidence": [str(item) for item in payload.get("evidence", []) if str(item or "").strip()],
        "window": {"from": from_date, "to": to_date},
        "error": str(payload.get("error") or "").strip() or None,
    }


def list_refunds(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    limit: int = 50,
) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "refunds")
    merchant_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not merchant_col:
        return {
            "rows": [],
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "error": "refunds table is missing merchant scope column",
        }

    refund_id_expr = "refund_id" if "refund_id" in cols else "NULL AS refund_id"
    if "status" in cols:
        status_expr = "status"
    elif "refund_status" in cols:
        status_expr = "refund_status AS status"
    else:
        status_expr = "'UNKNOWN' AS status"

    created_col = "created_at" if "created_at" in cols else ("p_date" if "p_date" in cols else "")
    if not created_col:
        return {
            "rows": [],
            "evidence": [],
            "window": {"from": from_date, "to": to_date},
            "error": "refunds table is missing refund date column",
        }

    amount_expr = "amount_rupees" if "amount_rupees" in cols else ("refund_amount AS amount_rupees" if "refund_amount" in cols else "NULL AS amount_rupees")
    tx_id_expr = "tx_id" if "tx_id" in cols else "NULL AS tx_id"
    query = text(
        f"""
        SELECT
          {refund_id_expr},
          {status_expr},
          {created_col} AS created_at,
          {amount_expr},
          {tx_id_expr}
        FROM refunds
        WHERE {merchant_col} = :mid
          AND DATE({created_col}) >= :d1
          AND DATE({created_col}) < :d2
        ORDER BY {created_col} DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            query,
            {"mid": merchant_id, "d1": from_date, "d2": to_date, "limit": max(1, min(int(limit or 50), 200))},
        ).mappings().all()
    out = [dict(row) for row in rows]
    evidence = [f"refund:{row.get('refund_id')}" for row in out if row.get("refund_id")]
    return {"rows": out, "evidence": evidence, "window": {"from": from_date, "to": to_date}}


def get_refund_detail(
    engine: Any,
    *,
    merchant_id: str,
    refund_id: str,
) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "refunds")
    merchant_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    if not merchant_col:
        return {"row": None, "evidence": [], "error": "refunds table is missing merchant scope column"}

    refund_id_col = "refund_id" if "refund_id" in cols else ""
    if not refund_id_col:
        return {"row": None, "evidence": [], "error": "refunds table is missing refund_id column"}

    if "status" in cols:
        status_expr = "status"
    elif "refund_status" in cols:
        status_expr = "refund_status AS status"
    else:
        status_expr = "'UNKNOWN' AS status"

    created_col = "created_at" if "created_at" in cols else ("p_date" if "p_date" in cols else ("refund_date" if "refund_date" in cols else ""))
    amount_expr = "amount_rupees" if "amount_rupees" in cols else ("refund_amount AS amount_rupees" if "refund_amount" in cols else "NULL AS amount_rupees")
    tx_id_expr = "tx_id" if "tx_id" in cols else "NULL AS tx_id"

    with engine.connect() as conn:
        refund = conn.execute(
            text(
                f"""
                SELECT
                  {refund_id_col} AS refund_id,
                  {merchant_col} AS merchant_id,
                  {status_expr},
                  {created_col or 'NULL'} AS created_at,
                  {amount_expr},
                  {tx_id_expr}
                FROM refunds
                WHERE {merchant_col} = :mid
                  AND {refund_id_col} = :refund_id
                LIMIT 1
                """
            ),
            {"mid": merchant_id, "refund_id": refund_id},
        ).mappings().first()

    evidence = [f"refund:{refund_id}"] if refund else []
    return {"row": dict(refund) if refund else None, "evidence": evidence}


def refund_summary(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "refunds")
    merchant_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    created_col = "created_at" if "created_at" in cols else ("p_date" if "p_date" in cols else "")
    amount_col = "amount_rupees" if "amount_rupees" in cols else ("refund_amount" if "refund_amount" in cols else "")
    if not merchant_col or not created_col:
        return {"refunds_count": 0, "refunds_amount": 0.0}

    amount_expr = amount_col or "0"
    query = text(
        f"""
        SELECT
          CAST(COUNT(*) AS INTEGER) AS refunds_count,
          COALESCE(SUM({amount_expr}),0) AS refunds_amount
        FROM refunds
        WHERE {merchant_col} = :mid
          AND DATE({created_col}) >= :d1
          AND DATE({created_col}) < :d2
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"mid": merchant_id, "d1": from_date, "d2": to_date}).mappings().first()
    return {
        "refunds_count": int(row.get("refunds_count") or 0) if row else 0,
        "refunds_amount": float(row.get("refunds_amount") or 0.0) if row else 0.0,
    }


def chargeback_count(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
) -> int | None:
    cols = merchant_ops_repository.table_columns(engine, "chargebacks")
    merchant_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else "")
    opened_col = next((col for col in ("opened_at", "chargeback_date", "created_at", "p_date") if col in cols), "")
    if not merchant_col or not opened_col:
        return None

    query = text(
        f"""
        SELECT CAST(COUNT(*) AS INTEGER) AS chargebacks_count
        FROM chargebacks
        WHERE {merchant_col} = :mid
          AND DATE({opened_col}) >= :d1
          AND DATE({opened_col}) < :d2
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"mid": merchant_id, "d1": from_date, "d2": to_date}).mappings().first()
    return int(row.get("chargebacks_count") or 0) if row else 0


def dispute_risk_signals(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    chargeback_payload = chargeback_summary(
        engine,
        merchant_id=merchant_id,
        from_date=from_date,
        to_date=to_date,
    )
    chargeback_rows_payload = list_chargebacks(
        engine,
        merchant_id=merchant_id,
        status="all",
        from_date=from_date,
        to_date=to_date,
        limit=25,
    )
    refund_payload = refund_summary(
        engine,
        merchant_id=merchant_id,
        from_date=from_date,
        to_date=to_date,
    )
    refund_rows_payload = list_refunds(
        engine,
        merchant_id=merchant_id,
        from_date=from_date,
        to_date=to_date,
        limit=25,
    )

    chargebacks_count = int(chargeback_payload.get("chargebacks_count") or 0)
    open_chargebacks_count = int(chargeback_payload.get("open_chargebacks_count") or 0)
    overdue_chargebacks_count = int(chargeback_payload.get("overdue_chargebacks_count") or 0)
    due_soon_chargebacks_count = int(chargeback_payload.get("due_soon_chargebacks_count") or 0)
    chargebacks_amount = float(chargeback_payload.get("chargebacks_amount") or 0.0)
    refunds_count = int(refund_payload.get("refunds_count") or 0)
    refunds_amount = float(refund_payload.get("refunds_amount") or 0.0)
    top_reason = chargeback_payload.get("top_reason") if isinstance(chargeback_payload.get("top_reason"), dict) else None

    signals: list[dict[str, Any]] = []
    if open_chargebacks_count > 0:
        signals.append(
            {
                "signal_type": "open_chargeback_exposure",
                "severity": "high" if open_chargebacks_count >= 2 else "medium",
                "value": open_chargebacks_count,
                "description": f"{open_chargebacks_count} open chargeback(s) remain in the selected window.",
            }
        )
    if overdue_chargebacks_count > 0:
        signals.append(
            {
                "signal_type": "overdue_chargeback_response",
                "severity": "high",
                "value": overdue_chargebacks_count,
                "description": f"{overdue_chargebacks_count} chargeback response deadline(s) are already overdue.",
            }
        )
    if due_soon_chargebacks_count > 0:
        signals.append(
            {
                "signal_type": "chargeback_due_soon",
                "severity": "medium",
                "value": due_soon_chargebacks_count,
                "description": f"{due_soon_chargebacks_count} open chargeback(s) are due within the requested window.",
            }
        )
    if refunds_count > 0 and chargebacks_count > 0:
        signals.append(
            {
                "signal_type": "concurrent_refund_and_chargeback_activity",
                "severity": "medium",
                "value": chargebacks_count + refunds_count,
                "description": "The merchant shows both refund activity and chargeback activity in the same window.",
            }
        )
    if top_reason and int(top_reason.get("count") or 0) >= 2:
        signals.append(
            {
                "signal_type": "repeated_chargeback_reason",
                "severity": "medium",
                "value": int(top_reason.get("count") or 0),
                "description": f"Chargeback reason {top_reason.get('reason_code')} appears repeatedly in the current window.",
            }
        )

    if signals:
        summary = (
            f"Detected {len(signals)} dispute-linked risk signal(s), "
            f"including {open_chargebacks_count} open chargeback(s) and {refunds_count} refund(s)."
        )
    elif chargebacks_count or refunds_count:
        summary = "Dispute activity is present in the current window, but no amplified dispute-risk signal was detected."
    else:
        summary = "No chargebacks or refunds were found in the requested window."

    evidence_ids: list[str] = []
    for payload in (chargeback_payload, chargeback_rows_payload, refund_rows_payload):
        evidence_ids.extend(str(item) for item in (payload.get("evidence") or []) if str(item or "").strip())
    seen: set[str] = set()
    deduped_evidence: list[str] = []
    for item in [f"dispute_risk:{merchant_id}:{from_date}:{to_date}"] + evidence_ids:
        if item not in seen:
            seen.add(item)
            deduped_evidence.append(item)

    error_parts = [
        str(chargeback_payload.get("error") or "").strip(),
        str(chargeback_rows_payload.get("error") or "").strip(),
        str(refund_rows_payload.get("error") or "").strip(),
    ]
    error_message = "; ".join(part for part in error_parts if part) or None

    return {
        "verified": not error_message,
        "signals": signals,
        "summary": summary,
        "metrics": {
            "chargebacks_count": chargebacks_count,
            "open_chargebacks_count": open_chargebacks_count,
            "overdue_chargebacks_count": overdue_chargebacks_count,
            "due_soon_chargebacks_count": due_soon_chargebacks_count,
            "chargebacks_amount": round(chargebacks_amount, 2),
            "refunds_count": refunds_count,
            "refunds_amount": round(refunds_amount, 2),
            "top_chargeback_reason": top_reason,
        },
        "latest_chargebacks": (chargeback_rows_payload.get("rows") or [])[:5],
        "latest_refunds": (refund_rows_payload.get("rows") or [])[:5],
        "evidence": deduped_evidence,
        "window": {"from": from_date, "to": to_date},
        "error": error_message,
    }

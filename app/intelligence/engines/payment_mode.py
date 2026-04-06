# app/intelligence/engines/payment_mode.py
import datetime as dt
import logging
import uuid

from sqlalchemy import text

from ..money import get_amount_scale, scale_inr
from ..source_adapters import normalized_text, resolve_transaction_source
from ..type import Recommendation


def _payment_mode_sql(provider) -> str:
    return f"""
    SELECT {normalized_text(provider.value('payment_mode'), uppercase=True)} AS payment_mode,
           COUNT(*) AS volume,
           SUM({provider.value('amount_rupees')}) AS revenue
    FROM {provider.source_table}
    WHERE {provider.value('merchant_id')} = :mid
      AND {provider.value('status')} = 'SUCCESS'
      AND {provider.value('p_date')} >= :start_date
      AND {provider.value('p_date')} < :end_date
    GROUP BY 1
    ORDER BY volume DESC
    """


def _deterministic_payment_mode_summary(top_mode: str, pct: int, revenue: float, share_delta_pp: float | None) -> str:
    trend = ""
    if share_delta_pp is not None:
        direction = "up" if share_delta_pp >= 0 else "down"
        trend = f" Share versus previous window is {direction} by {abs(share_delta_pp):.1f}pp."
    mode = str(top_mode or "").upper()
    if mode == "UPI":
        return (
            f"UPI is driving {pct}% of volume and Rs {revenue:,.0f} in successful revenue.{trend} "
            "Keep checkout fast with dynamic QR for walk-ins and use UPI Collect only for higher-ticket assisted payments."
        )
    if mode == "CARD":
        return (
            f"CARD is driving {pct}% of volume and Rs {revenue:,.0f} in successful revenue.{trend} "
            "Promote contactless and EMI on higher-ticket purchases so the card-heavy mix turns into bigger basket sizes."
        )
    return (
        f"{top_mode} is driving {pct}% of volume and Rs {revenue:,.0f} in successful revenue.{trend} "
        "Keep the dominant payment mode reliable at checkout and use it as the primary upsell path."
    )


def _mode_share_map(engine, provider, mid: str, start_date: dt.date, end_date: dt.date) -> dict[str, float]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(_payment_mode_sql(provider)),
            {"mid": mid, "start_date": start_date, "end_date": end_date},
        ).fetchall()
    total = sum(int(row[1] or 0) for row in rows)
    if total <= 0:
        return {}
    return {str(row[0] or "UNKNOWN").upper(): round((int(row[1] or 0) / total) * 100.0, 2) for row in rows}


def build_payment_mode_reco(engine, mid: str, window_days: int, start_date: dt.date, end_date: dt.date) -> Recommendation | None:
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status", "payment_mode", "amount_rupees")
    if missing:
        logging.getLogger("acquiguru").warning("Payment mode reco skipped; missing canonical fields: %s", ", ".join(sorted(missing)))
        return None
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(_payment_mode_sql(provider)),
                {"mid": mid, "start_date": start_date, "end_date": end_date},
            ).fetchall()
        if not rows:
            return None

        total_vol = sum(int(row[1] or 0) for row in rows)
        if total_vol == 0:
            return None

        top_mode, top_vol, top_revenue = rows[0]
        amount_scale = get_amount_scale(engine)
        top_revenue = scale_inr(top_revenue, amount_scale)
        pct = round((int(top_vol or 0) / total_vol) * 100)

        previous_start = start_date - (end_date - start_date)
        previous_shares = _mode_share_map(engine, provider, mid, previous_start, start_date)
        current_shares = _mode_share_map(engine, provider, mid, start_date, end_date)
        top_mode_normalized = str(top_mode or "UNKNOWN").upper()
        share_current = float(current_shares.get(top_mode_normalized) or 0.0)
        share_previous = float(previous_shares.get(top_mode_normalized) or 0.0)
        share_delta_pp = round(share_current - share_previous, 2)

        insight_summary = _deterministic_payment_mode_summary(top_mode_normalized, pct, float(top_revenue), share_delta_pp)

        if top_mode_normalized == "UPI":
            actions = [
                {"who": "merchant", "text": "Enable dynamic QR display at checkout."},
                {"who": "bank", "text": "Offer UPI Collect for high-value B2B or premium transactions."},
            ]
            icon = "📱"
        else:
            actions = [
                {"who": "merchant", "text": "Highlight accepted card brands and contactless tap-to-pay."},
                {"who": "bank", "text": "Activate low-cost EMI schemes to boost ticket size."},
            ]
            icon = "💳"

        return Recommendation(
            reco_id=f"reco_{uuid.uuid4().hex[:12]}",
            merchant_id=mid,
            window_days=window_days,
            category="growth",
            title=f"{icon} {top_mode_normalized} contributes {pct}% of volume",
            summary=(
                f"Between {start_date} and {end_date - dt.timedelta(days=1)}, this resulted in ₹{top_revenue:,.0f} revenue.\n\n"
                f"{insight_summary}"
            ),
            impact_rupees=float(top_revenue) * 0.02,
            confidence=0.90,
            priority_score=5.5,
            drivers=[
                {
                    "dimension": "payment_mode",
                    "value": top_mode_normalized,
                    "mode_share_current": share_current,
                    "mode_share_previous": share_previous,
                    "share_delta_pp": share_delta_pp,
                },
            ],
            actions=actions,
            evidence_ids=[],
            metadata={
                "mode_share_current": share_current,
                "mode_share_previous": share_previous,
                "share_delta_pp": share_delta_pp,
                "successful_revenue": float(top_revenue),
                "source_table": provider.source_table,
                "source_notes": list(provider.notes),
            },
        )
    except Exception as exc:
        logging.getLogger("acquiguru").error("Payment mode reco failed: %s", exc)
        return None

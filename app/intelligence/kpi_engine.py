from sqlalchemy import text

from .constants import FAILED_STATUS_SQL
from .money import get_amount_scale, scale_inr
from .source_adapters import resolve_transaction_source


def compute_kpis(engine, mid, start_date, end_date, table):
    """
    Returns baseline metrics for the merchant in a given window.
    """
    provider = resolve_transaction_source(engine, table=table)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return {
            "total_txns": 0,
            "success_txns": 0,
            "failed_txns": 0,
            "revenue": 0.0,
            "avg_ticket": 0.0,
            "success_rate": 0.0,
            "notes": list(provider.notes),
            "errors": [f"{provider.source_table or 'transaction source'} missing canonical fields: {', '.join(sorted(missing))}"],
        }

    sql = text(
        f"""
        SELECT
            COUNT(*) AS total_txns,
            SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
            SUM(CASE WHEN {provider.value('status')} IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS failed_txns,
            SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} ELSE 0 END) AS revenue,
            AVG(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN {provider.value('amount_rupees')} END) AS avg_ticket
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('p_date')} >= :start_date
          AND {provider.value('p_date')} < :end_date
        """
    )

    with engine.connect() as conn:
        row = conn.execute(
            sql,
            {"mid": mid, "start_date": start_date, "end_date": end_date},
        ).fetchone()

    total_txns = row.total_txns or 0
    success_txns = row.success_txns or 0
    failed_txns = row.failed_txns or 0
    amount_scale = get_amount_scale(engine)

    success_rate = success_txns / total_txns if total_txns else 0

    return {
        "total_txns": total_txns,
        "success_txns": success_txns,
        "failed_txns": failed_txns,
        "revenue": scale_inr(row.revenue, amount_scale),
        "avg_ticket": scale_inr(row.avg_ticket, amount_scale),
        "success_rate": success_rate,
        "notes": list(provider.notes),
        "errors": [],
    }

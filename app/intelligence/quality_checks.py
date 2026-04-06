from sqlalchemy import text

from .constants import VALID_PAYMENT_MODE_SQL, VALID_STATUS_SQL
from .source_adapters import resolve_transaction_source


def run_data_quality_checks(engine, mid: str, start_date, end_date, table: str = "transaction_features") -> dict:
    provider = resolve_transaction_source(engine, table=table)
    missing = provider.missing("merchant_id", "p_date", "status", "amount_rupees")
    if missing:
        return {
            "passed": False,
            "issues": ["missing_canonical_fields"],
            "metrics": {
                "total_rows": 0,
                "invalid_status_rows": 0,
                "invalid_payment_mode_rows": 0,
                "null_amount_rows": 0,
                "negative_amount_rows": 0,
                "null_date_rows": 0,
            },
            "notes": list(provider.notes),
        }

    invalid_payment_mode_sql = (
        f"SUM(CASE WHEN {provider.value('payment_mode')} IS NULL OR {provider.value('payment_mode')} NOT IN {VALID_PAYMENT_MODE_SQL} THEN 1 ELSE 0 END) AS invalid_payment_mode_rows"
        if provider.has("payment_mode")
        else "0 AS invalid_payment_mode_rows"
    )
    sql = text(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {provider.value('status')} IS NULL OR {provider.value('status')} NOT IN {VALID_STATUS_SQL} THEN 1 ELSE 0 END) AS invalid_status_rows,
            {invalid_payment_mode_sql},
            SUM(CASE WHEN {provider.value('amount_rupees')} IS NULL THEN 1 ELSE 0 END) AS null_amount_rows,
            SUM(CASE WHEN {provider.value('amount_rupees')} < 0 THEN 1 ELSE 0 END) AS negative_amount_rows,
            SUM(CASE WHEN {provider.value('p_date')} IS NULL THEN 1 ELSE 0 END) AS null_date_rows
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} = :mid
          AND {provider.value('p_date')} >= :start_date
          AND {provider.value('p_date')} < :end_date
        """
    )
    params = {"mid": mid, "start_date": start_date, "end_date": end_date}

    with engine.connect() as conn:
        row = conn.execute(sql, params).mappings().first() or {}

    metrics = {
        "total_rows": int(row.get("total_rows") or 0),
        "invalid_status_rows": int(row.get("invalid_status_rows") or 0),
        "invalid_payment_mode_rows": int(row.get("invalid_payment_mode_rows") or 0),
        "null_amount_rows": int(row.get("null_amount_rows") or 0),
        "negative_amount_rows": int(row.get("negative_amount_rows") or 0),
        "null_date_rows": int(row.get("null_date_rows") or 0),
    }

    issues = []
    if metrics["invalid_status_rows"] > 0:
        issues.append("invalid_status")
    if metrics["invalid_payment_mode_rows"] > 0:
        issues.append("invalid_payment_mode")
    if metrics["null_amount_rows"] > 0:
        issues.append("null_amount")
    if metrics["negative_amount_rows"] > 0:
        issues.append("negative_amount")
    if metrics["null_date_rows"] > 0:
        issues.append("null_date")

    return {
        "passed": len(issues) == 0,
        "issues": issues,
        "metrics": metrics,
        "notes": list(provider.notes),
    }

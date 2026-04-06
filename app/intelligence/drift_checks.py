from sqlalchemy import text

from .constants import FAILED_STATUS_SQL
from .source_adapters import normalized_text, resolve_transaction_source


def _to_dist(rows: list[dict], *, key_name: str) -> dict[str, float]:
    total = sum(int(row.get("c") or 0) for row in rows)
    if total <= 0:
        return {}
    return {str(row.get(key_name) or "UNKNOWN"): float(row.get("c") or 0) / total for row in rows}


def _tvd(p: dict[str, float], q: dict[str, float]) -> float:
    keys = set(p) | set(q)
    if not keys:
        return 0.0
    return 0.5 * sum(abs(p.get(k, 0.0) - q.get(k, 0.0)) for k in keys)


def _window_success_rate(engine, provider, mid: str, start_date, end_date) -> tuple[float, int]:
    params = {"mid": mid, "start_date": start_date, "end_date": end_date}
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                  COUNT(*) AS total_txns,
                  SUM(CASE WHEN {provider.value('status')} = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns
                FROM {provider.source_table}
                WHERE {provider.value('merchant_id')} = :mid
                  AND {provider.value('p_date')} >= :start_date
                  AND {provider.value('p_date')} < :end_date
                """
            ),
            params,
        ).mappings().first() or {}
    total = int(row.get("total_txns") or 0)
    success = int(row.get("success_txns") or 0)
    rate = (success / total * 100.0) if total else 0.0
    return rate, total


def run_drift_checks(
    engine,
    mid: str,
    current_start,
    current_end,
    baseline_start,
    baseline_end,
) -> dict:
    provider = resolve_transaction_source(engine)
    missing = provider.missing("merchant_id", "p_date", "status")
    if missing:
        return {
            "alerts": [],
            "metrics": {},
            "current_mode_distribution": {},
            "baseline_mode_distribution": {},
            "errors": [f"{provider.source_table or 'transaction source'} missing canonical fields: {', '.join(sorted(missing))}"],
            "notes": list(provider.notes),
        }

    current_params = {"mid": mid, "start_date": current_start, "end_date": current_end}
    baseline_params = {"mid": mid, "start_date": baseline_start, "end_date": baseline_end}

    with engine.connect() as conn:
        if provider.has("payment_mode"):
            mode_sql = text(
                f"""
                SELECT {normalized_text(provider.value('payment_mode'), uppercase=True)} AS payment_mode, COUNT(*) AS c
                FROM {provider.source_table}
                WHERE {provider.value('merchant_id')} = :mid
                  AND {provider.value('p_date')} >= :start_date
                  AND {provider.value('p_date')} < :end_date
                  AND {provider.value('status')} = 'SUCCESS'
                GROUP BY 1
                """
            )
            cur_mode_rows = conn.execute(mode_sql, current_params).mappings().all()
            base_mode_rows = conn.execute(mode_sql, baseline_params).mappings().all()
        else:
            cur_mode_rows = []
            base_mode_rows = []

        if provider.has("response_code"):
            fail_code_sql = text(
                f"""
                SELECT {normalized_text(provider.value('response_code'), uppercase=True)} AS response_code, COUNT(*) AS c
                FROM {provider.source_table}
                WHERE {provider.value('merchant_id')} = :mid
                  AND {provider.value('p_date')} >= :start_date
                  AND {provider.value('p_date')} < :end_date
                  AND {provider.value('status')} IN {FAILED_STATUS_SQL}
                GROUP BY 1
                """
            )
            cur_code_rows = conn.execute(fail_code_sql, current_params).mappings().all()
            base_code_rows = conn.execute(fail_code_sql, baseline_params).mappings().all()
        else:
            cur_code_rows = []
            base_code_rows = []

    cur_mode_dist = _to_dist(cur_mode_rows, key_name="payment_mode")
    base_mode_dist = _to_dist(base_mode_rows, key_name="payment_mode")
    cur_code_dist = _to_dist(cur_code_rows, key_name="response_code")
    base_code_dist = _to_dist(base_code_rows, key_name="response_code")

    mode_tvd = _tvd(cur_mode_dist, base_mode_dist)
    fail_code_tvd = _tvd(cur_code_dist, base_code_dist)
    cur_sr, cur_n = _window_success_rate(engine, provider, mid, current_start, current_end)
    base_sr, base_n = _window_success_rate(engine, provider, mid, baseline_start, baseline_end)
    sr_delta_pp = cur_sr - base_sr

    alerts = []
    if min(cur_n, base_n) >= 100 and mode_tvd > 0.20:
        alerts.append("payment_mode_mix_shift")
    if fail_code_tvd > 0.30:
        alerts.append("failure_signature_shift")
    if min(cur_n, base_n) >= 100 and sr_delta_pp <= -3.0:
        alerts.append("success_rate_drop")

    errors: list[str] = []
    if not provider.has("payment_mode"):
        errors.append(f"{provider.source_table} missing payment_mode; payment mix drift skipped")
    if not provider.has("response_code"):
        errors.append(f"{provider.source_table} missing response_code; failure signature drift skipped")

    return {
        "alerts": alerts,
        "metrics": {
            "payment_mode_tvd": round(mode_tvd, 4),
            "failure_code_tvd": round(fail_code_tvd, 4),
            "success_rate_delta_pp": round(sr_delta_pp, 2),
            "current_total_txns": cur_n,
            "baseline_total_txns": base_n,
        },
        "current_mode_distribution": cur_mode_dist,
        "baseline_mode_distribution": base_mode_dist,
        "errors": errors,
        "notes": list(provider.notes),
    }

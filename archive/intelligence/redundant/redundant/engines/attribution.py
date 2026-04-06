# app/intelligence/engines/attribution.py
import pandas as pd
from sqlalchemy import text
from ..constants import FAILED_STATUS_SQL
from ..money import get_amount_scale

TOP_CODES_SQL = f"""
SELECT response_code,
       COUNT(*) AS fail_count,
       SUM(amount_rupees) AS fail_gmv
FROM transaction_features
WHERE merchant_id = :mid
  AND status IN {FAILED_STATUS_SQL}
  AND p_date >= :start_date
  AND p_date <  :end_date
GROUP BY response_code
ORDER BY fail_count DESC
LIMIT 10
"""

FAIL_BY_HOUR_SQL = f"""
SELECT hour_of_day,
       ROUND(100.0 * SUM(CASE WHEN status IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS fail_rate_pct,
       COUNT(*) AS txn_count
FROM transaction_features
WHERE merchant_id = :mid
  AND p_date >= :start_date
  AND p_date <  :end_date
GROUP BY hour_of_day
ORDER BY fail_rate_pct DESC
LIMIT 5
"""

def compute_attribution(engine, mid: str, start_date, end_date) -> dict:
    params = {"mid": mid, "start_date": start_date, "end_date": end_date}
    top_codes = pd.read_sql(text(TOP_CODES_SQL), engine, params=params)
    fail_by_hour = pd.read_sql(text(FAIL_BY_HOUR_SQL), engine, params=params)
    amount_scale = get_amount_scale(engine)
    if "fail_gmv" in top_codes.columns and amount_scale != 1.0:
        top_codes["fail_gmv"] = pd.to_numeric(top_codes["fail_gmv"], errors="coerce").fillna(0) * amount_scale
    return {
        "top_codes": top_codes.to_dict(orient="records"),
        "fail_by_hour": fail_by_hour.to_dict(orient="records"),
        "params": params,
    }

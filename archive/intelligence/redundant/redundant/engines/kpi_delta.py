# app/intelligence/engines/kpi_delta.py
import pandas as pd
from sqlalchemy import text
from ..money import get_amount_scale

KPI_SQL = """
SELECT
  SUM(CASE WHEN status='SUCCESS' THEN amount_rupees ELSE 0 END) AS success_gmv,
  COUNT(*) AS total_txns,
  SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
  ROUND(100.0 * SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS success_rate_pct,
  ROUND(AVG(CASE WHEN status='SUCCESS' THEN amount_rupees END)::numeric, 2) AS avg_ticket
FROM transaction_features
WHERE merchant_id = :mid
  AND p_date >= :start_date
  AND p_date <  :end_date
"""

MODE_SQL = """
SELECT payment_mode,
       COUNT(*) AS txn_count,
       SUM(CASE WHEN status='SUCCESS' THEN amount_rupees ELSE 0 END) AS success_gmv
FROM transaction_features
WHERE merchant_id = :mid
  AND p_date >= :start_date
  AND p_date <  :end_date
GROUP BY payment_mode
ORDER BY txn_count DESC
"""

def compute_kpis(engine, mid: str, start_date, end_date) -> dict:
    params = {"mid": mid, "start_date": start_date, "end_date": end_date}
    kpi = pd.read_sql(text(KPI_SQL), engine, params=params).iloc[0].to_dict()
    mode = pd.read_sql(text(MODE_SQL), engine, params=params)
    amount_scale = get_amount_scale(engine)
    for key in ("success_gmv", "avg_ticket"):
        if key in kpi and kpi[key] is not None:
            kpi[key] = float(kpi[key]) * amount_scale
    if amount_scale != 1.0 and not mode.empty and "success_gmv" in mode.columns:
        mode["success_gmv"] = pd.to_numeric(mode["success_gmv"], errors="coerce").fillna(0) * amount_scale
    return {"kpi": kpi, "mode": mode.to_dict(orient="records"), "params": params}

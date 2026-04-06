from datetime import timedelta
from .kpi_engine import compute_kpis


def compute_deltas(engine, mid, end_date, window_days, table):

    start_current = end_date - timedelta(days=window_days)
    start_previous = start_current - timedelta(days=window_days)

    current = compute_kpis(engine, mid, start_current, end_date, table)
    previous = compute_kpis(engine, mid, start_previous, start_current, table)

    def delta(curr, prev):
        if prev == 0:
            return 0
        return (curr - prev) / prev

    return {
        "revenue_delta": delta(current["revenue"], previous["revenue"]),
        "success_rate_delta": current["success_rate"] - previous["success_rate"],
        "txn_delta": delta(current["total_txns"], previous["total_txns"]),
        "current": current,
        "previous": previous
    }
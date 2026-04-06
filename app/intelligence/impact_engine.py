def lost_revenue_estimate(kpis):
    """
    Estimate lost revenue from failures.
    """

    failed_txns = kpis["failed_txns"]
    avg_ticket = kpis["avg_ticket"]

    return failed_txns * avg_ticket


def success_improvement_value(kpis, improvement_pp):
    """
    Estimate revenue recovery if success rate improves.
    """

    total_txns = kpis["total_txns"]
    avg_ticket = kpis["avg_ticket"]

    recovered_txns = total_txns * (improvement_pp / 100)

    return recovered_txns * avg_ticket
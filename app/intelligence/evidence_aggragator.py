from app.ontology.signals.operational import collect_operational_signals
from app.ontology.signals.reconciliation import collect_reconciliation_signals
from app.ontology.signals.dispute_signals import collect_dispute_signals


def collect_phase2_evidence(engine, mid, window_days: int = 30):

    evidence = {}

    evidence["operational"] = collect_operational_signals(engine, mid, window_days=window_days)
    evidence["reconciliation"] = collect_reconciliation_signals(engine, mid, window_days=window_days)
    evidence["disputes"] = collect_dispute_signals(engine, mid, window_days=window_days)

    return evidence

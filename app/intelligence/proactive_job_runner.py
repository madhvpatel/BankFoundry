"""
proactive_job_runner.py — Proactive signal job registry and runner.

All 10 signal engines in app/intelligence/engines/ are wired here.
Each job registers with:
  - a function that computes signals for a merchant
  - the case_type it creates
  - a schedule interval

The runner provides idempotency via a case-window key — the same signal
for the same merchant in the same date window will not create a duplicate case.

Usage (APScheduler):
    from app.intelligence.proactive_job_runner import ProactiveJobRunner
    from apscheduler.schedulers.background import BackgroundScheduler

    runner = ProactiveJobRunner(engine)
    scheduler = BackgroundScheduler()
    runner.register_jobs(scheduler)
    scheduler.start()

Usage (manual trigger, e.g. for cron scripts):
    from app.intelligence.proactive_job_runner import ProactiveJobRunner
    runner = ProactiveJobRunner(engine)
    runner.run_all()
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger("proactive_job_runner")


# ---------------------------------------------------------------------------
# Job descriptor
# ---------------------------------------------------------------------------

@dataclass
class ProactiveJob:
    """
    One proactive signal job.

    Fields
    ------
    name            : unique job identifier (also used in idempotency keys)
    case_type       : the case type string written to the ops case store
    lane            : which bank ops lane owns this case
    signal_fn       : fn(engine, merchant_id, start_date, end_date) → dict
                      Must return {"triggered": bool, "payload": dict, "summary": str}
    interval_minutes: how often to run (used by APScheduler)
    lookback_days   : how far back the signal window extends
    severity        : default severity label for created cases
    """

    name: str
    case_type: str
    lane: str
    signal_fn: Callable[[Any, str, str, str], dict[str, Any]]
    interval_minutes: int = 60
    lookback_days: int = 7
    severity: str = "medium"


# ---------------------------------------------------------------------------
# Idempotency key
# ---------------------------------------------------------------------------

def _idempotency_key(job_name: str, merchant_id: str, window_start: str, window_end: str) -> str:
    """Stable key used to de-duplicate proactive cases."""
    raw = f"{job_name}::{merchant_id}::{window_start}::{window_end}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


# ---------------------------------------------------------------------------
# Signal adapters (thin wrappers over the existing engines)
# ---------------------------------------------------------------------------

def _payout_shortfall_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for payout_shortfall_monitor."""
    try:
        from app.intelligence.payout_shortfall_monitor import detect_payout_shortfall
        result = detect_payout_shortfall(engine, merchant_id, start_date=start_date, end_date=end_date)
        triggered = bool(result.get("shortfall_detected"))
        return {
            "triggered": triggered,
            "payload": result,
            "summary": result.get("summary") or "Payout shortfall detected.",
        }
    except Exception as exc:
        logger.warning("payout_shortfall_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _terminal_anomaly_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for operational_signals anomaly detection."""
    try:
        from app.intelligence.engines.operational_signals import compute_operational_signals
        result = compute_operational_signals(engine, merchant_id, start_date=start_date, end_date=end_date)
        signals = result.get("signals") or []
        high_signals = [s for s in signals if str(s.get("severity") or "").lower() in {"high", "critical"}]
        triggered = len(high_signals) > 0
        return {
            "triggered": triggered,
            "payload": {"signals": high_signals, "total_signals": len(signals)},
            "summary": high_signals[0].get("description") if high_signals else "",
        }
    except Exception as exc:
        logger.warning("terminal_anomaly_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _chargeback_anomaly_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for dispute_signals."""
    try:
        from app.intelligence.engines.dispute_signals import compute_dispute_signals
        result = compute_dispute_signals(engine, merchant_id, start_date=start_date, end_date=end_date)
        signals = result.get("signals") or []
        high_signals = [s for s in signals if str(s.get("severity") or "").lower() in {"high", "critical"}]
        triggered = len(high_signals) > 0
        return {
            "triggered": triggered,
            "payload": {"signals": high_signals},
            "summary": high_signals[0].get("description") if high_signals else "",
        }
    except Exception as exc:
        logger.warning("chargeback_anomaly_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _reconciliation_break_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for reconciliation_signals."""
    try:
        from app.intelligence.engines.reconciliation_signals import compute_reconciliation_signals
        result = compute_reconciliation_signals(engine, merchant_id, start_date=start_date, end_date=end_date)
        breaks = result.get("breaks") or []
        triggered = len(breaks) > 0
        return {
            "triggered": triggered,
            "payload": {"breaks": breaks[:5]},
            "summary": f"{len(breaks)} reconciliation break(s) detected." if breaks else "",
        }
    except Exception as exc:
        logger.warning("reconciliation_break_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _payment_mode_skew_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for payment_mode engine."""
    try:
        from app.intelligence.engines.payment_mode import compute_payment_mode_signals
        result = compute_payment_mode_signals(engine, merchant_id, start_date=start_date, end_date=end_date)
        signals = result.get("signals") or []
        skew_signals = [s for s in signals if str(s.get("type") or "").lower() == "mode_skew"]
        triggered = len(skew_signals) > 0
        return {
            "triggered": triggered,
            "payload": {"signals": skew_signals},
            "summary": skew_signals[0].get("description") if skew_signals else "",
        }
    except Exception as exc:
        logger.warning("payment_mode_skew_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _kpi_delta_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for kpi_delta engine."""
    try:
        from app.intelligence.engines.kpi_delta import compute_kpi_delta
        result = compute_kpi_delta(engine, merchant_id, start_date=start_date, end_date=end_date)
        deltas = result.get("deltas") or []
        material = [d for d in deltas if abs(float(d.get("pct_change") or 0)) >= 20]
        triggered = len(material) > 0
        return {
            "triggered": triggered,
            "payload": {"deltas": material},
            "summary": f"{len(material)} KPI(s) with ≥20% change detected." if material else "",
        }
    except Exception as exc:
        logger.warning("kpi_delta_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _anomaly_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for anomaly engine."""
    try:
        from app.intelligence.engines.anomaly import compute_anomaly_signals
        result = compute_anomaly_signals(engine, merchant_id, start_date=start_date, end_date=end_date)
        anomalies = result.get("anomalies") or []
        high = [a for a in anomalies if str(a.get("severity") or "").lower() in {"high", "critical"}]
        triggered = len(high) > 0
        return {
            "triggered": triggered,
            "payload": {"anomalies": high[:5]},
            "summary": high[0].get("description") if high else "",
        }
    except Exception as exc:
        logger.warning("anomaly_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _peak_hour_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for peak_hour engine — triggers if off-peak failure rate is elevated."""
    try:
        from app.intelligence.engines.peak_hour import compute_peak_hour_signals
        result = compute_peak_hour_signals(engine, merchant_id, start_date=start_date, end_date=end_date)
        signals = result.get("signals") or []
        triggered = any(str(s.get("type") or "") == "off_peak_failure_spike" for s in signals)
        return {
            "triggered": triggered,
            "payload": {"signals": signals[:3]},
            "summary": "Off-peak failure spike detected." if triggered else "",
        }
    except Exception as exc:
        logger.warning("peak_hour_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _lost_sales_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Adapter for lost_sales engine."""
    try:
        from app.intelligence.engines.lost_sales import compute_lost_sales
        result = compute_lost_sales(engine, merchant_id, start_date=start_date, end_date=end_date)
        lost = float(result.get("estimated_lost_inr") or 0)
        triggered = lost > 0
        return {
            "triggered": triggered,
            "payload": result,
            "summary": f"Estimated lost sales of Rs {lost:,.0f} detected." if triggered else "",
        }
    except Exception as exc:
        logger.warning("lost_sales_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


def _lending_eligibility_signal(engine: Any, merchant_id: str, start_date: str, end_date: str) -> dict[str, Any]:
    """Triggers if merchant crosses into a new lending tier."""
    try:
        from app.growth.lending_engine import get_lending_offers
        result = get_lending_offers(engine, merchant_id)
        tier = str(result.get("eligibility_tier") or "Tier 3 (Ineligible)")
        triggered = "Ineligible" not in tier
        return {
            "triggered": triggered,
            "payload": result,
            "summary": f"Merchant is now {tier} for lending products." if triggered else "",
        }
    except Exception as exc:
        logger.warning("lending_eligibility_signal failed for %s: %s", merchant_id, exc)
        return {"triggered": False, "payload": {}, "summary": ""}


# ---------------------------------------------------------------------------
# Canonical job registry
# ---------------------------------------------------------------------------

PROACTIVE_JOBS: list[ProactiveJob] = [
    ProactiveJob(
        name="payout_shortfall_monitor",
        case_type="settlement_shortfall_review",
        lane="settlement",
        signal_fn=_payout_shortfall_signal,
        interval_minutes=60,
        lookback_days=7,
        severity="high",
    ),
    ProactiveJob(
        name="terminal_anomaly_monitor",
        case_type="terminal_failure_review",
        lane="payments",
        signal_fn=_terminal_anomaly_signal,
        interval_minutes=30,
        lookback_days=3,
        severity="high",
    ),
    ProactiveJob(
        name="chargeback_anomaly_monitor",
        case_type="chargeback_review",
        lane="disputes",
        signal_fn=_chargeback_anomaly_signal,
        interval_minutes=120,
        lookback_days=7,
        severity="medium",
    ),
    ProactiveJob(
        name="reconciliation_break_monitor",
        case_type="reconciliation_mismatch",
        lane="settlement",
        signal_fn=_reconciliation_break_signal,
        interval_minutes=60,
        lookback_days=7,
        severity="medium",
    ),
    ProactiveJob(
        name="payment_mode_skew_monitor",
        case_type="payment_mode_skew",
        lane="payments",
        signal_fn=_payment_mode_skew_signal,
        interval_minutes=60,
        lookback_days=7,
        severity="medium",
    ),
    ProactiveJob(
        name="kpi_delta_monitor",
        case_type="payment_exception",
        lane="payments",
        signal_fn=_kpi_delta_signal,
        interval_minutes=240,
        lookback_days=14,
        severity="medium",
    ),
    ProactiveJob(
        name="anomaly_monitor",
        case_type="risk_triage",
        lane="risk",
        signal_fn=_anomaly_signal,
        interval_minutes=120,
        lookback_days=7,
        severity="high",
    ),
    ProactiveJob(
        name="peak_hour_monitor",
        case_type="payment_exception",
        lane="payments",
        signal_fn=_peak_hour_signal,
        interval_minutes=60,
        lookback_days=3,
        severity="low",
    ),
    ProactiveJob(
        name="lost_sales_monitor",
        case_type="payment_exception",
        lane="payments",
        signal_fn=_lost_sales_signal,
        interval_minutes=240,
        lookback_days=7,
        severity="medium",
    ),
    ProactiveJob(
        name="lending_eligibility_monitor",
        case_type="merchant_support_case",
        lane="growth",
        signal_fn=_lending_eligibility_signal,
        interval_minutes=1440,  # once per day
        lookback_days=30,
        severity="low",
    ),
]


# ---------------------------------------------------------------------------
# ProactiveJobRunner
# ---------------------------------------------------------------------------

class ProactiveJobRunner:
    """
    Runs all registered proactive jobs against a list of merchant IDs.

    De-duplication
    --------------
    The runner tracks fired keys in memory. For production use, pass a
    persistent `fired_keys` set backed by Redis or the ops case store.

    Parameters
    ----------
    engine      : SQLAlchemy engine
    jobs        : list of ProactiveJob (defaults to PROACTIVE_JOBS)
    fired_keys  : mutable set used for idempotency (pass a persistent store in prod)
    """

    def __init__(
        self,
        engine: Any,
        *,
        jobs: list[ProactiveJob] | None = None,
        fired_keys: set[str] | None = None,
    ) -> None:
        self._engine = engine
        self._jobs = jobs if jobs is not None else PROACTIVE_JOBS
        self._fired_keys: set[str] = fired_keys if fired_keys is not None else set()

    def run_job(self, job: ProactiveJob, merchant_id: str) -> dict[str, Any]:
        """Run one job for one merchant. Returns a result record."""
        today = dt.date.today()
        end_date = today.isoformat()
        start_date = (today - dt.timedelta(days=job.lookback_days)).isoformat()

        idempotency_key = _idempotency_key(job.name, merchant_id, start_date, end_date)
        if idempotency_key in self._fired_keys:
            logger.debug(
                "ProactiveJobRunner: skipping %s for %s (already fired in window)",
                job.name, merchant_id,
            )
            return {"job": job.name, "merchant_id": merchant_id, "status": "skipped", "reason": "idempotent"}

        try:
            signal = job.signal_fn(self._engine, merchant_id, start_date, end_date)
        except Exception as exc:
            logger.error("ProactiveJobRunner: %s raised for %s: %s", job.name, merchant_id, exc)
            return {"job": job.name, "merchant_id": merchant_id, "status": "error", "reason": str(exc)}

        if not signal.get("triggered"):
            return {"job": job.name, "merchant_id": merchant_id, "status": "no_signal"}

        # Mark as fired before writing the case — prevents retry storms
        self._fired_keys.add(idempotency_key)

        case_payload = self._build_case_payload(
            job=job,
            merchant_id=merchant_id,
            signal=signal,
            start_date=start_date,
            end_date=end_date,
            idempotency_key=idempotency_key,
        )

        written = self._write_case(case_payload)
        logger.info(
            "ProactiveJobRunner: %s fired for %s → case %s",
            job.name, merchant_id, written.get("case_id", "?"),
        )
        return {
            "job": job.name,
            "merchant_id": merchant_id,
            "status": "fired",
            "case_id": written.get("case_id"),
            "idempotency_key": idempotency_key,
        }

    def run_all(self, merchant_ids: list[str]) -> list[dict[str, Any]]:
        """Run all jobs for every merchant in the list."""
        results: list[dict[str, Any]] = []
        for job in self._jobs:
            for merchant_id in merchant_ids:
                result = self.run_job(job, merchant_id)
                results.append(result)
        return results

    def register_jobs(self, scheduler: Any) -> None:
        """
        Register all jobs with an APScheduler BackgroundScheduler.

        The caller must supply the merchant_ids list separately (e.g. via a
        DB query factory). Replace the lambda below with your merchant loader.

        Example
        -------
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler()
        runner.register_jobs(scheduler)
        scheduler.start()
        """
        for job in self._jobs:
            scheduler.add_job(
                self._apscheduler_task,
                "interval",
                minutes=job.interval_minutes,
                id=job.name,
                kwargs={"job": job},
                replace_existing=True,
            )
            logger.info(
                "ProactiveJobRunner: registered %s (every %d min)",
                job.name, job.interval_minutes,
            )

    def _apscheduler_task(self, job: ProactiveJob) -> None:
        """Called by APScheduler — loads live merchant IDs and runs the job."""
        merchant_ids = self._load_active_merchant_ids()
        for merchant_id in merchant_ids:
            self.run_job(job, merchant_id)

    def _load_active_merchant_ids(self) -> list[str]:
        """
        Returns the list of active merchant IDs from the database.
        Replace with your real merchant loader.
        """
        try:
            from sqlalchemy import text
            with self._engine.connect() as conn:
                rows = conn.execute(text("SELECT DISTINCT mid FROM merchants LIMIT 500")).fetchall()
            return [str(row[0]) for row in rows if row[0]]
        except Exception as exc:
            logger.error("ProactiveJobRunner: failed to load merchant IDs: %s", exc)
            return []

    def _build_case_payload(
        self,
        *,
        job: ProactiveJob,
        merchant_id: str,
        signal: dict[str, Any],
        start_date: str,
        end_date: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return {
            "merchant_id": merchant_id,
            "case_type": job.case_type,
            "lane": job.lane,
            "source": "proactive_monitor",
            "source_job": job.name,
            "severity": job.severity,
            "idempotency_key": idempotency_key,
            "title": f"[Auto] {job.label_title()} — {merchant_id}",
            "summary": signal.get("summary") or "",
            "source_payload": {
                "signal": signal.get("payload") or {},
                "window": {"start_date": start_date, "end_date": end_date},
            },
            "created_at": dt.datetime.utcnow().isoformat(timespec="seconds"),
        }

    def _write_case(self, case_payload: dict[str, Any]) -> dict[str, Any]:
        """
        Write a proactive case to the ops case store.
        Falls back to logging if the repository is not available.
        """
        try:
            from app.data.ops.repository import create_work_item
            return create_work_item(self._engine, case_payload)
        except ImportError:
            logger.warning(
                "ProactiveJobRunner: ops repository not available — logging case payload: %s",
                case_payload.get("title"),
            )
            return {"case_id": f"local-{case_payload['idempotency_key']}"}
        except Exception as exc:
            logger.error("ProactiveJobRunner: failed to write case: %s", exc)
            return {"case_id": None}


# ---------------------------------------------------------------------------
# Patch ProactiveJob to add a label_title helper
# ---------------------------------------------------------------------------

def _label_title(self: ProactiveJob) -> str:
    return self.name.replace("_", " ").title()

ProactiveJob.label_title = _label_title  # type: ignore[attr-defined]

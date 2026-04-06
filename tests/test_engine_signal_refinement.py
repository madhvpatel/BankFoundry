import datetime as dt
import json
import unittest

from sqlalchemy import create_engine, text

from app.intelligence.engines.anomaly import build_anomaly_reco
from app.intelligence.engines.attribution import compute_attribution
from app.intelligence.engines.dispute_signals import collect_dispute_signals
from app.intelligence.engines.kpi_delta import compute_kpi_delta
from app.intelligence.engines.payment_mode import build_payment_mode_reco
from app.intelligence.engines.peak_hour import build_peak_hour_reco
from app.intelligence.engines.reconciliation_signals import collect_reconciliation_signals
from app.intelligence.health_engine import build_health_vector
from app.intelligence.impact_engine_v2 import build_impact_vector
from app.intelligence.runner import run_intelligence


class EngineSignalRefinementTest(unittest.TestCase):
    MID = "merchant_refine_001"

    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE transaction_features (
                    merchant_id TEXT,
                    source_txn_id TEXT,
                    invoice_nr TEXT,
                    terminal_id TEXT,
                    payment_mode TEXT,
                    card_network TEXT,
                    status TEXT,
                    response_code TEXT,
                    response_desc TEXT,
                    payer_bank_code TEXT,
                    amount_rupees REAL,
                    p_date DATE,
                    initiated_at TIMESTAMP,
                    hour_of_day INTEGER,
                    day_of_week INTEGER
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE refunds (
                    mid TEXT,
                    refund_amount REAL,
                    refund_date DATE
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE chargebacks (
                    chargeback_id INTEGER PRIMARY KEY,
                    mid TEXT,
                    chargeback_amount REAL,
                    chargeback_stage TEXT,
                    chargeback_reason_code TEXT,
                    chargeback_reason_desc TEXT,
                    card_network TEXT,
                    created_at DATE,
                    due_by DATE
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE settlements (
                    settlement_id TEXT,
                    mid TEXT,
                    expected_date DATE,
                    settlement_status TEXT,
                    hold_reason TEXT,
                    gross_amount REAL,
                    net_settlement_amount REAL,
                    mdr_deducted REAL,
                    gst_on_mdr REAL,
                    tds_deducted REAL,
                    chargeback_deductions REAL,
                    reserve_held REAL,
                    adjustment_amount REAL
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE reconciliation_records (
                    mid TEXT,
                    recon_status TEXT,
                    exception_reason TEXT,
                    created_at DATE
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE merchant_actions (
                    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mid TEXT,
                    category TEXT,
                    title TEXT,
                    description TEXT,
                    impact_rupees REAL,
                    confidence REAL,
                    priority_score REAL,
                    owner TEXT,
                    evidence TEXT,
                    status TEXT DEFAULT 'OPEN',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE mdr_rates (
                    mid TEXT,
                    payment_mode TEXT,
                    card_network TEXT,
                    card_type TEXT,
                    mdr_percentage REAL,
                    gst_on_mdr_pct REAL
                )
                """
            )
        self._seed()

    def _insert_txn(self, *, p_date, payment_mode, status, amount, response_code=None, hour=12, terminal_id="T1", payer_bank_code="HDFC", card_network="VISA"):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features (
                        merchant_id, source_txn_id, invoice_nr, terminal_id, payment_mode, card_network, status, response_code,
                        response_desc, payer_bank_code, amount_rupees, p_date, initiated_at, hour_of_day, day_of_week
                    ) VALUES (
                        :merchant_id, :source_txn_id, :invoice_nr, :terminal_id, :payment_mode, :card_network, :status, :response_code,
                        :response_desc, :payer_bank_code, :amount_rupees, :p_date, :initiated_at, :hour_of_day, :day_of_week
                    )
                    """
                ),
                {
                    "merchant_id": self.MID,
                    "source_txn_id": f"{terminal_id}:{payment_mode}:{status}:{p_date}:{hour}",
                    "invoice_nr": f"inv:{terminal_id}:{payment_mode}:{p_date}:{hour}",
                    "terminal_id": terminal_id,
                    "payment_mode": payment_mode,
                    "card_network": None if payment_mode == "UPI" else card_network,
                    "status": status,
                    "response_code": response_code,
                    "response_desc": response_code,
                    "payer_bank_code": payer_bank_code if payment_mode == "UPI" else None,
                    "amount_rupees": amount,
                    "p_date": p_date,
                    "initiated_at": dt.datetime.combine(p_date, dt.time(hour=hour, minute=0)),
                    "hour_of_day": hour,
                    "day_of_week": p_date.weekday(),
                },
            )

    def _seed(self):
        previous_start = dt.date(2026, 2, 22)
        current_start = dt.date(2026, 3, 1)

        for day_offset in range(7):
            p_date = previous_start + dt.timedelta(days=day_offset)
            for _ in range(8):
                self._insert_txn(p_date=p_date, payment_mode="UPI", status="SUCCESS", amount=5000.0, hour=14, terminal_id="T1", payer_bank_code="HDFC")
            for _ in range(4):
                self._insert_txn(p_date=p_date, payment_mode="CARD", status="SUCCESS", amount=5000.0, hour=13, terminal_id="T2", card_network="VISA")
            self._insert_txn(p_date=p_date, payment_mode="UPI", status="FAILED", amount=5000.0, response_code="91", hour=14, terminal_id="T1", payer_bank_code="HDFC")

        for day_offset in range(7):
            p_date = current_start + dt.timedelta(days=day_offset)
            for _ in range(5):
                self._insert_txn(p_date=p_date, payment_mode="UPI", status="SUCCESS", amount=5000.0, hour=14, terminal_id="T1", payer_bank_code="HDFC")
            for _ in range(5):
                self._insert_txn(p_date=p_date, payment_mode="CARD", status="SUCCESS", amount=5000.0, hour=14, terminal_id="T2", card_network="VISA")
            for _ in range(2):
                self._insert_txn(p_date=p_date, payment_mode="UPI", status="FAILED", amount=5000.0, response_code="91", hour=14, terminal_id="T1", payer_bank_code="HDFC")
            self._insert_txn(p_date=p_date, payment_mode="UPI", status="FAILED", amount=5000.0, response_code="", hour=14, terminal_id="T1", payer_bank_code="")

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO refunds (mid, refund_amount, refund_date) VALUES
                    (:mid, 400.0, :d1),
                    (:mid, 300.0, :d2)
                    """
                ),
                {"mid": self.MID, "d1": dt.date(2026, 3, 3), "d2": dt.date(2026, 3, 4)},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO chargebacks (
                        chargeback_id, mid, chargeback_amount, chargeback_stage, chargeback_reason_code,
                        chargeback_reason_desc, card_network, created_at, due_by
                    ) VALUES
                    (1, :mid, 2500.0, 'CHARGEBACK', '4837', 'No cardholder authorization', 'VISA', :created_1, :due_1),
                    (2, :mid, 1200.0, 'OPEN', '4853', 'Cardholder dispute', 'MASTERCARD', :created_2, :due_2),
                    (3, :mid, 800.0, 'WON', '4837', 'No cardholder authorization', 'VISA', :created_3, :due_3),
                    (4, :mid, 500.0, 'LOST', 'U028', 'UPI dispute', 'NPCI_UPI', :created_4, :due_4)
                    """
                ),
                {
                    "mid": self.MID,
                    "created_1": dt.date(2026, 3, 1),
                    "due_1": dt.date(2026, 3, 5),
                    "created_2": dt.date(2026, 3, 4),
                    "due_2": dt.date(2026, 3, 12),
                    "created_3": dt.date(2026, 3, 2),
                    "due_3": dt.date(2026, 3, 6),
                    "created_4": dt.date(2026, 3, 2),
                    "due_4": dt.date(2026, 3, 7),
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements (
                        settlement_id, mid, expected_date, settlement_status, hold_reason,
                        gross_amount, net_settlement_amount, mdr_deducted, gst_on_mdr,
                        tds_deducted, chargeback_deductions, reserve_held, adjustment_amount
                    ) VALUES
                    ('s1', :mid, :d1, 'PROCESSED', NULL, 50000.0, 47000.0, 1000.0, 180.0, 500.0, 700.0, 300.0, 0.0),
                    ('s2', :mid, :d2, 'HELD', 'KYC_REVIEW', 30000.0, 28500.0, 600.0, 108.0, 300.0, 0.0, 200.0, -50.0)
                    """
                ),
                {"mid": self.MID, "d1": dt.date(2026, 3, 4), "d2": dt.date(2026, 3, 5)},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_records (mid, recon_status, exception_reason, created_at) VALUES
                    (:mid, 'UNMATCHED', 'settlement_gap', :d1),
                    (:mid, 'EXCEPTION', 'hold_applied', :d2),
                    (:mid, 'MATCHED', 'none', :d3)
                    """
                ),
                {"mid": self.MID, "d1": dt.date(2026, 3, 4), "d2": dt.date(2026, 3, 5), "d3": dt.date(2026, 3, 6)},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO mdr_rates (mid, payment_mode, card_network, card_type, mdr_percentage, gst_on_mdr_pct) VALUES
                    (:mid, 'UPI', '', '', 0.0, 18.0),
                    (:mid, 'CARD', 'VISA', '', 1.5, 18.0),
                    (:mid, 'CARD', '', '', 1.5, 18.0)
                    """
                ),
                {"mid": self.MID},
            )

    def test_reconciliation_signals_compute_deductions_and_residual(self):
        signals = collect_reconciliation_signals(self.engine, self.MID, window_days=7)
        metrics = signals["metrics"]
        self.assertEqual(metrics["settlement_batches"], 2)
        self.assertAlmostEqual(metrics["known_deductions_total"], 3838.0, places=2)
        self.assertAlmostEqual(metrics["unexplained_residual"], 662.0, places=2)
        self.assertEqual(metrics["held_batches"], 1)
        self.assertEqual(metrics["delayed_batches"], 1)
        self.assertIsNotNone(metrics["expected_mdr_pct"])
        self.assertIsNotNone(metrics["actual_mdr_pct"])
        self.assertTrue(signals["evidence"]["largest_shortfalls"])
        self.assertTrue(signals["evidence"]["deduction_components"])

    def test_dispute_signals_compute_open_overdue_and_reason_value(self):
        signals = collect_dispute_signals(self.engine, self.MID, window_days=7)
        metrics = signals["metrics"]
        self.assertEqual(metrics["chargeback_count"], 4)
        self.assertEqual(metrics["open_count"], 2)
        self.assertEqual(metrics["overdue_count"], 1)
        self.assertEqual(metrics["won_count"], 1)
        self.assertEqual(metrics["lost_count"], 1)
        self.assertAlmostEqual(metrics["resolution_rate_pct"], 50.0, places=2)
        self.assertEqual(signals["evidence"]["top_chargeback_reasons_by_value"][0]["code"], "4837")
        self.assertTrue(signals["evidence"]["oldest_open_cases"])

    def test_health_and_impact_vectors_include_new_breakdowns(self):
        signals = {
            "operational": {
                "metrics": {"success_rate_pct": 80.0, "success_txns": 70, "fail_txns": 21},
                "evidence": {
                    "top_failure_codes": [{"response_code": "UNKNOWN", "fail_count": 8}],
                    "by_payment_mode": [
                        {"payment_mode": "UPI", "success_rate_pct": 90.0},
                        {"payment_mode": "CARD", "success_rate_pct": 70.0},
                    ],
                },
            },
            "reconciliation": collect_reconciliation_signals(self.engine, self.MID, window_days=7),
            "disputes": collect_dispute_signals(self.engine, self.MID, window_days=7),
        }
        health = build_health_vector(signals)
        impact = build_impact_vector(signals)
        self.assertIn("sub_scores", health)
        self.assertIn("weights", health)
        self.assertIn("unexplained_reconciliation_gap", health["flags"])
        self.assertIn("overdue_disputes", health["flags"])
        self.assertGreater(impact["reconciliation_gap_explained"], 0.0)
        self.assertGreater(impact["reconciliation_gap_unexplained"], 0.0)
        self.assertGreater(impact["overdue_chargeback_risk"], 0.0)

    def test_kpi_delta_returns_overall_and_mode_deltas(self):
        result = compute_kpi_delta(self.engine, self.MID, start_date=dt.date(2026, 3, 1), end_date=dt.date(2026, 3, 8), window_days=7)
        self.assertEqual(result["engine"], "kpi_delta")
        self.assertIn("merchant_level", result)
        self.assertTrue(result["by_payment_mode"])
        self.assertIn("delta_abs", result["merchant_level"]["success_rate_pct"])

    def test_attribution_ranks_change_drivers(self):
        result = compute_attribution(
            self.engine,
            self.MID,
            metric="failed_gmv",
            dimension="response_code",
            start_date=dt.date(2026, 3, 1),
            end_date=dt.date(2026, 3, 8),
            window_days=7,
        )
        self.assertEqual(result["engine"], "attribution")
        self.assertTrue(result["attributions"])
        self.assertEqual(result["attributions"][0]["contribution_rank"], 1)

    def test_anomaly_uses_dynamic_baseline_and_impact_gate(self):
        reco = build_anomaly_reco(
            self.engine,
            self.MID,
            window_days=7,
            current_start=dt.date(2026, 3, 1),
            current_end=dt.date(2026, 3, 8),
            evidence_ids=["anomaly:test"],
        )
        self.assertIsNotNone(reco)
        assert reco is not None
        self.assertIn("merchant-relative baseline", reco.summary)
        self.assertGreater(reco.impact_rupees, 25000.0)

    def test_payment_mode_and_peak_hour_include_new_context(self):
        payment_mode_reco = build_payment_mode_reco(self.engine, self.MID, 7, dt.date(2026, 3, 1), dt.date(2026, 3, 8))
        peak_hour_reco = build_peak_hour_reco(self.engine, self.MID, 7, dt.date(2026, 3, 1), dt.date(2026, 3, 8))
        assert payment_mode_reco is not None
        assert peak_hour_reco is not None
        self.assertIn("share_delta_pp", payment_mode_reco.metadata)
        self.assertIn("peak_hour_failure_rate_pct", peak_hour_reco.metadata)
        self.assertIn("Failure rate in that hour", peak_hour_reco.summary)

    def test_runner_surfaces_reconciliation_and_dispute_actions_with_evidence(self):
        result = run_intelligence(self.engine, self.MID, window_days=7, enable_phase2_reasoning=False, persist_actions=True)
        self.assertIn("signals", result)
        self.assertIn("kpi_delta", result["signals"])
        self.assertIn("attribution", result["signals"])
        recos = result["recos"]
        categories = {reco.category for reco in recos}
        self.assertTrue({"reconciliation", "disputes"}.intersection(categories))
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT category, evidence FROM merchant_actions WHERE mid = :mid"), {"mid": self.MID}).fetchall()
        self.assertTrue(rows)
        parsed = [json.loads(row[1]) for row in rows if row[1]]
        self.assertTrue(any(item.get("source") == "deterministic_engine" for item in parsed))
        self.assertTrue(any(item.get("evidence_ids") for item in parsed))

    def test_active_engines_are_not_loaded_from_redundant_archive(self):
        import app.intelligence.engines.attribution as active_attribution
        import app.intelligence.engines.kpi_delta as active_kpi_delta

        self.assertNotIn("/redundant/", active_attribution.__file__)
        self.assertNotIn("/redundant/", active_kpi_delta.__file__)


if __name__ == "__main__":
    unittest.main()

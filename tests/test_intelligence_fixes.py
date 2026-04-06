import datetime as dt
import unittest

from sqlalchemy import create_engine, text

from app.intelligence.calibration import estimate_recovery_rate
from app.intelligence.drift_checks import run_drift_checks
from app.intelligence.engines.payment_mode import build_payment_mode_reco
from app.intelligence.engines.peak_hour import build_peak_hour_reco
from app.intelligence.experiments import run_recovery_backtest
from app.intelligence.quality_checks import run_data_quality_checks
from app.intelligence.runner import _requires_phase2_human_explanation, run_intelligence


class IntelligenceFixesTest(unittest.TestCase):
    MID = "merchant_test_001"

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
                    refund_amount REAL
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE chargebacks (
                    mid TEXT,
                    chargeback_amount REAL,
                    chargeback_stage TEXT,
                    chargeback_reason_code TEXT,
                    chargeback_reason_desc TEXT,
                    card_network TEXT
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE settlements (
                    mid TEXT,
                    gross_amount REAL,
                    net_settlement_amount REAL
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE reconciliation_records (
                    mid TEXT,
                    recon_status TEXT,
                    exception_reason TEXT
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

        self._seed_dataset()

    def _insert_row(
        self,
        merchant_id: str,
        source_txn_id: str,
        invoice_nr: str,
        payment_mode: str,
        status: str,
        response_code: str | None,
        amount_rupees: float,
        p_date: dt.date,
        initiated_at: dt.datetime,
        hour_of_day: int,
        day_of_week: int,
    ):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features (
                        merchant_id, source_txn_id, invoice_nr, terminal_id, payment_mode, status, response_code,
                        response_desc, payer_bank_code, amount_rupees, p_date, initiated_at, hour_of_day, day_of_week
                    ) VALUES (
                        :merchant_id, :source_txn_id, :invoice_nr, :terminal_id, :payment_mode, :status, :response_code,
                        :response_desc, :payer_bank_code, :amount_rupees, :p_date, :initiated_at, :hour_of_day, :day_of_week
                    )
                    """
                ),
                {
                    "merchant_id": merchant_id,
                    "source_txn_id": source_txn_id,
                    "invoice_nr": invoice_nr,
                    "terminal_id": "term_1",
                    "payment_mode": payment_mode,
                    "status": status,
                    "response_code": response_code,
                    "response_desc": "test_desc" if response_code else None,
                    "payer_bank_code": "HDFC" if status in {"FAILURE", "FAILED"} else "ICICI",
                    "amount_rupees": amount_rupees,
                    "p_date": p_date,
                    "initiated_at": initiated_at,
                    "hour_of_day": hour_of_day,
                    "day_of_week": day_of_week,
                },
            )

    def _seed_dataset(self):
        max_date = dt.date(2026, 1, 30)
        baseline_start = max_date - dt.timedelta(days=13)  # 2026-01-17
        current_start = max_date - dt.timedelta(days=6)    # 2026-01-24

        # Baseline: mostly UPI success, low failures.
        for i in range(120):
            p_date = baseline_start + dt.timedelta(days=i % 7)
            ts = dt.datetime.combine(p_date, dt.time(hour=(i % 10) + 8, minute=0))
            if i < 108:
                self._insert_row(
                    self.MID, f"b_s_{i}", f"b_inv_{i}", "UPI", "SUCCESS", None, 220.0, p_date, ts, ts.hour, p_date.weekday()
                )
            else:
                fail_status = "FAILURE" if i % 2 == 0 else "FAILED"
                self._insert_row(
                    self.MID, f"b_f_{i}", f"b_inv_{i}", "UPI", fail_status, "91", 210.0, p_date, ts, ts.hour, p_date.weekday()
                )

        # Current: stronger CARD share + more failures to trigger drift and anomaly paths.
        for i in range(140):
            p_date = current_start + dt.timedelta(days=i % 7)
            ts = dt.datetime.combine(p_date, dt.time(hour=(i % 8) + 10, minute=0))
            if i < 90:
                mode = "CARD" if i % 3 != 0 else "UPI"
                self._insert_row(
                    self.MID, f"c_s_{i}", f"c_inv_{i}", mode, "SUCCESS", None, 300.0, p_date, ts, ts.hour, p_date.weekday()
                )
            else:
                fail_status = "FAILURE" if i % 2 == 0 else "FAILED"
                code = "U16" if i % 2 == 0 else "91"
                self._insert_row(
                    self.MID, f"c_f_{i}", f"c_inv_{i}", "UPI", fail_status, code, 280.0, p_date, ts, ts.hour, p_date.weekday()
                )

        # Retry chains for calibrated recovery rate.
        for i in range(30):
            p_date = current_start + dt.timedelta(days=i % 7)
            fail_ts = dt.datetime.combine(p_date, dt.time(hour=14, minute=i % 50))
            succ_ts = fail_ts + dt.timedelta(minutes=2)
            invoice = f"retry_{i}"
            self._insert_row(
                self.MID, f"r_fail_{i}", invoice, "UPI", "FAILURE", "91", 450.0, p_date, fail_ts, fail_ts.hour, p_date.weekday()
            )
            self._insert_row(
                self.MID, f"r_succ_{i}", invoice, "UPI", "SUCCESS", None, 450.0, p_date, succ_ts, succ_ts.hour, p_date.weekday()
            )

        # Add one low-quality row for DQ checks.
        bad_date = current_start + dt.timedelta(days=1)
        bad_ts = dt.datetime.combine(bad_date, dt.time(hour=9, minute=15))
        self._insert_row(
            self.MID, "bad_1", "bad_inv_1", "WALLET", "DECLINED", "X1", -50.0, bad_date, bad_ts, bad_ts.hour, bad_date.weekday()
        )

    def test_runner_contract_and_failed_label_normalization(self):
        result = run_intelligence(self.engine, self.MID, window_days=7)
        self.assertTrue(isinstance(result, dict))
        self.assertIn("recos", result)
        self.assertIn("signals", result)
        self.assertIn("phase2_recos", result)
        recos = result["recos"]
        self.assertTrue(isinstance(recos, list))
        self.assertGreater(len(recos), 0)
        for reco in recos:
            self.assertTrue(reco.reco_id)
            self.assertEqual(reco.merchant_id, self.MID)
            self.assertEqual(reco.window_days, 7)

    def test_runner_supports_disabling_phase2_reasoning(self):
        result = run_intelligence(
            self.engine,
            self.MID,
            window_days=7,
            enable_phase2_reasoning=False,
        )
        self.assertTrue(isinstance(result, dict))
        self.assertIn("phase2_recos", result)

    def test_payment_mode_reco_uses_deterministic_summary(self):
        reco = build_payment_mode_reco(
            self.engine,
            self.MID,
            window_days=7,
            start_date=dt.date(2026, 1, 24),
            end_date=dt.date(2026, 1, 31),
        )
        self.assertIsNotNone(reco)
        assert reco is not None
        self.assertIn("driving", reco.summary)
        self.assertIn("successful revenue", reco.summary)

    def test_peak_hour_reco_uses_deterministic_summary(self):
        reco = build_peak_hour_reco(
            self.engine,
            self.MID,
            window_days=7,
            start_date=dt.date(2026, 1, 24),
            end_date=dt.date(2026, 1, 31),
        )
        self.assertIsNotNone(reco)
        assert reco is not None
        self.assertIn("strongest revenue hour", reco.summary)
        self.assertIn("terminal connectivity", reco.summary)

    def test_phase2_gate_skips_low_impact_signals(self):
        should_run, reason = _requires_phase2_human_explanation(
            {
                "health_vector": {"status": "Healthy", "flags": [], "drivers": {"negative": []}},
                "impact_vector": {
                    "lost_sales": 12000.0,
                    "unknown_failure_value": 0.0,
                    "chargeback_risk": 0.0,
                    "reconciliation_gap": 5000.0,
                },
                "operational": {"evidence": {"top_failure_codes": []}},
                "reconciliation": {"evidence": {"recon_status_breakdown": []}},
                "disputes": {"evidence": {"chargeback_stage_distribution": []}},
            }
        )
        self.assertFalse(should_run)
        self.assertIn("impact below threshold", reason)

    def test_phase2_gate_allows_material_complex_signals(self):
        should_run, reason = _requires_phase2_human_explanation(
            {
                "health_vector": {
                    "status": "Watchlist",
                    "flags": ["high_unknown_failure_codes"],
                    "drivers": {"negative": ["High share of UNKNOWN failure codes"]},
                },
                "impact_vector": {
                    "lost_sales": 180000.0,
                    "unknown_failure_value": 95000.0,
                    "chargeback_risk": 12000.0,
                    "reconciliation_gap": 61000.0,
                },
                "operational": {"evidence": {"top_failure_codes": [{"code": "UNKNOWN"}], "by_payment_mode": [{"payment_mode": "UPI"}]}},
                "reconciliation": {"evidence": {"recon_status_breakdown": [{"status": "UNMATCHED", "count": 4}]}},
                "disputes": {"evidence": {"chargeback_stage_distribution": [{"stage": "OPEN", "count": 2}]}},
            }
        )
        self.assertTrue(should_run)
        self.assertIn("material impact", reason)

    def test_data_quality_check_detects_invalid_rows(self):
        start_date = dt.date(2026, 1, 24)
        end_date = dt.date(2026, 1, 31)
        report = run_data_quality_checks(self.engine, self.MID, start_date, end_date)
        self.assertFalse(report["passed"])
        self.assertIn("invalid_status", report["issues"])
        self.assertIn("invalid_payment_mode", report["issues"])
        self.assertIn("negative_amount", report["issues"])

    def test_drift_detection_finds_distribution_shift(self):
        current_start = dt.date(2026, 1, 24)
        current_end = dt.date(2026, 1, 31)
        baseline_start = dt.date(2026, 1, 17)
        baseline_end = dt.date(2026, 1, 24)
        drift = run_drift_checks(
            self.engine,
            self.MID,
            current_start,
            current_end,
            baseline_start,
            baseline_end,
        )
        self.assertTrue(len(drift["alerts"]) > 0)
        self.assertGreaterEqual(drift["metrics"]["payment_mode_tvd"], 0.0)

    def test_calibration_and_backtest_framework(self):
        start_date = dt.date(2026, 1, 24)
        end_date = dt.date(2026, 1, 31)
        calibration = estimate_recovery_rate(self.engine, self.MID, start_date, end_date)
        self.assertGreater(calibration["retry_events"], 0)
        self.assertGreater(calibration["rate"], 0.0)
        self.assertLessEqual(calibration["rate"], 1.0)

        backtest = run_recovery_backtest(
            self.engine,
            self.MID,
            end_date=end_date,
            window_days=7,
            n_windows=2,
        )
        self.assertEqual(backtest.windows_evaluated, 2)
        self.assertEqual(len(backtest.details), 2)


if __name__ == "__main__":
    unittest.main()

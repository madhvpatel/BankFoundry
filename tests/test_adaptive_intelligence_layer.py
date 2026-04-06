import unittest

from sqlalchemy import create_engine, text

from app.intelligence.engines.operational_signals import collect_operational_signals
from app.intelligence.engines.reconciliation_signals import collect_reconciliation_signals
from app.intelligence.runner import run_intelligence


class AdaptiveIntelligenceLayerTest(unittest.TestCase):
    MID = "m_adaptive_001"

    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE payment_transactions (
                        transaction_id TEXT,
                        mid TEXT,
                        invoice_id TEXT,
                        tid TEXT,
                        created_at TEXT,
                        updated_at TEXT,
                        txn_status TEXT,
                        mode TEXT,
                        gateway_response_code TEXT,
                        response_message TEXT,
                        bank_code TEXT,
                        network TEXT,
                        amount REAL,
                        hour_of_day INTEGER,
                        day_of_week INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO payment_transactions (
                        transaction_id, mid, invoice_id, tid, created_at, updated_at, txn_status, mode,
                        gateway_response_code, response_message, bank_code, network, amount, hour_of_day, day_of_week
                    ) VALUES
                    ('tx_1', :mid, 'inv_1', 'T1', '2026-03-10T10:00:00', '2026-03-10T10:01:00', 'SUCCESS', 'UPI', '00', 'Approved', 'HDFC', NULL, 1000, 10, 2),
                    ('tx_2', :mid, 'inv_2', 'T1', '2026-03-10T11:00:00', '2026-03-10T11:03:00', 'FAILED', 'UPI', '91', 'Issuer down', 'UBIN', NULL, 700, 11, 2),
                    ('tx_3', :mid, 'inv_3', 'T2', '2026-03-11T12:00:00', '2026-03-11T12:01:00', 'SUCCESS', 'CARD', '00', 'Approved', NULL, 'VISA', 1800, 12, 3),
                    ('tx_4', :mid, 'inv_4', 'T2', '2026-03-11T13:00:00', '2026-03-11T13:05:00', 'FAILED', 'CARD', '', '', NULL, 'VISA', 500, 13, 3)
                    """
                ),
                {"mid": self.MID},
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE settlement_records (
                        payout_id TEXT,
                        mid TEXT,
                        settlement_status TEXT,
                        settlement_date TEXT,
                        gross_amount REAL,
                        net_settlement_amount REAL,
                        mdr_deducted REAL,
                        gst_on_mdr REAL,
                        settlement_utr TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlement_records (
                        payout_id, mid, settlement_status, settlement_date, gross_amount, net_settlement_amount,
                        mdr_deducted, gst_on_mdr, settlement_utr
                    ) VALUES
                    ('st_1', :mid, 'PROCESSED', '2026-03-11', 10000, 8500, 1200, 300, 'utr_123')
                    """
                ),
                {"mid": self.MID},
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE reconciliation_records (
                        mid TEXT,
                        recon_status TEXT,
                        exception_reason TEXT,
                        created_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_records (mid, recon_status, exception_reason, created_at)
                    VALUES (:mid, 'EXCEPTION', 'settlement_gap', '2026-03-11')
                    """
                ),
                {"mid": self.MID},
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE mdr_rates (
                        mid TEXT,
                        payment_mode TEXT,
                        card_network TEXT,
                        mdr_percentage REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO mdr_rates (mid, payment_mode, card_network, mdr_percentage) VALUES
                    (:mid, 'UPI', '', 0.0),
                    (:mid, 'CARD', 'VISA', 1.5)
                    """
                ),
                {"mid": self.MID},
            )

    def test_signal_engines_fall_back_to_legacy_payment_and_settlement_sources(self):
        operational = collect_operational_signals(self.engine, self.MID, table="transaction_features", window_days=7)
        reconciliation = collect_reconciliation_signals(self.engine, self.MID, window_days=7)

        self.assertEqual(operational["tables"]["primary"], "payment_transactions")
        self.assertEqual(operational["metrics"]["attempts"], 4)
        self.assertEqual(operational["metrics"]["success_txns"], 2)
        self.assertTrue(any("payment_transactions" in note for note in operational.get("notes", [])))

        self.assertEqual(reconciliation["tables"]["settlements"], "settlement_records")
        self.assertEqual(reconciliation["metrics"]["settlement_batches"], 1)
        self.assertGreater(float(reconciliation["metrics"]["known_deductions_total"] or 0.0), 0.0)
        self.assertTrue(any("settlement_records" in note for note in reconciliation.get("notes", [])))

    def test_runner_uses_adaptive_sources_end_to_end(self):
        result = run_intelligence(
            self.engine,
            self.MID,
            window_days=7,
            enable_phase2_reasoning=False,
            persist_actions=False,
        )

        self.assertGreater(len(result["recos"]), 0)
        self.assertEqual(result["signals"]["operational"]["tables"]["primary"], "payment_transactions")
        self.assertEqual(result["signals"]["reconciliation"]["tables"]["settlements"], "settlement_records")
        self.assertTrue(any("payment_transactions" in note for note in result["signals"]["operational"].get("notes", [])))


if __name__ == "__main__":
    unittest.main()

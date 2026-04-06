import unittest

from sqlalchemy import create_engine, text

from app.data.actions import repository as action_repository
from app.data.disputes import repository as disputes_repository
from app.data.merchants import repository as merchants_repository
from app.data.settlements import repository as settlements_repository
from app.data.terminals import repository as terminals_repository
from app.data.transactions import repository as transactions_repository


class CopilotToolRepositoriesTest(unittest.TestCase):
    def test_fetch_merchant_context_returns_profile_risk_and_kyc(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchants (
                        mid TEXT,
                        merchant_trade_name TEXT,
                        nature_of_business TEXT,
                        business_city TEXT,
                        merchant_risk_category TEXT,
                        merchant_status TEXT,
                        annual_turnover REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_risk_profiles (
                        merchant_id TEXT,
                        risk_score REAL,
                        risk_band TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_kyc_documents (
                        merchant_id TEXT,
                        status TEXT,
                        expiry_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchants VALUES ('m_001', 'Demo Store', 'Retail', 'Mumbai', 'LOW', 'ACTIVE', 1000000)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_risk_profiles VALUES ('m_001', 0.12, 'LOW', '2026-03-22T10:00:00')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_kyc_documents VALUES
                    ('m_001', 'APPROVED', '2026-06-01'),
                    ('m_001', 'APPROVED', '2026-05-01')
                    """
                )
            )

        payload = merchants_repository.fetch_merchant_context(engine, "m_001")

        self.assertEqual(payload["merchant"]["merchant_trade_name"], "Demo Store")
        self.assertEqual(payload["risk_profile"]["band"], "LOW")
        self.assertEqual(payload["kyc"]["status"], "APPROVED")
        self.assertEqual(payload["kyc"]["next_expiry_at"], "2026-05-01")

    def test_transaction_repositories_return_scoped_metrics_and_details(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE transaction_features (
                        transaction_fact_id TEXT,
                        merchant_id TEXT,
                        terminal_id TEXT,
                        source_system TEXT,
                        source_txn_id TEXT,
                        p_date TEXT,
                        initiated_at TEXT,
                        completed_at TEXT,
                        payment_mode TEXT,
                        status TEXT,
                        response_code TEXT,
                        response_desc TEXT,
                        amount_rupees REAL,
                        hour_of_day INTEGER,
                        card_network TEXT,
                        pos_type TEXT,
                        device_type TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features VALUES
                    ('tx_1', 'm_001', 'T1', 'pg', 's1', '2026-03-10', '2026-03-10T10:00:00', '2026-03-10T10:01:00', 'UPI', 'SUCCESS', '00', 'Approved', 1000, 10, 'VISA', 'POS', 'ANDROID'),
                    ('tx_2', 'm_001', 'T1', 'pg', 's2', '2026-03-11', '2026-03-11T11:00:00', '2026-03-11T11:01:00', 'CARD', 'FAILED', '91', 'Issuer timeout', 500, 11, 'VISA', 'POS', 'ANDROID'),
                    ('tx_3', 'm_001', 'T2', 'pg', 's3', '2026-03-12', '2026-03-12T12:00:00', '2026-03-12T12:01:00', 'CARD', 'SUCCESS', '00', 'Approved', 1500, 12, 'MASTERCARD', 'SOUND_BOX', 'LINUX')
                    """
                )
            )

        kpis = transactions_repository.compute_kpis(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            group_by="payment_mode",
            source_table="transaction_features",
        )
        drivers = transactions_repository.verify_failure_drivers(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            by="response_code",
            source_table="transaction_features",
        )
        rows = transactions_repository.list_transactions(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            limit=5,
            source_table="transaction_features",
        )
        detail = transactions_repository.get_transaction_detail(
            engine,
            merchant_id="m_001",
            tx_id="tx_2",
            source_table="transaction_features",
        )
        terminals = transactions_repository.terminal_performance(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            source_table="transaction_features",
        )
        fail_codes = transactions_repository.top_failure_codes(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            limit=5,
            source_table="transaction_features",
        )
        device_slice = transactions_repository.slice_performance_by_column(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            column="device_type",
            limit=5,
            source_table="transaction_features",
        )

        self.assertEqual(len(kpis["rows"]), 2)
        self.assertEqual(drivers["rows"][0]["driver"], "91")
        self.assertEqual(rows["rows"][0]["tx_id"], "tx_3")
        self.assertEqual(detail["row"]["response_code"], "91")
        self.assertEqual(terminals["rows"][0]["terminal_id"], "T1")
        self.assertEqual(fail_codes[0]["response_code"], "91")
        self.assertEqual(device_slice[0]["bucket"], "ANDROID")

    def test_settlement_and_dispute_repositories_return_windowed_payloads(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE settlements (
                        settlement_id TEXT,
                        mid TEXT,
                        settlement_status TEXT,
                        settlement_date TEXT,
                        gross_amount REAL,
                        net_settlement_amount REAL,
                        mdr_deducted REAL,
                        gst_on_mdr REAL,
                        tds_deducted REAL,
                        chargeback_deductions REAL,
                        reserve_held REAL,
                        adjustment_amount REAL,
                        hold_reason TEXT,
                        settlement_utr TEXT,
                        payment_mode TEXT,
                        txn_count INTEGER,
                        refund_count INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE reconciliation_records (
                        merchant_id TEXT,
                        settlement_id TEXT,
                        status TEXT,
                        reason TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE refunds (
                        refund_id TEXT,
                        merchant_id TEXT,
                        status TEXT,
                        created_at TEXT,
                        amount_rupees REAL,
                        tx_id TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE chargebacks (
                        chargeback_id TEXT,
                        merchant_id TEXT,
                        status TEXT,
                        opened_at TEXT,
                        due_by TEXT,
                        amount_rupees REAL,
                        reason_code TEXT,
                        network TEXT,
                        tx_id TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements VALUES
                    ('s_1', 'm_001', 'HELD', '2026-03-10', 25000, 24882, 100, 18, 0, 0, 0, 0, 'Risk review', 'utr_1', 'UPI', 20, 1),
                    ('s_2', 'm_001', 'PROCESSED', '2026-03-09', 10000, 9900, 80, 20, 0, 0, 0, 0, NULL, 'utr_2', 'CARD', 10, 0)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_records VALUES
                    ('m_001', 's_1', 'OPEN', 'Risk review')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO refunds VALUES
                    ('r_1', 'm_001', 'SUCCESS', '2026-03-11', 120.0, 'tx_1')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO chargebacks VALUES
                    ('c_1', 'm_001', 'OPEN', '2026-03-12', '2026-03-20', 500.0, '10.4', 'VISA', 'tx_2')
                    """
                )
            )

        settlements = settlements_repository.list_settlements(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            limit=10,
        )
        cashflow = settlements_repository.cashflow_snapshot(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
        )
        shortfall = settlements_repository.explain_settlement_shortfall(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            expected_amount=25000,
            received_amount=24882,
        )
        settlement_detail = settlements_repository.get_settlement_detail(
            engine,
            merchant_id="m_001",
            settlement_id="s_1",
        )
        refunds = disputes_repository.list_refunds(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            limit=10,
        )
        chargebacks = disputes_repository.list_chargebacks(
            engine,
            merchant_id="m_001",
            status="open",
            from_date="2026-03-01",
            to_date="2026-03-20",
            limit=10,
        )
        chargeback_detail = disputes_repository.get_chargeback_detail(
            engine,
            merchant_id="m_001",
            chargeback_id="c_1",
        )

        self.assertEqual(settlements["rows"][0]["settlement_id"], "s_1")
        self.assertEqual({row["status"] for row in cashflow["by_status"]}, {"HELD", "PROCESSED"})
        self.assertEqual(shortfall["shortfall"]["settlement_id"], "s_1")
        self.assertEqual(shortfall["shortfall"]["difference_amount"], 118.0)
        self.assertEqual(settlement_detail["reconciliation"][0]["reason"], "Risk review")
        self.assertEqual(refunds["rows"][0]["refund_id"], "r_1")
        self.assertEqual(chargebacks["rows"][0]["chargeback_id"], "c_1")
        self.assertEqual(chargeback_detail["row"]["network"], "VISA")
        self.assertEqual(disputes_repository.refund_summary(engine, merchant_id="m_001", from_date="2026-03-01", to_date="2026-03-20")["refunds_count"], 1)
        self.assertEqual(disputes_repository.chargeback_count(engine, merchant_id="m_001", from_date="2026-03-01", to_date="2026-03-20"), 1)

    def test_terminal_and_action_repositories_cover_health_correlation_and_write_path(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE terminals (
                        tid TEXT,
                        mid TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE terminal_health_snapshots (
                        tid TEXT,
                        captured_at TEXT,
                        low_network_strength INTEGER,
                        battery_status REAL,
                        quick_battery_drainage INTEGER,
                        latitude_longitude_deviation INTEGER,
                        printer_status TEXT,
                        ram_rom_utilization REAL,
                        latitude REAL,
                        longitude REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE transaction_features (
                        transaction_fact_id TEXT,
                        merchant_id TEXT,
                        terminal_id TEXT,
                        p_date TEXT,
                        status TEXT,
                        amount_rupees REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        merchant_id TEXT,
                        action_type TEXT,
                        payload TEXT,
                        status TEXT
                    )
                    """
                )
            )
            conn.execute(text("INSERT INTO terminals VALUES ('T1', 'm_001'), ('T2', 'm_001')"))
            conn.execute(
                text(
                    """
                    INSERT INTO terminal_health_snapshots VALUES
                    ('T1', '2026-03-10T10:00:00', 1, 60.0, 0, 0, 'OK', 50.0, 19.1, 72.8),
                    ('T1', '2026-03-10T11:00:00', 1, 58.0, 1, 1, 'OK', 52.0, 19.2, 72.85),
                    ('T2', '2026-03-10T10:00:00', 0, 80.0, 0, 0, 'OK', 40.0, 19.1, 72.8)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features VALUES
                    ('tx_1', 'm_001', 'T1', '2026-03-10', 'FAILED', 1000),
                    ('tx_2', 'm_001', 'T1', '2026-03-11', 'SUCCESS', 500),
                    ('tx_3', 'm_001', 'T2', '2026-03-10', 'SUCCESS', 900)
                    """
                )
            )

        summary = terminals_repository.terminal_health_summary(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            group_by="tid",
            limit=10,
        )
        drift = terminals_repository.geo_drift_check(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            terminal_id="T1",
        )
        correlation = terminals_repository.terminal_issue_correlator(
            engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            flag="low_network_strength",
            limit=10,
            source_table="transaction_features",
        )
        action = action_repository.create_merchant_action(
            engine,
            merchant_id="m_001",
            preview={
                "merchant_id": "m_001",
                "action_type": "CHECK_TERMINAL",
                "payload": {"title": "Inspect terminal T1"},
            },
        )

        self.assertEqual(summary["rows"][0]["tid"], "T1")
        self.assertEqual(summary["rows"][0]["snapshots"], 2)
        self.assertEqual(drift["rows"][0]["tid"], "T1")
        self.assertEqual(correlation["rows"][0]["tid"], "T1")
        self.assertIsNotNone(action["action_id"])


if __name__ == "__main__":
    unittest.main()

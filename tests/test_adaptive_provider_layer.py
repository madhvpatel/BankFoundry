import unittest
from datetime import date

from sqlalchemy import create_engine, text

from app.data.merchant_ops import repository as merchant_ops_repository
from app.data.merchants import repository as merchants_repository
from app.data.settlements import repository as settlements_repository
from app.data.transactions import repository as transactions_repository


class AdaptiveProviderLayerTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    def test_fetch_dashboard_metrics_and_merchant_options_fall_back_from_missing_primary_table(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE payment_transactions (
                        transaction_id TEXT,
                        mid TEXT,
                        tid TEXT,
                        created_at TEXT,
                        txn_status TEXT,
                        mode TEXT,
                        gateway_response_code TEXT,
                        amount REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO payment_transactions (
                        transaction_id, mid, tid, created_at, txn_status, mode, gateway_response_code, amount
                    ) VALUES
                    ('tx_1', 'm_001', 'T1', '2026-03-20T10:00:00', 'SUCCESS', 'UPI', '00', 1000),
                    ('tx_2', 'm_001', 'T1', '2026-03-21T11:00:00', 'FAILED', 'CARD', '91', 500),
                    ('tx_3', 'm_001', 'T2', '2026-03-22T12:00:00', 'SUCCESS', 'CARD', '00', 1500)
                    """
                )
            )

        payload = transactions_repository.fetch_dashboard_metrics(
            self.engine,
            merchant_id="m_001",
            terminal_id=None,
            lookback_days=30,
            reference_date=date(2026, 3, 22),
        )
        merchant_options = merchants_repository.load_merchant_options(
            self.engine,
            limit=10,
            query_source_table="transaction_features",
            default_merchant_id_loader=lambda _engine: None,
        )

        self.assertEqual(payload["kpis"]["attempts"], 3)
        self.assertEqual(payload["kpis"]["success_txns"], 2)
        self.assertEqual(payload["kpis"]["fail_txns"], 1)
        self.assertEqual(payload["kpis"]["success_gmv"], 2500.0)
        self.assertEqual(merchant_options, [{"merchant_id": "m_001", "label": "m_001"}])
        self.assertTrue(any("payment_transactions" in note for note in payload.get("notes", [])))

    def test_transaction_queries_use_legacy_payment_transaction_schema(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE payment_transactions (
                        transaction_id TEXT,
                        mid TEXT,
                        tid TEXT,
                        created_at TEXT,
                        completed_at TEXT,
                        txn_status TEXT,
                        mode TEXT,
                        gateway_response_code TEXT,
                        amount REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO payment_transactions (
                        transaction_id, mid, tid, created_at, completed_at, txn_status, mode, gateway_response_code, amount
                    ) VALUES
                    ('tx_1', 'm_001', 'T1', '2026-03-10T10:00:00', '2026-03-10T10:01:00', 'SUCCESS', 'UPI', '00', 1000),
                    ('tx_2', 'm_001', 'T1', '2026-03-11T11:00:00', '2026-03-11T11:02:00', 'FAILED', 'CARD', '91', 500),
                    ('tx_3', 'm_001', 'T2', '2026-03-12T12:00:00', '2026-03-12T12:03:00', 'SUCCESS', 'CARD', '00', 1500)
                    """
                )
            )

        kpis = transactions_repository.compute_kpis(
            self.engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            group_by="payment_mode",
            source_table="transaction_features",
        )
        rows = transactions_repository.list_transactions(
            self.engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            limit=5,
            source_table="transaction_features",
        )
        detail = transactions_repository.get_transaction_detail(
            self.engine,
            merchant_id="m_001",
            tx_id="tx_2",
            source_table="transaction_features",
        )
        top_codes = transactions_repository.top_failure_codes(
            self.engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
            limit=5,
            source_table="transaction_features",
        )

        self.assertEqual(len(kpis["rows"]), 2)
        self.assertEqual(rows["rows"][0]["tx_id"], "tx_3")
        self.assertEqual(rows["rows"][0]["terminal_id"], "T2")
        self.assertEqual(detail["row"]["response_code"], "91")
        self.assertEqual(top_codes[0]["response_code"], "91")
        self.assertTrue(any("payment_transactions" in note for note in rows.get("notes", [])))

    def test_settlement_queries_and_coverage_fall_back_to_legacy_settlement_source(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE payment_transactions (
                        transaction_id TEXT,
                        mid TEXT,
                        created_at TEXT,
                        txn_status TEXT,
                        mode TEXT,
                        amount REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO payment_transactions (
                        transaction_id, mid, created_at, txn_status, mode, amount
                    ) VALUES
                    ('tx_1', 'm_001', '2026-03-10T10:00:00', 'SUCCESS', 'UPI', 1000)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE settlement_records (
                        settlement_id TEXT,
                        mid TEXT,
                        settlement_status TEXT,
                        settlement_date TEXT,
                        gross_amount REAL,
                        net_settlement_amount REAL,
                        mdr_deducted REAL,
                        settlement_utr TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlement_records (
                        settlement_id, mid, settlement_status, settlement_date, gross_amount, net_settlement_amount, mdr_deducted, settlement_utr
                    ) VALUES
                    ('st_001', 'm_001', 'PROCESSED', '2026-03-12', 10000, 9700, 300, 'utr_123')
                    """
                )
            )

        settlements = settlements_repository.list_settlements(
            self.engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
        )
        shortfall = settlements_repository.explain_settlement_shortfall(
            self.engine,
            merchant_id="m_001",
            from_date="2026-03-01",
            to_date="2026-03-20",
        )
        coverage = merchant_ops_repository.detect_connected_systems(
            self.engine,
            "m_001",
            integration_table_candidates={
                "erp": (),
                "accounting": (),
                "pos": (),
                "api": (),
            },
            query_source_table="transaction_features",
        )

        self.assertEqual(settlements["rows"][0]["settlement_id"], "st_001")
        self.assertEqual(shortfall["shortfall"]["difference_amount"], 300.0)
        self.assertEqual(coverage["data_domains"]["payments"]["source_table"], "payment_transactions")
        self.assertEqual(coverage["data_domains"]["settlements"]["source_table"], "settlement_records")
        self.assertTrue(any("settlement_records" in note for note in settlements.get("notes", [])))


if __name__ == "__main__":
    unittest.main()

import unittest

from sqlalchemy import create_engine, text

from app.data.merchant_ops import repository as merchant_ops_repository


class MerchantOpsRepositoryTest(unittest.TestCase):
    def test_detect_connected_systems_reports_payments_and_erp(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE transaction_features (
                        merchant_id TEXT,
                        p_date TEXT,
                        status TEXT,
                        payment_mode TEXT,
                        amount_rupees REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE settlements (
                        merchant_id TEXT,
                        status TEXT,
                        expected_date TEXT,
                        settled_at TEXT,
                        amount_rupees REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_integrations (
                        merchant_id TEXT,
                        integration_type TEXT,
                        status TEXT,
                        provider TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_integrations (merchant_id, integration_type, status, provider)
                    VALUES ('m_001', 'erp', 'ACTIVE', 'tally')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features (merchant_id, p_date, status, payment_mode, amount_rupees)
                    VALUES
                        ('m_001', '2026-02-01', 'SUCCESS', 'UPI', 100.0),
                        ('m_001', '2026-02-02', 'FAILED', 'CARD', 50.0)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements (merchant_id, status, expected_date, settled_at, amount_rupees)
                    VALUES ('m_001', 'PROCESSED', '2026-02-03', NULL, 120.0)
                    """
                )
            )

        coverage = merchant_ops_repository.detect_connected_systems(
            engine,
            "m_001",
            integration_table_candidates={
                "erp": ("merchant_integrations",),
                "accounting": (),
                "pos": (),
                "api": (),
            },
            query_source_table="transaction_features",
        )

        self.assertEqual(coverage["coverage_label"], "Payments + ERP")
        self.assertTrue(coverage["systems"]["payments"]["connected"])
        self.assertTrue(coverage["systems"]["erp"]["connected"])
        self.assertEqual(coverage["data_domains"]["payments"]["row_count"], 2)

    def test_terminal_scope_queries_return_scoped_metrics(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE transaction_features (
                        merchant_id TEXT,
                        terminal_id TEXT,
                        p_date TEXT,
                        status TEXT,
                        payment_mode TEXT,
                        response_code TEXT,
                        amount_rupees REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features
                    (merchant_id, terminal_id, p_date, status, payment_mode, response_code, amount_rupees)
                    VALUES
                    ('m_001', 'T1', '2026-01-10', 'SUCCESS', 'CARD', '00', 1000.0),
                    ('m_001', 'T1', '2026-01-11', 'FAILED', 'CARD', '05', 200.0),
                    ('m_001', 'T1', '2026-01-12', 'FAILED', 'UPI', '91', 300.0),
                    ('m_001', 'T2', '2026-01-10', 'SUCCESS', 'UPI', '00', 700.0)
                    """
                )
            )

        summary = merchant_ops_repository.terminal_scope_summary_from_source(
            engine,
            "m_001",
            "T1",
            "2026-01-01",
            "2026-02-01",
            source_table="transaction_features",
        )
        by_mode = merchant_ops_repository.terminal_scope_kpis_by_mode(
            engine,
            "m_001",
            "T1",
            "2026-01-01",
            "2026-02-01",
            source_table="transaction_features",
        )
        drivers = merchant_ops_repository.terminal_scope_failure_drivers(
            engine,
            "m_001",
            "T1",
            "2026-01-01",
            "2026-02-01",
            by="response_code",
            limit=5,
            source_table="transaction_features",
        )

        self.assertEqual(summary["attempts"], 3)
        self.assertEqual(summary["success_txns"], 1)
        self.assertEqual(summary["fail_txns"], 2)
        self.assertEqual(by_mode[0]["bucket"], "CARD")
        self.assertEqual(drivers["rows"][0]["driver"], "91")


if __name__ == "__main__":
    unittest.main()

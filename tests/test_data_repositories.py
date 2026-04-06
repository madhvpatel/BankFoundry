import unittest
from datetime import date

from sqlalchemy import create_engine, text

from app.data.merchants import load_merchant_options
from app.data.transactions import fetch_dashboard_metrics


class DataRepositoriesTest(unittest.TestCase):
    def test_load_merchant_options_prefers_merchants_table_when_available(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchants (
                        merchant_id TEXT,
                        merchant_trade_name TEXT,
                        business_city TEXT,
                        nature_of_business TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchants (merchant_id, merchant_trade_name, business_city, nature_of_business)
                    VALUES
                    ('merchant_001', 'Demo Store', 'Mumbai', 'Retail'),
                    ('merchant_002', 'Cafe Sample', 'Delhi', 'Food')
                    """
                )
            )

        options = load_merchant_options(
            engine,
            limit=10,
            query_source_table="transaction_features",
            default_merchant_id_loader=lambda _engine: "fallback_mid",
        )

        self.assertEqual(options[0]["merchant_id"], "merchant_001")
        self.assertEqual(options[0]["label"], "Demo Store | Mumbai")
        self.assertEqual(options[1]["merchant_id"], "merchant_002")

    def test_fetch_dashboard_metrics_returns_kpis_and_modes(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE transaction_features (
                        merchant_id TEXT,
                        terminal_id TEXT,
                        p_date DATE,
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
                    INSERT INTO transaction_features (
                        merchant_id, terminal_id, p_date, status, payment_mode, amount_rupees
                    ) VALUES
                    ('merchant_001', 'T1', '2026-03-20', 'SUCCESS', 'UPI', 1000),
                    ('merchant_001', 'T1', '2026-03-21', 'FAILED', 'CARD', 500),
                    ('merchant_001', 'T2', '2026-03-22', 'SUCCESS', 'CARD', 1500)
                    """
                )
            )

        payload = fetch_dashboard_metrics(
            engine,
            merchant_id="merchant_001",
            terminal_id=None,
            lookback_days=30,
            reference_date=date(2026, 3, 22),
        )

        self.assertEqual(payload["window"]["from"], date(2026, 2, 20))
        self.assertEqual(payload["window"]["to"], date(2026, 3, 22))
        self.assertEqual(payload["kpis"]["attempts"], 3)
        self.assertEqual(payload["kpis"]["success_txns"], 2)
        self.assertEqual(payload["kpis"]["fail_txns"], 1)
        self.assertEqual(payload["kpis"]["success_gmv"], 2500.0)
        self.assertEqual(payload["kpis"]["success_rate_pct"], 66.67)
        self.assertEqual(len(payload["charts"]["payment_modes"]), 2)


if __name__ == "__main__":
    unittest.main()

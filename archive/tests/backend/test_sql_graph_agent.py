import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

from app.copilot.sql_catalog import load_catalog
from app.copilot.sql_graph_agent import run_sql_langgraph


class SQLCatalogDiscoveryTest(unittest.TestCase):
    def test_load_catalog_includes_new_discovered_table(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE merchant_kpi_daily (
                    merchant_id TEXT,
                    p_date DATE,
                    success_rate_pct REAL
                )
                """
            )
        with patch("config.Config.SQL_GRAPH_AUTO_DISCOVER_TABLES", True):
            catalog = load_catalog(engine)

        names = {str(t.get("name")) for t in (catalog.get("tables") or []) if isinstance(t, dict)}
        self.assertIn("merchant_kpi_daily", names)


class SQLGraphAgentTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE transaction_features (
                    merchant_id TEXT,
                    p_date DATE,
                    status TEXT,
                    response_code TEXT,
                    payment_mode TEXT,
                    amount_rupees REAL
                )
                """
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features
                    (merchant_id, p_date, status, response_code, payment_mode, amount_rupees) VALUES
                    ('merchant_001', '2026-01-10', 'FAILED', '51', 'CARD', 100.0),
                    ('merchant_001', '2026-01-11', 'FAILED', '55', 'CARD', 200.0),
                    ('merchant_001', '2026-01-11', 'SUCCESS', NULL, 'UPI', 500.0)
                    """
                )
            )

    def test_run_sql_langgraph_returns_verified_rows(self):
        with patch("app.copilot.sql_graph_agent._ask_json", side_effect=lambda _s, _u, fallback: fallback), patch(
            "app.copilot.sql_graph_agent._ask_text", side_effect=lambda _s, _u, fallback: fallback
        ):
            out = run_sql_langgraph(
                engine=self.engine,
                merchant_id="merchant_001",
                question="top failure drivers by response code",
                lane="growth",
                from_date="2026-01-01",
                to_date="2026-02-01",
            )

        self.assertTrue(out.get("verified"))
        self.assertGreater(int(out.get("row_count") or 0), 0)
        self.assertIn("transaction_features", out.get("selected_views", []))
        self.assertTrue(any(str(e).startswith("sqlgraph:") for e in (out.get("evidence") or [])))


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

from types import SimpleNamespace

from app.copilot.runtime import _filter_tools_for_lane
from app.copilot.toolcalling import make_tools
from app.copilot.tools import (
    ToolContext,
    explain_settlement_shortfall,
    intelligence_probe,
    list_chargebacks,
    list_refunds,
)
from app.intelligence.type import Recommendation


class ToolcallingWiringTest(unittest.TestCase):
    def _tools(self):
        ctx = ToolContext(engine=object(), merchant_id="merchant_001")
        return {t.name: t for t in make_tools(ctx=ctx, default_from="2026-01-01", default_to="2026-01-31")}

    @patch("app.copilot.toolcalling.list_chargebacks")
    def test_list_chargebacks_wrapper_passes_supported_args(self, mock_list_chargebacks):
        mock_list_chargebacks.return_value = {"rows": [], "evidence": []}
        tools = self._tools()

        out = tools["list_chargebacks"].invoke(
            {
                "from_date": "2026-01-10",
                "to_date": "2026-01-20",
                "status": "all",
                "limit": 12,
            }
        )

        self.assertEqual(out, {"rows": [], "evidence": []})
        _, kwargs = mock_list_chargebacks.call_args
        self.assertEqual(kwargs["status"], "all")
        self.assertEqual(kwargs["from_date"], "2026-01-10")
        self.assertEqual(kwargs["to_date"], "2026-01-20")
        self.assertEqual(kwargs["limit"], 12)

    @patch("app.copilot.toolcalling.get_settlement_detail")
    def test_get_settlement_detail_wrapper_accepts_integer_ids(self, mock_get_settlement_detail):
        mock_get_settlement_detail.return_value = {"row": {}, "evidence": []}
        tools = self._tools()

        out = tools["get_settlement_detail"].invoke({"settlement_id": 10})

        self.assertEqual(out, {"row": {}, "evidence": []})
        _, kwargs = mock_get_settlement_detail.call_args
        self.assertEqual(kwargs["settlement_id"], "10")

    @patch("app.copilot.toolcalling.compare_kpis")
    def test_compare_kpis_wrapper_accepts_window_dicts(self, mock_compare_kpis):
        mock_compare_kpis.return_value = {"a": {}, "b": {}, "evidence": []}
        tools = self._tools()

        tools["compare_kpis"].invoke(
            {
                "window_a": {"from_date": "2026-01-10", "to_date": "2026-01-20"},
                "window_b": {"from": "2025-12-31", "to": "2026-01-10"},
                "group_by": "payment_mode",
            }
        )

        _, kwargs = mock_compare_kpis.call_args
        self.assertEqual(kwargs["from_date_a"], "2026-01-10")
        self.assertEqual(kwargs["to_date_a"], "2026-01-20")
        self.assertEqual(kwargs["from_date_b"], "2025-12-31")
        self.assertEqual(kwargs["to_date_b"], "2026-01-10")
        self.assertEqual(kwargs["group_by"], "payment_mode")

    @patch("app.copilot.toolcalling.compare_kpis")
    def test_compare_kpis_wrapper_derives_previous_window(self, mock_compare_kpis):
        mock_compare_kpis.return_value = {"a": {}, "b": {}, "evidence": []}
        tools = self._tools()

        tools["compare_kpis"].invoke(
            {
                "from_date_a": "2026-01-10",
                "to_date_a": "2026-01-20",
            }
        )

        _, kwargs = mock_compare_kpis.call_args
        self.assertEqual(kwargs["from_date_a"], "2026-01-10")
        self.assertEqual(kwargs["to_date_a"], "2026-01-20")
        self.assertEqual(kwargs["from_date_b"], "2025-12-31")
        self.assertEqual(kwargs["to_date_b"], "2026-01-10")


class ChargebackToolTest(unittest.TestCase):
    def test_list_chargebacks_applies_date_filter(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE chargebacks (
                    chargeback_id TEXT,
                    merchant_id TEXT,
                    status TEXT,
                    opened_at TEXT,
                    due_by TEXT,
                    amount_rupees REAL,
                    reason_code TEXT
                )
                """
            )
            conn.execute(
                text(
                    """
                    INSERT INTO chargebacks (
                        chargeback_id, merchant_id, status, opened_at, due_by, amount_rupees, reason_code
                    ) VALUES
                        ('cb_old', 'merchant_001', 'OPEN', '2026-01-05', '2026-01-15', 1000, '55'),
                        ('cb_new', 'merchant_001', 'OPEN', '2026-01-18', '2026-01-28', 1500, '91')
                    """
                )
            )

        ctx = ToolContext(engine=engine, merchant_id="merchant_001")
        result = list_chargebacks(
            ctx,
            status="all",
            from_date="2026-01-10",
            to_date="2026-01-25",
            limit=10,
        )

        ids = [row["chargeback_id"] for row in result.get("rows", [])]
        self.assertEqual(ids, ["cb_new"])


class RefundToolTest(unittest.TestCase):
    def test_list_refunds_normalizes_legacy_schema(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE refunds (
                    mid TEXT,
                    refund_amount REAL,
                    p_date TEXT
                )
                """
            )
            conn.execute(
                text(
                    """
                    INSERT INTO refunds (mid, refund_amount, p_date)
                    VALUES
                      ('merchant_001', 250.0, '2026-01-05'),
                      ('merchant_001', 500.0, '2026-01-18')
                    """
                )
            )

        ctx = ToolContext(engine=engine, merchant_id="merchant_001")
        result = list_refunds(
            ctx,
            from_date="2026-01-10",
            to_date="2026-01-25",
            limit=10,
        )

        rows = result.get("rows", [])
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["refund_id"])
        self.assertEqual(rows[0]["status"], "UNKNOWN")
        self.assertEqual(rows[0]["amount_rupees"], 500.0)
        self.assertEqual(rows[0]["created_at"], "2026-01-18")


class ShortfallToolTest(unittest.TestCase):
    def test_explain_settlement_shortfall_uses_legacy_deduction_fields(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE settlements (
                    settlement_id TEXT,
                    mid TEXT,
                    settlement_date TEXT,
                    settlement_status TEXT,
                    gross_amount REAL,
                    mdr_deducted REAL,
                    gst_on_mdr REAL,
                    tds_deducted REAL,
                    chargeback_deductions REAL,
                    reserve_held REAL,
                    adjustment_amount REAL,
                    net_settlement_amount REAL,
                    settlement_utr TEXT,
                    hold_reason TEXT
                )
                """
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements (
                        settlement_id, mid, settlement_date, settlement_status, gross_amount,
                        mdr_deducted, gst_on_mdr, tds_deducted, chargeback_deductions,
                        reserve_held, adjustment_amount, net_settlement_amount, settlement_utr, hold_reason
                    ) VALUES (
                        's_001', 'merchant_001', '2026-02-16', 'PROCESSED', 20000.0,
                        700.0, 126.0, 0.0, 0.0,
                        174.0, 0.0, 19000.0, 'UTR001', NULL
                    )
                    """
                )
            )

        ctx = ToolContext(engine=engine, merchant_id="merchant_001")
        result = explain_settlement_shortfall(
            ctx,
            from_date="2026-02-01",
            to_date="2026-03-01",
            expected_amount=20000.0,
            received_amount=19000.0,
        )

        self.assertTrue(result["verified"])
        self.assertEqual(result["shortfall"]["settlement_id"], "s_001")
        self.assertEqual(result["shortfall"]["difference_amount"], 1000.0)
        self.assertEqual(result["shortfall"]["unexplained_amount"], 0.0)
        self.assertIn("settlement:s_001", result["evidence"])
        self.assertIn("MDR", result["deduction_explanation"])


class RuntimeAllowlistTest(unittest.TestCase):
    def test_operations_lane_allows_verify_failure_drivers(self):
        tools = [
            SimpleNamespace(name="list_settlements"),
            SimpleNamespace(name="verify_failure_drivers"),
            SimpleNamespace(name="explain_settlement_shortfall"),
            SimpleNamespace(name="terminal_performance"),
        ]

        filtered = _filter_tools_for_lane(tools, "operations")
        filtered_names = {tool.name for tool in filtered}

        self.assertIn("verify_failure_drivers", filtered_names)
        self.assertIn("explain_settlement_shortfall", filtered_names)
        self.assertIn("list_settlements", filtered_names)
        self.assertNotIn("terminal_performance", filtered_names)


class IntelligenceProbeNormalizationTest(unittest.TestCase):
    @patch("app.copilot.tools.run_intelligence")
    def test_intelligence_probe_accepts_dataclass_recommendations(self, mock_run_intelligence):
        reco = Recommendation(
            reco_id="reco_001",
            merchant_id="merchant_001",
            window_days=30,
            category="growth",
            title="Expand peak-hour throughput",
            summary="Target staffing and device uptime during peak hour.",
            impact_rupees=12000.0,
            confidence=0.8,
            priority_score=7.2,
            drivers=[],
            actions=[{"who": "merchant", "text": "Add one backup terminal."}],
            evidence_ids=["ev_001"],
        )
        mock_run_intelligence.return_value = {"recos": [reco], "phase2_recos": [], "signals": {}}

        ctx = ToolContext(engine=object(), merchant_id="merchant_001")
        out = intelligence_probe(ctx, window_days=30, enable_reasoning=False)

        self.assertEqual(len(out["recommendations"]), 1)
        self.assertEqual(out["recommendations"][0]["title"], "Expand peak-hour throughput")
        self.assertIn("ev_001", out["evidence"])
        _, kwargs = mock_run_intelligence.call_args
        self.assertFalse(kwargs["persist_actions"])


if __name__ == "__main__":
    unittest.main()

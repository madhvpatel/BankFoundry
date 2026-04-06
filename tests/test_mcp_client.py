import unittest

from sqlalchemy import create_engine, text

from app.agent.mcp_client import BankFoundryMCPClient, FailureDiagnosticsMCPAgent, OpsCaseCopilotMCPAgent
from app.mcp_server import BankFoundryMCPServer


class MCPClientTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
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
                    CREATE TABLE transaction_features (
                        transaction_fact_id TEXT,
                        merchant_id TEXT,
                        terminal_id TEXT,
                        p_date TEXT,
                        initiated_at TEXT,
                        payment_mode TEXT,
                        status TEXT,
                        response_code TEXT,
                        amount_rupees REAL,
                        hour_of_day INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchants VALUES
                    ('m_001', 'Demo Store', 'Retail', 'Mumbai', 'LOW', 'ACTIVE', 1000000)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features VALUES
                    ('tx_1', 'm_001', 'T1', '2026-03-10', '2026-03-10T10:00:00', 'UPI', 'SUCCESS', '00', 1000, 10),
                    ('tx_2', 'm_001', 'T1', '2026-03-11', '2026-03-11T11:00:00', 'CARD', 'FAILED', '91', 500, 11),
                    ('tx_3', 'm_001', 'T2', '2026-03-12', '2026-03-12T12:00:00', 'CARD', 'FAILED', '91', 700, 12)
                    """
                )
            )
        self.server = BankFoundryMCPServer(self.engine)

    def test_client_tool_filter_hides_tools(self):
        client = BankFoundryMCPClient(self.server, tool_filter=["get_window_kpis"])

        tools = client.list_tools()

        self.assertEqual([tool.name for tool in tools], ["get_window_kpis"])
        with self.assertRaises(PermissionError):
            client.call_tool("get_merchant_profile", {"merchant_id": "m_001"})

    def test_failure_diagnostics_agent_uses_mcp_tools_and_returns_evidence(self):
        client = BankFoundryMCPClient(
            self.server,
            tool_filter=["get_merchant_profile", "get_window_kpis", "get_failure_breakdown"],
        )
        agent = FailureDiagnosticsMCPAgent(client)

        result = agent.analyze_failure_increase(
            merchant_id="m_001",
            start_date="2026-03-01",
            end_date="2026-03-20",
        )

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(len(result["tool_calls"]), 3)
        self.assertIn("Demo Store shows 2 failed transactions", result["answer"])
        self.assertIn("91 with 2 failures", result["answer"])
        self.assertIn("merchant:m_001", result["evidence_ids"])
        self.assertIn("kpi:none:2026-03-01:2026-03-20", result["evidence_ids"])

    def test_ops_case_copilot_agent_summarizes_case_using_mcp_tools(self):
        client = BankFoundryMCPClient(
            self.server,
            tool_filter=["get_merchant_profile", "get_window_kpis", "get_failure_breakdown"],
        )
        agent = OpsCaseCopilotMCPAgent(client)

        summary = agent.summarize_case(
            case_detail={
                "work_item": {
                    "case_id": "case_123",
                    "merchant_id": "m_001",
                    "title": "Held settlement 261",
                    "summary": "Settlement 261 remains held beyond the expected date.",
                    "status": "OPEN",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": ["settlement:261"],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [
                    {
                        "step_id": "verify_settlement",
                        "title": "Verify settlement state",
                        "description": "Check the latest hold status and reconciliation context.",
                        "status": "OPEN",
                    }
                ],
            }
        )

        self.assertEqual(summary["verification"], "verified")
        self.assertIn("Held settlement 261 is currently open", summary["summary"])
        self.assertEqual(summary["tool_calls"][0]["tool_name"], "get_merchant_profile")
        self.assertIn("settlement:261", summary["evidence_ids"])
        self.assertIn("Verify settlement state", summary["answer_sections"]["next_best_action"])


if __name__ == "__main__":
    unittest.main()

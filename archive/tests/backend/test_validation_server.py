import http.client
import json
import threading
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

from app.copilot.types import CopilotTurn
from app.copilot.validation_server import (
    AGENT_DIR,
    build_test_ask_response,
    create_validation_server,
)


class ValidationServerTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
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
                    ('m_001', 'T1', '2026-03-01', 'SUCCESS', 'UPI', '00', 100.0),
                    ('m_001', 'T2', '2026-03-01', 'FAILED', 'CARD', '55', 200.0)
                    """
                )
            )

    def _turn(self, *, active_lane: str = "growth", terminal_focus: str | None = None) -> CopilotTurn:
        return CopilotTurn(
            answer="Test answer",
            tool_calls=[],
            tool_results=[],
            intent="general",
            evidence=["ev:1"],
            operations_section={"summary": "ops", "verification_status": "Verified", "evidence_ids": ["ev:1"]},
            growth_section={"summary": "growth", "verification_status": "Verified", "evidence_ids": ["ev:1"]},
            primary_lane="operations",
            secondary_lane="growth",
            active_lane=active_lane,
            terminal_focus=terminal_focus,
        )

    def test_build_test_ask_response_returns_runtime_payload(self):
        with patch("app.copilot.validation_server.run_turn", return_value=self._turn(active_lane="growth", terminal_focus="T2")) as mock_run:
            out = build_test_ask_response(
                self.engine,
                {
                    "merchant_id": "m_001",
                    "prompt": "What are my top growth opportunities?",
                    "lane": "growth",
                    "terminal_id": "T2",
                },
            )

        self.assertEqual(out["merchant_id"], "m_001")
        self.assertEqual(out["lane"], "growth")
        self.assertEqual(out["terminal_id"], "T2")
        self.assertEqual(out["answer"], "Test answer")
        self.assertEqual(out["terminal_focus"], "T2")
        self.assertEqual(out["tool_calls"], [])
        self.assertEqual(out["tool_results"], [])
        mock_run.assert_called_once_with(
            engine=self.engine,
            agent_dir=AGENT_DIR,
            merchant_id="m_001",
            question="What are my top growth opportunities?",
            forced_lane="growth",
            terminal_id="T2",
        )

    def test_build_test_ask_response_defaults_merchant_from_source_table(self):
        with patch("app.copilot.validation_server.run_turn", return_value=self._turn(active_lane="operations")) as mock_run:
            out = build_test_ask_response(
                self.engine,
                {
                    "merchant_id": "",
                    "prompt": "Explain the shortfall.",
                    "lane": "operations",
                },
            )

        self.assertEqual(out["merchant_id"], "m_001")
        mock_run.assert_called_once_with(
            engine=self.engine,
            agent_dir=AGENT_DIR,
            merchant_id="m_001",
            question="Explain the shortfall.",
            forced_lane="operations",
            terminal_id=None,
        )

    def test_http_endpoint_returns_json_contract(self):
        server = create_validation_server("127.0.0.1", 0, engine=self.engine)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch("app.copilot.validation_server.run_turn", return_value=self._turn(active_lane="operations")):
                conn = http.client.HTTPConnection(server.server_address[0], server.server_address[1], timeout=5)
                body = json.dumps(
                    {
                        "merchant_id": "m_001",
                        "prompt": "I expected Rs 20,000 settlement but got Rs 19,000. Explain the shortfall.",
                        "lane": "operations",
                    }
                )
                conn.request("POST", "/test/ask", body=body, headers={"Content-Type": "application/json"})
                response = conn.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                conn.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(response.status, 200)
        self.assertEqual(payload["merchant_id"], "m_001")
        self.assertEqual(payload["lane"], "operations")
        self.assertIn("operations_section", payload)
        self.assertIn("tool_calls", payload)
        self.assertIn("tool_results", payload)


if __name__ == "__main__":
    unittest.main()

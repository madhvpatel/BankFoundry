import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, text

from config import Config
from app.copilot.runtime import run_turn
from app.copilot.tools import ToolContext, compute_kpis, verify_failure_drivers


class VerifyFailureDriversToolTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE transaction_features (
                    merchant_id TEXT,
                    terminal_id TEXT,
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
                    (merchant_id, terminal_id, p_date, status, response_code, payment_mode, amount_rupees)
                    VALUES
                    ('merchant_001', 'T1', '2026-01-10', 'FAILED', NULL, 'UPI', 100),
                    ('merchant_001', 'T1', '2026-01-11', 'FAILED', NULL, 'UPI', 200),
                    ('merchant_001', 'T1', '2026-01-12', 'FAILURE', '', 'UPI', 300),
                    ('merchant_001', 'T2', '2026-01-12', 'FAILED', '51', 'CARD', 50),
                    ('merchant_001', 'T2', '2026-01-13', 'FAILED', '51', 'CARD', 75),
                    ('merchant_001', 'T2', '2026-01-14', 'SUCCESS', NULL, 'CARD', 500)
                    """
                )
            )
        self.ctx = ToolContext(engine=self.engine, merchant_id="merchant_001")

    def test_verify_failure_drivers_by_response_code(self):
        out = verify_failure_drivers(
            self.ctx,
            from_date="2026-01-01",
            to_date="2026-01-31",
            by="response_code",
            limit=5,
        )
        self.assertTrue(out.get("verified"))
        self.assertEqual(out.get("dimension"), "response_code")
        self.assertEqual(out.get("rows", [])[0]["driver"], "UNKNOWN")
        self.assertEqual(out.get("rows", [])[0]["failed_txns"], 3)

    def test_verify_failure_drivers_by_payment_mode(self):
        out = verify_failure_drivers(
            self.ctx,
            from_date="2026-01-01",
            to_date="2026-01-31",
            by="payment_mode",
            limit=5,
        )
        self.assertTrue(out.get("verified"))
        self.assertEqual(out.get("dimension"), "payment_mode")
        self.assertEqual(out.get("rows", [])[0]["driver"], "UPI")
        self.assertEqual(out.get("rows", [])[0]["failed_txns"], 3)

    def test_verify_failure_drivers_respects_terminal_scope(self):
        scoped_ctx = ToolContext(engine=self.engine, merchant_id="merchant_001", terminal_id="T2")
        out = verify_failure_drivers(
            scoped_ctx,
            from_date="2026-01-01",
            to_date="2026-01-31",
            by="payment_mode",
            limit=5,
        )
        self.assertTrue(out.get("verified"))
        self.assertEqual(out.get("rows", [])[0]["driver"], "CARD")
        self.assertEqual(out.get("rows", [])[0]["failed_txns"], 2)
        self.assertEqual(out.get("scope", {}).get("terminal_id"), "T2")
        self.assertIn("terminal:T2", out.get("evidence", []))

    def test_compute_kpis_respects_terminal_scope(self):
        scoped_ctx = ToolContext(engine=self.engine, merchant_id="merchant_001", terminal_id="T1")
        out = compute_kpis(
            scoped_ctx,
            from_date="2026-01-01",
            to_date="2026-01-31",
            group_by="none",
        )
        self.assertEqual(out.get("scope", {}).get("terminal_id"), "T1")
        self.assertEqual(out.get("rows", [])[0]["attempts"], 3)
        self.assertEqual(out.get("rows", [])[0]["fail_txns"], 3)
        self.assertIn("terminal:T1", out.get("evidence", []))


class RuntimeVerificationGuardTest(unittest.TestCase):
    def _run_with(self, question: str, tool_results: list[dict], final_model_answer: str) -> str:
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 4
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            return_value=([], tool_results, final_model_answer),
        ), patch(
            "app.copilot.runtime._render_answer",
            side_effect=AssertionError("should not call narrator when final model answer exists"),
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question=question,
            )
        return turn.answer

    def test_verified_state_keeps_verified_status(self):
        answer = self._run_with(
            "Please verify top failure drivers for last month",
            [
                {
                    "tool": "verify_failure_drivers",
                    "ok": True,
                    "output": {
                        "verified": True,
                        "verification_type": "failure_driver_ranking",
                        "dimension": "payment_mode",
                        "rows": [{"driver": "UPI", "failed_txns": 10, "failed_gmv": 5000}],
                        "evidence": ["verify_faildrivers:payment_mode:2026-01-01:2026-02-01"],
                        "error": None,
                    },
                    "error": None,
                }
            ],
            "Top failure drivers are UPI.",
        )
        self.assertIn("Verification status: Verified", answer)
        self.assertIn("Evidence IDs: verify_faildrivers:payment_mode:2026-01-01:2026-02-01", answer)

    def test_unverified_supported_downgrades_claims(self):
        answer = self._run_with(
            "Please verify top failure drivers for last month",
            [
                {
                    "tool": "verify_failure_drivers",
                    "ok": True,
                    "output": {
                        "verified": False,
                        "verification_type": "failure_driver_ranking",
                        "dimension": "response_code",
                        "rows": [],
                        "evidence": ["verify_faildrivers:response_code:2026-01-01:2026-02-01"],
                        "error": "relation does not exist",
                    },
                    "error": None,
                },
                {
                    "tool": "startup_kpis",
                    "ok": True,
                    "output": {
                        "kpi_snapshot": {"fail_txns": 100, "failed_gmv": 9000},
                        "evidence": ["startup_kpis:merchant_001:2026-01-01:2026-02-01"],
                    },
                    "error": None,
                },
            ],
            "Top failure drivers are verified. Root cause is UPI.",
        )
        self.assertIn("Verification status: Unverified (supported)", answer)
        self.assertIn("likely failure concentration", answer.lower())
        self.assertNotIn("Root cause", answer)
        self.assertIn("Evidence IDs:", answer)

    def test_insufficient_evidence_marks_insufficient(self):
        answer = self._run_with(
            "Please verify top failure drivers for last month",
            [
                {
                    "tool": "sql_database",
                    "ok": True,
                    "output": {
                        "verified": False,
                        "rows": [],
                        "evidence": ["sql:merchant_001:error"],
                        "error_code": "undefined_table",
                        "error": "relation does not exist",
                    },
                    "error": None,
                }
            ],
            "Top failure drivers are verified.",
        )
        self.assertIn("Verification status: Insufficient evidence", answer)
        self.assertIn("unverified", answer.lower())
        self.assertIn("Evidence IDs: sql:merchant_001:error", answer)


if __name__ == "__main__":
    unittest.main()

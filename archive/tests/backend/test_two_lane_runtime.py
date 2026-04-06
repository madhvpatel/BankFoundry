import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, text

from config import Config
from app.copilot.runtime import run_turn
from app.copilot.types import ToolResult


class TwoLaneRuntimeTest(unittest.TestCase):
    def _invoke_payload(self):
        return (
            [{"name": "startup_kpis", "args": {"from_date": "2026-01-01", "to_date": "2026-02-01"}}],
            [
                {
                    "tool": "startup_kpis",
                    "ok": True,
                    "output": {
                        "kpi_snapshot": {
                            "attempts": 100,
                            "success_txns": 94,
                            "fail_txns": 6,
                            "failed_gmv": 12000,
                        },
                        "kpi_by_mode": [
                            {"bucket": "CARD", "fail_txns": 4, "failed_gmv": 7000},
                            {"bucket": "UPI", "fail_txns": 2, "failed_gmv": 5000},
                        ],
                        "window": {"from": "2026-01-01", "to": "2026-02-01"},
                        "evidence": ["startup_kpis:merchant_001:2026-01-01:2026-02-01"],
                    },
                    "error": None,
                }
            ],
            "Top failure drivers are verified. Root cause is transient issuer declines.",
        )

    def test_run_turn_returns_dual_sections_with_metadata(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 5
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=[self._invoke_payload(), self._invoke_payload()],
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="I expected 20000 but got 19000 settlement, also suggest growth actions",
            )

        self.assertEqual(turn.primary_lane, "operations")
        self.assertEqual(turn.secondary_lane, "growth")
        self.assertIn("On operations,", turn.answer)
        self.assertIn("On growth,", turn.answer)
        self.assertIn("Verification status:", turn.answer)
        self.assertIn("Evidence IDs:", turn.answer)
        self.assertNotIn("Summary:", turn.answer)
        self.assertIn("verification_status", turn.operations_section)
        self.assertIn("verification_status", turn.growth_section)
        self.assertTrue(isinstance(turn.proactive_cards, list))
        self.assertEqual(len(turn.proactive_cards), 2)

    def test_lane_router_sets_growth_primary_for_growth_questions(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 5
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=[self._invoke_payload(), self._invoke_payload()],
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="How can I improve growth with better card acceptance and dcc?",
            )

        self.assertEqual(turn.primary_lane, "growth")
        self.assertEqual(turn.secondary_lane, "")

    def test_forced_lane_runs_single_operations_agent(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 5
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=[self._invoke_payload()],
        ) as mock_invoke:
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="Why was my payout short?",
                forced_lane="operations",
            )

        self.assertEqual(mock_invoke.call_count, 1)
        self.assertEqual(turn.active_lane, "operations")
        self.assertEqual(turn.primary_lane, "operations")
        self.assertEqual(turn.secondary_lane, "")
        self.assertNotIn("On growth,", turn.answer)
        self.assertNotIn("Summary:", turn.answer)
        self.assertEqual(len(turn.proactive_cards), 1)

    def test_run_turn_propagates_terminal_focus_into_runtime_and_lane_payload(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 5
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=[self._invoke_payload()],
        ) as mock_invoke:
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="How is terminal T1 performing?",
                forced_lane="growth",
                terminal_id="T1",
            )

        self.assertEqual(turn.terminal_focus, "T1")
        first_user_payload = mock_invoke.call_args.kwargs["user"]
        self.assertEqual(first_user_payload["terminal_focus"], "T1")
        self.assertIn("Selected terminal T1", first_user_payload["tooling_hint"]["terminal_scope_note"])

    def test_standard_growth_turn_uses_compact_budget(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 6
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=[self._invoke_payload()],
        ) as mock_invoke:
            run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="What are my top growth opportunities in the last 30 days?",
                forced_lane="growth",
            )

        self.assertEqual(mock_invoke.call_args.kwargs["max_steps"], 2)

    def test_shortfall_turn_uses_richer_operations_budget(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 6
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=[self._invoke_payload()],
        ) as mock_invoke:
            run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="I expected 20000 but got 19000 settlement. Explain the shortfall.",
                forced_lane="operations",
            )

        self.assertEqual(mock_invoke.call_args.kwargs["max_steps"], 3)

    def test_narrow_operations_question_does_not_auto_bootstrap(self):
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
            return_value=([], [], ""),
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="I expected 20000 but got 19000 settlement. Explain the shortfall.",
                forced_lane="operations",
            )

        self.assertEqual(turn.evidence, [])
        self.assertIn("Insufficient evidence", turn.answer)

    def test_verified_shortfall_fast_path_skips_model_loop(self):
        shortfall_output = {
            "verified": True,
            "directional_support": True,
            "summary": "Settlement s_001 expected Rs 20,000.00 and received Rs 19,000.00, so the payout is short by Rs 1,000.00. Known components: MDR Rs 700.00, GST on MDR Rs 126.00, Reserve held Rs 174.00.",
            "deduction_explanation": "Settlement s_001 expected Rs 20,000.00 and received Rs 19,000.00, so the payout is short by Rs 1,000.00. Known components: MDR Rs 700.00, GST on MDR Rs 126.00, Reserve held Rs 174.00.",
            "recommended_actions": ["Confirm fee deductions against the merchant pricing and tax configuration."],
            "shortfall": {
                "settlement_id": "s_001",
                "difference_amount": 1000.0,
            },
            "evidence": ["settlement:s_001", "shortfall:settlement:s_001"],
            "window": {"from": "2026-01-01", "to": "2026-02-01"},
        }
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime._tool_dispatch",
            return_value=ToolResult(name="explain_settlement_shortfall", ok=True, output=shortfall_output),
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=AssertionError("verified shortfall should skip the model loop"),
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="I expected 20000 but got 19000 settlement. Explain the shortfall.",
                forced_lane="operations",
            )

        self.assertIn("Rs 1,000.00", turn.answer)
        self.assertIn("Verification status: Verified - deterministic payout shortfall attribution succeeded", turn.answer)
        self.assertEqual(turn.operations_section["evidence_ids"], ["settlement:s_001", "shortfall:settlement:s_001"])

    def test_pure_greeting_short_circuits_without_tools(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch(
            "app.copilot.runtime.default_window_from_max_date",
            side_effect=AssertionError("greeting should not resolve a window"),
        ), patch(
            "app.copilot.runtime.make_tools",
            side_effect=AssertionError("greeting should not build tools"),
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=AssertionError("greeting should not call the model tool loop"),
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="hello",
            )

        self.assertEqual(turn.answer, "Hi. I can help with your merchant payments, settlements, failures, and growth opportunities.")
        self.assertEqual(turn.evidence, [])
        self.assertEqual(turn.tool_calls, [])
        self.assertEqual(turn.tool_results, [])

    def test_mixed_greeting_and_analytics_does_not_short_circuit(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 5
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=[self._invoke_payload()],
        ) as mock_invoke:
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="hi what are my last 10 transactions",
                forced_lane="operations",
            )

        self.assertEqual(mock_invoke.call_count, 1)
        self.assertIn("Verification status:", turn.answer)

    def test_out_of_scope_question_short_circuits_without_tools(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch(
            "app.copilot.runtime.default_window_from_max_date",
            side_effect=AssertionError("out-of-scope prompt should not resolve a window"),
        ), patch(
            "app.copilot.runtime.make_tools",
            side_effect=AssertionError("out-of-scope prompt should not build tools"),
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=AssertionError("out-of-scope prompt should not call the model tool loop"),
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="What is the capital of France?",
            )

        self.assertEqual(
            turn.answer,
            "I'm focused on your merchant data here. Ask about transactions, settlements, chargebacks, refunds, failures, terminals, or growth opportunities.",
        )
        self.assertEqual(turn.evidence, [])

    def test_proactive_cards_are_deduped_across_identical_turns(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

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
            side_effect=[self._invoke_payload(), self._invoke_payload(), self._invoke_payload(), self._invoke_payload()],
        ):
            run_turn(
                engine=engine,
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="Review performance and suggest actions",
            )
            run_turn(
                engine=engine,
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="Review performance and suggest actions",
            )

        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM proactive_cards")).scalar()
        self.assertEqual(int(count or 0), 2)

    def test_runtime_uses_langgraph_sql_path_when_enabled(self):
        graph_output = {
            "verified": True,
            "verification_type": "langgraph_sql_pipeline",
            "lane": "growth",
            "window": {"from": "2026-01-01", "to": "2026-02-01"},
            "selected_views": ["transaction_features"],
            "sql_query": "SELECT 1",
            "rows": [{"bucket": "CARD", "fail_txns": 4}],
            "row_count": 1,
            "summary": "Retrieved growth evidence from SQL graph.",
            "assumptions": [],
            "caveats": [],
            "next_actions": [],
            "requires_human_review": False,
            "review_reason": "",
            "review_token": "",
            "directional_failure_support": True,
            "directional_support": True,
            "evidence": ["sqlgraph:growth:2026-01-01:2026-02-01"],
            "error": None,
        }
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 5
        ), patch.object(
            Config, "SQL_LANGGRAPH_ENABLED", True
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.run_sql_langgraph",
            return_value=graph_output,
        ) as mock_graph, patch(
            "app.copilot.runtime.invoke_with_tools",
            side_effect=AssertionError("invoke_with_tools should not be used in SQL graph mode"),
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="Why did card failures increase?",
            )

        self.assertTrue(mock_graph.called)
        self.assertIn("sqlgraph:growth:2026-01-01:2026-02-01", turn.answer)
        self.assertIn("Verification status:", turn.answer)


if __name__ == "__main__":
    unittest.main()

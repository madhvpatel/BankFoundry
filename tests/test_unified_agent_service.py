import unittest
from unittest.mock import patch

from app.agent.service import run_agent_turn


class UnifiedAgentServiceTest(unittest.TestCase):
    class _FakeResponse:
        def __init__(self, content):
            self.content = content

    class _FakeComposer:
        def __init__(self, *args, **kwargs):
            pass

        def invoke(self, messages):
            return UnifiedAgentServiceTest._FakeResponse(
                """
                {
                  "answer": "Settlement S-11 is short by 1000 based on the settlement output.",
                  "follow_ups": ["Show the settlement row."],
                  "clarifying_question": null,
                  "structured_result": null,
                  "action_preview": null,
                  "claims": [
                    {
                      "text": "Settlement S-11 is short by 1000 based on the settlement output.",
                      "kind": "number",
                      "status": "fact",
                      "evidence_ids": ["settlement:S-11"]
                    }
                  ],
                  "plan_summary": "Checked one settlement explanation tool."
                }
                """
            )

    class _SimpleComposer:
        def __init__(self, *args, **kwargs):
            pass

        def invoke(self, messages):
            return UnifiedAgentServiceTest._FakeResponse(
                """
                {
                  "answer": "Here is the grounded answer.",
                  "follow_ups": ["Show the evidence."],
                  "clarifying_question": null,
                  "structured_result": null,
                  "action_preview": null,
                  "claims": [],
                  "plan_summary": "Completed."
                }
                """
            )

    class _EvidenceSelectingComposer:
        def __init__(self, *args, **kwargs):
            pass

        def invoke(self, messages):
            return UnifiedAgentServiceTest._FakeResponse(
                """
                {
                  "answer": "Settlement 27 is the requested row.",
                  "follow_ups": ["Show settlement 27 again."],
                  "clarifying_question": null,
                  "structured_result": {
                    "title": "Settlement details",
                    "kind": "get_settlement_detail",
                    "evidence_ids": ["settlement:27"]
                  },
                  "action_preview": null,
                  "claims": [
                    {
                      "text": "Settlement 27 is the requested row.",
                      "kind": "evidence",
                      "status": "fact",
                      "evidence_ids": ["settlement:27"]
                    }
                  ],
                  "plan_summary": "Used the selected settlement evidence."
                }
                """
            )

    class _ImplementationLeakComposer:
        def __init__(self, *args, **kwargs):
            pass

        def invoke(self, messages):
            return UnifiedAgentServiceTest._FakeResponse(
                """
                {
                  "answer": "Here are your top transactions. The query was adjusted to use the correct table `transaction_features`.",
                  "follow_ups": ["Show the evidence."],
                  "clarifying_question": null,
                  "structured_result": null,
                  "action_preview": null,
                  "claims": [
                    {
                      "text": "Here are your top transactions.",
                      "kind": "evidence",
                      "status": "fact",
                      "evidence_ids": ["tx:9836"]
                    }
                  ],
                  "plan_summary": "Used transaction listing evidence."
                }
                """
            )

    @patch("app.agent.service.make_tools", return_value=[])
    @patch("app.agent.service.default_window_from_max_date", return_value=("2026-02-01", "2026-03-01"))
    @patch(
        "app.agent.service.invoke_with_tools",
        return_value=(
            [{"name": "explain_settlement_shortfall", "args": {"from_date": "2026-02-01", "to_date": "2026-03-01"}}],
            [
                {
                    "tool": "explain_settlement_shortfall",
                    "ok": True,
                    "output": {
                        "verified": True,
                        "summary": "Settlement S-11 is short by 1000.",
                        "shortfall": {"settlement_id": "S-11", "difference_amount": 1000.0},
                        "evidence": ["settlement:S-11"],
                        "row_count": 1,
                    },
                    "error": None,
                }
            ],
            "Draft answer",
        ),
    )
    @patch("app.agent.service.ChatOllama", _FakeComposer)
    def test_run_agent_turn_returns_grounded_trace_and_sources(self, *_mocks):
        payload = run_agent_turn(
            object(),
            merchant_id="merchant_001",
            prompt="Why was my settlement short?",
            history=[{"role": "user", "text": "Check settlement shortfall"}],
        )

        self.assertEqual(payload["answer_source"], "agent")
        self.assertEqual(payload["verification_status"], "Verified - grounded in tool evidence")
        self.assertEqual(payload["sources"], ["settlement:S-11"])
        self.assertEqual(payload["trace"]["tool_calls"][0]["name"], "explain_settlement_shortfall")
        self.assertEqual(payload["trace"]["evidence_ids"], ["settlement:S-11"])
        self.assertEqual(payload["trace"]["plan_summary"], "Checked one settlement explanation tool.")

    @patch("app.agent.service.ChatOllama", _SimpleComposer)
    @patch("app.agent.service.default_window_from_max_date", return_value=("2026-02-09", "2026-03-11"))
    def test_run_agent_turn_normalizes_named_month_before_tool_loop(self, *_mocks):
        captured: dict[str, object] = {}

        def fake_make_tools(*, ctx, default_from, default_to):
            captured["default_from"] = default_from
            captured["default_to"] = default_to
            return []

        def fake_invoke_with_tools(*, system, user, tools, temperature, max_steps):
            captured["user"] = user
            return [], [], ""

        with patch("app.agent.service.make_tools", side_effect=fake_make_tools), patch(
            "app.agent.service.invoke_with_tools",
            side_effect=fake_invoke_with_tools,
        ):
            payload = run_agent_turn(
                object(),
                merchant_id="merchant_001",
                prompt="What was my average ticket size in February?",
            )

        self.assertEqual(captured["default_from"], "2026-02-01")
        self.assertEqual(captured["default_to"], "2026-03-01")
        self.assertEqual(captured["user"]["default_window"]["from_date"], "2026-02-01")
        self.assertEqual(captured["user"]["default_window"]["to_date"], "2026-03-01")
        self.assertEqual(captured["user"]["normalized_time_window"]["reason"], "named_month_inferred_year")
        self.assertEqual(payload["trace"]["normalized_time_window"]["label"], "February 2026")

    @patch("app.agent.service.ChatOllama", _SimpleComposer)
    @patch("app.agent.service.default_window_from_max_date", return_value=("2026-02-09", "2026-03-11"))
    def test_run_agent_turn_can_reuse_session_memory_window(self, *_mocks):
        captured: dict[str, object] = {}

        def fake_make_tools(*, ctx, default_from, default_to):
            captured["default_from"] = default_from
            captured["default_to"] = default_to
            return []

        def fake_invoke_with_tools(*, system, user, tools, temperature, max_steps):
            captured["user"] = user
            return [], [], ""

        with patch("app.agent.service.make_tools", side_effect=fake_make_tools), patch(
            "app.agent.service.invoke_with_tools",
            side_effect=fake_invoke_with_tools,
        ):
            payload = run_agent_turn(
                object(),
                merchant_id="merchant_001",
                prompt="Show the rows again for the same period.",
                memory_context={
                    "preferred_window": {
                        "from_date": "2026-01-01",
                        "to_date": "2026-01-31",
                        "label": "January 2026",
                    }
                },
            )

        self.assertEqual(captured["default_from"], "2026-01-01")
        self.assertEqual(captured["default_to"], "2026-01-31")
        self.assertEqual(captured["user"]["memory_context"]["preferred_window"]["from_date"], "2026-01-01")
        self.assertEqual(payload["trace"]["normalized_time_window"]["reason"], "session_memory_window")

    @patch("app.agent.service.make_tools", return_value=[])
    @patch("app.agent.service.default_window_from_max_date", return_value=("2026-02-09", "2026-03-11"))
    @patch(
        "app.agent.service.invoke_with_tools",
        return_value=(
            [
                {"name": "list_settlements", "args": {"limit": 10}},
                {"name": "get_settlement_detail", "args": {"settlement_id": "27"}},
            ],
            [
                {
                    "tool": "list_settlements",
                    "ok": True,
                    "output": {
                        "rows": [{"settlement_id": 22, "status": "PROCESSED"}],
                        "columns": ["settlement_id", "status"],
                        "window": {"from": "2026-02-09", "to": "2026-03-11"},
                        "evidence": ["settlement:22"],
                    },
                    "error": None,
                },
                {
                    "tool": "get_settlement_detail",
                    "ok": True,
                    "output": {
                        "row": {"settlement_id": 27, "status": "PROCESSED"},
                        "window": {"from": "2026-02-09", "to": "2026-03-11"},
                        "evidence": ["settlement:27"],
                    },
                    "error": None,
                },
            ],
            "Draft answer",
        ),
    )
    @patch("app.agent.service.ChatOllama", _EvidenceSelectingComposer)
    def test_run_agent_turn_binds_structured_result_to_selected_evidence(self, *_mocks):
        payload = run_agent_turn(
            object(),
            merchant_id="merchant_001",
            prompt="Show me settlement 27.",
        )

        self.assertIsNotNone(payload["structured_result"])
        self.assertEqual(payload["structured_result"]["kind"], "get_settlement_detail")
        self.assertEqual(payload["structured_result"]["rows"], [{"settlement_id": 27, "status": "PROCESSED"}])
        self.assertEqual(payload["structured_result"]["evidence_ids"], ["settlement:27"])

    @patch("app.agent.service.make_tools", return_value=[])
    @patch("app.agent.service.default_window_from_max_date", return_value=("2026-02-09", "2026-03-11"))
    @patch(
        "app.agent.service.invoke_with_tools",
        return_value=(
            [{"name": "list_transactions", "args": {"limit": 10}}],
            [
                {
                    "tool": "list_transactions",
                    "ok": True,
                    "output": {
                        "rows": [{"tx_id": "9836", "amount_rupees": 500000}],
                        "columns": ["tx_id", "amount_rupees"],
                        "window": {"from": "2026-02-09", "to": "2026-03-11"},
                        "evidence": ["tx:9836"],
                    },
                    "error": None,
                }
            ],
            "Draft answer",
        ),
    )
    @patch("app.agent.service.ChatOllama", _ImplementationLeakComposer)
    def test_run_agent_turn_strips_internal_mechanics_and_uses_specific_followups(self, *_mocks):
        payload = run_agent_turn(
            object(),
            merchant_id="merchant_001",
            prompt="Show me my top transactions.",
        )

        self.assertNotIn("query was adjusted", payload["answer"].lower())
        self.assertEqual(payload["answer_sections"]["executive_summary"], payload["answer"])
        self.assertIn("Show the failed transactions in this list.", payload["follow_ups"])

    @patch("app.agent.service.make_tools", return_value=[])
    @patch("app.agent.service.default_window_from_max_date", return_value=("2026-02-09", "2026-03-11"))
    @patch(
        "app.agent.service.invoke_with_tools",
        return_value=(
            [{"name": "list_settlements", "args": {"limit": 10}}],
            [
                {
                    "tool": "list_settlements",
                    "ok": True,
                    "output": {
                        "rows": [{"settlement_id": 261, "status": "HELD"}],
                        "columns": ["settlement_id", "status"],
                        "window": {"from": "2026-02-09", "to": "2026-03-11"},
                        "evidence": ["settlement:261"],
                    },
                    "error": None,
                }
            ],
            "Draft answer",
        ),
    )
    @patch(
        "app.agent.service.validate_reasoning_output",
        return_value={
            "verification_summary": "1 claim(s) verified, 1 claim(s) need review.",
            "validation_status": "partial",
            "verified_claims": [{"text": "Settlement 261 is held.", "kind": "evidence", "status": "fact", "evidence_ids": ["settlement:261"]}],
            "invalid_claims": [{"text": "The hold is definitely fraud-related.", "kind": "ranking", "status": "fact", "issues": [{"type": "unsupported_top_rank_claim"}]}],
            "validation_issues": [{"type": "unsupported_top_rank_claim", "claim": "The hold is definitely fraud-related."}],
            "display_notice": {"title": "Some details are directional", "summary": "The main answer is grounded, but a few details still need review."},
        },
    )
    @patch("app.agent.service.ChatOllama", _SimpleComposer)
    def test_run_agent_turn_adds_caveats_when_validation_is_partial(self, *_mocks):
        payload = run_agent_turn(
            object(),
            merchant_id="merchant_001",
            prompt="Why is this settlement held?",
        )

        self.assertTrue(payload["answer"].startswith("Based on the current evidence,"))
        self.assertGreater(len(payload["answer_sections"]["caveats"]), 0)
        self.assertEqual(payload["answer_sections"]["next_best_action"], "Review the supporting rows before acting on this conclusion.")
        self.assertEqual(payload["follow_ups"][0], "Show the exact rows behind this.")


if __name__ == "__main__":
    unittest.main()

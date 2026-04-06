import asyncio
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine

from app.api.server import (
    AskRequest,
    ask_endpoint,
    get_merchant_snapshot,
)


class ApiServerTest(unittest.TestCase):
    def test_ask_endpoint_returns_unified_agent_payload(self):
        with patch(
            "app.api.server.run_agent_turn",
            return_value={
                "answer": "Here is the grounded answer.",
                "verification_status": "Verified - grounded in tool evidence",
                "verification_summary": "2 claim(s) verified.",
                "validation_status": "clean",
                "validation_issues": [],
                "display_notice": None,
                "clarifying_question": None,
                "answer_source": "agent",
                "sources": ["settlement:11"],
                "structured_result": None,
                "follow_ups": ["Show the exact rows behind this."],
                "action_preview": None,
                "scope": {"merchant_id": "merchant_001", "terminal_id": None, "level": "merchant"},
                "intent": "agent_turn",
                "trace": {"turn_id": "turn_demo", "tool_calls": [], "tool_results": [], "evidence_ids": ["settlement:11"]},
            },
        ) as mock_run_agent_turn:
            payload = asyncio.run(
                ask_endpoint(
                    AskRequest(
                        merchant_id="merchant_001",
                        prompt="What is my business?",
                    )
                )
            )
        self.assertEqual(payload["intent"], "agent_turn")
        self.assertEqual(payload["answer_source"], "agent")
        self.assertEqual(payload["trace"]["turn_id"], "turn_demo")
        self.assertEqual(mock_run_agent_turn.call_args.kwargs["terminal_id"], None)

    def test_snapshot_endpoint_returns_scoped_snapshot_and_terminal_options(self):
        base_snapshot = {
            "merchant_id": "merchant_001",
            "terminals": {"rows": [{"terminal_id": "T1"}, {"terminal_id": "T2"}]},
            "terminal_health": {"rows": [{"terminal_id": "T1"}]},
            "existing_actions": [],
            "proactive_cards": [],
        }
        scoped_snapshot = {
            "merchant_id": "merchant_001",
            "scope": {"level": "terminal", "label": "Terminal T1"},
            "existing_actions": [],
            "proactive_cards": [],
        }
        with patch("app.api.server.ensure_background_proactive_refresh", return_value={"due": False}), patch(
            "app.api.server.get_merchant_os_snapshot",
            return_value=base_snapshot,
        ), patch(
            "app.api.server.scope_snapshot_to_terminal",
            return_value=scoped_snapshot,
        ), patch(
            "app.api.server.get_background_refresh_status",
            return_value={"due": False, "next_refresh_at": "2026-03-10T10:00:00+00:00"},
        ):
            payload = asyncio.run(get_merchant_snapshot(merchant_id="merchant_001", terminal_id="T1"))

        self.assertEqual(payload["merchant_id"], "merchant_001")
        self.assertEqual(payload["terminal_id"], "T1")
        self.assertEqual(payload["snapshot"]["selected_terminal_id"], "T1")
        self.assertEqual(payload["snapshot"]["scope"]["level"], "terminal")
        self.assertEqual(payload["snapshot"]["terminal_options"], ["T1", "T2"])

    def test_ask_endpoint_hides_debug_by_default(self):
        with patch(
            "app.api.server.run_agent_turn",
            return_value={
                "answer": "You are Delhi Airport Parking.",
                "verification_status": "Not applicable",
                "verification_summary": "No claim-level validation was needed.",
                "validation_status": "clean",
                "validation_issues": [],
                "display_notice": None,
                "clarifying_question": None,
                "answer_source": "agent",
                "sources": ["payment_mode:mid:window"],
                "structured_result": None,
                "follow_ups": ["How is my business doing?"],
                "action_preview": None,
                "scope": {"merchant_id": "merchant_001", "terminal_id": None, "level": "merchant"},
                "intent": "agent_turn",
                "trace": {"turn_id": "turn_demo"},
                "debug": {"turn_id": "turn_demo"},
            },
        ):
            payload = asyncio.run(
                ask_endpoint(
                    AskRequest(merchant_id="merchant_001", prompt="What is my business?", debug=False)
                )
            )

        self.assertEqual(payload["intent"], "agent_turn")
        self.assertEqual(payload["answer_source"], "agent")
        self.assertEqual(payload["validation_status"], "clean")
        self.assertNotIn("debug", payload)
        self.assertIn("trace", payload)

    def test_ask_endpoint_returns_debug_payload_only_when_requested(self):
        with patch(
            "app.api.server.run_agent_turn",
            return_value={
                "answer": "Here are your recent settlements.",
                "verification_status": "Verified - deterministic list retrieval",
                "verification_summary": "No claim-level validation was needed.",
                "validation_status": "clean",
                "validation_issues": [],
                "display_notice": None,
                "clarifying_question": None,
                "answer_source": "agent",
                "sources": ["settlement:11"],
                "structured_result": {"kind": "settlements", "columns": ["settlement_id"], "rows": [{"settlement_id": 11}], "window": {}},
                "follow_ups": [],
                "action_preview": None,
                "scope": {"merchant_id": "merchant_001", "terminal_id": None, "level": "merchant"},
                "intent": "agent_turn",
                "trace": {"turn_id": "turn_demo"},
                "debug": {"turn_id": "turn_demo", "tool_calls": [{"name": "list_settlements", "args": {}}]},
            },
        ):
            payload = asyncio.run(
                ask_endpoint(
                    AskRequest(merchant_id="merchant_001", prompt="Show my recent settlements", debug=True)
                )
            )

        self.assertEqual(payload["structured_result"]["kind"], "settlements")
        self.assertEqual(payload["debug"]["turn_id"], "turn_demo")

    def test_ask_endpoint_recovers_session_memory_on_follow_up_turn(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        captured_calls = []

        def fake_run_agent_turn(*args, **kwargs):
            captured_calls.append(kwargs)
            if len(captured_calls) == 1:
                return {
                    "answer": "Settlement 261 is held for risk review.",
                    "verification_status": "Verified - grounded in tool evidence",
                    "verification_summary": "2 claim(s) verified.",
                    "validation_status": "clean",
                    "validation_issues": [],
                    "display_notice": None,
                    "clarifying_question": None,
                    "answer_source": "agent",
                    "sources": ["settlement:261"],
                    "structured_result": None,
                    "follow_ups": ["Show the exact settlement row behind this answer."],
                    "action_preview": None,
                    "answer_sections": {
                        "executive_summary": "Settlement 261 is held for risk review.",
                        "key_findings": ["The hold is still active."],
                        "next_best_action": "Review the settlement row and escalation notes.",
                        "caveats": [],
                    },
                    "scope": {"merchant_id": "merchant_001", "terminal_id": None, "level": "merchant"},
                    "intent": "agent_turn",
                    "trace": {
                        "turn_id": "turn_demo_1",
                        "tool_calls": [{"name": "get_settlement_detail", "args": {"settlement_id": "261"}}],
                        "normalized_time_window": {
                            "from_date": "2026-03-01",
                            "to_date": "2026-03-31",
                            "label": "March 2026",
                            "reason": "explicit_month_year",
                            "source_phrase": "March 2026",
                        },
                    },
                }
            return {
                "answer": "Here are the same settlement rows again.",
                "verification_status": "Verified - grounded in tool evidence",
                "verification_summary": "1 claim(s) verified.",
                "validation_status": "clean",
                "validation_issues": [],
                "display_notice": None,
                "clarifying_question": None,
                "answer_source": "agent",
                "sources": ["settlement:261"],
                "structured_result": None,
                "follow_ups": [],
                "action_preview": None,
                "answer_sections": {
                    "executive_summary": "Here are the same settlement rows again.",
                    "key_findings": [],
                    "next_best_action": "",
                    "caveats": [],
                },
                "scope": {"merchant_id": "merchant_001", "terminal_id": None, "level": "merchant"},
                "intent": "agent_turn",
                "trace": {"turn_id": "turn_demo_2", "tool_calls": []},
            }

        with patch("app.api.server.engine", engine), patch(
            "app.api.server.run_agent_turn",
            side_effect=fake_run_agent_turn,
        ):
            first = asyncio.run(
                ask_endpoint(
                    AskRequest(
                        merchant_id="merchant_001",
                        prompt="Why is settlement 261 held in March 2026?",
                        thread_scope="memory-demo",
                    )
                )
            )
            second = asyncio.run(
                ask_endpoint(
                    AskRequest(
                        merchant_id="merchant_001",
                        prompt="Show the rows again for that settlement.",
                        thread_scope="memory-demo",
                    )
                )
            )

        self.assertEqual(first["memory"]["selected_entities"]["settlement_id"], "261")
        self.assertEqual(second["thread_scope"], "memory-demo")
        self.assertEqual(captured_calls[1]["memory_context"]["selected_entities"]["settlement_id"], "261")
        self.assertEqual(captured_calls[1]["memory_context"]["preferred_window"]["from_date"], "2026-03-01")
        self.assertGreaterEqual(len(captured_calls[1]["history"]), 2)
        self.assertEqual(second["memory"]["selected_entities"]["settlement_id"], "261")

if __name__ == "__main__":
    unittest.main()

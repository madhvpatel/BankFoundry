import unittest

from sqlalchemy import create_engine

from app.application.control_plane.chat_memory import ChatMemoryService


class ChatMemoryServiceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        self.service = ChatMemoryService(self.engine)

    def test_memory_service_persists_entities_window_and_facts(self):
        bundle = self.service.remember_turn(
            session_key="merchant:merchant_001:chat:web_chat:default",
            merchant_id="merchant_001",
            terminal_id="T1",
            thread_scope="default",
            prompt="Why is settlement 261 held this month?",
            payload={
                "answer": "Settlement 261 is held for risk review.",
                "sources": ["settlement:261", "terminal:T1"],
                "follow_ups": ["Show the exact settlement row behind this answer."],
                "validation_status": "clean",
                "answer_sections": {
                    "executive_summary": "Settlement 261 is held for risk review.",
                    "key_findings": ["The hold is still open in the current window."],
                    "next_best_action": "Review the settlement row and escalation notes.",
                },
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
            },
        )

        session = bundle["session"]
        self.assertEqual(session["selected_entities"]["settlement_id"], "261")
        self.assertEqual(session["selected_entities"]["terminal_id"], "T1")
        self.assertEqual(session["active_window"]["from_date"], "2026-03-01")
        self.assertIn("settlements", session["active_topics"])
        self.assertEqual(session["verified_facts"][0]["text"], "Settlement 261 is held for risk review.")

    def test_memory_context_recalls_prior_turn_for_follow_up(self):
        self.service.remember_turn(
            session_key="merchant:merchant_001:chat:web_chat:default",
            merchant_id="merchant_001",
            terminal_id=None,
            thread_scope="default",
            prompt="Why is settlement 261 held in March 2026?",
            payload={
                "answer": "Settlement 261 is held and still pending review.",
                "sources": ["settlement:261"],
                "follow_ups": ["Show the exact settlement row behind this answer."],
                "validation_status": "clean",
                "answer_sections": {
                    "executive_summary": "Settlement 261 is held and still pending review.",
                    "key_findings": ["This answer is tied to settlement 261."],
                },
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
            },
        )

        bundle = self.service.load_session(
            session_key="merchant:merchant_001:chat:web_chat:default",
            merchant_id="merchant_001",
            thread_scope="default",
        )
        history = self.service.merged_history(bundle, request_history=[])
        memory = self.service.agent_memory_context(bundle, prompt="Show the rows again for that settlement.")

        self.assertEqual(history[0]["role"], "user")
        self.assertEqual(history[1]["role"], "assistant")
        self.assertEqual(memory["selected_entities"]["settlement_id"], "261")
        self.assertEqual(memory["preferred_window"]["from_date"], "2026-03-01")
        self.assertGreaterEqual(len(memory["relevant_memories"]), 1)
        self.assertEqual(memory["relevant_memories"][0]["selected_entities"]["settlement_id"], "261")


if __name__ == "__main__":
    unittest.main()

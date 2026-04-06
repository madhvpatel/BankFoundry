import asyncio
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine

from app.api.server import AskRequest, OpsCaseCreateRequest, ask_endpoint, create_ops_case


class ControlPlaneReplaySuiteTest(unittest.TestCase):
    def test_replay_merchant_chat_and_ops_case_flow(self):
        with patch(
            "app.api.server.run_agent_turn",
            return_value={
                "answer": "Your latest settlement appears held and needs review.",
                "verification_status": "Verified - grounded in tool evidence",
                "verification_summary": "2 claim(s) verified.",
                "validation_status": "clean",
                "validation_issues": [],
                "display_notice": None,
                "clarifying_question": None,
                "answer_source": "agent",
                "sources": ["settlement:261", "alert:held"],
                "structured_result": None,
                "follow_ups": ["Create an ops case for this settlement."],
                "action_preview": None,
                "scope": {"merchant_id": "merchant_001", "terminal_id": None, "level": "merchant"},
                "intent": "agent_turn",
                "trace": {"turn_id": "turn_demo"},
            },
        ):
            merchant_payload = asyncio.run(
                ask_endpoint(
                    AskRequest(
                        merchant_id="merchant_001",
                        prompt="Why is my latest settlement held?",
                    )
                )
            )

        self.assertEqual(merchant_payload["answer_source"], "agent")
        self.assertIn("settlement:261", merchant_payload["sources"])

        engine = create_engine("sqlite+pysqlite:///:memory:")
        with patch("app.api.server.engine", engine):
            ops_payload = asyncio.run(
                create_ops_case(
                    OpsCaseCreateRequest(
                        merchant_id="merchant_001",
                        lane="operations",
                        role="acquiring_ops",
                        case_type="held_settlement",
                        title="Held settlement 261",
                        summary="Created from replay suite after merchant investigation.",
                        priority="high",
                        evidence_ids=["settlement:261", "alert:held"],
                    )
                )
            )

        self.assertEqual(ops_payload["work_item"]["lane"], "operations")
        self.assertEqual(ops_payload["work_item"]["case_type"], "held_settlement")
        self.assertEqual(len(ops_payload["runbook_steps"]), 4)


if __name__ == "__main__":
    unittest.main()

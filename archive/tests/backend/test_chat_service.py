import unittest
from unittest.mock import patch

from app.chat_service import ask_chat


class ChatServiceTest(unittest.TestCase):
    def setUp(self):
        self.router_patcher = patch("app.chat_service.route_chat_intent", return_value=None)
        self.router_patcher.start()

    def tearDown(self):
        self.router_patcher.stop()

    def test_business_identity_uses_engine_bundle_without_side_effects(self):
        with patch(
            "app.chat_service.get_merchant_context",
            return_value={
                "merchant": {
                    "merchant_trade_name": "Delhi Airport Parking",
                    "nature_of_business": "Automobile Parking & Valet Services",
                    "business_city": "New Delhi",
                }
            },
        ), patch(
            "app.chat_service.run_intelligence",
            return_value={
                "recommendations": [
                    {
                        "category": "growth",
                        "title": "UPI contributes 80% of volume",
                        "summary": "UPI dominates the checkout mix.",
                        "priority_score": 10,
                        "impact_rupees": 1000,
                        "confidence": 0.9,
                        "actions": [{"who": "merchant", "text": "Keep QR reliable."}],
                        "evidence_ids": ["payment_mode:mid:window"],
                    }
                ],
                "signals": {
                    "health_vector": {"health_score": 88, "status": "Healthy"},
                    "operational": {"metrics": {"attempts": 100, "success_rate_pct": 97.5}},
                    "kpi_delta": {},
                    "attribution": {},
                },
            },
        ) as mock_run_intelligence, patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="What is my business?")

        self.assertIn("Delhi Airport Parking", payload["answer"])
        self.assertEqual(payload["verification_status"], "Verified - deterministic evidence with AI-led synthesis")
        self.assertEqual(payload["answer_source"], "engine")
        self.assertEqual(payload["validation_status"], "clean")
        self.assertNotIn("debug", payload)
        self.assertEqual(mock_run_intelligence.call_args.kwargs["persist_actions"], False)
        self.assertEqual(mock_run_intelligence.call_args.kwargs["enable_phase2_reasoning"], False)

    def test_recent_settlements_returns_structured_result(self):
        with patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ), patch(
            "app.chat_service.list_settlements",
            return_value={
                "rows": [
                    {
                        "settlement_id": 11,
                        "status": "PROCESSED",
                        "expected_date": "2026-03-10",
                        "settled_at": None,
                        "amount_rupees": "1300.00",
                    }
                ],
                "evidence": ["settlement:11"],
                "window": {"from": "2026-02-10", "to": "2026-03-11"},
            },
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="Show my recent settlements")

        self.assertEqual(payload["structured_result"]["kind"], "settlements")
        self.assertEqual(payload["structured_result"]["rows"][0]["settlement_id"], 11)
        self.assertEqual(payload["scope"]["level"], "merchant")
        self.assertEqual(payload["answer_source"], "tool")
        self.assertNotIn("debug", payload)

    def test_what_are_my_settlements_routes_to_list_intent(self):
        with patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ), patch(
            "app.chat_service.list_settlements",
            return_value={
                "rows": [
                    {
                        "settlement_id": 11,
                        "status": "PROCESSED",
                        "expected_date": "2026-03-10",
                        "settled_at": None,
                        "amount_rupees": "1300.00",
                    }
                ],
                "evidence": ["settlement:11"],
                "window": {"from": "2026-02-10", "to": "2026-03-11"},
            },
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="what are my settlements")

        self.assertEqual(payload["intent"], "recent_settlements")
        self.assertEqual(payload["answer_source"], "tool")
        self.assertEqual(payload["structured_result"]["kind"], "settlements")

    def test_settlement_typo_is_normalized_to_list_intent(self):
        with patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ), patch(
            "app.chat_service.list_settlements",
            return_value={
                "rows": [
                    {
                        "settlement_id": 11,
                        "status": "PROCESSED",
                        "expected_date": "2026-03-10",
                        "settled_at": None,
                        "amount_rupees": "1300.00",
                    }
                ],
                "evidence": ["settlement:11"],
                "window": {"from": "2026-02-10", "to": "2026-03-11"},
            },
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="show my recent settlemetns")

        self.assertEqual(payload["intent"], "recent_settlements")
        self.assertEqual(payload["answer_source"], "tool")
        self.assertEqual(payload["structured_result"]["kind"], "settlements")

    def test_debug_true_includes_debug_payload(self):
        with patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ), patch(
            "app.chat_service.list_settlements",
            return_value={
                "rows": [],
                "evidence": [],
                "window": {"from": "2026-02-10", "to": "2026-03-11"},
            },
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="Show my recent settlements", debug=True)

        self.assertIn("debug", payload)
        self.assertEqual(payload["debug"]["route"], "tool-first")

    def test_regex_classified_settlement_request_does_not_need_router(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value={"route": "analysis", "intent": "business_overview", "confidence": 0.99, "reason": "should not be used"},
        ) as mock_router, patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ), patch(
            "app.chat_service.list_settlements",
            return_value={
                "rows": [{"settlement_id": 11, "status": "PROCESSED", "expected_date": "2026-03-10", "settled_at": None, "amount_rupees": "1300.00"}],
                "evidence": ["settlement:11"],
                "window": {"from": "2026-02-10", "to": "2026-03-11"},
            },
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="Show my recent settlements")

        self.assertEqual(payload["answer_source"], "tool")
        self.assertEqual(payload["intent"], "recent_settlements")
        mock_router.assert_not_called()

    def test_unknown_prompt_without_router_clarifies_instead_of_overview(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value=None,
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="blorp")

        self.assertEqual(payload["answer_source"], "clarifying_question")
        self.assertEqual(payload["intent"], "general")
        self.assertTrue(payload["answer"])

    def test_casual_greeting_fast_paths_without_router(self):
        with patch("app.chat_service.route_chat_intent") as mock_router:
            payload = ask_chat(object(), merchant_id="mid_001", prompt="hey man")

        self.assertEqual(payload["answer_source"], "direct")
        self.assertEqual(payload["intent"], "greeting")
        self.assertIn("Hi.", payload["answer"])
        mock_router.assert_not_called()

    def test_capability_question_fast_paths_without_router(self):
        with patch("app.chat_service.route_chat_intent") as mock_router:
            payload = ask_chat(object(), merchant_id="mid_001", prompt="what can you do for me?")

        self.assertEqual(payload["answer_source"], "direct")
        self.assertEqual(payload["intent"], "assistant_identity")
        self.assertIn("I’m AcquiGuru", payload["answer"])
        mock_router.assert_not_called()

    def test_low_confidence_router_falls_back_to_clarify(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value={"route": "analysis", "intent": "business_overview", "confidence": 0.21, "reason": "weak guess"},
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="blorp")

        self.assertEqual(payload["answer_source"], "clarifying_question")
        self.assertEqual(payload["intent"], "general")

    def test_soft_confidence_social_router_is_accepted(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value={"route": "social_ack", "intent": "social_ack", "confidence": 0.58, "reason": "short follow-up"},
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="why?")

        self.assertEqual(payload["answer_source"], "direct")
        self.assertEqual(payload["intent"], "social_ack")

    def test_shortfall_route_skips_engine_runner(self):
        with patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ), patch(
            "app.chat_service.explain_settlement_shortfall",
            return_value={
                "verified": True,
                "directional_support": True,
                "deduction_explanation": "Settlement 9 expected Rs 315,200.00 and received Rs 302,437.25.",
                "summary": "Settlement shortfall explained.",
                "recommended_actions": ["Review MDR and TDS deductions."],
                "shortfall": {"settlement_id": 9},
                "evidence": ["settlement:9"],
            },
        ), patch("app.chat_service.run_intelligence") as mock_run_intelligence:
            payload = ask_chat(
                object(),
                merchant_id="mid_001",
                prompt="I expected Rs 315,200 but received Rs 302,437.25. Explain the shortfall.",
            )

        self.assertEqual(payload["verification_status"], "Verified - deterministic payout shortfall attribution succeeded")
        self.assertIsNotNone(payload["action_preview"])
        self.assertEqual(payload["answer_source"], "tool")
        mock_run_intelligence.assert_not_called()

    def test_engine_question_uses_agentic_synthesis_and_surfaces_validation_notice(self):
        with patch(
            "app.chat_service.get_merchant_context",
            return_value={"merchant": {"merchant_trade_name": "Delhi Airport Parking"}},
        ), patch(
            "app.chat_service.run_intelligence",
            return_value={
                "recommendations": [
                    {
                        "category": "growth",
                        "title": "Recover revenue from failed UPI payments",
                        "summary": "High-value UPI failures are concentrated in a few response codes.",
                        "priority_score": 12,
                        "impact_rupees": 250000,
                        "confidence": 0.86,
                        "actions": [{"who": "merchant", "text": "Review retry flows for UPI failures."}],
                        "evidence_ids": ["lost_sales:mid:window"],
                    }
                ],
                "signals": {
                    "health_vector": {"health_score": 72, "status": "Watchlist"},
                    "operational": {"metrics": {"attempts": 1000, "success_rate_pct": 96.2}},
                    "kpi_delta": {},
                    "attribution": {},
                },
            },
        ), patch(
            "app.chat_service.synthesize_chat_answer",
            return_value={
                "raw_answer": "Your clearest growth opportunity is to recover high-value UPI failures first.",
                "follow_ups": ["Why are my payments failing?"],
                "claims": [
                    {"text": "Recover high-value UPI failures first.", "kind": "ranking", "status": "fact", "evidence_ids": ["lost_sales:mid:window"]}
                ],
            },
        ), patch(
            "app.chat_service.validate_reasoning_output",
            return_value={
                "verification_summary": "1 claim(s) verified, 1 claim(s) need review.",
                "validation_status": "partial",
                "validation_issues": [{"type": "number_not_found_in_evidence", "claim": "Rs 5,00,000 impact"}],
                "display_notice": {"title": "Some claims could not be validated", "summary": "1 claim(s) verified, 1 claim(s) need review.", "issues": [{"type": "number_not_found_in_evidence", "claim": "Rs 5,00,000 impact"}]},
            },
        ), patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="What are my top growth opportunities in the last 30 days?")

        self.assertEqual(payload["answer_source"], "agentic_synthesis")
        self.assertEqual(payload["validation_status"], "partial")
        self.assertIsNotNone(payload["display_notice"])
        self.assertEqual(payload["follow_ups"], ["Why are my payments failing?"])

    def test_terminal_expansion_can_return_clarifying_question(self):
        with patch(
            "app.chat_service.get_merchant_context",
            return_value={"merchant": {"merchant_trade_name": "Delhi Airport Parking"}},
        ), patch(
            "app.chat_service.run_intelligence",
            return_value={
                "recommendations": [],
                "signals": {
                    "health_vector": {"health_score": 72, "status": "Watchlist"},
                    "operational": {"metrics": {"attempts": 1000, "distinct_terminals": 1}},
                    "kpi_delta": {},
                    "attribution": {},
                },
            },
        ), patch(
            "app.chat_service.propose_clarifying_question",
            return_value={
                "question": "Are you trying to add checkout capacity or reduce single-terminal risk?",
                "choices": ["Add capacity", "Reduce single-terminal risk"],
                "reason": "Terminal expansion depends on the goal.",
            },
        ), patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="Should I get more POS terminals?")

        self.assertEqual(payload["answer_source"], "clarifying_question")

    def test_router_can_force_deterministic_settlement_route(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value={"route": "deterministic", "intent": "recent_settlements", "confidence": 0.99, "reason": "List request"},
        ), patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ), patch(
            "app.chat_service.list_settlements",
            return_value={
                "rows": [{"settlement_id": 99, "status": "PROCESSED", "expected_date": "2026-03-10", "settled_at": None, "amount_rupees": "999.00"}],
                "evidence": ["settlement:99"],
                "window": {"from": "2026-02-10", "to": "2026-03-11"},
            },
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="What are my settlements?")

        self.assertEqual(payload["answer_source"], "tool")
        self.assertEqual(payload["intent"], "recent_settlements")
        self.assertEqual(payload["structured_result"]["rows"][0]["settlement_id"], 99)

    def test_router_can_force_social_ack(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value={"route": "social_ack", "intent": "social_ack", "confidence": 0.99, "reason": "Short acknowledgement"},
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="Interesting")

        self.assertEqual(payload["answer_source"], "direct")
        self.assertEqual(payload["intent"], "social_ack")
        self.assertIn("Understood", payload["answer"])

    def test_router_can_force_direct_assistant_identity(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value={"route": "direct", "intent": "assistant_identity", "confidence": 0.98, "reason": "identity question"},
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="who are you?")

        self.assertEqual(payload["answer_source"], "direct")
        self.assertEqual(payload["intent"], "assistant_identity")
        self.assertIn("I’m AcquiGuru", payload["answer"])

    def test_router_can_force_direct_social_challenge(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value={"route": "direct", "intent": "social_challenge", "confidence": 0.96, "reason": "pushback"},
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="did I ask?")

        self.assertEqual(payload["answer_source"], "direct")
        self.assertEqual(payload["intent"], "social_challenge")
        self.assertIn("No.", payload["answer"])

    def test_router_can_force_analysis_path(self):
        with patch(
            "app.chat_service.route_chat_intent",
            return_value={"route": "analysis", "intent": "top_growth_opportunities", "confidence": 0.93, "reason": "Broad analysis question"},
        ), patch(
            "app.chat_service.get_merchant_context",
            return_value={"merchant": {"merchant_trade_name": "Delhi Airport Parking"}},
        ), patch(
            "app.chat_service.run_intelligence",
            return_value={
                "recommendations": [
                    {
                        "category": "growth",
                        "title": "Recover revenue from failed UPI payments",
                        "summary": "High-value UPI failures are concentrated in a few response codes.",
                        "priority_score": 12,
                        "impact_rupees": 250000,
                        "confidence": 0.86,
                        "actions": [{"who": "merchant", "text": "Review retry flows for UPI failures."}],
                        "evidence_ids": ["lost_sales:mid:window"],
                    }
                ],
                "signals": {
                    "health_vector": {"health_score": 72, "status": "Watchlist"},
                    "operational": {"metrics": {"attempts": 1000, "success_rate_pct": 96.2}},
                    "kpi_delta": {},
                    "attribution": {},
                },
            },
        ), patch(
            "app.chat_service.synthesize_chat_answer",
            return_value={
                "raw_answer": "Your clearest growth opportunity is to recover high-value UPI failures first.",
                "follow_ups": ["Why are my payments failing?"],
                "claims": [
                    {"text": "Recover high-value UPI failures first.", "kind": "ranking", "status": "fact", "evidence_ids": ["lost_sales:mid:window"]}
                ],
            },
        ), patch(
            "app.chat_service.validate_reasoning_output",
            return_value={
                "verification_summary": "1 claim(s) verified.",
                "validation_status": "clean",
                "validation_issues": [],
                "display_notice": None,
            },
        ), patch(
            "app.chat_service.default_window_from_max_date",
            return_value=("2026-02-10", "2026-03-11"),
        ):
            payload = ask_chat(object(), merchant_id="mid_001", prompt="What are my top growth opportunities in the last 30 days?")

        self.assertEqual(payload["answer_source"], "agentic_synthesis")
        self.assertEqual(payload["intent"], "top_growth_opportunities")
        self.assertIsNone(payload["clarifying_question"])


if __name__ == "__main__":
    unittest.main()

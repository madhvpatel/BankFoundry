import json
import datetime as dt
import unittest

from sqlalchemy import create_engine
from sqlalchemy import text

from app.data.proactive import repository as proactive_repository


class ProactiveRepositoryTest(unittest.TestCase):
    def test_persist_list_and_status_flow(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

        result = proactive_repository.persist_background_proactive_cards(
            engine,
            "m_001",
            window_from="2026-03-01",
            window_to="2026-03-11",
            ranked_cards=[
                {
                    "id": "success_rate_drop",
                    "title": "Success rate dropped",
                    "body": "Investigate response codes",
                    "verification_status": "Background signal",
                    "impact_rupees": 1200.0,
                    "confidence": 0.8,
                }
            ],
            shortfall_by_card_id={},
            lane_resolver=lambda card: "growth",
            create_action_fn=lambda merchant_id, action: None,
        )

        cards = proactive_repository.list_background_proactive_cards(engine, "m_001", limit=8)
        update = proactive_repository.update_background_proactive_card_state(
            engine,
            "m_001",
            dedupe_key=cards[0]["dedupe_key"],
            state="ACKNOWLEDGED",
            card_notes="Seen",
        )
        link = proactive_repository.link_background_proactive_card_case(
            engine,
            "m_001",
            dedupe_key=cards[0]["dedupe_key"],
            case_id="case_123",
        )
        proactive_repository.upsert_background_refresh_schedule(
            engine,
            "m_001",
            days=30,
            current_time=dt.datetime(2026, 3, 23, 10, 0, tzinfo=dt.timezone.utc),
            next_refresh_at=dt.datetime(2026, 3, 23, 10, 30, tzinfo=dt.timezone.utc),
            window_from="2026-03-01",
            window_to="2026-03-11",
            generated_count=1,
            inserted_count=1,
        )
        status = proactive_repository.get_background_refresh_status(
            engine,
            "m_001",
            days=30,
            interval_minutes=30,
            auto_enabled=True,
            now=dt.datetime(2026, 3, 23, 10, 1, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result["inserted_count"], 1)
        self.assertEqual(len(cards), 1)
        self.assertTrue(update["updated"])
        self.assertTrue(link["updated"])
        self.assertEqual(proactive_repository.list_background_proactive_cards(engine, "m_001", limit=8)[0]["linked_case_id"], "case_123")
        self.assertFalse(status["due"])
        self.assertEqual(status["last_generated_count"], 1)

    def test_list_background_proactive_cards_flattens_nested_evidence_ids(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        proactive_repository.ensure_proactive_cards_schema(engine)

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO proactive_cards (
                        dedupe_key, merchant_id, lane, verification_status, evidence_ids,
                        action_preview_token, payload_json, window_from, window_to
                    ) VALUES (
                        :dedupe_key, :merchant_id, :lane, :verification_status, :evidence_ids,
                        NULL, :payload_json, :window_from, :window_to
                    )
                    """
                ),
                {
                    "dedupe_key": "bg:m_001:operations:payout_shortfall_s_001:2026-03-01:2026-03-11",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "verification_status": "Verified",
                    "evidence_ids": json.dumps(
                        [
                            ["settlement:s_001", "shortfall:settlement:s_001"],
                            "['insight_card:payout_shortfall_s_001:2026-03-01:2026-03-11', 'window:2026-03-01:2026-03-11']",
                            {"evidence_id": "merchant:m_001"},
                        ]
                    ),
                    "payload_json": json.dumps({"title": "Shortfall", "body": "Review payout"}),
                    "window_from": "2026-03-01",
                    "window_to": "2026-03-11",
                },
            )

        cards = proactive_repository.list_background_proactive_cards(engine, "m_001", limit=8)

        self.assertEqual(len(cards), 1)
        self.assertEqual(
            cards[0]["evidence_ids"],
            [
                "settlement:s_001",
                "shortfall:settlement:s_001",
                "insight_card:payout_shortfall_s_001:2026-03-01:2026-03-11",
                "window:2026-03-01:2026-03-11",
                "merchant:m_001",
            ],
        )


if __name__ == "__main__":
    unittest.main()

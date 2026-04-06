import unittest

from sqlalchemy import create_engine, text

from app.data.actions import repository as action_repository


class ActionRepositoryTest(unittest.TestCase):
    def test_list_existing_actions_dedupes_and_filters(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        status TEXT,
                        evidence TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_actions (mid, category, title, description, status, evidence, updated_at)
                    VALUES
                    ('m_001', 'growth', 'Expand peak-hour throughput', 'Add one more device', 'OPEN', '{"source":"rule","evidence_ids":["e1"]}', '2026-03-10'),
                    ('m_001', 'growth', 'Expand peak-hour throughput', 'Add one more device', 'OPEN', '{"source":"rule","evidence_ids":["e2"]}', '2026-03-09'),
                    ('m_001', 'growth', 'Low signal task', 'Ignore me', 'OPEN', '{}', '2026-03-08')
                    """
                )
            )

        rows = action_repository.list_existing_actions(
            engine,
            "m_001",
            limit=10,
            low_signal_titles={"low signal task"},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Expand peak-hour throughput")
        self.assertEqual(rows[0]["evidence_ids"], ["e1"])


if __name__ == "__main__":
    unittest.main()

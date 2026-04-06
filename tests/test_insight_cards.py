import datetime as dt
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine, text

from app.intelligence.insight_cards import (
    evaluate_trigger,
    generate_insight_cards,
    load_card_templates,
)


class InsightCardsTest(unittest.TestCase):
    MID = "merchant_cards_001"

    def test_load_card_templates_and_trigger_eval(self):
        with tempfile.TemporaryDirectory() as td:
            card_path = Path(td) / "sample.md"
            card_path.write_text(
                """# CARD: Sample Card
severity: warning
icon: ⚠️
impact_metric: impact_sr_drop_revenue
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: success_rate_drop_pp >= 1.5 and attempts_24h >= 50
copy:
  title: \"Success dropped\"
  explanation: \"Drop is {success_rate_drop_pp:.2f}pp\"
actions:
  - \"Investigate top failures\"
""",
                encoding="utf-8",
            )

            templates = load_card_templates(Path(td))
            self.assertEqual(len(templates), 1)
            tpl = templates[0]
            self.assertEqual(tpl.card_id, "sample")
            self.assertEqual(tpl.trigger_tool, "compute_kpis")
            self.assertTrue(evaluate_trigger(tpl.condition, {"success_rate_drop_pp": 2.0, "attempts_24h": 60}))
            self.assertFalse(evaluate_trigger(tpl.condition, {"success_rate_drop_pp": 0.5, "attempts_24h": 60}))
            self.assertFalse(evaluate_trigger("__import__('os').system('echo hacked')", {}))

    def test_generate_insight_cards_from_markdown_templates(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE transaction_features (
                    merchant_id TEXT,
                    source_txn_id TEXT,
                    terminal_id TEXT,
                    payment_mode TEXT,
                    status TEXT,
                    response_code TEXT,
                    amount_rupees REAL,
                    p_date DATE,
                    initiated_at TIMESTAMP,
                    completed_at TIMESTAMP
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE refunds (
                    mid TEXT,
                    refund_amount REAL,
                    p_date DATE
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE chargebacks (
                    mid TEXT,
                    chargeback_amount REAL,
                    response_due_date DATE,
                    resolution_outcome TEXT
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE settlements (
                    mid TEXT,
                    settlement_status TEXT,
                    settlement_date DATE,
                    net_settlement_amount REAL
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE merchant_kyc_documents (
                    mid TEXT,
                    expiry_date DATE,
                    kyc_status TEXT
                )
                """
            )

            # Previous 7 days: strong success profile.
            for i in range(140):
                p_date = dt.date(2026, 2, 20) + dt.timedelta(days=i % 7)
                status = "SUCCESS" if i < 133 else "FAILURE"
                conn.execute(
                    text(
                        """
                        INSERT INTO transaction_features (
                            merchant_id, source_txn_id, terminal_id, payment_mode, status,
                            response_code, amount_rupees, p_date, initiated_at, completed_at
                        ) VALUES (
                            :merchant_id, :source_txn_id, :terminal_id, :payment_mode, :status,
                            :response_code, :amount_rupees, :p_date, :initiated_at, :completed_at
                        )
                        """
                    ),
                    {
                        "merchant_id": self.MID,
                        "source_txn_id": f"prev_{i}",
                        "terminal_id": "T1",
                        "payment_mode": "UPI",
                        "status": status,
                        "response_code": "91" if status == "FAILURE" else None,
                        "amount_rupees": 1000.0,
                        "p_date": p_date,
                        "initiated_at": dt.datetime.combine(p_date, dt.time(hour=10, minute=0)),
                        "completed_at": dt.datetime.combine(p_date, dt.time(hour=10, minute=1)),
                    },
                )

            # Latest day: degraded success profile.
            latest_day = dt.date(2026, 2, 27)
            for i in range(80):
                status = "SUCCESS" if i < 64 else "FAILURE"
                conn.execute(
                    text(
                        """
                        INSERT INTO transaction_features (
                            merchant_id, source_txn_id, terminal_id, payment_mode, status,
                            response_code, amount_rupees, p_date, initiated_at, completed_at
                        ) VALUES (
                            :merchant_id, :source_txn_id, :terminal_id, :payment_mode, :status,
                            :response_code, :amount_rupees, :p_date, :initiated_at, :completed_at
                        )
                        """
                    ),
                    {
                        "merchant_id": self.MID,
                        "source_txn_id": f"latest_{i}",
                        "terminal_id": "T1",
                        "payment_mode": "UPI",
                        "status": status,
                        "response_code": "91" if status == "FAILURE" else None,
                        "amount_rupees": 1000.0,
                        "p_date": latest_day,
                        "initiated_at": dt.datetime.combine(latest_day, dt.time(hour=12, minute=0)),
                        "completed_at": dt.datetime.combine(latest_day, dt.time(hour=12, minute=1)),
                    },
                )

        with tempfile.TemporaryDirectory() as td:
            card_path = Path(td) / "success_rate_drop.md"
            card_path.write_text(
                """# CARD: Success Rate Drop Detected
severity: warning
icon: 📉
impact_metric: impact_sr_drop_revenue
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: success_rate_drop_pp >= 5 and attempts_24h >= 50
copy:
  title: \"Success rate down {success_rate_drop_pp:.2f}pp\"
  explanation: \"24h {success_rate_24h:.2f}% vs 7d {success_rate_7d_avg:.2f}%\"
actions:
  - \"Open support ticket\"
""",
                encoding="utf-8",
            )

            cards = generate_insight_cards(
                engine=engine,
                merchant_id=self.MID,
                window_days=30,
                cards_dir=Path(td),
            )

        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card["id"], "success_rate_drop")
        self.assertIn("Success rate down", card["title"])
        self.assertGreater(float(card.get("impact_rupees") or 0.0), 0.0)


if __name__ == "__main__":
    unittest.main()

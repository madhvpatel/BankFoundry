import datetime as dt
import random
import unittest

from app.intelligence.demo_activity_generator import (
    IST,
    DemoActivityGenerator,
    GeneratedTransaction,
    GeneratorConfig,
    MerchantContext,
    build_settlement_rows,
)


class DemoActivityGeneratorTest(unittest.TestCase):
    def _make_generator(self, *, once: bool = False) -> DemoActivityGenerator:
        gen = DemoActivityGenerator.__new__(DemoActivityGenerator)
        gen.rng = random.Random(7)
        gen.context = MerchantContext(
            mid="100000000121215",
            tid="EP070270",
            merchant_trade_name="Delhi Airport Parking",
            merchant_legal_name="MS Delhi Airport Parking Services Private Limited",
            business_city="New Delhi",
            business_state="DELHI",
            mcc_code="7523",
            expected_avg_ticket_size=550.0,
            max_transaction_limit=100000.0,
            vpa="ibkPOS.EP070270@icici",
            terminal_make="PAX",
            terminal_model="A910S",
            app_version="ICICI_A910S_v1.0.0.18",
            primary_bank_account_id=1,
        )
        gen.config = GeneratorConfig(
            database_url="postgresql://demo:demo@localhost:5433/payments_demo",
            once=once,
            batch_min=2,
            batch_max=2,
            upi_share=0.75,
            upi_success_rate=1.0,
            card_success_rate=1.0,
            settlement_every_batches=10,
        )
        gen.mdr_rates = {
            ("CARD", "VISA", "DEBIT"): (0.4, 18.0),
            ("CARD", "MASTERCARD", "DEBIT"): (0.4, 18.0),
            ("CARD", "RUPAY", "DEBIT"): (0.0, 18.0),
            ("CARD", "VISA", "CREDIT"): (1.8, 18.0),
            ("UPI", "", ""): (0.0, 18.0),
        }
        gen._raw_id = 1000
        gen._rrn_counter = 604000000000
        gen._invoice_counter = 570000
        gen._stan_counter = 220000
        gen._continue = True
        gen._batch_counter = 1
        gen.contexts = [gen.context]
        return gen

    def test_build_upi_transaction_populates_raw_and_curated_rows(self):
        gen = self._make_generator()
        when_local = dt.datetime(2026, 3, 6, 10, 15, tzinfo=IST)

        txn = gen._build_upi_transaction(when_local)

        self.assertEqual(txn.payment_mode, "UPI")
        self.assertEqual(txn.status, "SUCCESS")
        self.assertEqual(txn.fact_row["merchant_id"], gen.context.mid)
        self.assertEqual(txn.fact_row["terminal_id"], gen.context.tid)
        self.assertEqual(txn.feature_row["payment_mode"], "UPI")
        self.assertEqual(txn.feature_row["amount_rupees"], txn.amount_paise / 100.0)
        self.assertEqual(txn.feature_row["p_date"], when_local.date())
        self.assertLessEqual(txn.amount_rupees, gen.context.max_transaction_limit)

        raw_tables = {spec.table for spec in txn.raw_inserts}
        self.assertEqual(
            raw_tables,
            {
                "raw_upi_transactions",
                "raw_upi_notifications",
                "raw_upi_callback_logs",
                "raw_upi_qr_records",
                "raw_upi_mqtt_logs",
            },
        )

    def test_build_card_transaction_populates_card_shape(self):
        gen = self._make_generator()
        when_local = dt.datetime(2026, 3, 6, 10, 30, tzinfo=IST)

        txn = gen._build_card_transaction(when_local)

        self.assertEqual(txn.payment_mode, "CARD")
        self.assertEqual(len(txn.raw_inserts), 1)
        self.assertEqual(txn.raw_inserts[0].table, "raw_card_transactions")
        self.assertEqual(txn.fact_row["raw_card_autoid"], str(txn.raw_inserts[0].row["autoid"]))
        self.assertEqual(txn.feature_row["payment_mode"], "CARD")
        self.assertIsNotNone(txn.feature_row["card_bin"])
        self.assertIn(txn.raw_inserts[0].row["card_type"], {"VISA", "MASTERCARD", "RUPAY"})
        self.assertGreaterEqual(txn.mdr_rate_pct, 0.0)

    def test_build_settlement_rows_aggregates_by_payment_mode(self):
        context = self._make_generator().context
        when_local = dt.datetime(2026, 3, 6, 11, 0, tzinfo=IST)

        txns = [
            GeneratedTransaction("UPI", "SUCCESS", 500.0, 50000, "1", "1", 0.0, [], {}, {}),
            GeneratedTransaction("UPI", "SUCCESS", 700.0, 70000, "2", "2", 0.0, [], {}, {}),
            GeneratedTransaction("CARD", "SUCCESS", 1000.0, 100000, "3", "3", 0.4, [], {}, {}),
        ]

        rows = build_settlement_rows(txns, context=context, when_local=when_local, batch_number=5)

        self.assertEqual(len(rows), 2)
        by_mode = {row["payment_mode"]: row for row in rows}
        self.assertEqual(by_mode["UPI"]["txn_count"], 2)
        self.assertEqual(by_mode["UPI"]["mdr_deducted"], 0.0)
        self.assertEqual(by_mode["CARD"]["txn_count"], 1)
        self.assertGreater(by_mode["CARD"]["gross_amount"], by_mode["CARD"]["net_settlement_amount"])

    def test_generate_batch_emits_settlements_in_once_mode(self):
        gen = self._make_generator(once=True)
        now_local = dt.datetime(2026, 3, 6, 12, 0, tzinfo=IST)

        batch = gen.generate_batch(now_local=now_local)

        self.assertEqual(len(batch.transactions), 2)
        self.assertTrue(batch.settlements)

    def test_build_terminal_health_snapshot_uses_context_terminal(self):
        gen = self._make_generator()
        when_local = dt.datetime(2026, 3, 6, 12, 5, tzinfo=IST)

        snapshot = gen._build_terminal_health_snapshot(when_local)

        self.assertEqual(snapshot["tid"], gen.context.tid)
        self.assertEqual(snapshot["mid"], gen.context.mid)
        self.assertIn("demo-live-feed", snapshot["application_list"])
        self.assertIn("airtel", snapshot["sim_details"])

    def test_build_fee_ledger_rows_creates_rows_for_successful_transactions(self):
        gen = self._make_generator()
        when_local = dt.datetime(2026, 3, 6, 12, 15, tzinfo=IST)
        txns = [
            GeneratedTransaction(
                "CARD",
                "SUCCESS",
                1000.0,
                100000,
                "1",
                "1",
                0.4,
                [],
                {"invoice_nr": "001001"},
                {},
                card_rate_network="VISA",
                card_rate_type="DEBIT",
            ),
            GeneratedTransaction(
                "UPI",
                "SUCCESS",
                500.0,
                50000,
                "2",
                "2",
                0.0,
                [],
                {"invoice_nr": "001002"},
                {},
            ),
        ]

        rows = gen._build_fee_ledger_rows(txns, when_local)

        self.assertEqual(len(rows), 2)
        self.assertGreater(rows[0]["mdr_amount"], 0.0)
        self.assertEqual(rows[1]["mdr_amount"], 0.0)


if __name__ == "__main__":
    unittest.main()

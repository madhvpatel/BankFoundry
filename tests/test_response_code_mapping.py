import datetime as dt
import unittest

from sqlalchemy import create_engine, text

from app.intelligence.engines.operational_signals import collect_operational_signals
from app.intelligence.response_codes import (
    canonical_response_category,
    canonical_response_desc,
    format_response_code_label,
)


class ResponseCodeMappingTest(unittest.TestCase):
    MID = "merchant_codes_001"

    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE transaction_features (
                    merchant_id TEXT,
                    terminal_id TEXT,
                    payment_mode TEXT,
                    status TEXT,
                    response_code TEXT,
                    response_desc TEXT,
                    payer_bank_code TEXT,
                    amount_rupees REAL,
                    p_date DATE,
                    hour_of_day INTEGER,
                    day_of_week INTEGER
                )
                """
            )

            today = dt.date(2026, 1, 30)
            rows = [
                {
                    "merchant_id": self.MID,
                    "terminal_id": "TERM001",
                    "payment_mode": "CARD",
                    "status": "FAILURE",
                    "response_code": "55",
                    "response_desc": "bad desc from source",
                    "payer_bank_code": "HDFC",
                    "amount_rupees": 500.0,
                    "p_date": today,
                    "hour_of_day": 10,
                    "day_of_week": today.weekday(),
                },
                {
                    "merchant_id": self.MID,
                    "terminal_id": "TERM001",
                    "payment_mode": "UPI",
                    "status": "FAILURE",
                    "response_code": None,
                    "response_desc": None,
                    "payer_bank_code": "",
                    "amount_rupees": 650.0,
                    "p_date": today,
                    "hour_of_day": 10,
                    "day_of_week": today.weekday(),
                },
                {
                    "merchant_id": self.MID,
                    "terminal_id": "TERM001",
                    "payment_mode": "UPI",
                    "status": "FAILURE",
                    "response_code": "91",
                    "response_desc": "issuer down",
                    "payer_bank_code": "UBIN",
                    "amount_rupees": 800.0,
                    "p_date": today,
                    "hour_of_day": 10,
                    "day_of_week": today.weekday(),
                },
                {
                    "merchant_id": self.MID,
                    "terminal_id": "TERM001",
                    "payment_mode": "CARD",
                    "status": "FAILURE",
                    "response_code": "55",
                    "response_desc": "bad desc from source",
                    "payer_bank_code": "",
                    "amount_rupees": 450.0,
                    "p_date": today,
                    "hour_of_day": 10,
                    "day_of_week": today.weekday(),
                },
                {
                    "merchant_id": self.MID,
                    "terminal_id": "TERM001",
                    "payment_mode": "CARD",
                    "status": "FAILED",
                    "response_code": "X1",
                    "response_desc": None,
                    "payer_bank_code": "HDFC",
                    "amount_rupees": 700.0,
                    "p_date": today,
                    "hour_of_day": 11,
                    "day_of_week": today.weekday(),
                },
                {
                    "merchant_id": self.MID,
                    "terminal_id": "TERM001",
                    "payment_mode": "CARD",
                    "status": "SUCCESS",
                    "response_code": None,
                    "response_desc": None,
                    "payer_bank_code": "HDFC",
                    "amount_rupees": 1000.0,
                    "p_date": today,
                    "hour_of_day": 12,
                    "day_of_week": today.weekday(),
                },
            ]
            for row in rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO transaction_features (
                            merchant_id, terminal_id, payment_mode, status, response_code, response_desc,
                            payer_bank_code, amount_rupees, p_date, hour_of_day, day_of_week
                        )
                        VALUES (
                            :merchant_id, :terminal_id, :payment_mode, :status, :response_code, :response_desc,
                            :payer_bank_code, :amount_rupees, :p_date, :hour_of_day, :day_of_week
                        )
                        """
                    ),
                    row,
                )

    def test_canonical_mapping_helpers(self):
        self.assertEqual(canonical_response_desc("55"), "Incorrect PIN")
        self.assertEqual(canonical_response_category("55"), "Customer authentication")
        self.assertEqual(format_response_code_label("91"), "91 - Issuer or Switch Inoperative")

    def test_operational_signals_apply_mapping(self):
        signals = collect_operational_signals(self.engine, self.MID, table="transaction_features", window_days=30)
        top_codes = signals.get("evidence", {}).get("top_failure_codes", [])
        by_code = {str(r.get("response_code")): r for r in top_codes}

        self.assertIn("UPI_FAILURE", by_code)
        self.assertEqual(by_code["UPI_FAILURE"]["response_desc"], "UPI failure (response code unavailable)")
        self.assertEqual(by_code["UPI_FAILURE"]["response_category"], "UPI network / issuer issue")

        self.assertIn("55", by_code)
        self.assertEqual(by_code["55"]["response_desc"], "Incorrect PIN")
        self.assertEqual(by_code["55"]["response_category"], "Customer authentication")

        self.assertIn("X1", by_code)
        self.assertEqual(by_code["X1"]["response_desc"], "Failure code unavailable")
        self.assertEqual(by_code["X1"]["response_category"], "Unclassified failure")

    def test_top_payer_banks_excludes_blank_and_non_upi_artifacts(self):
        signals = collect_operational_signals(self.engine, self.MID, table="transaction_features", window_days=30)
        top_banks = signals.get("evidence", {}).get("top_payer_banks_in_failures", [])
        bank_codes = [str(r.get("payer_bank_code") or "") for r in top_banks]

        self.assertIn("UBIN", bank_codes)
        self.assertNotIn("", bank_codes)
        self.assertNotIn("UNKNOWN", bank_codes)

        metrics = signals.get("metrics", {})
        self.assertIn("upi_failures_missing_bank_code", metrics)
        self.assertGreaterEqual(int(metrics.get("upi_failures_missing_bank_code") or 0), 1)


if __name__ == "__main__":
    unittest.main()

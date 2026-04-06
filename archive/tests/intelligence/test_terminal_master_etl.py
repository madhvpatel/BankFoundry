import unittest

from app.intelligence.etl_terminal_master import (
    map_postransaction_row,
    map_upitransaction_row,
    normalize_records,
)


class TerminalMasterETLTest(unittest.TestCase):
    def test_map_postransaction_row(self):
        row = {
            "mid": "M001",
            "tid": "T001",
            "tran_id": "POS123",
            "invoice_nr": "INV-1",
            "rsp_code": "00",
            "rsp_desc": "Approved",
            "amount": "1250.50",
            "tran_date": "2026-02-01",
            "request_datetime": "2026-02-01 10:30:00",
            "rsp_datetime": "2026-02-01 10:30:05",
            "card_type": "VISA",
            "network_type": "Credit",
            "pos_entry_mode": "CHIP",
        }
        out = map_postransaction_row(row)
        self.assertEqual(out["merchant_id"], "M001")
        self.assertEqual(out["terminal_id"], "T001")
        self.assertEqual(out["source_system"], "POS")
        self.assertEqual(out["payment_mode"], "CARD")
        self.assertEqual(out["status"], "SUCCESS")
        self.assertEqual(out["response_code"], "00")
        self.assertEqual(out["amount_rupees"], 1250.50)
        self.assertEqual(out["p_date"], "2026-02-01")
        self.assertEqual(out["hour_of_day"], 10)
        self.assertEqual(out["day_of_week"], 6)  # 2026-02-01 is Sunday

    def test_map_upitransaction_row(self):
        row = {
            "mid": "M001",
            "tid": "T002",
            "upitranlogid": "UPI123",
            "invoice_nr": "INV-2",
            "txnstatus": "FAILED",
            "responsecode": "U16",
            "amount": "99.00",
            "p_date": "2026-02-02",
            "txninitdate": "2026-02-02 11:00:00",
            "txncompletiondate": "2026-02-02 11:00:03",
            "payee_mcccode": "4789",
        }
        out = map_upitransaction_row(row)
        self.assertEqual(out["merchant_id"], "M001")
        self.assertEqual(out["terminal_id"], "T002")
        self.assertEqual(out["source_system"], "UPI")
        self.assertEqual(out["payment_mode"], "UPI")
        self.assertEqual(out["status"], "FAILED")
        self.assertEqual(out["response_code"], "U16")
        self.assertEqual(out["amount_rupees"], 99.0)
        self.assertEqual(out["p_date"], "2026-02-02")
        self.assertEqual(out["hour_of_day"], 11)
        self.assertEqual(out["mcc"], "4789")

    def test_normalize_records_combines_sources(self):
        rows = normalize_records(
            pos_rows=[{"mid": "M1", "tid": "T1", "rsp_code": "05", "amount": "100", "tran_date": "2026-01-01"}],
            upi_rows=[{"mid": "M1", "tid": "T2", "txnstatus": "SUCCESS", "amount": "50", "p_date": "2026-01-01"}],
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["payment_mode"], "CARD")
        self.assertEqual(rows[1]["payment_mode"], "UPI")


if __name__ == "__main__":
    unittest.main()

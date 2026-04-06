import unittest

from app.application.workflows.reporting import build_report_briefs, build_report_packs, rows_to_csv


class ReportingWorkflowTest(unittest.TestCase):
    def test_report_builders_generate_packs_briefs_and_csv(self):
        snapshot = {
            "merchant_id": "m_001",
            "merchant_profile": {"merchant": {"merchant_trade_name": "Demo Merchant"}},
            "window": {"from": "2026-01-01", "to": "2026-02-01"},
            "scope": {"level": "terminal", "label": "Terminal T1"},
            "summary": {
                "attempts": 3,
                "success_rate_pct": 33.33,
                "success_gmv": 1000.0,
                "open_chargebacks": 1,
                "refund_count": 1,
                "terminal_count": 1,
            },
            "cashflow": {
                "amounts": {"settled_amount": 1500.0, "pending_amount": 200.0},
                "past_expected": {"past_expected_count": 1, "past_expected_amount": 200.0},
                "recent": [{"settlement_id": "s1"}],
            },
            "kpi_by_mode": [{"bucket": "CARD", "attempts": 2}],
            "failure_drivers": {
                "payment_mode": {"rows": [{"driver": "UPI", "failed_txns": 2}]},
                "response_code": {"rows": [{"driver": "91", "failed_txns": 2}]},
            },
            "growth_tasks": [{"title": "Growth task"}],
            "operations_tasks": [{"title": "Ops task"}],
            "settlements": {"rows": [{"settlement_id": "s1"}]},
            "chargebacks": {"rows": [{"chargeback_id": "cb1"}]},
            "refunds": {"rows": [{"refund_id": "r1"}]},
            "terminals": {"rows": [{"terminal_id": "T1"}]},
        }

        packs = build_report_packs(snapshot)
        briefs = build_report_briefs(snapshot)
        csv_bytes = rows_to_csv([{"a": 1, "b": "two"}])

        self.assertEqual([pack["id"] for pack in packs], ["finance", "operations", "growth"])
        self.assertTrue(briefs[0]["subject"].startswith("Finance Pack | Demo Merchant"))
        self.assertIn("Scope: Terminal T1", briefs[0]["email_text"])
        self.assertEqual(csv_bytes.decode("utf-8").splitlines()[0], "a,b")


if __name__ == "__main__":
    unittest.main()

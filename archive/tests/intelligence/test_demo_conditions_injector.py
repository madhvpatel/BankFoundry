import datetime as dt
import unittest

from app.intelligence.demo_conditions_injector import plan_alert_scenario


class DemoConditionsInjectorTest(unittest.TestCase):
    def test_plan_alert_scenario_builds_expected_shape(self):
        anchor_date = dt.date(2026, 3, 10)
        plan = plan_alert_scenario(
            merchant_id="100000000121215",
            primary_terminal_id="EP070270",
            anomaly_terminal_id="EP070271",
            anchor_date=anchor_date,
            run_tag="testrun",
        )

        self.assertEqual(len(plan.transactions), 200)
        self.assertEqual(len(plan.refunds), 3)
        self.assertEqual(len(plan.chargebacks), 1)
        self.assertEqual(len(plan.settlements), 1)
        self.assertEqual(len(plan.kyc_documents), 2)

        today_rows = [pair for pair in plan.transactions if pair.feature_row["p_date"] == anchor_date]
        prev7_rows = [pair for pair in plan.transactions if pair.feature_row["p_date"] < anchor_date]
        self.assertEqual(len(today_rows), 60)
        self.assertEqual(len(prev7_rows), 140)

        today_failures = [pair for pair in today_rows if pair.feature_row["status"] == "FAILED"]
        anomaly_terminal_rows = [pair for pair in today_rows if pair.feature_row["terminal_id"] == "EP070271"]
        self.assertEqual(len(today_failures), 16)
        self.assertEqual(len(anomaly_terminal_rows), 20)
        self.assertTrue(any(pair.feature_row["amount_rupees"] >= 5000.0 for pair in today_failures))


if __name__ == "__main__":
    unittest.main()

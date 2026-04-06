import datetime as dt
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

from app.intelligence.scenario_engine.baseline import fetch_baseline
from app.intelligence.scenario_engine.planner import _normalize_spec
from app.intelligence.scenario_engine.service import run_scenario
from app.intelligence.scenario_engine.simulators import simulate_success_rate
from app.intelligence.scenario_engine.types import ScenarioSpec


class ScenarioEngineTest(unittest.TestCase):
    MID = "merchant_scenario_001"

    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE transaction_features (
                    merchant_id TEXT,
                    payment_mode TEXT,
                    status TEXT,
                    amount_rupees REAL,
                    p_date DATE,
                    hour_of_day INTEGER,
                    day_of_week INTEGER
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE refunds (
                    mid TEXT,
                    refund_amount REAL
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE chargebacks (
                    mid TEXT,
                    chargeback_amount REAL
                )
                """
            )

        self._seed()

    def _seed(self):
        start = dt.date(2026, 2, 1)
        with self.engine.begin() as conn:
            for i in range(10):
                status = "SUCCESS" if i < 8 else "FAILURE"
                mode = "UPI" if i % 2 == 0 else "CARD"
                conn.execute(
                    text(
                        """
                        INSERT INTO transaction_features (
                            merchant_id, payment_mode, status, amount_rupees, p_date, hour_of_day, day_of_week
                        ) VALUES (
                            :merchant_id, :payment_mode, :status, :amount_rupees, :p_date, :hour_of_day, :day_of_week
                        )
                        """
                    ),
                    {
                        "merchant_id": self.MID,
                        "payment_mode": mode,
                        "status": status,
                        "amount_rupees": 1000.0,
                        "p_date": start + dt.timedelta(days=i),
                        "hour_of_day": 10,
                        "day_of_week": (start + dt.timedelta(days=i)).weekday(),
                    },
                )
            conn.execute(
                text("INSERT INTO refunds (mid, refund_amount) VALUES (:mid, :amt)"),
                [{"mid": self.MID, "amt": 5000.0}, {"mid": self.MID, "amt": 2000.0}],
            )
            conn.execute(
                text("INSERT INTO chargebacks (mid, chargeback_amount) VALUES (:mid, :amt)"),
                [{"mid": self.MID, "amt": 3000.0}],
            )

    def test_baseline_and_success_rate_simulation(self):
        baseline = fetch_baseline(
            self.engine,
            self.MID,
            start_date=dt.date(2026, 2, 1),
            end_date=dt.date(2026, 2, 20),
        )
        self.assertEqual(baseline["attempts"], 10)
        self.assertEqual(baseline["success_txns"], 8)
        self.assertEqual(baseline["fail_txns"], 2)
        self.assertAlmostEqual(baseline["success_rate"], 80.0, places=2)
        self.assertAlmostEqual(baseline["avg_ticket_success"], 1000.0, places=2)
        self.assertEqual(baseline["refund_count"], 2)
        self.assertAlmostEqual(baseline["refund_gmv"], 7000.0, places=2)
        self.assertEqual(baseline["chargeback_count"], 1)
        self.assertAlmostEqual(baseline["chargeback_gmv"], 3000.0, places=2)

        projection = simulate_success_rate(baseline, {"delta_success_rate_pct": 2.0})
        self.assertAlmostEqual(projection["recovered_txns"], 0.2, places=2)
        self.assertAlmostEqual(projection["recovered_revenue"], 200.0, places=2)
        self.assertAlmostEqual(projection["new_success_rate"], 82.0, places=2)
        self.assertAlmostEqual(projection["requested_delta_success_rate_pct"], 2.0, places=2)

    def test_success_uplift_clamps_to_available_headroom(self):
        baseline = {
            "attempts": 5597,
            "success_rate": 96.69,
            "avg_ticket_success": 3009.4,
        }
        projection = simulate_success_rate(baseline, {"delta_success_rate_pct": 10.0})
        self.assertAlmostEqual(projection["requested_delta_success_rate_pct"], 10.0, places=2)
        self.assertAlmostEqual(projection["delta_success_rate_pct"], 3.31, places=2)
        self.assertAlmostEqual(projection["new_success_rate"], 100.0, places=2)
        # recovered_txns should never exceed failed txns headroom.
        self.assertLessEqual(projection["recovered_txns"], 5597 * (3.31 / 100.0) + 0.01)

    def test_planner_filters_irrelevant_knobs(self):
        spec = _normalize_spec(
            {
                "scenario_type": "SUCCESS_RATE_UPLIFT",
                "knobs": {
                    "delta_success_rate_pct": 10,
                    "discount_percentage": 10,
                    "price_reduction_percentage": 10,
                },
                "missing": [],
            },
            "what if success rate improves by 10%",
        )
        self.assertEqual(spec.scenario_type, "SUCCESS_RATE_UPLIFT")
        self.assertEqual(spec.knobs, {"delta_success_rate_pct": 10.0})

    def test_service_orchestration_without_live_llm(self):
        with patch(
            "app.intelligence.scenario_engine.service.plan_scenario",
            return_value=ScenarioSpec(
                scenario_type="REFUND_REDUCTION",
                knobs={"reduction_pct": 20.0},
                missing=[],
            ),
        ), patch(
            "app.intelligence.scenario_engine.service.narrate_scenario",
            return_value="Deterministic narrative output.",
        ):
            result = run_scenario(self.engine, self.MID, "What if refunds reduce by 20%?", window_days=30)

        self.assertEqual(result.projections["scenario_type"], "REFUND_REDUCTION")
        self.assertAlmostEqual(result.projections["saved_revenue"], 1400.0, places=2)
        self.assertTrue(result.assumptions)
        self.assertEqual(result.narrative, "Deterministic narrative output.")

    def test_sqlite_forces_deterministic_mode_even_when_experimental_enabled(self):
        with patch(
            "app.intelligence.scenario_engine.service.plan_scenario",
            return_value=ScenarioSpec(
                scenario_type="SUCCESS_RATE_UPLIFT",
                knobs={"delta_success_rate_pct": 2.0},
                missing=[],
            ),
        ), patch(
            "app.intelligence.scenario_engine.service.narrate_scenario",
            return_value="Deterministic narrative output.",
        ), patch(
            "app.intelligence.scenario_engine.service._experimental_reasoning",
            side_effect=RuntimeError("Should not be called for sqlite engine"),
        ):
            result = run_scenario(
                self.engine,
                self.MID,
                "What if success rate improves by 2%?",
                window_days=30,
                experimental=True,
            )

        self.assertEqual(result.projections.get("reasoning_mode"), "guided_deterministic")
        self.assertNotIn("_experimental", result.projections)


if __name__ == "__main__":
    unittest.main()

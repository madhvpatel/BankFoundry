import json
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

from app.data.ops import repository as ops_repository


class _FakeHTTPResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


class OpsRepositoryTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE settlements (
                        settlement_id TEXT,
                        merchant_id TEXT,
                        status TEXT,
                        expected_date TEXT,
                        settled_at TEXT,
                        amount_rupees REAL,
                        currency TEXT,
                        reference TEXT,
                        gross_amount REAL,
                        net_settlement_amount REAL,
                        mdr_deducted REAL,
                        gst_on_mdr REAL,
                        tds_deducted REAL,
                        chargeback_deductions REAL,
                        reserve_held REAL,
                        adjustment_amount REAL,
                        hold_reason TEXT,
                        payment_mode TEXT,
                        txn_count INTEGER,
                        refund_count INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE reconciliation_records (
                        merchant_id TEXT,
                        settlement_id TEXT,
                        status TEXT,
                        reason TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements VALUES
                    ('261', 'merchant_001', 'HELD', '2026-03-15', NULL, 24882, 'INR', 'utr_261',
                     25000, 24882, 100, 18, 0, 0, 0, 0, 'Risk review', 'UPI', 12, 0)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_records VALUES
                    ('merchant_001', '261', 'OPEN', 'Risk review'),
                    ('merchant_001', '261', 'PENDING', 'Manual verification')
                    """
                )
            )

    def test_create_case_seeds_tasks_and_supports_full_lifecycle(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="held_settlement",
            title="Held settlement review",
            summary="Settlement 261 remains held.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            source="proactive",
            source_ref="bg:merchant_001:operations:settlement_delay:2026-03-01:2026-03-10",
            evidence_ids=["settlement:261", "alert:held"],
            priority="high",
            tasks=[
                {"title": "Verify hold state", "description": "Confirm the settlement remains held.", "priority": "high", "metadata": {"step_id": "verify_hold"}},
                {"title": "Inspect deductions and hold reason", "description": "Review deductions.", "priority": "high", "metadata": {"step_id": "inspect_deductions"}},
                {"title": "Attach evidence", "description": "Attach the latest settlement evidence.", "priority": "high", "metadata": {"step_id": "attach_evidence"}},
                {"title": "Draft escalation", "description": "Prepare the escalation request.", "priority": "high", "metadata": {"step_id": "draft_escalation"}},
            ],
        )
        self.assertTrue(created["created"])

        detail = ops_repository.get_case_detail(self.engine, created["case_id"])
        self.assertIsNotNone(detail)
        self.assertEqual(detail["case"]["status"], "OPEN")
        self.assertEqual(len(detail["tasks"]), 4)
        self.assertEqual(detail["timeline"][0]["event_type"], "case_created")

        assignment = ops_repository.assign_case(
            self.engine,
            case_id=created["case_id"],
            owner="acquiring_ops",
            actor_id="operator_1",
            actor_role="acquiring_ops",
        )
        self.assertTrue(assignment["updated"])

        note = ops_repository.add_case_note(
            self.engine,
            case_id=created["case_id"],
            body="Hold reason validated against latest payout rows.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
        )
        self.assertTrue(note["updated"])

        approval = ops_repository.request_case_approval(
            self.engine,
            case_id=created["case_id"],
            action_type="SETTLEMENT_ESCALATION",
            payload_summary="Escalate settlement 261",
            payload={"case_id": created["case_id"]},
            actor_id="operator_1",
            actor_role="acquiring_ops",
        )
        self.assertTrue(approval["updated"])
        approval_id = approval["approval"]["approval_id"]

        decision = ops_repository.decide_approval(
            self.engine,
            approval_id=approval_id,
            decision="APPROVED",
            actor_id="ops_manager",
            actor_role="admin",
            notes="Approved for settlement desk follow-through.",
        )
        self.assertEqual(decision["connector_status"], "SUCCESS")
        self.assertIsNotNone(decision["receipt_ref"])
        self.assertIsNotNone(decision["connector_result"])

        resolved = ops_repository.resolve_case(
            self.engine,
            case_id=created["case_id"],
            actor_id="operator_1",
            actor_role="acquiring_ops",
            resolution_note="Escalation queued and case wrapped up.",
        )
        self.assertTrue(resolved["updated"])

        final_detail = ops_repository.get_case_detail(self.engine, created["case_id"])
        self.assertEqual(final_detail["case"]["status"], "RESOLVED")
        self.assertEqual(final_detail["case"]["approval_state"], "approved")
        self.assertEqual(len(final_detail["connector_runs"]), 1)
        self.assertEqual(final_detail["connector_runs"][0]["status"], "SUCCESS")

    def test_create_case_reuses_open_source_reference(self):
        first = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="settlement_shortfall_review",
            title="Settlement shortfall",
            summary="Net amount lower than expected.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            source="proactive",
            source_ref="bg:merchant_001:operations:shortfall:2026-03-01:2026-03-10",
        )
        second = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="settlement_shortfall_review",
            title="Settlement shortfall",
            summary="Net amount lower than expected.",
            actor_id="operator_2",
            actor_role="acquiring_ops",
            source="proactive",
            source_ref="bg:merchant_001:operations:shortfall:2026-03-01:2026-03-10",
        )

        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertTrue(second["reused"])
        self.assertEqual(first["case_id"], second["case_id"])

    def test_case_memory_persists_pinned_context(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="held_settlement",
            title="Held settlement review",
            summary="Settlement 261 remains held.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            evidence_ids=["settlement:261"],
        )

        memory = ops_repository.upsert_case_memory(
            self.engine,
            case_id=created["case_id"],
            memory={
                "pinned_entities": {
                    "merchant_id": "merchant_001",
                    "case_type": "held_settlement",
                    "settlement_id": "261",
                },
                "active_window": {
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-20",
                    "reason": "case_source_window",
                },
                "confirmed_evidence_ids": ["settlement:261", "merchant:merchant_001"],
                "latest_summary": {
                    "executive_summary": "Held settlement 261 remains open.",
                    "verification": "verified",
                },
                "latest_tool_calls": [{"tool_name": "get_settlement_detail", "verification": "verified"}],
            },
        )

        self.assertEqual(memory["pinned_entities"]["settlement_id"], "261")
        detail = ops_repository.get_case_detail(self.engine, created["case_id"])
        self.assertEqual(detail["memory"]["pinned_entities"]["settlement_id"], "261")
        self.assertEqual(detail["memory"]["active_window"]["start_date"], "2026-03-01")
        self.assertIn("settlement:261", detail["memory"]["confirmed_evidence_ids"])

    def test_list_cases_ignores_oversized_evidence_blob(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="held_settlement",
            title="Held settlement review",
            summary="Settlement 261 remains held.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            source="proactive",
            source_ref="bg:merchant_001:operations:payout_shortfall_261:2026-03-01:2026-03-20",
            evidence_ids=["settlement:261"],
        )

        oversized_blob = json.dumps(["[" + ("nested:" * 7000) + "]"])
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE ops_cases SET evidence_ids_json = :payload WHERE case_id = :case_id"),
                {"case_id": created["case_id"], "payload": oversized_blob},
            )

        queue = ops_repository.list_cases(self.engine, merchant_id="merchant_001", status="ACTIVE")
        detail = ops_repository.get_case_detail(self.engine, created["case_id"])

        self.assertEqual(len(queue["cases"]), 1)
        self.assertEqual(queue["cases"][0]["case_id"], created["case_id"])
        self.assertEqual(queue["cases"][0]["evidence_ids"], [])
        self.assertEqual(detail["case"]["evidence_ids"], [])

    def test_update_case_memory_context_tracks_operator_pins(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="held_settlement",
            title="Held settlement review",
            summary="Settlement 261 remains held.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            evidence_ids=["settlement:261"],
        )

        updated = ops_repository.update_case_memory_context(
            self.engine,
            case_id=created["case_id"],
            actor_id="operator_1",
            actor_role="acquiring_ops",
            settlement_id="261",
            start_date="2026-03-01",
            end_date="2026-03-20",
            evidence_ids=["settlement:261", "merchant:merchant_001"],
        )

        self.assertEqual(updated["pinned_entities"]["settlement_id"], "261")
        self.assertEqual(updated["active_window"]["reason"], "operator_pinned_window")
        detail = ops_repository.get_case_detail(self.engine, created["case_id"])
        self.assertEqual(detail["timeline"][-1]["event_type"], "memory_updated")
        self.assertEqual(detail["memory"]["active_window"]["start_date"], "2026-03-01")

    def test_queue_summary_and_order_reflect_sla_and_blocked_state(self):
        open_case = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="held_settlement",
            title="Held settlement review",
            summary="Settlement 261 remains held.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            priority="high",
        )
        blocked_case = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="reconciliation_mismatch",
            title="Reconciliation mismatch",
            summary="Gross and net do not reconcile.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            priority="critical",
        )
        awaiting_case = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="delayed_payout_exception",
            title="Delayed payout",
            summary="Payout still pending past expected date.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            priority="medium",
        )

        ops_repository.request_case_approval(
            self.engine,
            case_id=awaiting_case["case_id"],
            action_type="PAYOUT_DELAY_INTERVENTION",
            payload_summary="Escalate delayed payout",
            payload={"case_id": awaiting_case["case_id"]},
            actor_id="operator_1",
            actor_role="acquiring_ops",
        )
        approval = ops_repository.request_case_approval(
            self.engine,
            case_id=blocked_case["case_id"],
            action_type="RECONCILIATION_REVIEW",
            payload_summary="Review reconciliation mismatch",
            payload={"case_id": blocked_case["case_id"]},
            actor_id="operator_1",
            actor_role="acquiring_ops",
        )
        ops_repository.decide_approval(
            self.engine,
            approval_id=approval["approval"]["approval_id"],
            decision="REJECTED",
            actor_id="ops_manager",
            actor_role="admin",
            notes="Need supporting settlement evidence first.",
        )

        queue = ops_repository.list_cases(self.engine, merchant_id="merchant_001", lane="operations", status="ACTIVE")

        self.assertEqual(queue["queue_summary"]["total"], 3)
        self.assertEqual(queue["queue_summary"]["blocked"], 1)
        self.assertEqual(queue["queue_summary"]["awaiting_approval"], 1)
        self.assertEqual(queue["queue_summary"]["unassigned"], 3)
        self.assertEqual(queue["cases"][0]["status"], "BLOCKED")
        self.assertEqual(queue["cases"][0]["waiting_on"], "Need supporting settlement evidence first.")
        self.assertEqual(queue["cases"][1]["status"], "AWAITING_APPROVAL")
        self.assertEqual(queue["cases"][1]["waiting_on"], "approval_decision")
        self.assertEqual(queue["cases"][2]["status"], "OPEN")

        open_detail = ops_repository.get_case_detail(self.engine, open_case["case_id"])
        self.assertEqual(open_detail["case"]["waiting_on"], "assignment")

    def test_connector_failure_blocks_case_for_follow_up(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="held_settlement",
            title="Held settlement review",
            summary="Settlement 261 remains held.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            priority="high",
        )
        approval = ops_repository.request_case_approval(
            self.engine,
            case_id=created["case_id"],
            action_type="SETTLEMENT_ESCALATION",
            payload_summary="Escalate settlement 261",
            payload={"case_id": created["case_id"]},
            actor_id="operator_1",
            actor_role="acquiring_ops",
        )

        with patch("app.data.connectors.settlement_ops.Config.SETTLEMENT_OPS_CONNECTOR_MODE", "real"):
            decision = ops_repository.decide_approval(
                self.engine,
                approval_id=approval["approval"]["approval_id"],
                decision="APPROVED",
                actor_id="ops_manager",
                actor_role="admin",
                notes="Approved for downstream execution.",
            )

        self.assertEqual(decision["connector_status"], "FAILED")
        detail = ops_repository.get_case_detail(self.engine, created["case_id"])
        self.assertEqual(detail["case"]["status"], "BLOCKED")
        self.assertEqual(detail["case"]["attention_level"], "critical")
        self.assertEqual(detail["case"]["connector_status"], "FAILED")
        self.assertEqual(detail["case"]["waiting_on"], "connector_failed")
        self.assertIn("SETTLEMENT_OPS_CONNECTOR_BASE_URL", detail["case"]["blocked_reason"])

    def test_http_connector_mode_dispatches_real_request_contract(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="merchant_001",
            lane="operations",
            case_type="held_settlement",
            title="Held settlement review",
            summary="Settlement 261 remains held.",
            actor_id="operator_1",
            actor_role="acquiring_ops",
            priority="high",
        )
        approval = ops_repository.request_case_approval(
            self.engine,
            case_id=created["case_id"],
            action_type="SETTLEMENT_ESCALATION",
            payload_summary="Escalate settlement 261",
            payload={"case_id": created["case_id"], "merchant_id": "merchant_001", "settlement_id": "261"},
            actor_id="operator_1",
            actor_role="acquiring_ops",
        )

        with patch("app.data.connectors.settlement_ops.httpx.post") as mock_post, \
             patch("app.data.connectors.settlement_ops.Config.SETTLEMENT_OPS_CONNECTOR_MODE", "http"), \
             patch("app.data.connectors.settlement_ops.Config.SETTLEMENT_OPS_CONNECTOR_BASE_URL", "https://bank.example"), \
             patch("app.data.connectors.settlement_ops.Config.SETTLEMENT_OPS_CONNECTOR_ENDPOINT", "/v1/settlements/interventions"), \
             patch("app.data.connectors.settlement_ops.Config.SETTLEMENT_OPS_CONNECTOR_AUTH_MODE", "bearer"), \
             patch("app.data.connectors.settlement_ops.Config.SETTLEMENT_OPS_CONNECTOR_BEARER_TOKEN", "test-token"), \
             patch("app.data.connectors.settlement_ops.Config.SETTLEMENT_OPS_CONNECTOR_VERIFY_SSL", False), \
             patch("app.data.connectors.settlement_ops.Config.SETTLEMENT_OPS_CONNECTOR_TIMEOUT_SECONDS", 15.0):
            mock_post.return_value = _FakeHTTPResponse(
                202,
                {
                    "message": "accepted",
                    "receipt_ref": "receipt_bank_001",
                    "external_ref": "bank_req_001",
                },
            )
            decision = ops_repository.decide_approval(
                self.engine,
                approval_id=approval["approval"]["approval_id"],
                decision="APPROVED",
                actor_id="ops_manager",
                actor_role="admin",
                notes="Approved for downstream execution.",
            )

        self.assertEqual(decision["connector_status"], "QUEUED")
        self.assertEqual(decision["connector_result"]["connector_mode"], "http")
        self.assertEqual(decision["connector_result"]["receipt_ref"], "receipt_bank_001")
        self.assertEqual(decision["connector_result"]["external_ref"], "bank_req_001")
        detail = ops_repository.get_case_detail(self.engine, created["case_id"])
        self.assertEqual(detail["case"]["status"], "IN_PROGRESS")
        self.assertEqual(detail["connector_runs"][0]["status"], "QUEUED")
        self.assertEqual(detail["connector_runs"][0]["endpoint_url"], "https://bank.example/v1/settlements/interventions")
        self.assertEqual(detail["connector_runs"][0]["http_status_code"], 202)
        self.assertTrue(detail["connector_runs"][0]["idempotency_key"].startswith("bank-foundry:"))

        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://bank.example/v1/settlements/interventions")
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer test-token")
        self.assertIn("Idempotency-Key", kwargs["headers"])
        self.assertEqual(kwargs["json"]["request_id"], approval["approval"]["approval_id"])
        self.assertEqual(kwargs["json"]["payload"]["settlement_id"], "261")
        self.assertEqual(kwargs["json"]["payload"]["merchant_id"], "merchant_001")
        self.assertEqual(kwargs["json"]["payload"]["case_context"]["case_type"], "held_settlement")
        self.assertEqual(kwargs["json"]["payload"]["settlement_context"]["settlement"]["settlement_id"], "261")
        self.assertEqual(kwargs["json"]["payload"]["settlement_context"]["settlement"]["status"], "HELD")
        self.assertEqual(kwargs["json"]["payload"]["settlement_context"]["reconciliation"][0]["reason"], "Risk review")
        self.assertEqual(kwargs["json"]["payload"]["settlement_context"]["deduction_breakdown"]["difference_amount"], 118.0)
        self.assertEqual(kwargs["json"]["payload"]["settlement_context"]["payout_delay"]["hold_reason"], "Risk review")


if __name__ == "__main__":
    unittest.main()

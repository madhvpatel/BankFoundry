import asyncio
import json
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from sqlalchemy import create_engine, text

from app.api.server import (
    OpsCaseCreateRequest,
    OpsCaseCopilotRequest,
    OpsCaseMemoryUpdateRequest,
    OpsCasePromoteRequest,
    OpsCaseNoteRequest,
    OpsCaseResolveRequest,
    OpsCaseApprovalRequest,
    OpsApprovalDecisionRequest,
    request_ops_case_approval,
    decide_ops_approval,
    create_ops_case,
    get_ops_case_detail,
    get_ops_case_copilot,
    get_ops_queue,
    promote_ops_case,
    add_ops_case_note,
    resolve_ops_case,
    update_ops_case_memory,
)
from app.data.proactive import ensure_proactive_cards_schema


class OpsApiServerTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:")

    def test_ops_case_endpoints_create_and_resolve_case(self):
        with patch("app.api.server.engine", self.engine):
            created = asyncio.run(
                create_ops_case(
                    OpsCaseCreateRequest(
                        merchant_id="merchant_001",
                        lane="operations",
                        role="acquiring_ops",
                        case_type="held_settlement",
                        title="Held settlement 261",
                        summary="Settlement 261 is still held.",
                        priority="high",
                        evidence_ids=["settlement:261"],
                    )
                )
            )

            case_id = created["work_item"]["case_id"]
            detail = asyncio.run(get_ops_case_detail(case_id, merchant_id="merchant_001", role="acquiring_ops"))
            self.assertEqual(detail["work_item"]["case_id"], case_id)
            self.assertEqual(len(detail["runbook_steps"]), 4)

            noted = asyncio.run(
                add_ops_case_note(
                    case_id,
                    OpsCaseNoteRequest(merchant_id="merchant_001", role="acquiring_ops", body="Investigating the hold reason."),
                )
            )
            self.assertEqual(noted["timeline"][-1]["event_type"], "note")

            memory_updated = asyncio.run(
                update_ops_case_memory(
                    case_id,
                    OpsCaseMemoryUpdateRequest(
                        merchant_id="merchant_001",
                        role="acquiring_ops",
                        settlement_id="261",
                        start_date="2026-03-01",
                        end_date="2026-03-20",
                        evidence_ids=["settlement:261", "alert:held"],
                    ),
                )
            )
            self.assertEqual(memory_updated["memory"]["pinned_entities"]["settlement_id"], "261")
            self.assertEqual(memory_updated["memory"]["active_window"]["reason"], "operator_pinned_window")

            approval = asyncio.run(
                request_ops_case_approval(
                    case_id,
                    OpsCaseApprovalRequest(
                        merchant_id="merchant_001",
                        role="acquiring_ops",
                        action_type="SETTLEMENT_ESCALATION",
                        payload_summary="Escalate settlement 261",
                        payload={"case_id": case_id},
                    ),
                )
            )
            self.assertEqual(approval["approval_state"]["status"], "pending")

            approval_id = approval["approvals"][0]["approval_id"]
            decided = asyncio.run(
                decide_ops_approval(
                    approval_id,
                    OpsApprovalDecisionRequest(
                        merchant_id="merchant_001",
                        lane="operations",
                        role="admin",
                        decision="APPROVED",
                    ),
                )
            )
            self.assertEqual(decided["connector_status"], "SUCCESS")
            self.assertEqual(decided["connector_runs"][0]["status"], "SUCCESS")

            resolved = asyncio.run(
                resolve_ops_case(
                    case_id,
                    OpsCaseResolveRequest(merchant_id="merchant_001", role="acquiring_ops", resolution_note="Completed follow-up."),
                )
            )
            self.assertEqual(resolved["work_item"]["status"], "RESOLVED")

            queue = asyncio.run(get_ops_queue(merchant_id="merchant_001", lane="operations", role="acquiring_ops"))
            self.assertEqual(queue["queue_summary"]["resolved"], 1)

    def test_promote_proactive_card_into_ops_case(self):
        ensure_proactive_cards_schema(self.engine)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO proactive_cards (
                        dedupe_key, merchant_id, lane, verification_status, evidence_ids, action_preview_token,
                        payload_json, window_from, window_to, card_state, card_notes, converted_action_id, updated_at, created_at
                    ) VALUES (
                        :dedupe_key, :merchant_id, :lane, :verification_status, :evidence_ids, NULL,
                        :payload_json, :window_from, :window_to, 'NEW', NULL, NULL, :updated_at, :created_at
                    )
                    """
                ),
                {
                    "dedupe_key": "bg:merchant_001:operations:settlement_delay:2026-03-01:2026-03-10",
                    "merchant_id": "merchant_001",
                    "lane": "operations",
                    "verification_status": "Verified - held settlement signal",
                    "evidence_ids": json.dumps(["settlement:261", "alert:held"]),
                    "payload_json": json.dumps(
                        {
                            "title": "Held settlement needs review",
                            "body": "Settlement 261 remains held beyond the expected date.",
                            "lane": "operations",
                            "source": "background_monitor",
                        }
                    ),
                    "window_from": "2026-03-01",
                    "window_to": "2026-03-10",
                    "updated_at": "2026-03-10T10:00:00+00:00",
                    "created_at": "2026-03-10T10:00:00+00:00",
                },
            )

        with patch("app.api.server.engine", self.engine):
            payload = asyncio.run(
                promote_ops_case(
                    OpsCasePromoteRequest(
                        merchant_id="merchant_001",
                        lane="operations",
                        role="acquiring_ops",
                        source_type="proactive_card",
                        source_ref="bg:merchant_001:operations:settlement_delay:2026-03-01:2026-03-10",
                    )
                )
            )

        self.assertEqual(payload["work_item"]["source"], "proactive")
        self.assertEqual(payload["work_item"]["title"], "Held settlement needs review")
        self.assertIn("settlement:261", payload["work_item"]["evidence_ids"])

    def test_lane_authorization_blocks_unsupported_role(self):
        with patch("app.api.server.engine", self.engine):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(get_ops_queue(merchant_id="merchant_001", lane="risk", role="support"))

        self.assertEqual(raised.exception.status_code, 403)

    def test_ops_case_copilot_returns_mcp_backed_summary(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchants (
                        mid TEXT,
                        merchant_trade_name TEXT,
                        nature_of_business TEXT,
                        business_city TEXT,
                        merchant_risk_category TEXT,
                        merchant_status TEXT,
                        annual_turnover REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE transaction_features (
                        transaction_fact_id TEXT,
                        merchant_id TEXT,
                        terminal_id TEXT,
                        p_date TEXT,
                        initiated_at TEXT,
                        payment_mode TEXT,
                        status TEXT,
                        response_code TEXT,
                        amount_rupees REAL,
                        hour_of_day INTEGER
                    )
                    """
                )
            )
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
                    INSERT INTO merchants VALUES
                    ('merchant_001', 'Demo Store', 'Retail', 'Mumbai', 'LOW', 'ACTIVE', 1000000)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features VALUES
                    ('tx_1', 'merchant_001', 'T1', '2026-03-10', '2026-03-10T10:00:00', 'UPI', 'SUCCESS', '00', 1000, 10),
                    ('tx_2', 'merchant_001', 'T1', '2026-03-11', '2026-03-11T11:00:00', 'CARD', 'FAILED', '91', 500, 11)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements VALUES
                    ('261', 'merchant_001', 'HELD', '2026-03-15', NULL, 24882, 'INR', 'utr_261', 25000, 24882, 100, 18, 0, 0, 0, 0, 'Risk review', 'UPI', 12, 0)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_records VALUES
                    ('merchant_001', '261', 'OPEN', 'Risk review')
                    """
                )
            )

        with patch("app.api.server.engine", self.engine):
            created = asyncio.run(
                create_ops_case(
                    OpsCaseCreateRequest(
                        merchant_id="merchant_001",
                        lane="operations",
                        role="acquiring_ops",
                        case_type="held_settlement",
                        title="Held settlement 261",
                        summary="Settlement 261 is still held.",
                        priority="high",
                        evidence_ids=["settlement:261"],
                    )
                )
            )

            case_id = created["work_item"]["case_id"]
            copilot = asyncio.run(
                get_ops_case_copilot(
                    case_id,
                    OpsCaseCopilotRequest(
                        merchant_id="merchant_001",
                        role="acquiring_ops",
                    ),
                )
            )
            detail = asyncio.run(get_ops_case_detail(case_id, merchant_id="merchant_001", role="acquiring_ops"))

        self.assertEqual(copilot["copilot"]["verification"], "verified")
        self.assertIn("Held settlement 261 is currently open", copilot["copilot"]["summary"])
        self.assertEqual(copilot["copilot"]["tool_calls"][0]["tool_name"], "get_merchant_profile")
        self.assertEqual(copilot["copilot"]["agents"][0]["name"], "settlement_case_summary_agent")
        self.assertEqual(copilot["copilot"]["drafts"]["operator_note"]["status"], "ready")
        self.assertEqual(copilot["copilot"]["drafts"]["approval_request"]["status"], "ready")
        self.assertIn("settlement:261", copilot["copilot"]["evidence_ids"])
        self.assertEqual(copilot["memory"]["pinned_entities"]["settlement_id"], "261")
        self.assertEqual(copilot["memory"]["active_window"]["reason"], "opened_at_30d_window")
        self.assertEqual(detail["memory"]["pinned_entities"]["settlement_id"], "261")
        self.assertIn("settlement:261", detail["memory"]["confirmed_evidence_ids"])

    def test_ops_case_memory_update_clears_pinned_context(self):
        with patch("app.api.server.engine", self.engine):
            created = asyncio.run(
                create_ops_case(
                    OpsCaseCreateRequest(
                        merchant_id="merchant_001",
                        lane="operations",
                        role="acquiring_ops",
                        case_type="held_settlement",
                        title="Held settlement 261",
                        summary="Settlement 261 is still held.",
                        priority="high",
                        evidence_ids=["settlement:261"],
                    )
                )
            )
            case_id = created["work_item"]["case_id"]
            updated = asyncio.run(
                update_ops_case_memory(
                    case_id,
                    OpsCaseMemoryUpdateRequest(
                        merchant_id="merchant_001",
                        role="acquiring_ops",
                        settlement_id="261",
                        start_date="2026-03-01",
                        end_date="2026-03-20",
                        evidence_ids=["settlement:261"],
                    ),
                )
            )
            self.assertEqual(updated["memory"]["pinned_entities"]["settlement_id"], "261")

            cleared = asyncio.run(
                update_ops_case_memory(
                    case_id,
                    OpsCaseMemoryUpdateRequest(
                        merchant_id="merchant_001",
                        role="acquiring_ops",
                        clear_pinned_context=True,
                        clear_window=True,
                        clear_evidence=True,
                    ),
                )
            )

        self.assertIsNone(cleared["memory"]["pinned_entities"].get("settlement_id"))
        self.assertEqual(cleared["memory"]["active_window"], {})
        self.assertEqual(cleared["memory"]["confirmed_evidence_ids"], [])
        self.assertEqual(cleared["timeline"][-1]["event_type"], "memory_updated")

    def test_ops_case_copilot_routes_chargeback_case_to_specialist_agent(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchants (
                        mid TEXT,
                        merchant_trade_name TEXT,
                        nature_of_business TEXT,
                        business_city TEXT,
                        merchant_risk_category TEXT,
                        merchant_status TEXT,
                        annual_turnover REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE chargebacks (
                        chargeback_id TEXT,
                        merchant_id TEXT,
                        status TEXT,
                        opened_at TEXT,
                        due_by TEXT,
                        amount_rupees REAL,
                        reason_code TEXT,
                        network TEXT,
                        tx_id TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchants VALUES
                    ('merchant_001', 'Demo Store', 'Retail', 'Mumbai', 'LOW', 'ACTIVE', 1000000)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO chargebacks VALUES
                    ('cb_1', 'merchant_001', 'OPEN', '2026-03-12T09:00:00', '2026-03-18', 2500, '4837', 'VISA', 'tx_1')
                    """
                )
            )

        with patch("app.api.server.engine", self.engine):
            created = asyncio.run(
                create_ops_case(
                    OpsCaseCreateRequest(
                        merchant_id="merchant_001",
                        lane="operations",
                        role="acquiring_ops",
                        case_type="chargeback_review",
                        title="Chargeback cb_1 review",
                        summary="Chargeback cb_1 remains open.",
                        priority="high",
                        evidence_ids=["chargeback:cb_1"],
                    )
                )
            )
            case_id = created["work_item"]["case_id"]
            copilot = asyncio.run(
                get_ops_case_copilot(
                    case_id,
                    OpsCaseCopilotRequest(
                        merchant_id="merchant_001",
                        role="acquiring_ops",
                    ),
                )
            )

        self.assertEqual(copilot["copilot"]["agents"][0]["name"], "chargeback_review_agent")
        self.assertEqual(copilot["copilot"]["drafts"]["approval_request"]["action_type"], "CHARGEBACK_REVIEW")
        self.assertEqual(copilot["memory"]["pinned_entities"]["chargeback_id"], "cb_1")

    def test_ops_case_copilot_routes_risk_case_to_specialist_agent(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchants (
                        mid TEXT,
                        merchant_trade_name TEXT,
                        nature_of_business TEXT,
                        business_city TEXT,
                        merchant_risk_category TEXT,
                        merchant_status TEXT,
                        annual_turnover REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_risk_profiles (
                        merchant_id TEXT,
                        risk_score REAL,
                        risk_band TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_kyc_documents (
                        merchant_id TEXT,
                        status TEXT,
                        expiry_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchants VALUES
                    ('merchant_001', 'Demo Store', 'Retail', 'Mumbai', 'LOW', 'ACTIVE', 1000000)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_risk_profiles VALUES
                    ('merchant_001', 0.12, 'LOW', '2026-03-28T10:00:00')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_kyc_documents VALUES
                    ('merchant_001', 'APPROVED', '2026-06-01')
                    """
                )
            )

        with patch("app.api.server.engine", self.engine):
            created = asyncio.run(
                create_ops_case(
                    OpsCaseCreateRequest(
                        merchant_id="merchant_001",
                        lane="risk",
                        role="risk_fraud",
                        case_type="risk_triage",
                        title="Merchant risk review",
                        summary="Risk profile requires review.",
                        priority="high",
                    )
                )
            )
            case_id = created["work_item"]["case_id"]
            copilot = asyncio.run(
                get_ops_case_copilot(
                    case_id,
                    OpsCaseCopilotRequest(
                        merchant_id="merchant_001",
                        role="risk_fraud",
                    ),
                )
            )

        self.assertEqual(copilot["copilot"]["agents"][0]["name"], "risk_triage_agent")
        self.assertEqual(copilot["copilot"]["drafts"]["operator_note"]["status"], "ready")

    def test_ops_case_copilot_routes_incident_case_to_specialist_agent(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchants (
                        mid TEXT,
                        merchant_trade_name TEXT,
                        nature_of_business TEXT,
                        business_city TEXT,
                        merchant_risk_category TEXT,
                        merchant_status TEXT,
                        annual_turnover REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE proactive_cards (
                        dedupe_key TEXT PRIMARY KEY,
                        merchant_id TEXT NOT NULL,
                        lane TEXT NOT NULL,
                        verification_status TEXT NOT NULL,
                        evidence_ids TEXT NOT NULL,
                        action_preview_token TEXT NULL,
                        payload_json TEXT NOT NULL,
                        window_from TEXT NOT NULL,
                        window_to TEXT NOT NULL,
                        card_state TEXT DEFAULT 'NEW',
                        card_notes TEXT NULL,
                        converted_action_id TEXT NULL,
                        linked_case_id TEXT NULL,
                        updated_at TEXT NULL,
                        created_at TEXT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE proactive_refresh_schedule (
                        merchant_id TEXT NOT NULL,
                        window_days INTEGER NOT NULL,
                        status TEXT DEFAULT 'IDLE',
                        last_refresh_at TEXT NULL,
                        next_refresh_at TEXT NULL,
                        last_window_from TEXT NULL,
                        last_window_to TEXT NULL,
                        last_generated_count INTEGER DEFAULT 0,
                        last_inserted_count INTEGER DEFAULT 0,
                        updated_at TEXT NULL,
                        PRIMARY KEY (merchant_id, window_days)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchants VALUES
                    ('merchant_001', 'Demo Store', 'Retail', 'Mumbai', 'LOW', 'ACTIVE', 1000000)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO proactive_refresh_schedule VALUES
                    ('merchant_001', 30, 'REFRESHED', '2026-03-29T10:00:00+00:00', '2026-03-30T10:00:00+00:00', '2026-03-01', '2026-03-30', 4, 2, '2026-03-29T10:00:00+00:00')
                    """
                )
            )

        with patch("app.api.server.engine", self.engine):
            created = asyncio.run(
                create_ops_case(
                    OpsCaseCreateRequest(
                        merchant_id="merchant_001",
                        lane="operations",
                        role="acquiring_ops",
                        case_type="incident_response",
                        title="Ops incident review",
                        summary="Background refresh requires review.",
                        priority="high",
                    )
                )
            )
            case_id = created["work_item"]["case_id"]
            copilot = asyncio.run(
                get_ops_case_copilot(
                    case_id,
                    OpsCaseCopilotRequest(
                        merchant_id="merchant_001",
                        role="acquiring_ops",
                    ),
                )
            )

        self.assertEqual(copilot["copilot"]["agents"][0]["name"], "incident_response_agent")
        self.assertEqual(copilot["copilot"]["drafts"]["operator_note"]["status"], "ready")


if __name__ == "__main__":
    unittest.main()

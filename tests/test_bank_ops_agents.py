import unittest

from sqlalchemy import create_engine, text

from app.agent.bank_ops_agents import build_bank_ops_case_copilot_summary
from app.data.ops import repository as ops_repository
from app.ontology.ops import runbook_for_case_type
from tests.bank_foundry_eval_harness import assert_agent_summary_contract, assert_agent_verification_downgrade_contract


class BankOpsAgentsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS merchants (
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
                    CREATE TABLE IF NOT EXISTS settlements (
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
                    CREATE TABLE IF NOT EXISTS merchant_risk_profiles (
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
                    CREATE TABLE IF NOT EXISTS merchant_kyc_documents (
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
                    CREATE TABLE IF NOT EXISTS proactive_cards (
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
                    CREATE TABLE IF NOT EXISTS proactive_refresh_schedule (
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
                    CREATE TABLE IF NOT EXISTS transaction_features (
                        transaction_fact_id TEXT,
                        merchant_id TEXT,
                        terminal_id TEXT,
                        source_system TEXT,
                        source_txn_id TEXT,
                        p_date TEXT,
                        initiated_at TEXT,
                        completed_at TEXT,
                        payment_mode TEXT,
                        status TEXT,
                        response_code TEXT,
                        response_desc TEXT,
                        amount_rupees REAL,
                        hour_of_day INTEGER,
                        card_network TEXT,
                        device_type TEXT,
                        os_name TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS terminals (
                        tid TEXT,
                        mid TEXT,
                        terminal_model TEXT,
                        terminal_status TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS terminal_health_snapshots (
                        tid TEXT,
                        captured_at TEXT,
                        low_network_strength INTEGER,
                        battery_status REAL,
                        quick_battery_drainage INTEGER,
                        latitude_longitude_deviation INTEGER,
                        printer_status TEXT,
                        ram_rom_utilization REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS reconciliation_records (
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
                    CREATE TABLE IF NOT EXISTS chargebacks (
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
                    CREATE TABLE IF NOT EXISTS refunds (
                        refund_id TEXT,
                        merchant_id TEXT,
                        status TEXT,
                        created_at TEXT,
                        amount_rupees REAL,
                        tx_id TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchants VALUES
                    ('m_001', 'Demo Store', 'Retail', 'Mumbai', 'LOW', 'ACTIVE', 1000000)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_risk_profiles VALUES
                    ('m_001', 0.12, 'LOW', '2026-03-28T10:00:00')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_kyc_documents VALUES
                    ('m_001', 'APPROVED', '2026-06-01')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements VALUES
                    ('261', 'm_001', 'HELD', '2026-03-15', NULL, 24882, 'INR', 'utr_261', 25000, 24882, 100, 18, 0, 0, 0, 0, 'Risk review', 'UPI', 12, 0),
                    ('263', 'm_001', 'PROCESSED', '2000-01-01', NULL, 125000, 'INR', 'utr_263', 125000, 125000, 0, 0, 0, 0, 0, 0, 'Partner queue', 'CARD', 50, 2)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features (
                        transaction_fact_id, merchant_id, terminal_id, source_system, source_txn_id,
                        p_date, initiated_at, completed_at, payment_mode, status, response_code,
                        response_desc, amount_rupees, hour_of_day, card_network, device_type, os_name
                    ) VALUES
                    ('tx_1', 'm_001', 'T1', 'pg', 'src_tx_1', '2026-03-10', '2026-03-10T10:00:00', '2026-03-10T10:01:00', 'UPI', 'SUCCESS', '00', 'Approved', 1000, 10, 'RUPAY', 'SOUND_BOX', 'LINUX'),
                    ('tx_2', 'm_001', 'T1', 'pg', 'src_tx_2', '2026-03-11', '2026-03-11T11:00:00', '2026-03-11T11:02:00', 'CARD', 'FAILED', '91', 'Issuer or switch inoperative', 500, 11, 'VISA', 'POS', 'LINUX'),
                    ('tx_3', 'm_001', 'T2', 'pg', 'src_tx_3', '2026-03-12', '2026-03-12T12:00:00', '2026-03-12T12:01:00', 'CARD', 'SUCCESS', '00', 'Approved', 1500, 12, 'MASTERCARD', 'POS', 'LINUX'),
                    ('tx_4', 'm_001', 'T2', 'pg', 'src_tx_4', '2026-03-12', '2026-03-12T12:30:00', '2026-03-12T12:33:00', 'CARD', 'FAILED', '91', 'Issuer or switch inoperative', 700, 12, 'VISA', 'POS', 'LINUX')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO terminals VALUES
                    ('T1', 'm_001', 'Soundbox Pro', 'ACTIVE'),
                    ('T2', 'm_001', 'Counter POS', 'ACTIVE')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO terminal_health_snapshots VALUES
                    ('T1', '2026-03-11T10:30:00', 1, 24.0, 1, 0, 'OK', 61.0),
                    ('T1', '2026-03-11T11:30:00', 1, 22.0, 1, 0, 'OK', 63.0),
                    ('T2', '2026-03-11T10:30:00', 0, 82.0, 0, 0, 'OK', 40.0)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_records VALUES
                    ('m_001', '261', 'OPEN', 'Risk review'),
                    ('m_001', '261', 'PENDING', 'Manual verification'),
                    ('m_001', '263', 'OPEN', 'Partner queue')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO chargebacks VALUES
                    ('cb_1', 'm_001', 'OPEN', '2026-03-12T09:00:00', '2026-03-18', 2500, '4837', 'VISA', 'tx_1')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO refunds VALUES
                    ('rf_1', 'm_001', 'PROCESSED', '2026-03-16T10:00:00', 1200, 'tx_2')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO proactive_refresh_schedule VALUES
                    ('m_001', 30, 'REFRESHED', '2026-03-29T10:00:00+00:00', '2026-03-30T10:00:00+00:00', '2026-03-01', '2026-03-30', 4, 2, '2026-03-29T10:00:00+00:00')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO proactive_cards VALUES
                    ('bg:m_001:ops:settlement_delay', 'm_001', 'operations', 'verified', '["settlement:261"]', NULL, '{"title":"Held settlement needs review"}', '2026-03-01', '2026-03-30', 'NEW', NULL, NULL, NULL, '2026-03-29T10:00:00+00:00', '2026-03-29T10:00:00+00:00')
                    """
                )
            )

    def _render_case_detail(self, case_id: str) -> dict[str, object]:
        detail = ops_repository.get_case_detail(self.engine, case_id)
        self.assertIsNotNone(detail)
        case = detail["case"]
        runbook = runbook_for_case_type(str(case.get("case_type") or "manual_ops_review"))
        approvals = detail.get("approvals") if isinstance(detail.get("approvals"), list) else []
        approval_state = approvals[0] if approvals else {"status": str(case.get("approval_state") or "not_requested")}
        return {
            "work_item": case,
            "approval_state": approval_state,
            "runbook_steps": [
                {
                    "step_id": step.step_id,
                    "title": step.title,
                    "description": step.description,
                    "status": "OPEN",
                }
                for step in runbook.steps
            ],
            "memory": detail.get("memory") or {},
            "timeline": detail.get("timeline") or [],
            "tasks": detail.get("tasks") or [],
            "approvals": approvals,
        }

    def test_settlement_case_router_returns_specialized_summary_and_drafts(self):
        result = build_bank_ops_case_copilot_summary(
            self.engine,
            {
                "work_item": {
                    "case_id": "case_123",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "case_type": "held_settlement",
                    "title": "Held settlement 261",
                    "summary": "Settlement 261 remains held beyond the expected date.",
                    "status": "OPEN",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": ["settlement:261"],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [
                    {
                        "step_id": "verify_hold",
                        "title": "Verify hold state",
                        "description": "Confirm the settlement remains held and capture the latest status.",
                        "status": "OPEN",
                    }
                ],
            },
        )

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["case_context"]["settlement_id"], "261")
        self.assertEqual(result["agents"][0]["name"], "settlement_case_summary_agent")
        self.assertIn("Settlement 261 is HELD", result["answer_sections"]["key_findings"][1])
        self.assertEqual(result["drafts"]["operator_note"]["status"], "ready")
        self.assertEqual(result["drafts"]["approval_request"]["action_type"], "SETTLEMENT_ESCALATION")
        self.assertTrue(result["drafts"]["approval_request"]["approval_required"])
        self.assertEqual(result["drafts"]["approval_request"]["dispatch_readiness"], "approval_required")
        self.assertIn("settlement:261", result["evidence_ids"])

    def test_settlement_case_router_uses_persisted_memory_when_case_evidence_is_sparse(self):
        result = build_bank_ops_case_copilot_summary(
            self.engine,
            {
                "work_item": {
                    "case_id": "case_456",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "case_type": "held_settlement",
                    "title": "Held settlement review",
                    "summary": "Settlement issue still needs review.",
                    "status": "IN_PROGRESS",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": [],
                },
                "memory": {
                    "pinned_entities": {
                        "merchant_id": "m_001",
                        "case_type": "held_settlement",
                        "settlement_id": "261",
                    },
                    "active_window": {
                        "start_date": "2026-03-01",
                        "end_date": "2026-03-20",
                        "reason": "case_memory_window",
                    },
                    "confirmed_evidence_ids": ["settlement:261"],
                    "latest_summary": {},
                    "latest_tool_calls": [],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [],
            },
        )

        self.assertEqual(result["case_context"]["settlement_id"], "261")
        self.assertEqual(result["window"]["reason"], "case_memory_window")
        self.assertIn("settlement:261", result["memory_snapshot"]["confirmed_evidence_ids"])

    def test_reconciliation_case_routes_to_specialist_agent(self):
        result = build_bank_ops_case_copilot_summary(
            self.engine,
            {
                "work_item": {
                    "case_id": "case_789",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "case_type": "reconciliation_mismatch",
                    "title": "Settlement 261 reconciliation mismatch",
                    "summary": "Gross and net amounts do not fully reconcile.",
                    "status": "OPEN",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": ["settlement:261"],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [],
            },
        )

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "reconciliation_investigation_agent")
        self.assertEqual(result["drafts"]["approval_request"]["action_type"], "RECONCILIATION_REVIEW")
        self.assertIn("payout delta", " ".join(result["answer_sections"]["key_findings"]).lower())
        self.assertIn("reconciliation shows", " ".join(result["answer_sections"]["key_findings"]).lower())
        self.assertIn("get_reconciliation_breaks", [item["tool_name"] for item in result["tool_calls"]])
        self.assertIn("list_settlements", [item["tool_name"] for item in result["tool_calls"]])

    def test_delayed_payout_case_routes_to_specialist_agent(self):
        result = build_bank_ops_case_copilot_summary(
            self.engine,
            {
                "work_item": {
                    "case_id": "case_990",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "case_type": "delayed_payout_exception",
                    "title": "Delayed payout for settlement 263",
                    "summary": "Settlement 263 is still not settled.",
                    "status": "IN_PROGRESS",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": ["settlement:263"],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [],
            },
        )

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "delayed_payout_agent")
        self.assertEqual(result["drafts"]["approval_request"]["action_type"], "PAYOUT_DELAY_INTERVENTION")
        self.assertIn("past the expected date", " ".join(result["answer_sections"]["key_findings"]).lower())
        self.assertIn("partner queue", " ".join(result["answer_sections"]["key_findings"]).lower())

    def test_delayed_payout_case_does_not_infer_fake_settlement_id_from_settlement_delay_text(self):
        result = build_bank_ops_case_copilot_summary(
            self.engine,
            {
                "work_item": {
                    "case_id": "case_delay_text",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "case_type": "delayed_payout_exception",
                    "title": "Settlement delay review",
                    "summary": "Settlement delay needs review before escalation.",
                    "status": "OPEN",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": [],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [],
            },
        )

        self.assertEqual(result["agents"][0]["name"], "delayed_payout_agent")
        self.assertIsNone(result["case_context"]["settlement_id"])
        self.assertIn(
            "settlement id should be pinned",
            " ".join(result["answer_sections"]["caveats"]).lower(),
        )
        self.assertEqual([item["tool_name"] for item in result["tool_calls"]], ["get_merchant_profile"])

    def test_chargeback_case_routes_to_specialist_agent_and_mcp_drafts(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="chargeback_review",
            title="Chargeback cb_1 review",
            summary="Chargeback cb_1 remains open.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["chargeback:cb_1"],
            tasks=[{"title": "Verify chargeback", "description": "Confirm due date and response package.", "priority": "high"}],
        )
        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "chargeback_review_agent")
        self.assertEqual(result["drafts"]["operator_note"]["status"], "ready")
        self.assertEqual(result["drafts"]["approval_request"]["action_type"], "CHARGEBACK_REVIEW")
        self.assertEqual(result["drafts"]["merchant_update"]["status"], "ready")
        self.assertEqual(result["memory_snapshot"]["pinned_entities"]["chargeback_id"], "cb_1")
        self.assertIn("get_customer_service_context", {item["tool_name"] for item in result["tool_calls"]})
        self.assertIn("chargeback exposure", result["summary"].lower())

    def test_refund_case_routes_to_specialist_agent_and_mcp_drafts(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="refund_exception",
            title="Refund rf_1 follow-up",
            summary="Refund rf_1 needs follow-up.",
            actor_id="tester",
            actor_role="admin",
            priority="medium",
            evidence_ids=["refund:rf_1"],
        )
        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "refund_exception_agent")
        self.assertEqual(result["drafts"]["operator_note"]["status"], "ready")
        self.assertEqual(result["drafts"]["approval_request"]["action_type"], "REFUND_FOLLOW_UP")
        self.assertEqual(result["drafts"]["merchant_update"]["status"], "ready")
        self.assertEqual(result["memory_snapshot"]["pinned_entities"]["refund_id"], "rf_1")
        self.assertIn("get_customer_service_context", {item["tool_name"] for item in result["tool_calls"]})
        self.assertIn("refund activity", result["summary"].lower())

    def test_payment_exception_case_routes_to_specialist_agent_for_terminal_failures(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="payment_exception",
            title="Terminal T1 failures are spiking",
            summary="CARD failures increased on terminal T1 and need RCA.",
            actor_id="tester",
            actor_role="admin",
            terminal_id="T1",
            priority="high",
            source_payload={
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "terminal_id": "T1",
                "tx_id": "tx_2",
            },
            evidence_ids=["terminal:T1", "tx:tx_2"],
        )

        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "payments_exception_agent")
        self.assertEqual(result["drafts"]["operator_note"]["status"], "ready")
        self.assertEqual(result["drafts"]["approval_request"]["status"], "ready")
        self.assertEqual(result["memory_snapshot"]["pinned_entities"]["terminal_id"], "T1")
        self.assertIn("get_payment_mode_mix", {item["tool_name"] for item in result["tool_calls"]})
        self.assertIn("get_terminal_failure_breakdown", {item["tool_name"] for item in result["tool_calls"]})
        self.assertIn("retrieve_payments_knowledge", {item["tool_name"] for item in result["tool_calls"]})
        self.assertIn("terminal-linked failure context", result["summary"].lower())
        self.assertIn("terminal t1", " ".join(result["answer_sections"]["key_findings"]).lower())
        assert_agent_summary_contract(result)

    def test_support_case_routes_to_support_agent_and_merchant_update_draft(self):
        ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="chargeback_review",
            title="Chargeback cb_1 review",
            summary="Chargeback cb_1 remains open.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["chargeback:cb_1"],
        )
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="support",
            case_type="merchant_support_case",
            title="Support ticket SUP-500",
            summary="Merchant asked for the latest chargeback status.",
            actor_id="tester",
            actor_role="support",
            priority="medium",
            source="support_ticket",
            source_ref="SUP-500",
            source_payload={
                "ticket_id": "SUP-500",
                "channel": "email",
                "contacts": [
                    {
                        "contact_id": "cnt_1",
                        "name": "Demo Store Support",
                        "role": "merchant_owner",
                        "channel": "email",
                        "last_contact_at": "2026-03-18T09:00:00Z",
                        "notes": "Merchant asked for the latest dispute update.",
                    }
                ],
            },
            evidence_ids=["chargeback:cb_1"],
        )
        ops_repository.request_case_approval(
            self.engine,
            case_id=created["case_id"],
            action_type="FOLLOW_UP",
            payload_summary="Escalate support follow-up",
            payload={"case_id": created["case_id"], "ticket_id": "SUP-500"},
            actor_id="tester",
            actor_role="support",
        )

        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "merchant_support_case_agent")
        self.assertEqual(set(result["drafts"]), {"merchant_update"})
        self.assertEqual(result["drafts"]["merchant_update"]["status"], "ready")
        self.assertEqual(result["memory_snapshot"]["pinned_entities"]["chargeback_id"], "cb_1")
        self.assertIn("get_support_case_history", {item["tool_name"] for item in result["tool_calls"]})
        self.assertIn("safe customer update", result["summary"].lower())

    def test_risk_case_routes_to_specialist_agent(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="risk",
            case_type="risk_triage",
            title="Merchant risk review",
            summary="Risk profile requires review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            source_payload={"window_from": "2026-03-01", "window_to": "2026-03-20"},
        )

        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        assert_agent_summary_contract(result)
        assert_agent_verification_downgrade_contract(result)
        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "risk_triage_agent")
        self.assertEqual(result["drafts"]["operator_note"]["status"], "ready")
        self.assertEqual(result["drafts"]["approval_request"]["action_type"], "FOLLOW_UP")
        self.assertIn("velocity or dispute signals", result["summary"].lower())
        self.assertTrue(any("velocity" in item.lower() for item in result["answer_sections"]["key_findings"]))

    def test_aml_case_routes_to_specialist_agent_with_unverified_fixture_context(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="risk",
            case_type="aml_investigation",
            title="AML watchlist review",
            summary="Potential screening match needs review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            source_ref="wl_001",
            source_payload={"watchlist_name": "sanctions_screening", "source_ref": "wl_001"},
            evidence_ids=["watchlist:wl_001"],
        )

        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        self.assertEqual(result["verification"], "unverified")
        self.assertEqual(result["agents"][0]["name"], "aml_investigation_agent")
        self.assertEqual(result["drafts"]["operator_note"]["status"], "ready")
        self.assertEqual(result["drafts"]["approval_request"]["action_type"], "FOLLOW_UP")
        self.assertIn("screening evidence", result["summary"].lower())
        self.assertIn("watchlist:wl_001", result["evidence_ids"])

    def test_connector_follow_up_case_routes_to_supervisor_agent(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="connector_follow_up",
            title="Connector follow-up for settlement 261",
            summary="Connector dispatch requires review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["settlement:261"],
        )
        approval = ops_repository.request_case_approval(
            self.engine,
            case_id=created["case_id"],
            action_type="SETTLEMENT_ESCALATION",
            payload_summary="Escalate settlement 261",
            payload={"case_id": created["case_id"], "settlement_id": "261"},
            actor_id="tester",
            actor_role="admin",
        )
        ops_repository.decide_approval(
            self.engine,
            approval_id=approval["approval"]["approval_id"],
            decision="APPROVED",
            actor_id="admin_1",
            actor_role="admin",
        )

        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        assert_agent_summary_contract(result)
        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "connector_supervisor_agent")
        self.assertEqual(result["drafts"]["operator_note"]["status"], "ready")
        self.assertEqual(
            [item["tool_name"] for item in result["tool_calls"]],
            [
                "get_merchant_profile",
                "get_case_timeline",
                "list_connector_runs",
                "get_sla_snapshot",
                "get_api_health",
                "get_monitoring_alerts",
                "get_job_failures",
            ],
        )
        self.assertIn("connector execution state", result["summary"].lower())

    def test_connector_supervisor_highlights_failed_api_health_and_jobs(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="connector_follow_up",
            title="Connector retry review",
            summary="Connector execution failed and needs review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["settlement:261"],
        )
        approval = ops_repository.request_case_approval(
            self.engine,
            case_id=created["case_id"],
            action_type="SETTLEMENT_ESCALATION",
            payload_summary="Escalate settlement 261",
            payload={"case_id": created["case_id"], "settlement_id": "261"},
            actor_id="tester",
            actor_role="admin",
        )
        ops_repository.decide_approval(
            self.engine,
            approval_id=approval["approval"]["approval_id"],
            decision="APPROVED",
            actor_id="admin_1",
            actor_role="admin",
        )
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO ops_connector_runs (
                        run_id, approval_id, case_id, connector_name, connector_mode, action_type, status,
                        request_payload_json, response_payload_json, receipt_ref, external_ref, endpoint_url,
                        idempotency_key, http_status_code, error_message,
                        dispatched_at, completed_at, updated_at, created_at
                    ) VALUES (
                        'run_failed_latest', :approval_id, :case_id, 'settlement_ops_core', 'simulated', 'SETTLEMENT_ESCALATION', 'FAILED',
                        '{}', '{}', NULL, NULL, 'https://bank.example/v1/settlements/interventions',
                        'bank-foundry:failed', 504, 'Timeout from sandbox endpoint',
                        '9999-12-31T23:59:50+00:00', '9999-12-31T23:59:55+00:00', '9999-12-31T23:59:55+00:00', '9999-12-31T23:59:55+00:00'
                    )
                    """
                ),
                {"approval_id": approval["approval"]["approval_id"], "case_id": created["case_id"]},
            )

        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "connector_supervisor_agent")
        self.assertIn("api health is degraded", " ".join(result["answer_sections"]["key_findings"]).lower())
        self.assertIn("latest job failure", " ".join(result["answer_sections"]["key_findings"]).lower())
        self.assertIn("retry or escalate manually", result["answer_sections"]["next_best_action"].lower())

    def test_incident_case_routes_to_incident_response_agent(self):
        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="incident_response",
            title="Ops incident review",
            summary="Background refresh and connector state need review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["alert:ops"],
        )

        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        self.assertEqual(result["verification"], "verified")
        self.assertEqual(result["agents"][0]["name"], "incident_response_agent")
        self.assertEqual(result["drafts"]["operator_note"]["status"], "ready")
        self.assertIn("internal operational state", result["summary"].lower())
        self.assertEqual(
            [item["tool_name"] for item in result["tool_calls"]],
            [
                "get_merchant_profile",
                "get_incident_context",
                "get_api_health",
                "get_monitoring_alerts",
                "get_job_failures",
                "get_data_quality_checks",
                "get_policy_rule_explanation",
            ],
        )

    def test_incident_response_degrades_when_monitoring_is_fixture_backed(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE proactive_refresh_schedule
                    SET next_refresh_at = '2099-01-01T00:00:00+00:00'
                    WHERE merchant_id = 'm_001' AND window_days = 30
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features (
                        transaction_fact_id, merchant_id, terminal_id, source_system, source_txn_id,
                        p_date, initiated_at, completed_at, payment_mode, status, response_code,
                        response_desc, amount_rupees, hour_of_day, card_network, device_type, os_name
                    ) VALUES (
                        'tx_bad', 'm_001', 'T9', 'pg', 'src_tx_bad',
                        '2026-03-13', '2026-03-13T13:00:00', '2026-03-13T13:05:00', 'BROKEN_MODE', 'UNKNOWN', 'ERR',
                        'Broken row', -5, 13, NULL, NULL, NULL
                    )
                    """
                )
            )

        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="incident_response",
            title="Monitoring alert review",
            summary="External monitoring alert needs review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["alert:ops"],
        )
        ops_repository.upsert_case_memory(
            self.engine,
            case_id=created["case_id"],
            memory={
                "pinned_entities": {"merchant_id": "m_001", "case_type": "incident_response"},
                "active_window": {
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-31",
                    "reason": "case_memory_window",
                },
                "confirmed_evidence_ids": ["alert:ops"],
                "latest_summary": {},
                "latest_tool_calls": [],
            },
        )

        result = build_bank_ops_case_copilot_summary(self.engine, self._render_case_detail(created["case_id"]))

        self.assertEqual(result["verification"], "unverified")
        self.assertEqual(result["agents"][0]["name"], "incident_response_agent")
        self.assertIn("fixture-backed", " ".join(result["answer_sections"]["caveats"]).lower())
        self.assertIn("data quality checks found", " ".join(result["answer_sections"]["key_findings"]).lower())
        self.assertTrue(any(call["tool_name"] == "get_monitoring_alerts" for call in result["tool_calls"]))
        self.assertTrue(any(call["tool_name"] == "get_data_quality_checks" for call in result["tool_calls"]))


if __name__ == "__main__":
    unittest.main()

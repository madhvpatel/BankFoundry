import unittest
from pathlib import Path

from sqlalchemy import create_engine, text

from app.agent.bank_ops_agents import build_bank_ops_case_copilot_summary
from app.agent.bank_ops_contracts import BANK_AGENT_TOOL_FILTERS
from app.data.ops import repository as ops_repository
from app.mcp_server import BankFoundryMCPServer, ToolClassification
from app.mcp_server.schemas import VerificationStatus
from app.mcp_server.tool_registry import TOOLS
from app.ontology.ops import runbook_for_case_type
from tests.bank_foundry_eval_harness import (
    assert_agent_summary_contract,
    assert_agent_verification_downgrade_contract,
    assert_tool_classification_contract,
    assert_tool_descriptor_contract,
    assert_tool_envelope_contract,
    assert_tool_evidence_contract,
    load_blocked_integration_fixture,
)


ROOT = Path(__file__).resolve().parents[1]


class BankFoundryProgramControlsTest(unittest.TestCase):
    def _build_engine(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
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
                    CREATE TABLE refunds (
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
                    CREATE TABLE ops_connector_runs (
                        run_id TEXT PRIMARY KEY,
                        approval_id TEXT NOT NULL,
                        case_id TEXT NOT NULL,
                        connector_name TEXT NOT NULL,
                        connector_mode TEXT NOT NULL,
                        action_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        request_payload_json TEXT NOT NULL,
                        response_payload_json TEXT NOT NULL,
                        receipt_ref TEXT NULL,
                        external_ref TEXT NULL,
                        endpoint_url TEXT NULL,
                        idempotency_key TEXT NULL,
                        http_status_code INTEGER NULL,
                        error_message TEXT NULL,
                        dispatched_at TEXT NULL,
                        completed_at TEXT NULL,
                        updated_at TEXT NOT NULL,
                        created_at TEXT NOT NULL
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
                    INSERT INTO transaction_features VALUES
                    ('tx_1', 'm_001', 'T1', '2026-03-10', '2026-03-10T10:00:00', 'UPI', 'SUCCESS', '00', 1000, 10),
                    ('tx_2', 'm_001', 'T1', '2026-03-11', '2026-03-11T11:00:00', 'CARD', 'FAILED', '91', 500, 11),
                    ('tx_3', 'm_001', 'T2', '2026-03-12', '2026-03-12T12:00:00', 'CARD', 'SUCCESS', '00', 1500, 12),
                    ('tx_4', 'm_001', 'T2', '2026-03-12', '2026-03-12T12:30:00', 'CARD', 'FAILED', '91', 700, 12)
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
                    ('cb_1', 'm_001', 'OPEN', '2026-03-12T09:00:00', '2026-03-18', 2500, '4837', 'VISA', 'tx_2')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO refunds VALUES
                    ('rf_1', 'm_001', 'PROCESSED', '2026-03-16T10:00:00', 1200, 'tx_1')
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
        return engine

    def _render_case_detail(self, engine, case_id: str) -> dict[str, object]:
        detail = ops_repository.get_case_detail(engine, case_id)
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

    def test_all_mcp_descriptors_follow_shared_contract(self):
        for name, (descriptor, _handler) in TOOLS.items():
            assert_tool_descriptor_contract(name, descriptor)
            assert_tool_classification_contract(descriptor)

    def test_all_bank_agent_tool_filters_reference_registered_non_write_tools(self):
        registered = set(TOOLS)
        write_capable_agents = {"settlement_approval_draft_agent"}
        for agent_name, tool_names in BANK_AGENT_TOOL_FILTERS.items():
            self.assertTrue(tool_names, f"{agent_name} must declare at least one MCP tool")
            self.assertTrue(set(tool_names).issubset(registered), f"{agent_name} references unknown MCP tools")
            write_tools = [
                tool_name
                for tool_name in tool_names
                if TOOLS[tool_name][0].classification == ToolClassification.write
            ]
            if agent_name in write_capable_agents:
                self.assertTrue(write_tools, f"{agent_name} should explicitly declare its approval-gated write tools")
            else:
                self.assertFalse(write_tools, f"{agent_name} references write-classified MCP tools: {write_tools}")

    def test_server_tool_filter_respects_shared_agent_contracts(self):
        server = BankFoundryMCPServer(engine=None)
        for agent_name, tool_names in BANK_AGENT_TOOL_FILTERS.items():
            descriptors = server.list_tools(tool_filter=tool_names)
            self.assertEqual({item.name for item in descriptors}, set(tool_names), agent_name)

    def test_bank_agent_modules_do_not_import_data_repositories_directly(self):
        for relative_path in ("app/agent/bank_ops_agents.py", "app/agent/mcp_client.py"):
            source = (ROOT / relative_path).read_text()
            self.assertNotIn("from app.data", source, relative_path)
            self.assertNotIn("import app.data", source, relative_path)

    def test_blocked_integration_fixtures_are_seeded(self):
        fixture_names = [
            "support_case_history.json",
            "watchlist_hits.json",
            "screening_results.json",
            "monitoring_alerts.json",
            "connector_health.json",
            "compliance_guidance.json",
        ]
        for fixture_name in fixture_names:
            payload = load_blocked_integration_fixture(fixture_name)
            self.assertIsInstance(payload, dict)
            self.assertTrue(payload, fixture_name)

    def test_unknown_tool_behavior_uses_shared_error_envelope(self):
        server = BankFoundryMCPServer(engine=None)

        result = server.call_tool("missing_tool", {"merchant_id": "m_001"})

        self.assertTrue(result.is_error)
        envelope = result.envelope()
        assert_tool_envelope_contract(envelope)
        assert_tool_evidence_contract(envelope)
        self.assertEqual(envelope.status.value, "error")
        self.assertEqual(envelope.verification.value, VerificationStatus.not_applicable.value)
        self.assertIn("Unknown tool", str(envelope.error_message))

    def test_guard_failures_use_shared_error_envelope(self):
        engine = self._build_engine()
        server = BankFoundryMCPServer(engine)

        result = server.call_tool(
            "get_failure_breakdown",
            {
                "merchant_id": "m_001",
                "start_date": "2025-01-01",
                "end_date": "2026-03-20",
            },
        )

        self.assertTrue(result.is_error)
        envelope = result.envelope()
        assert_tool_envelope_contract(envelope)
        assert_tool_evidence_contract(envelope)
        self.assertEqual(envelope.status.value, "error")
        self.assertEqual(envelope.verification.value, VerificationStatus.not_applicable.value)
        self.assertIn("date window exceeds", str(envelope.error_message))

    def test_verified_tool_results_emit_evidence(self):
        engine = self._build_engine()
        server = BankFoundryMCPServer(engine)

        envelopes = [
            server.call_tool("get_merchant_profile", {"merchant_id": "m_001"}).envelope(),
            server.call_tool(
                "get_window_kpis",
                {
                    "merchant_id": "m_001",
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-20",
                },
            ).envelope(),
            server.call_tool(
                "get_settlement_detail",
                {
                    "merchant_id": "m_001",
                    "settlement_id": "261",
                },
            ).envelope(),
        ]

        for envelope in envelopes:
            assert_tool_envelope_contract(envelope)
            assert_tool_evidence_contract(envelope)

    def test_bank_case_agent_outputs_keep_required_sections_across_routes(self):
        engine = self._build_engine()
        chargeback_case = ops_repository.create_case(
            engine,
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
        refund_case = ops_repository.create_case(
            engine,
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
        risk_case = ops_repository.create_case(
            engine,
            merchant_id="m_001",
            lane="risk",
            case_type="risk_triage",
            title="Merchant risk review",
            summary="Risk profile requires review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
        )
        connector_case = ops_repository.create_case(
            engine,
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
        connector_approval = ops_repository.request_case_approval(
            engine,
            case_id=connector_case["case_id"],
            action_type="SETTLEMENT_ESCALATION",
            payload_summary="Escalate settlement 261",
            payload={"case_id": connector_case["case_id"], "settlement_id": "261"},
            actor_id="tester",
            actor_role="admin",
        )
        ops_repository.decide_approval(
            engine,
            approval_id=connector_approval["approval"]["approval_id"],
            decision="APPROVED",
            actor_id="admin_1",
            actor_role="admin",
        )
        incident_case = ops_repository.create_case(
            engine,
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

        case_details = [
            {
                "work_item": {
                    "case_id": "case_generic_contract",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "case_type": "manual_ops_review",
                    "title": "Payments review",
                    "summary": "Acceptance needs review.",
                    "status": "OPEN",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": ["merchant:m_001"],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [],
            },
            {
                "work_item": {
                    "case_id": "case_settlement_contract",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "case_type": "held_settlement",
                    "title": "Held settlement 261",
                    "summary": "Settlement 261 remains held.",
                    "status": "OPEN",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": ["settlement:261"],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [],
            },
            {
                "work_item": {
                    "case_id": "case_reconciliation_contract",
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
            {
                "work_item": {
                    "case_id": "case_delay_contract",
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
            self._render_case_detail(engine, chargeback_case["case_id"]),
            self._render_case_detail(engine, refund_case["case_id"]),
            self._render_case_detail(engine, risk_case["case_id"]),
            self._render_case_detail(engine, connector_case["case_id"]),
            self._render_case_detail(engine, incident_case["case_id"]),
        ]

        for case_detail in case_details:
            summary = build_bank_ops_case_copilot_summary(engine, case_detail)
            assert_agent_summary_contract(summary)

    def test_bank_case_agent_verification_downgrades_when_tool_calls_are_partial(self):
        engine = self._build_engine()

        summary = build_bank_ops_case_copilot_summary(
            engine,
            {
                "work_item": {
                    "case_id": "case_partial_contract",
                    "merchant_id": "m_001",
                    "lane": "operations",
                    "case_type": "held_settlement",
                    "title": "Held settlement 999",
                    "summary": "Settlement 999 remains held.",
                    "status": "OPEN",
                    "opened_at": "2026-03-20T10:00:00+00:00",
                    "evidence_ids": ["settlement:999"],
                },
                "approval_state": {"status": "not_requested"},
                "runbook_steps": [],
            },
        )

        assert_agent_summary_contract(summary)
        assert_agent_verification_downgrade_contract(summary)
        self.assertEqual(summary["verification"], VerificationStatus.unverified.value)
        self.assertTrue(
            any(
                item.get("verification") != VerificationStatus.verified.value
                for item in summary["tool_calls"]
            )
        )


if __name__ == "__main__":
    unittest.main()

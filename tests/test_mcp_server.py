import unittest

from sqlalchemy import create_engine, text

from app.data.ops import repository as ops_repository
from app.mcp_server import BankFoundryMCPServer
from app.mcp_server.tool_registry import TOOLS


class MCPServerTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
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
                    CREATE TABLE transaction_features (
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
                    CREATE TABLE terminals (
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
                    CREATE TABLE terminal_health_snapshots (
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
                    ('tx_1', 'm_001', 'T1', 'pg', 'src_tx_1', '2026-03-10', '2026-03-10T10:00:00', '2026-03-10T10:01:00', 'UPI', 'SUCCESS', '00', 'Approved', 1000, 10, 'RUPAY', 'SOUND_BOX', 'LINUX'),
                    ('tx_2', 'm_001', 'T1', 'pg', 'src_tx_2', '2026-03-11', '2026-03-11T11:00:00', '2026-03-11T11:02:00', 'CARD', 'FAILED', '91', 'Issuer or switch inoperative', 500, 11, 'VISA', 'POS', 'LINUX'),
                    ('tx_3', 'm_001', 'T2', 'pg', 'src_tx_3', '2026-03-12', '2026-03-12T12:00:00', '2026-03-12T12:01:00', 'CARD', 'SUCCESS', '00', 'Approved', 1500, 12, 'MASTERCARD', 'POS', 'LINUX'),
                    ('tx_4', 'm_001', 'T1', 'pg', 'src_tx_4', '2026-03-12', '2026-03-12T12:30:00', '2026-03-12T12:33:00', 'CARD', 'FAILED', '91', 'Issuer or switch inoperative', 700, 12, 'VISA', 'POS', 'LINUX')
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
                    INSERT INTO settlements VALUES
                    ('261', 'm_001', 'HELD', '2026-03-15', NULL, 24882, 'INR', 'utr_261', 25000, 24882, 100, 18, 0, 0, 0, 0, 'Risk review', 'UPI', 12, 0),
                    ('262', 'm_001', 'PROCESSED', '2026-03-18', '2026-03-18T08:00:00', 50000, 'INR', 'utr_262', 50000, 50000, 0, 0, 0, 0, 0, 0, NULL, 'CARD', 20, 1),
                    ('263', 'm_001', 'PROCESSED', '2000-01-01', NULL, 125000, 'INR', 'utr_263', 125000, 125000, 0, 0, 0, 0, 0, 0, 'Partner queue', 'CARD', 50, 2)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_records VALUES
                    ('m_001', '261', 'OPEN', 'Risk review'),
                    ('m_001', '261', 'OPEN', 'Risk review'),
                    ('m_001', '261', 'PENDING', 'Manual verification')
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
                    ('rf_1', 'm_001', 'PROCESSED', '2026-03-14T10:00:00', 1200, 'tx_1')
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

        created = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="chargeback_review",
            title="Chargeback cb_1 review",
            summary="Chargeback cb_1 is open and needs review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["chargeback:cb_1"],
            tasks=[
                {
                    "title": "Verify chargeback state",
                    "description": "Confirm the chargeback row and due date.",
                    "priority": "high",
                }
            ],
        )
        self.case_id = created["case_id"]
        ops_repository.add_case_note(
            self.engine,
            case_id=self.case_id,
            body="Initial dispute note.",
            actor_id="tester",
            actor_role="admin",
        )
        risk_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="risk",
            case_type="risk_triage",
            title="Risk review",
            summary="Merchant risk profile needs review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            source_payload={"window_from": "2026-03-01", "window_to": "2026-03-20"},
        )
        self.risk_case_id = risk_case["case_id"]
        aml_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="risk",
            case_type="aml_investigation",
            title="AML watchlist review",
            summary="Potential watchlist match needs review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            source_ref="wl_001",
            source_payload={"watchlist_name": "sanctions_screening", "source_ref": "wl_001"},
            evidence_ids=["watchlist:wl_001"],
        )
        self.aml_case_id = aml_case["case_id"]
        connector_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="connector_follow_up",
            title="Connector follow-up for settlement 261",
            summary="Latest connector dispatch needs review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["settlement:261"],
        )
        connector_approval = ops_repository.request_case_approval(
            self.engine,
            case_id=connector_case["case_id"],
            action_type="SETTLEMENT_ESCALATION",
            payload_summary="Escalate settlement 261",
            payload={"case_id": connector_case["case_id"], "settlement_id": "261"},
            actor_id="tester",
            actor_role="admin",
        )
        ops_repository.decide_approval(
            self.engine,
            approval_id=connector_approval["approval"]["approval_id"],
            decision="APPROVED",
            actor_id="admin_1",
            actor_role="admin",
        )
        self.connector_case_id = connector_case["case_id"]
        incident_case = ops_repository.create_case(
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
        self.incident_case_id = incident_case["case_id"]
        support_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="support",
            case_type="merchant_support_case",
            title="Support ticket SUP-500",
            summary="Merchant needs a chargeback status update.",
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
            case_id=support_case["case_id"],
            action_type="FOLLOW_UP",
            payload_summary="Escalate support follow-up",
            payload={"case_id": support_case["case_id"], "ticket_id": "SUP-500"},
            actor_id="tester",
            actor_role="support",
        )
        self.support_case_id = support_case["case_id"]
        fixture_support_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_demo_support",
            lane="support",
            case_type="merchant_support_case",
            title="Support ticket SUP-FIX",
            summary="Fixture-backed support history should be used when local history is missing.",
            actor_id="tester",
            actor_role="support",
            priority="medium",
            source="support_ticket",
            source_ref="SUP-FIX",
            source_payload={"ticket_id": "SUP-FIX"},
        )
        self.fixture_support_case_id = fixture_support_case["case_id"]

        self.server = BankFoundryMCPServer(self.engine)

    def test_list_tools_returns_safe_tool_descriptors(self):
        tools = self.server.list_tools()

        self.assertEqual([tool.name for tool in tools], list(TOOLS))
        self.assertIn("merchant_id", tools[0].input_schema["properties"])

    def test_case_read_tools_return_normalized_case_substrate(self):
        case_detail = self.server.call_tool(
            "get_case_detail",
            {"merchant_id": "m_001", "case_id": self.case_id},
        ).envelope()
        timeline = self.server.call_tool(
            "get_case_timeline",
            {"merchant_id": "m_001", "case_id": self.case_id},
        ).envelope()
        tasks = self.server.call_tool(
            "get_case_tasks",
            {"merchant_id": "m_001", "case_id": self.case_id},
        ).envelope()
        memory = self.server.call_tool(
            "get_case_memory",
            {"merchant_id": "m_001", "case_id": self.case_id},
        ).envelope()
        sla = self.server.call_tool(
            "get_sla_snapshot",
            {"merchant_id": "m_001", "case_id": self.case_id},
        ).envelope()

        self.assertEqual(case_detail.verification.value, "verified")
        self.assertEqual(case_detail.data["work_item"]["case_id"], self.case_id)
        self.assertEqual(case_detail.data["approval_state"]["status"], "not_requested")
        self.assertEqual(len(case_detail.data["tasks"]), 1)
        self.assertIn(f"case:{self.case_id}", case_detail.evidence_ids)

        self.assertEqual(timeline.data["event_count"], 2)
        self.assertEqual(timeline.data["latest_event"]["event_type"], "note")
        self.assertIn("Latest case event", " ".join(timeline.data["summary_lines"]))

        self.assertEqual(tasks.data["task_summary"]["open_task_count"], 1)
        self.assertEqual(tasks.data["next_open_task"]["title"], "Verify chargeback state")

        self.assertEqual(memory.data["memory"]["confirmed_evidence_ids"], [])
        self.assertIn("empty default memory shape", " ".join(memory.notes))

        self.assertEqual(sla.data["sla"]["target_hours"], 8)
        self.assertEqual(sla.data["sla"]["waiting_on"], "assignment")
        self.assertFalse(sla.data["sla"]["approval_pending"])

    def test_queue_and_connector_run_tools_return_current_state(self):
        queue = self.server.call_tool(
            "list_ops_queue",
            {"merchant_id": "m_001", "lane": "operations", "status": "ACTIVE", "limit": 10},
        ).envelope()
        connector_runs = self.server.call_tool(
            "list_connector_runs",
            {"merchant_id": "m_001", "case_id": self.connector_case_id},
        ).envelope()

        self.assertEqual(queue.verification.value, "verified")
        self.assertEqual(queue.data["lane"], "operations")
        self.assertEqual(queue.data["queue_summary"]["total"], 3)
        self.assertEqual(queue.data["queue_summary"]["unassigned"], 3)
        self.assertEqual(queue.data["approvals"], [])
        self.assertIn("ops_queue:m_001:operations:active:10", queue.evidence_ids)

        self.assertEqual(connector_runs.verification.value, "verified")
        self.assertEqual(connector_runs.data["run_count"], 1)
        self.assertEqual(connector_runs.data["latest_run"]["status"], "SUCCESS")
        self.assertFalse(connector_runs.data["connector_attention"])

    def test_get_window_kpis_returns_structured_envelope(self):
        result = self.server.call_tool(
            "get_window_kpis",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
            },
        ).envelope()

        self.assertEqual(result.tool_name, "get_window_kpis")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["kpis"]["attempts"], 4)
        self.assertEqual(result.data["kpis"]["success_txns"], 2)
        self.assertEqual(result.data["kpis"]["fail_txns"], 2)
        self.assertEqual(result.evidence_ids, ["kpi:none:2026-03-01:2026-03-20"])

    def test_failure_breakdown_returns_guarded_error_for_oversized_window(self):
        result = self.server.call_tool(
            "get_failure_breakdown",
            {
                "merchant_id": "m_001",
                "start_date": "2025-01-01",
                "end_date": "2026-03-20",
            },
        )

        self.assertTrue(result.is_error)
        envelope = result.envelope()
        self.assertEqual(envelope.status.value, "error")
        self.assertIn("date window exceeds", envelope.error_message)

    def test_get_failure_breakdown_returns_ranked_rows(self):
        result = self.server.call_tool(
            "get_failure_breakdown",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "dimension": "response_code",
                "limit": 5,
            },
        ).envelope()

        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["dimension"], "response_code")
        self.assertEqual(result.data["breakdown"][0]["driver"], "91")
        self.assertIn("verify_faildrivers:response_code:2026-03-01:2026-03-20", result.evidence_ids)

    def test_payment_diagnostics_tools_return_transaction_terminal_and_knowledge_context(self):
        payment_mode_mix = self.server.call_tool(
            "get_payment_mode_mix",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
            },
        ).envelope()
        recent_transactions = self.server.call_tool(
            "get_recent_transactions",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "status": "FAILURE",
                "terminal_id": "T1",
            },
        ).envelope()
        transaction_detail = self.server.call_tool(
            "get_transaction_detail",
            {"merchant_id": "m_001", "tx_id": "tx_2"},
        ).envelope()
        terminal_profile = self.server.call_tool(
            "get_terminal_profile",
            {"merchant_id": "m_001", "terminal_id": "T1"},
        ).envelope()
        terminal_health = self.server.call_tool(
            "get_terminal_health_summary",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "terminal_id": "T1",
                "group_by": "tid",
            },
        ).envelope()
        terminal_failures = self.server.call_tool(
            "get_terminal_failure_breakdown",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "terminal_id": "T1",
                "dimension": "response_code",
            },
        ).envelope()
        payments_knowledge = self.server.call_tool(
            "retrieve_payments_knowledge",
            {"merchant_id": "m_001", "query": "terminal failures retries smart routing"},
        ).envelope()

        self.assertEqual(payment_mode_mix.verification.value, "verified")
        self.assertEqual(payment_mode_mix.data["rows"][0]["payment_mode"], "CARD")
        self.assertEqual(recent_transactions.verification.value, "verified")
        self.assertEqual(recent_transactions.data["rows"][0]["tx_id"], "tx_4")
        self.assertEqual(transaction_detail.verification.value, "verified")
        self.assertEqual(transaction_detail.data["transaction"]["source_txn_id"], "src_tx_2")
        self.assertEqual(terminal_profile.verification.value, "verified")
        self.assertEqual(terminal_profile.data["terminal"]["terminal_model"], "Soundbox Pro")
        self.assertEqual(terminal_health.verification.value, "verified")
        self.assertEqual(terminal_health.data["rows"][0]["tid"], "T1")
        self.assertEqual(terminal_failures.verification.value, "verified")
        self.assertEqual(terminal_failures.data["breakdown"][0]["driver"], "91")
        self.assertEqual(payments_knowledge.verification.value, "verified")
        self.assertGreaterEqual(payments_knowledge.data["result_count"], 1)

    def test_get_settlement_detail_returns_structured_envelope(self):
        result = self.server.call_tool(
            "get_settlement_detail",
            {
                "merchant_id": "m_001",
                "settlement_id": "261",
            },
        ).envelope()

        self.assertEqual(result.tool_name, "get_settlement_detail")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["settlement"]["status"], "HELD")
        self.assertEqual(result.data["reconciliation"][0]["reason"], "Risk review")
        self.assertIn("settlement:261", result.evidence_ids)

    def test_get_settlement_reconciliation_returns_summary_counts(self):
        result = self.server.call_tool(
            "get_settlement_reconciliation",
            {
                "merchant_id": "m_001",
                "settlement_id": "261",
            },
        ).envelope()

        self.assertEqual(result.tool_name, "get_settlement_reconciliation")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["total_rows"], 3)
        self.assertEqual(result.data["open_row_count"], 3)
        self.assertEqual(result.data["top_reason"]["reason"], "Risk review")
        self.assertIn("reconciliation:settlement:261", result.evidence_ids)

    def test_list_settlements_returns_bounded_rows(self):
        result = self.server.call_tool(
            "list_settlements",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "limit": 10,
            },
        ).envelope()

        self.assertEqual(result.tool_name, "list_settlements")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["row_count"], 2)
        self.assertEqual(result.data["rows"][0]["settlement_id"], "262")
        self.assertIn("settlement:261", result.evidence_ids)

    def test_get_settlement_timeline_returns_lifecycle_summary(self):
        result = self.server.call_tool(
            "get_settlement_timeline",
            {
                "merchant_id": "m_001",
                "settlement_id": "263",
            },
        ).envelope()

        self.assertEqual(result.tool_name, "get_settlement_timeline")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["current_stage"], "delayed_unsettled")
        self.assertIn("delayed", result.data["summary"].lower())
        self.assertGreaterEqual(len(result.data["events"]), 3)
        self.assertIn("timeline:settlement:263", result.evidence_ids)

    def test_get_reconciliation_breaks_returns_open_break_buckets(self):
        result = self.server.call_tool(
            "get_reconciliation_breaks",
            {
                "merchant_id": "m_001",
                "settlement_id": "261",
            },
        ).envelope()

        self.assertEqual(result.tool_name, "get_reconciliation_breaks")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["total_break_rows"], 3)
        self.assertEqual(result.data["distinct_break_count"], 2)
        self.assertEqual(result.data["top_break"]["reason"], "Risk review")
        self.assertIn("reconciliation_breaks:settlement:261", result.evidence_ids)

    def test_get_hold_reason_returns_explicit_hold_context(self):
        result = self.server.call_tool(
            "get_hold_reason",
            {
                "merchant_id": "m_001",
                "settlement_id": "261",
            },
        ).envelope()

        self.assertEqual(result.tool_name, "get_hold_reason")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["hold_reason"], "Risk review")
        self.assertEqual(result.data["status"], "HELD")

    def test_get_payout_delay_context_returns_delayed_unsettled_state(self):
        result = self.server.call_tool(
            "get_payout_delay_context",
            {
                "merchant_id": "m_001",
                "settlement_id": "263",
            },
        ).envelope()

        self.assertEqual(result.tool_name, "get_payout_delay_context")
        self.assertEqual(result.verification.value, "verified")
        self.assertTrue(result.data["is_delayed"])
        self.assertEqual(result.data["delay_state"], "delayed_unsettled")
        self.assertEqual(result.data["hold_reason"], "Partner queue")

    def test_get_deduction_breakdown_returns_structured_components(self):
        result = self.server.call_tool(
            "get_deduction_breakdown",
            {
                "merchant_id": "m_001",
                "settlement_id": "261",
            },
        ).envelope()

        self.assertEqual(result.tool_name, "get_deduction_breakdown")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["difference_amount"], 118.0)
        self.assertEqual(result.data["components"][0]["label"], "MDR")
        self.assertEqual(result.data["components"][1]["label"], "GST on MDR")
        self.assertIn("Known components", result.data["summary"])

    def test_settlement_write_intent_wrappers_stay_approval_gated(self):
        settlement_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="held_settlement",
            title="Held settlement 261",
            summary="Settlement 261 remains held beyond the expected date.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["settlement:261"],
        )
        reconciliation_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="reconciliation_mismatch",
            title="Settlement 261 reconciliation mismatch",
            summary="Gross and net amounts do not fully reconcile.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["settlement:261"],
        )
        delayed_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="delayed_payout_exception",
            title="Delayed payout for settlement 263",
            summary="Settlement 263 is still not settled.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["settlement:263"],
        )
        ops_repository.request_case_approval(
            self.engine,
            case_id=delayed_case["case_id"],
            action_type="PAYOUT_DELAY_INTERVENTION",
            payload_summary="Escalate delayed payout",
            payload={"case_id": delayed_case["case_id"], "settlement_id": "263"},
            actor_id="tester",
            actor_role="admin",
        )
        settlement_wrapper = self.server.call_tool(
            "submit_settlement_intervention",
            {
                "merchant_id": "m_001",
                "case_id": settlement_case["case_id"],
                "recommended_action": "Escalate the held settlement after hold review.",
            },
        ).envelope()
        reconciliation_wrapper = self.server.call_tool(
            "submit_reconciliation_review",
            {
                "merchant_id": "m_001",
                "case_id": reconciliation_case["case_id"],
                "recommended_action": "Resolve the top reconciliation break before connector follow-through.",
            },
        ).envelope()
        pending_wrapper = self.server.call_tool(
            "submit_settlement_intervention",
            {
                "merchant_id": "m_001",
                "case_id": delayed_case["case_id"],
            },
        ).envelope()

        self.assertEqual(settlement_wrapper.verification.value, "verified")
        self.assertEqual(settlement_wrapper.data["action_type"], "SETTLEMENT_ESCALATION")
        self.assertTrue(settlement_wrapper.data["approval_required"])
        self.assertEqual(settlement_wrapper.data["dispatch_readiness"], "approval_required")
        self.assertEqual(reconciliation_wrapper.data["action_type"], "RECONCILIATION_REVIEW")
        self.assertEqual(reconciliation_wrapper.data["payload"]["settlement_id"], "261")
        self.assertEqual(pending_wrapper.data["status"], "blocked")
        self.assertEqual(pending_wrapper.data["approval_state"], "pending")
        self.assertEqual(pending_wrapper.data["dispatch_readiness"], "pending_approval")

    def test_dispute_tools_return_structured_chargeback_and_refund_context(self):
        chargeback_summary = self.server.call_tool(
            "get_chargeback_summary",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
            },
        ).envelope()
        chargeback_detail = self.server.call_tool(
            "get_chargeback_detail",
            {"merchant_id": "m_001", "chargeback_id": "cb_1"},
        ).envelope()
        refund_summary = self.server.call_tool(
            "get_refund_summary",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
            },
        ).envelope()
        refund_detail = self.server.call_tool(
            "get_refund_detail",
            {"merchant_id": "m_001", "refund_id": "rf_1"},
        ).envelope()

        self.assertEqual(chargeback_summary.verification.value, "verified")
        self.assertEqual(chargeback_summary.data["open_chargebacks_count"], 1)
        self.assertEqual(chargeback_summary.data["top_reason"]["reason_code"], "4837")
        self.assertEqual(chargeback_detail.data["chargeback"]["network"], "VISA")
        self.assertEqual(refund_summary.data["refunds_count"], 1)
        self.assertEqual(refund_detail.data["refund"]["status"], "PROCESSED")

    def test_chargeback_detail_handles_legacy_chargebacks_schema(self):
        legacy_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with legacy_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE chargebacks (
                        chargeback_id TEXT,
                        mid TEXT,
                        chargeback_stage TEXT,
                        created_at TEXT,
                        response_due_date TEXT,
                        chargeback_amount REAL,
                        chargeback_reason_code TEXT,
                        card_network TEXT,
                        transaction_id TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO chargebacks VALUES
                    ('cb_legacy', 'm_legacy', 'OPEN', '2026-03-12T09:00:00', '2026-03-18', 2500, '4837', 'VISA', 'tx_legacy')
                    """
                )
            )

        legacy_server = BankFoundryMCPServer(legacy_engine)
        result = legacy_server.call_tool(
            "get_chargeback_detail",
            {"merchant_id": "m_legacy", "chargeback_id": "cb_legacy"},
        ).envelope()

        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["chargeback"]["status"], "OPEN")
        self.assertEqual(result.data["chargeback"]["opened_at"], "2026-03-12T09:00:00")
        self.assertEqual(result.data["chargeback"]["due_by"], "2026-03-18")
        self.assertEqual(result.data["chargeback"]["amount_rupees"], 2500)
        self.assertEqual(result.data["chargeback"]["reason_code"], "4837")
        self.assertEqual(result.data["chargeback"]["network"], "VISA")
        self.assertEqual(result.data["chargeback"]["tx_id"], "tx_legacy")

    def test_support_context_tools_and_merchant_update_use_local_case_state(self):
        support_history = self.server.call_tool(
            "get_support_case_history",
            {"merchant_id": "m_001", "case_id": self.support_case_id},
        ).envelope()
        contact_context = self.server.call_tool(
            "get_contact_and_escalation_context",
            {"merchant_id": "m_001", "case_id": self.support_case_id},
        ).envelope()
        customer_service = self.server.call_tool(
            "get_customer_service_context",
            {"merchant_id": "m_001", "case_id": self.support_case_id},
        ).envelope()
        merchant_update = self.server.call_tool(
            "draft_merchant_update",
            {"merchant_id": "m_001", "case_id": self.support_case_id},
        ).envelope()

        self.assertEqual(support_history.verification.value, "verified")
        self.assertGreaterEqual(support_history.data["related_case_count"], 1)
        self.assertEqual(contact_context.verification.value, "verified")
        self.assertEqual(contact_context.data["contacts"][0]["channel"], "email")
        self.assertEqual(contact_context.data["escalations"][0]["status"], "PENDING")
        self.assertEqual(customer_service.verification.value, "verified")
        self.assertEqual(customer_service.data["preferred_channel"], "email")
        self.assertEqual(merchant_update.data["status"], "ready")
        self.assertEqual(merchant_update.data["channel_hint"], "email")
        self.assertIn("chargeback cb_1", merchant_update.data["body"].lower())

    def test_support_history_falls_back_to_seeded_fixture_when_local_history_is_missing(self):
        support_history = self.server.call_tool(
            "get_support_case_history",
            {"merchant_id": "m_demo_support", "case_id": self.fixture_support_case_id},
        ).envelope()
        merchant_update = self.server.call_tool(
            "draft_merchant_update",
            {"merchant_id": "m_demo_support", "case_id": self.fixture_support_case_id},
        ).envelope()

        self.assertEqual(support_history.verification.value, "unverified")
        self.assertEqual(support_history.data["source"], "fixture_fallback")
        self.assertEqual(support_history.data["recent_cases"][0]["ticket_id"], "SUP-101")
        self.assertEqual(merchant_update.verification.value, "unverified")
        self.assertEqual(merchant_update.data["channel_hint"], "merchant_portal")

    def test_case_timeline_and_draft_tools_use_current_case_state(self):
        timeline = self.server.call_tool(
            "summarize_case_timeline",
            {"merchant_id": "m_001", "case_id": self.case_id},
        ).envelope()
        note_draft = self.server.call_tool(
            "draft_case_note",
            {"merchant_id": "m_001", "case_id": self.case_id},
        ).envelope()
        approval_draft = self.server.call_tool(
            "draft_approval_request",
            {"merchant_id": "m_001", "case_id": self.case_id},
        ).envelope()

        self.assertEqual(timeline.verification.value, "verified")
        self.assertEqual(timeline.data["open_task_count"], 1)
        self.assertIn("Latest case event", " ".join(timeline.data["summary_lines"]))
        self.assertEqual(note_draft.data["status"], "ready")
        self.assertIn("Case note for", note_draft.data["body"])
        self.assertEqual(approval_draft.data["status"], "ready")
        self.assertEqual(approval_draft.data["action_type"], "CHARGEBACK_REVIEW")

    def test_risk_and_connector_tools_return_structured_state(self):
        risk = self.server.call_tool(
            "get_risk_profile",
            {"merchant_id": "m_001"},
        ).envelope()
        kyc = self.server.call_tool(
            "get_kyc_status",
            {"merchant_id": "m_001"},
        ).envelope()
        velocity = self.server.call_tool(
            "get_velocity_anomalies",
            {"merchant_id": "m_001", "start_date": "2026-03-01", "end_date": "2026-03-20"},
        ).envelope()
        dispute = self.server.call_tool(
            "get_dispute_risk_signals",
            {"merchant_id": "m_001", "start_date": "2026-03-01", "end_date": "2026-03-20"},
        ).envelope()
        policy = self.server.call_tool(
            "get_policy_rule_explanation",
            {"merchant_id": "m_001", "case_id": self.risk_case_id},
        ).envelope()
        connector = self.server.call_tool(
            "get_connector_health",
            {"merchant_id": "m_001", "case_id": self.connector_case_id},
        ).envelope()

        self.assertEqual(risk.verification.value, "verified")
        self.assertEqual(risk.data["risk_profile"]["band"], "LOW")
        self.assertEqual(kyc.verification.value, "verified")
        self.assertEqual(kyc.data["kyc"]["status"], "APPROVED")
        self.assertEqual(velocity.verification.value, "verified")
        self.assertIn("summary", velocity.data)
        self.assertEqual(dispute.verification.value, "verified")
        self.assertEqual(dispute.data["metrics"]["open_chargebacks_count"], 1)
        self.assertEqual(policy.data["runbook_code"], "risk_triage")
        self.assertEqual(connector.verification.value, "verified")
        self.assertEqual(connector.data["latest_run"]["status"], "SUCCESS")

    def test_fixture_backed_aml_tools_return_unverified_state(self):
        watchlist = self.server.call_tool(
            "get_watchlist_hits",
            {"merchant_id": "m_001"},
        ).envelope()
        screening = self.server.call_tool(
            "get_screening_results",
            {"merchant_id": "m_001"},
        ).envelope()
        aml_context = self.server.call_tool(
            "get_aml_case_context",
            {"merchant_id": "m_001", "case_id": self.aml_case_id},
        ).envelope()
        guidance = self.server.call_tool(
            "retrieve_compliance_guidance",
            {"merchant_id": "m_001", "topic": "aml_investigation"},
        ).envelope()

        self.assertEqual(watchlist.verification.value, "unverified")
        self.assertEqual(watchlist.data["hit_count"], 1)
        self.assertEqual(screening.verification.value, "unverified")
        self.assertEqual(screening.data["overall_status"], "needs_review")
        self.assertEqual(aml_context.verification.value, "verified")
        self.assertEqual(aml_context.data["case_id"], self.aml_case_id)
        self.assertIn("watchlist:wl_001", aml_context.evidence_ids)
        self.assertEqual(guidance.verification.value, "unverified")
        self.assertEqual(guidance.data["topic"], "aml_investigation")

    def test_background_refresh_health_returns_schedule_and_card_counts(self):
        refresh = self.server.call_tool(
            "get_background_refresh_health",
            {"merchant_id": "m_001", "days": 30},
        ).envelope()

        self.assertEqual(refresh.verification.value, "verified")
        self.assertEqual(refresh.data["refresh_status"]["status"], "REFRESHED")
        self.assertEqual(refresh.data["stored_card_count"], 1)
        self.assertEqual(refresh.data["state_counts"]["NEW"], 1)

    def test_tech_ops_tools_return_internal_state_context(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE proactive_refresh_schedule
                    SET next_refresh_at = '2000-01-01T00:00:00+00:00'
                    WHERE merchant_id = 'm_001' AND window_days = 30
                    """
                )
            )

        api_health = self.server.call_tool(
            "get_api_health",
            {"merchant_id": "m_001", "case_id": self.connector_case_id},
        ).envelope()
        alerts = self.server.call_tool(
            "get_monitoring_alerts",
            {"merchant_id": "m_001", "case_id": self.incident_case_id, "limit": 5},
        ).envelope()
        incident = self.server.call_tool(
            "get_incident_context",
            {"merchant_id": "m_001", "case_id": self.incident_case_id},
        ).envelope()
        job_failures = self.server.call_tool(
            "get_job_failures",
            {"merchant_id": "m_001", "case_id": self.incident_case_id, "limit": 5},
        ).envelope()

        self.assertEqual(api_health.verification.value, "verified")
        self.assertEqual(api_health.data["status"], "healthy")
        self.assertEqual(api_health.data["source"], "internal_state")
        self.assertEqual(alerts.verification.value, "verified")
        self.assertGreaterEqual(alerts.data["alert_count"], 1)
        self.assertEqual(alerts.data["source"], "internal_state")
        self.assertEqual(incident.verification.value, "verified")
        self.assertGreaterEqual(len(incident.data["summary_lines"]), 1)
        self.assertGreaterEqual(incident.data["job_failure_count"], 1)
        self.assertEqual(job_failures.verification.value, "verified")
        self.assertTrue(job_failures.data["attention_required"])

    def test_monitoring_and_api_health_fallback_to_fixtures_when_blocked(self):
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

        blocked_case = ops_repository.create_case(
            self.engine,
            merchant_id="m_001",
            lane="operations",
            case_type="incident_response",
            title="External alert review",
            summary="Monitoring alert needs review.",
            actor_id="tester",
            actor_role="admin",
            priority="high",
            evidence_ids=["alert:ops"],
        )

        alerts = self.server.call_tool(
            "get_monitoring_alerts",
            {"merchant_id": "m_001", "case_id": blocked_case["case_id"], "limit": 5},
        ).envelope()
        api_health = self.server.call_tool(
            "get_api_health",
            {"merchant_id": "m_001", "case_id": blocked_case["case_id"]},
        ).envelope()

        self.assertEqual(alerts.verification.value, "unverified")
        self.assertEqual(alerts.data["source"], "fixture")
        self.assertEqual(alerts.data["alerts"][0]["alert_id"], "ALERT-201")
        self.assertEqual(api_health.verification.value, "unverified")
        self.assertEqual(api_health.data["source"], "fixture")
        self.assertEqual(api_health.data["status"], "degraded")

    def test_data_quality_checks_return_detected_issues(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features (
                        transaction_fact_id, merchant_id, terminal_id, source_system, source_txn_id,
                        p_date, initiated_at, completed_at, payment_mode, status, response_code,
                        response_desc, amount_rupees, hour_of_day, card_network, device_type, os_name
                    ) VALUES (
                        'tx_bad', 'm_001', 'T9', 'pg', 'src_tx_bad',
                        '2026-03-13', '2026-03-13T13:00:00', '2026-03-13T13:05:00', 'BROKEN_MODE', 'UNKNOWN', 'ZZ',
                        'Broken row', -5, 13, NULL, NULL, NULL
                    )
                    """
                )
            )

        data_quality = self.server.call_tool(
            "get_data_quality_checks",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
            },
        ).envelope()

        self.assertEqual(data_quality.verification.value, "verified")
        self.assertFalse(data_quality.data["passed"])
        self.assertIn("invalid_status", data_quality.data["issues"])
        self.assertIn("invalid_payment_mode", data_quality.data["issues"])
        self.assertIn("negative_amount", data_quality.data["issues"])

    def test_explain_settlement_shortfall_returns_verified_attribution(self):
        result = self.server.call_tool(
            "explain_settlement_shortfall",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "expected_amount": 25000,
                "received_amount": 24882,
            },
        ).envelope()

        self.assertEqual(result.tool_name, "explain_settlement_shortfall")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["shortfall"]["difference_amount"], 118.0)
        self.assertIn("Known components", result.data["summary"])
        self.assertIn("shortfall:settlement:261", result.evidence_ids)

    def test_run_verified_sql_returns_structured_rows(self):
        result = self.server.call_tool(
            "run_verified_sql",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "query": """
                    SELECT payment_mode, COUNT(*) AS attempts
                    FROM transaction_features
                    WHERE merchant_id = :mid
                      AND p_date >= :start_date
                      AND p_date < :end_date
                    GROUP BY payment_mode
                    ORDER BY attempts DESC
                """,
                "limit": 10,
            },
        ).envelope()

        self.assertEqual(result.tool_name, "run_verified_sql")
        self.assertEqual(result.verification.value, "verified")
        self.assertEqual(result.data["row_count"], 2)
        self.assertEqual(result.data["rows"][0]["payment_mode"], "CARD")
        self.assertIn("sql:", result.evidence_ids[0])

    def test_run_verified_sql_blocks_missing_scope_placeholder(self):
        result = self.server.call_tool(
            "run_verified_sql",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "query": """
                    SELECT payment_mode, COUNT(*) AS attempts
                    FROM transaction_features
                    WHERE p_date >= :start_date
                      AND p_date < :end_date
                    GROUP BY payment_mode
                """,
            },
        )

        self.assertTrue(result.is_error)
        envelope = result.envelope()
        self.assertEqual(envelope.status.value, "error")
        self.assertIn("query must include :mid", envelope.error_message)

    def test_run_verified_sql_blocks_join_queries(self):
        result = self.server.call_tool(
            "run_verified_sql",
            {
                "merchant_id": "m_001",
                "start_date": "2026-03-01",
                "end_date": "2026-03-20",
                "query": """
                    SELECT t.payment_mode, m.merchant_trade_name
                    FROM transaction_features t
                    JOIN merchants m ON m.mid = t.merchant_id
                    WHERE t.merchant_id = :mid
                      AND t.p_date >= :start_date
                      AND t.p_date < :end_date
                """,
            },
        )

        self.assertTrue(result.is_error)
        envelope = result.envelope()
        self.assertEqual(envelope.status.value, "error")
        self.assertIn("does not allow joins", envelope.error_message)


if __name__ == "__main__":
    unittest.main()

import datetime as dt
import json
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

from app.intelligence.action_center import create_action
from app.merchant_os import (
    build_report_briefs,
    build_report_packs,
    build_growth_tasks,
    build_operational_tasks,
    classify_merchant,
    cleanup_legacy_actions,
    confirm_background_proactive_card_action,
    confirm_merchant_action,
    detect_connected_systems,
    ensure_background_proactive_refresh,
    get_background_refresh_status,
    get_merchant_os_snapshot,
    list_background_proactive_cards,
    list_existing_actions,
    preview_background_proactive_card_action,
    preview_merchant_action,
    refresh_background_proactive_cards,
    rows_to_csv,
    scope_snapshot_to_terminal,
    terminal_scope_options,
    update_background_proactive_card_state,
    update_existing_action_details,
    update_existing_action_status,
)


class MerchantOSTest(unittest.TestCase):
    def test_detect_connected_systems_prefers_explicit_erp_over_payments_only(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE transaction_features (
                        merchant_id TEXT,
                        p_date TEXT,
                        status TEXT,
                        payment_mode TEXT,
                        amount_rupees REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE settlements (
                        merchant_id TEXT,
                        status TEXT,
                        expected_date TEXT,
                        settled_at TEXT,
                        amount_rupees REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_integrations (
                        merchant_id TEXT,
                        integration_type TEXT,
                        status TEXT,
                        provider TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_integrations (merchant_id, integration_type, status, provider)
                    VALUES ('m_001', 'erp', 'ACTIVE', 'tally')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features (merchant_id, p_date, status, payment_mode, amount_rupees)
                    VALUES
                        ('m_001', '2026-02-01', 'SUCCESS', 'UPI', 100.0),
                        ('m_001', '2026-02-02', 'FAILED', 'CARD', 50.0)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements (merchant_id, status, expected_date, settled_at, amount_rupees)
                    VALUES ('m_001', 'PROCESSED', '2026-02-03', NULL, 120.0)
                    """
                )
            )

        coverage = detect_connected_systems(engine, "m_001")

        self.assertEqual(coverage["coverage_label"], "Payments + ERP")
        self.assertTrue(coverage["systems"]["payments"]["connected"])
        self.assertTrue(coverage["systems"]["erp"]["connected"])
        self.assertEqual(coverage["systems"]["erp"]["provider"], "tally")
        self.assertEqual(coverage["data_domains"]["payments"]["source_table"], "transaction_features")
        self.assertEqual(coverage["data_domains"]["payments"]["row_count"], 2)
        self.assertTrue(coverage["data_domains"]["settlements"]["available"])
        self.assertEqual(coverage["data_domains"]["settlements"]["row_count"], 1)

    def test_classify_merchant_returns_control_plane_for_mid_complexity(self):
        classification = classify_merchant(
            merchant_profile={
                "merchant": {
                    "annual_turnover": "120000000.00",
                }
            },
            kpi_snapshot={"attempts": 5597},
            data_coverage={
                "systems": {
                    "erp": {"connected": False},
                    "accounting": {"connected": False},
                    "pos": {"connected": False},
                    "settlements": {"connected": True},
                    "chargebacks": {"row_count": 0},
                    "refunds": {"row_count": 0},
                }
            },
            operating_signals={
                "distinct_terminals": 1,
                "invoice_reference_coverage_pct": 72.0,
            },
        )

        self.assertEqual(classification["code"], "control_plane")
        self.assertEqual(classification["label"], "Control Plane Merchant")
        self.assertTrue(any("Moderate complexity detected" in reason for reason in classification["reasons"]))

    def test_get_merchant_os_snapshot_composes_sections(self):
        merchant_profile = {
            "merchant": {
                "mid": "m_001",
                "merchant_trade_name": "Demo Merchant",
                "annual_turnover": "120000000.00",
            }
        }
        overall = {
            "rows": [
                {
                    "attempts": 100,
                    "success_txns": 96,
                    "fail_txns": 4,
                    "success_rate_pct": 96.0,
                    "success_gmv": 500000.0,
                    "failed_gmv": 15000.0,
                }
            ]
        }
        by_mode = {"rows": [{"bucket": "CARD", "fail_txns": 4, "failed_gmv": 15000.0}]}
        settlements = {"rows": [{"settlement_id": "s1", "status": "PENDING", "amount_rupees": 12000.0}]}
        chargebacks = {"rows": [{"chargeback_id": "cb1", "status": "OPEN"}]}
        refunds = {"rows": [{"refund_id": "r1", "status": "PROCESSED"}]}
        terminals = {"rows": [{"terminal_id": "T1", "attempts": 100}]}
        terminal_health = {"rows": [{"tid": "T1", "snapshots": 3}]}
        cashflow = {
            "amounts": {"pending_amount": 12000.0, "settled_amount": 48000.0},
            "past_expected": {"past_expected_count": 1, "past_expected_amount": 12000.0},
            "recent": [{"settlement_id": "s1"}],
        }
        fail_by_mode = {"rows": [{"driver": "CARD", "failed_txns": 4, "failed_gmv": 15000.0}]}
        fail_by_code = {"rows": [{"driver": "05", "failed_txns": 3, "failed_gmv": 12000.0}]}
        coverage = {"coverage_label": "Payments + acquiring ops", "systems": {"erp": {"connected": False}}}
        signals = {"distinct_terminals": 1, "invoice_reference_coverage_pct": 50.0}
        classification = {"code": "guided", "label": "Guided Ops", "reasons": ["Test reason"]}

        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-01-01", "2026-02-01")), patch(
            "app.merchant_os.get_merchant_context", return_value=merchant_profile
        ), patch(
            "app.merchant_os.compute_kpis", side_effect=[overall, by_mode]
        ), patch(
            "app.merchant_os.cashflow_snapshot", return_value=cashflow
        ), patch(
            "app.merchant_os.list_settlements", return_value=settlements
        ), patch(
            "app.merchant_os.list_chargebacks", return_value=chargebacks
        ), patch(
            "app.merchant_os.list_refunds", return_value=refunds
        ), patch(
            "app.merchant_os.terminal_performance", return_value=terminals
        ), patch(
            "app.merchant_os.terminal_health_summary", return_value=terminal_health
        ), patch(
            "app.merchant_os.verify_failure_drivers", side_effect=[fail_by_mode, fail_by_code]
        ), patch(
            "app.merchant_os.detect_connected_systems", return_value=coverage
        ), patch(
            "app.merchant_os._operating_signals", return_value=signals
        ), patch(
            "app.merchant_os.classify_merchant", return_value=classification
        ):
            snapshot = get_merchant_os_snapshot(object(), "m_001")

        self.assertEqual(snapshot["merchant_id"], "m_001")
        self.assertEqual(snapshot["window"], {"from": "2026-01-01", "to": "2026-02-01"})
        self.assertEqual(snapshot["summary"]["open_chargebacks"], 1)
        self.assertEqual(snapshot["summary"]["terminal_count"], 1)
        self.assertEqual(snapshot["classification"]["label"], "Guided Ops")
        self.assertEqual(snapshot["failure_drivers"]["payment_mode"]["rows"][0]["driver"], "CARD")
        self.assertGreaterEqual(len(snapshot["operations_tasks"]), 1)
        self.assertGreaterEqual(len(snapshot["growth_tasks"]), 1)

    def test_task_builders_generate_ops_and_growth_cards(self):
        snapshot = {
            "merchant_id": "m_001",
            "window": {"from": "2026-01-01", "to": "2026-02-01"},
            "cashflow": {
                "past_expected": {"past_expected_count": 2, "past_expected_amount": 23000.0},
            },
            "summary": {"open_chargebacks": 3},
            "settlements": {"rows": [{"settlement_id": "s1", "status": "PENDING", "amount_rupees": 12000.0}]},
            "failure_drivers": {
                "payment_mode": {
                    "rows": [{"driver": "CARD", "failed_txns": 9, "failed_gmv": 18000.0}],
                }
            },
            "intelligence": {
                "recommendations": [
                    {
                        "category": "growth",
                        "title": "Expand peak-hour throughput",
                        "summary": "Add one more device during peak hour.",
                        "priority_score": 7.2,
                        "confidence": 0.8,
                        "impact_rupees": 12000.0,
                        "evidence_ids": ["intel:1"],
                    }
                ]
            },
        }

        ops_tasks = build_operational_tasks(snapshot)
        growth_tasks = build_growth_tasks(snapshot)

        self.assertTrue(any(task["action_type"] == "SETTLEMENT_INVESTIGATION" for task in ops_tasks))
        self.assertTrue(any(task["action_type"] == "CHARGEBACK_REVIEW" for task in ops_tasks))
        self.assertEqual(growth_tasks[0]["title"], "Expand peak-hour throughput")
        self.assertTrue(any(task["action_type"] == "ACCEPTANCE_REVIEW" for task in growth_tasks))

    def test_scope_snapshot_to_terminal_filters_terminal_views_and_growth_metrics(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE transaction_features (
                        merchant_id TEXT,
                        terminal_id TEXT,
                        p_date TEXT,
                        status TEXT,
                        payment_mode TEXT,
                        response_code TEXT,
                        amount_rupees REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features
                    (merchant_id, terminal_id, p_date, status, payment_mode, response_code, amount_rupees)
                    VALUES
                    ('m_001', 'T1', '2026-01-10', 'SUCCESS', 'CARD', '00', 1000.0),
                    ('m_001', 'T1', '2026-01-11', 'FAILED', 'CARD', '05', 200.0),
                    ('m_001', 'T1', '2026-01-12', 'FAILED', 'UPI', '91', 300.0),
                    ('m_001', 'T2', '2026-01-10', 'SUCCESS', 'UPI', '00', 700.0),
                    ('m_001', 'T2', '2026-01-11', 'FAILED', 'UPI', '91', 150.0)
                    """
                )
            )

        snapshot = {
            "merchant_id": "m_001",
            "window": {"from": "2026-01-01", "to": "2026-02-01"},
            "summary": {
                "attempts": 5,
                "success_txns": 2,
                "fail_txns": 3,
                "success_rate_pct": 40.0,
                "success_gmv": 1700.0,
                "failed_gmv": 650.0,
                "terminal_count": 2,
                "open_chargebacks": 1,
                "refund_count": 1,
                "settlement_count": 1,
            },
            "terminals": {
                "rows": [
                    {"terminal_id": "T1", "attempts": 3, "success_rate_pct": 33.33, "success_gmv": 1000.0},
                    {"terminal_id": "T2", "attempts": 2, "success_rate_pct": 50.0, "success_gmv": 700.0},
                ]
            },
            "terminal_health": {"rows": [{"tid": "T1", "snapshots": 2}, {"tid": "T2", "snapshots": 1}]},
            "kpi_by_mode": [{"bucket": "CARD", "attempts": 2}, {"bucket": "UPI", "attempts": 3}],
            "failure_drivers": {
                "payment_mode": {"rows": [{"driver": "UPI", "failed_txns": 2, "failed_gmv": 450.0}]},
                "response_code": {"rows": [{"driver": "91", "failed_txns": 2, "failed_gmv": 450.0}]},
            },
            "intelligence": {
                "recommendations": [
                    {
                        "category": "growth",
                        "title": "T1 retry flow",
                        "summary": "Target terminal T1",
                        "evidence_ids": ["terminal:T1"],
                        "priority_score": 8.0,
                        "confidence": 0.9,
                    },
                    {
                        "category": "growth",
                        "title": "Merchant-wide note",
                        "summary": "No terminal binding",
                    },
                ]
            },
            "existing_actions": [
                {"title": "T1 action", "evidence_ids": ["terminal:T1"], "evidence_payload": {"terminal_id": "T1"}},
                {"title": "T2 action", "evidence_ids": ["terminal:T2"], "evidence_payload": {"terminal_id": "T2"}},
            ],
            "proactive_cards": [
                {"title": "T1 card", "lane": "growth", "evidence_ids": ["terminal:T1"]},
                {"title": "T2 card", "lane": "growth", "evidence_ids": ["terminal:T2"]},
            ],
            "operations_tasks": [
                {"title": "Terminal task", "payload": {"terminal_id": "T1"}},
                {"title": "Other terminal task", "payload": {"terminal_id": "T2"}},
            ],
            "growth_tasks": [],
            "settlements": {"rows": []},
            "chargebacks": {"rows": []},
            "refunds": {"rows": []},
            "cashflow": {"amounts": {"pending_amount": 0.0, "settled_amount": 0.0}, "recent": [], "past_expected": {}},
        }

        self.assertEqual(terminal_scope_options(snapshot), ["T1", "T2"])

        scoped = scope_snapshot_to_terminal(engine, snapshot, "T1")

        self.assertEqual(scoped["scope"]["terminal_id"], "T1")
        self.assertEqual(scoped["summary"]["attempts"], 3)
        self.assertEqual(scoped["summary"]["success_txns"], 1)
        self.assertEqual(scoped["summary"]["fail_txns"], 2)
        self.assertEqual(len(scoped["terminals"]["rows"]), 1)
        self.assertEqual(scoped["terminals"]["rows"][0]["terminal_id"], "T1")
        self.assertEqual(len(scoped["terminal_health"]["rows"]), 1)
        self.assertEqual(scoped["kpi_by_mode"][0]["bucket"], "CARD")
        self.assertEqual(scoped["failure_drivers"]["payment_mode"]["rows"][0]["driver"], "UPI")
        self.assertEqual(len(scoped["existing_actions"]), 1)
        self.assertEqual(scoped["existing_actions"][0]["title"], "T1 action")
        self.assertEqual(len(scoped["proactive_cards"]), 1)
        self.assertEqual(scoped["proactive_cards"][0]["title"], "T1 card")
        self.assertTrue(any(task["payload"].get("terminal_id") == "T1" for task in scoped["growth_tasks"]))

        packs = build_report_packs(scoped)
        self.assertTrue(packs[0]["summary_lines"][0].startswith("Scope: Terminal T1"))
        self.assertTrue(packs[2]["summary_lines"][0].startswith("Scope: Terminal T1"))

    def test_preview_merchant_action_returns_preview_token(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

        preview = preview_merchant_action(
            engine,
            "m_001",
            action_type="SETTLEMENT_INVESTIGATION",
            payload={"settlement_id": "s1"},
        )

        self.assertIn("preview", preview)
        self.assertIn("confirmation_token", preview)

    def test_confirm_merchant_action_supports_legacy_action_table(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        impact_rupees REAL,
                        confidence REAL,
                        priority_score REAL,
                        owner TEXT,
                        evidence TEXT,
                        status TEXT,
                        created_at TEXT
                    )
                    """
                )
            )

        preview = preview_merchant_action(
            engine,
            "m_001",
            action_type="SETTLEMENT_INVESTIGATION",
            payload={"description": "Investigate delayed settlement", "priority_score": 7.5},
        )
        result = confirm_merchant_action(
            engine,
            "m_001",
            confirmation_token=str(preview["confirmation_token"]),
        )

        self.assertIn("action_id", result)
        self.assertTrue(result["action_id"])

    def test_update_existing_action_status_updates_legacy_action_row(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        status TEXT,
                        created_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_actions (mid, category, title, description, status, created_at)
                    VALUES ('m_001', 'growth', 'Expand peak-hour throughput', 'Add one more device', 'REQUESTED', '2026-03-08')
                    """
                )
            )

        result = update_existing_action_status(
            engine,
            "m_001",
            action_id=1,
            status="CLOSED",
        )

        self.assertTrue(result["updated"])
        with engine.connect() as conn:
            stored = conn.execute(text("SELECT status FROM merchant_actions WHERE action_id = 1")).scalar()
        self.assertEqual(stored, "CLOSED")

    def test_update_existing_action_details_uses_columns_when_present(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        title TEXT,
                        status TEXT,
                        owner TEXT,
                        notes TEXT,
                        blocked_reason TEXT,
                        follow_up_date TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_actions (mid, title, status)
                    VALUES ('m_001', 'Review open chargebacks', 'OPEN')
                    """
                )
            )

        result = update_existing_action_details(
            engine,
            "m_001",
            action_id=1,
            owner="finance_team",
            notes="Waiting for invoice copy.",
            blocked_reason="Merchant has not uploaded proof yet",
            follow_up_date="2026-03-12",
        )

        self.assertTrue(result["updated"])
        with engine.connect() as conn:
            stored = conn.execute(
                text(
                    """
                    SELECT owner, notes, blocked_reason, follow_up_date
                    FROM merchant_actions
                    WHERE action_id = 1
                    """
                )
            ).fetchone()
        self.assertEqual(
            stored,
            ("finance_team", "Waiting for invoice copy.", "Merchant has not uploaded proof yet", "2026-03-12"),
        )

    def test_update_existing_action_details_falls_back_to_evidence_meta(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        title TEXT,
                        evidence TEXT,
                        status TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_actions (mid, title, evidence, status)
                    VALUES ('m_001', 'Review open chargebacks', '{"source":"phase2_reasoning"}', 'OPEN')
                    """
                )
            )

        result = update_existing_action_details(
            engine,
            "m_001",
            action_id=1,
            owner="merchant_ops",
            notes="Waiting on receipt copy.",
            blocked_reason="Need outlet manager input",
            follow_up_date="2026-03-14",
        )

        self.assertTrue(result["updated"])
        rows = list_existing_actions(engine, "m_001")
        self.assertEqual(rows[0]["owner"], "merchant_ops")
        self.assertEqual(rows[0]["notes"], "Waiting on receipt copy.")
        self.assertEqual(rows[0]["blocked_reason"], "Need outlet manager input")
        self.assertEqual(rows[0]["follow_up_date"], "2026-03-14")

    def test_list_existing_actions_dedupes_and_filters_low_signal_titles(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        status TEXT,
                        created_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_actions (mid, category, title, description, status, created_at)
                    VALUES
                    ('m_001', 'growth', 'Peak revenue at 23:00', 'Keep staff ready', 'OPEN', '2026-03-08T10:00:00'),
                    ('m_001', 'growth', 'Peak revenue at 23:00', 'Keep staff ready', 'OPEN', '2026-03-08T09:00:00'),
                    ('m_001', 'performance', 'Monitor Performance Metrics', 'Generic recommendation', 'OPEN', '2026-03-08T08:00:00')
                    """
                )
            )

        rows = list_existing_actions(engine, "m_001")
        titles = [row["title"] for row in rows]

        self.assertEqual(titles, ["Peak revenue at 23:00"])

    def test_list_existing_actions_returns_provenance(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        status TEXT,
                        priority_score REAL,
                        evidence TEXT,
                        created_at TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_actions
                    (mid, category, title, description, status, priority_score, evidence, created_at, updated_at)
                    VALUES
                    (
                        'm_001',
                        'growth',
                        'Reduce CARD acceptance failures',
                        'Review issuer-side dropoffs and retry behavior',
                        'OPEN',
                        8.5,
                        '{"source":"phase2_reasoning","evidence_ids":["ev:1","ev:2"],"payload":{"note":"x"}}',
                        '2026-03-08T09:00:00',
                        '2026-03-08T10:00:00'
                    )
                    """
                )
            )

        rows = list_existing_actions(engine, "m_001")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "phase2_reasoning")
        self.assertEqual(rows[0]["evidence_ids"], ["ev:1", "ev:2"])
        self.assertEqual(rows[0]["updated_at"], "2026-03-08T10:00:00")

    def test_cleanup_legacy_actions_hides_low_signal_and_duplicate_open_rows(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        status TEXT,
                        created_at TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO merchant_actions (mid, category, title, description, status, created_at, updated_at)
                    VALUES
                    ('m_001', 'performance', 'Improve Data Quality', 'Review and improve the data collection process.', 'OPEN', '2026-03-08T08:00:00', '2026-03-08T08:00:00'),
                    ('m_001', 'growth', 'Peak revenue at 23:00', 'Keep staff ready', 'OPEN', '2026-03-08T10:00:00', '2026-03-08T10:00:00'),
                    ('m_001', 'growth', 'Peak revenue at 23:00', 'Keep staff ready', 'OPEN', '2026-03-08T09:00:00', '2026-03-08T09:00:00'),
                    ('m_001', 'growth', 'Reduce CARD acceptance failures', 'Investigate issuer declines', 'OPEN', '2026-03-08T11:00:00', '2026-03-08T11:00:00')
                    """
                )
            )

        result = cleanup_legacy_actions(engine, "m_001")

        self.assertTrue(result["updated"])
        self.assertEqual(result["hidden_count"], 2)
        with engine.connect() as conn:
            statuses = conn.execute(
                text("SELECT title, status FROM merchant_actions WHERE mid = 'm_001' ORDER BY action_id")
            ).fetchall()
        self.assertEqual(
            statuses,
            [
                ("Improve Data Quality", "HIDDEN"),
                ("Peak revenue at 23:00", "OPEN"),
                ("Peak revenue at 23:00", "HIDDEN"),
                ("Reduce CARD acceptance failures", "OPEN"),
            ],
        )

    def test_create_action_skips_duplicates_and_low_signal_titles(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        impact_rupees REAL,
                        confidence REAL,
                        priority_score REAL,
                        owner TEXT,
                        evidence TEXT,
                        status TEXT DEFAULT 'OPEN',
                        created_at TEXT
                    )
                    """
                )
            )

        create_action(
            engine,
            "m_001",
            {
                "category": "growth",
                "title": "Peak revenue at 23:00",
                "description": "Keep staff ready",
                "impact_rupees": 1000.0,
                "confidence": 0.7,
                "priority_score": 7.0,
                "owner": "merchant_ops",
                "evidence": {},
            },
        )
        create_action(
            engine,
            "m_001",
            {
                "category": "growth",
                "title": "Peak revenue at 23:00",
                "description": "Keep staff ready",
                "impact_rupees": 1000.0,
                "confidence": 0.7,
                "priority_score": 7.0,
                "owner": "merchant_ops",
                "evidence": {},
            },
        )
        create_action(
            engine,
            "m_001",
            {
                "category": "performance",
                "title": "Monitor Performance Metrics",
                "description": "Generic recommendation",
                "impact_rupees": 0.0,
                "confidence": 0.4,
                "priority_score": 1.0,
                "owner": "merchant_ops",
                "evidence": {},
            },
        )

        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM merchant_actions")).scalar()
        self.assertEqual(int(count or 0), 1)

    def test_create_action_persists_provenance_payload(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        impact_rupees REAL,
                        confidence REAL,
                        priority_score REAL,
                        owner TEXT,
                        evidence TEXT,
                        status TEXT DEFAULT 'OPEN',
                        created_at TEXT
                    )
                    """
                )
            )

        action_id = create_action(
            engine,
            "m_001",
            {
                "category": "growth",
                "title": "Reduce CARD acceptance failures",
                "description": "Investigate issuer declines and retry coverage.",
                "impact_rupees": 2300.0,
                "confidence": 0.8,
                "priority_score": 7.5,
                "owner": "merchant_ops",
                "source": "phase2_reasoning",
                "evidence_ids": ["ev:1", "ev:2"],
                "workflow_steps": [{"who": "merchant", "text": "Review terminal routing"}],
                "evidence": {"signals": {"failed_txns": 10}},
            },
        )

        self.assertTrue(action_id)
        with engine.connect() as conn:
            evidence = conn.execute(text("SELECT evidence FROM merchant_actions WHERE action_id = :id"), {"id": action_id}).scalar()
        self.assertIn('"source": "phase2_reasoning"', str(evidence))
        self.assertIn('"evidence_ids": ["ev:1", "ev:2"]', str(evidence))
        self.assertIn('"workflow_steps"', str(evidence))

    def test_build_report_packs_returns_finance_operations_and_growth(self):
        snapshot = {
            "merchant_id": "m_001",
            "window": {"from": "2026-01-01", "to": "2026-02-01"},
            "merchant_profile": {"merchant": {"merchant_trade_name": "Demo Merchant"}},
            "summary": {
                "attempts": 100,
                "success_rate_pct": 96.0,
                "success_gmv": 500000.0,
                "open_chargebacks": 2,
                "refund_count": 3,
                "terminal_count": 1,
            },
            "cashflow": {
                "amounts": {"settled_amount": 480000.0, "pending_amount": 20000.0},
                "past_expected": {"past_expected_count": 1, "past_expected_amount": 12000.0},
                "recent": [{"settlement_id": "s1"}],
            },
            "settlements": {"rows": [{"settlement_id": "s1"}]},
            "chargebacks": {"rows": [{"chargeback_id": "cb1"}]},
            "refunds": {"rows": [{"refund_id": "r1"}]},
            "terminals": {"rows": [{"terminal_id": "T1"}]},
            "kpi_by_mode": [{"bucket": "CARD", "fail_txns": 2}],
            "failure_drivers": {
                "payment_mode": {"rows": [{"driver": "CARD", "failed_txns": 2}]},
                "response_code": {"rows": [{"driver": "05", "failed_txns": 2}]},
            },
            "growth_tasks": [{"title": "Reduce CARD acceptance failures"}],
            "operations_tasks": [{"title": "Review open chargebacks"}],
        }

        packs = build_report_packs(snapshot)

        self.assertEqual([pack["id"] for pack in packs], ["finance", "operations", "growth"])
        self.assertTrue(any(dataset["key"] == "finance_settlements" for dataset in packs[0]["datasets"]))
        self.assertTrue(any(dataset["key"] == "ops_failure_codes" for dataset in packs[1]["datasets"]))
        self.assertTrue(any(dataset["key"] == "growth_payment_modes" for dataset in packs[2]["datasets"]))

    def test_build_report_briefs_returns_text_and_print_exports(self):
        snapshot = {
            "merchant_id": "m_001",
            "window": {"from": "2026-01-01", "to": "2026-02-01"},
            "merchant_profile": {"merchant": {"merchant_trade_name": "Demo Merchant"}},
            "scope": {"level": "terminal", "label": "Terminal T1", "terminal_id": "T1"},
            "summary": {
                "attempts": 100,
                "success_rate_pct": 96.0,
                "success_gmv": 500000.0,
                "open_chargebacks": 2,
                "refund_count": 3,
                "terminal_count": 1,
            },
            "cashflow": {
                "amounts": {"settled_amount": 480000.0, "pending_amount": 20000.0},
                "past_expected": {"past_expected_count": 1, "past_expected_amount": 12000.0},
                "recent": [{"settlement_id": "s1"}],
            },
            "settlements": {"rows": [{"settlement_id": "s1"}]},
            "chargebacks": {"rows": [{"chargeback_id": "cb1"}]},
            "refunds": {"rows": [{"refund_id": "r1"}]},
            "terminals": {"rows": [{"terminal_id": "T1"}]},
            "kpi_by_mode": [{"bucket": "CARD", "fail_txns": 2}],
            "failure_drivers": {
                "payment_mode": {"rows": [{"driver": "CARD", "failed_txns": 2}]},
                "response_code": {"rows": [{"driver": "05", "failed_txns": 2}]},
            },
            "growth_tasks": [{"title": "Reduce CARD acceptance failures"}],
            "operations_tasks": [{"title": "Review open chargebacks"}],
        }

        briefs = build_report_briefs(snapshot)

        self.assertEqual([brief["id"] for brief in briefs], ["finance", "operations", "growth"])
        self.assertIn("Finance Pack | Demo Merchant | 2026-01-01 to 2026-02-01", briefs[0]["subject"])
        self.assertIn("Scope: Terminal T1", briefs[0]["email_text"])
        self.assertIn("Included datasets", briefs[1]["email_text"])
        self.assertIn("<html", briefs[2]["print_html"].lower())
        self.assertIn("Terminal T1", briefs[2]["print_html"])

    def test_refresh_background_proactive_cards_persists_and_replaces_same_window(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        first_cards = [
            {
                "id": "settlement_delay",
                "icon": "🏦",
                "title": "1 settlement delayed",
                "type": "warning",
                "confidence": 0.8,
                "impact_rupees": 12000.0,
                "actions": ["Review hold reasons"],
                "body": "Delayed settlement requires review.",
            },
            {
                "id": "success_rate_drop",
                "icon": "📉",
                "title": "Success rate down 2pp",
                "type": "warning",
                "confidence": 0.7,
                "impact_rupees": 8000.0,
                "actions": ["Check failures by hour"],
                "body": "Success rate degraded in the last 24h.",
            },
        ]
        second_cards = [
            {
                "id": "settlement_delay",
                "icon": "🏦",
                "title": "2 settlements delayed",
                "type": "warning",
                "confidence": 0.85,
                "impact_rupees": 18000.0,
                "actions": ["Review hold reasons"],
                "body": "Delayed settlements increased.",
            }
        ]

        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-01", "2026-03-02")), patch(
            "app.merchant_os.generate_insight_cards",
            side_effect=[first_cards, second_cards],
        ):
            first = refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)
            second = refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)

        self.assertEqual(first["generated_count"], 2)
        self.assertEqual(second["generated_count"], 1)
        rows = list_background_proactive_cards(engine, "m_001", limit=8)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "2 settlements delayed")
        self.assertEqual(rows[0]["lane"], "operations")
        self.assertTrue(any(ev.startswith("insight_card:settlement_delay") for ev in rows[0]["evidence_ids"]))

    def test_update_background_proactive_card_state_hides_dismissed_cards(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-01", "2026-03-02")), patch(
            "app.merchant_os.generate_insight_cards",
            return_value=[
                {
                    "id": "settlement_delay",
                    "icon": "🏦",
                    "title": "1 settlement delayed",
                    "type": "warning",
                    "confidence": 0.8,
                    "impact_rupees": 12000.0,
                    "actions": ["Review hold reasons"],
                    "body": "Delayed settlement requires review.",
                }
            ],
        ):
            refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)

        rows = list_background_proactive_cards(engine, "m_001", limit=8)
        dedupe_key = rows[0]["dedupe_key"]
        result = update_background_proactive_card_state(
            engine,
            "m_001",
            dedupe_key=dedupe_key,
            state="DISMISSED",
            card_notes="Handled manually",
        )

        self.assertTrue(result["updated"])
        visible = list_background_proactive_cards(engine, "m_001", limit=8)
        self.assertEqual(visible, [])

    def test_refresh_background_proactive_cards_preserves_acknowledged_state(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        card = {
            "id": "settlement_delay",
            "icon": "🏦",
            "title": "1 settlement delayed",
            "type": "warning",
            "confidence": 0.8,
            "impact_rupees": 12000.0,
            "actions": ["Review hold reasons"],
            "body": "Delayed settlement requires review.",
        }

        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-01", "2026-03-02")), patch(
            "app.merchant_os.generate_insight_cards",
            side_effect=[[card], [card]],
        ):
            refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)
            dedupe_key = list_background_proactive_cards(engine, "m_001", limit=8)[0]["dedupe_key"]
            update_background_proactive_card_state(
                engine,
                "m_001",
                dedupe_key=dedupe_key,
                state="ACKNOWLEDGED",
                card_notes="Reviewed by ops",
            )
            refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)

        preserved = list_background_proactive_cards(engine, "m_001", limit=8)[0]
        self.assertEqual(preserved["card_state"], "ACKNOWLEDGED")
        self.assertEqual(preserved["card_notes"], "Reviewed by ops")

    def test_preview_and_confirm_background_proactive_card_action(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        impact_rupees REAL,
                        confidence REAL,
                        priority_score REAL,
                        owner TEXT,
                        evidence TEXT,
                        status TEXT DEFAULT 'OPEN',
                        created_at TEXT
                    )
                    """
                )
            )

        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-01", "2026-03-02")), patch(
            "app.merchant_os.generate_insight_cards",
            return_value=[
                {
                    "id": "terminal_anomaly",
                    "icon": "🖥️",
                    "title": "Terminal T1 failure rate is elevated",
                    "type": "warning",
                    "confidence": 0.75,
                    "impact_rupees": 5000.0,
                    "actions": ["Run connectivity checks"],
                    "body": "Terminal anomaly requires review.",
                }
            ],
        ):
            refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)

        card = list_background_proactive_cards(engine, "m_001", limit=8)[0]
        preview = preview_background_proactive_card_action(engine, "m_001", dedupe_key=card["dedupe_key"])
        self.assertIn("confirmation_token", preview)

        result = confirm_background_proactive_card_action(
            engine,
            "m_001",
            dedupe_key=card["dedupe_key"],
            confirmation_token=str(preview["confirmation_token"]),
        )

        self.assertIn("action_id", result)
        updated_card = list_background_proactive_cards(engine, "m_001", limit=8)[0]
        self.assertEqual(updated_card["card_state"], "CONVERTED")
        self.assertEqual(updated_card["converted_action_id"], str(result["action_id"]))

    def test_refresh_background_proactive_cards_syncs_payout_shortfall_to_action_center(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE settlements (
                        settlement_id TEXT,
                        mid TEXT,
                        settlement_date TEXT,
                        settlement_status TEXT,
                        gross_amount REAL,
                        mdr_deducted REAL,
                        gst_on_mdr REAL,
                        tds_deducted REAL,
                        chargeback_deductions REAL,
                        reserve_held REAL,
                        adjustment_amount REAL,
                        net_settlement_amount REAL,
                        settlement_utr TEXT,
                        hold_reason TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements (
                        settlement_id, mid, settlement_date, settlement_status, gross_amount,
                        mdr_deducted, gst_on_mdr, tds_deducted, chargeback_deductions,
                        reserve_held, adjustment_amount, net_settlement_amount, settlement_utr, hold_reason
                    ) VALUES (
                        's_001', 'm_001', '2026-02-16', 'PROCESSED', 20000.0,
                        700.0, 126.0, 0.0, 0.0,
                        174.0, 0.0, 19000.0, 'UTR001', NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        impact_rupees REAL,
                        confidence REAL,
                        priority_score REAL,
                        owner TEXT,
                        evidence TEXT,
                        status TEXT DEFAULT 'OPEN',
                        created_at TEXT
                    )
                    """
                )
            )

        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-01", "2026-03-01")), patch(
            "app.merchant_os.generate_insight_cards",
            return_value=[],
        ):
            first = refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)
            second = refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)

        self.assertEqual(first["generated_count"], 1)
        self.assertEqual(second["generated_count"], 1)
        cards = list_background_proactive_cards(engine, "m_001", limit=8)
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["lane"], "operations")
        self.assertIn("settlement s_001", cards[0]["title"].lower())
        self.assertEqual(cards[0]["verification_status"], "Verified - deterministic payout shortfall attribution succeeded")
        self.assertIsNotNone(cards[0]["linked_action_id"])

        preview = preview_background_proactive_card_action(engine, "m_001", dedupe_key=cards[0]["dedupe_key"])
        self.assertEqual(preview["status"], "already_linked")
        self.assertEqual(preview["existing_action_id"], cards[0]["linked_action_id"])

        with engine.connect() as conn:
            actions = conn.execute(
                text("SELECT title, status FROM merchant_actions WHERE mid = 'm_001' ORDER BY action_id")
            ).fetchall()
        self.assertEqual(len(actions), 1)
        self.assertIn("Investigate payout shortfall", actions[0][0])
        self.assertEqual(actions[0][1], "OPEN")

    def test_refresh_background_proactive_cards_auto_intakes_settlement_ops_case(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE settlements (
                        settlement_id TEXT,
                        mid TEXT,
                        settlement_date TEXT,
                        settlement_status TEXT,
                        gross_amount REAL,
                        mdr_deducted REAL,
                        gst_on_mdr REAL,
                        tds_deducted REAL,
                        chargeback_deductions REAL,
                        reserve_held REAL,
                        adjustment_amount REAL,
                        net_settlement_amount REAL,
                        settlement_utr TEXT,
                        hold_reason TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements (
                        settlement_id, mid, settlement_date, settlement_status, gross_amount,
                        mdr_deducted, gst_on_mdr, tds_deducted, chargeback_deductions,
                        reserve_held, adjustment_amount, net_settlement_amount, settlement_utr, hold_reason
                    ) VALUES (
                        's_010', 'm_001', '2026-02-16', 'PROCESSED', 25000.0,
                        700.0, 126.0, 0.0, 0.0,
                        174.0, 0.0, 24000.0, 'UTR010', 'Risk review'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        impact_rupees REAL,
                        confidence REAL,
                        priority_score REAL,
                        owner TEXT,
                        evidence TEXT,
                        status TEXT DEFAULT 'OPEN',
                        created_at TEXT
                    )
                    """
                )
            )

        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-01", "2026-03-01")), patch(
            "app.merchant_os.generate_insight_cards",
            return_value=[],
        ):
            result = refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)

        self.assertEqual(result["ops_case_intake"]["created_count"], 1)
        cards = list_background_proactive_cards(engine, "m_001", limit=8)
        self.assertEqual(len(cards), 1)
        self.assertIsNotNone(cards[0]["linked_case_id"])

        with engine.connect() as conn:
            case_row = conn.execute(
                text(
                    """
                    SELECT lane, case_type, source, source_ref, title
                    FROM ops_cases
                    WHERE merchant_id = 'm_001'
                    """
                )
            ).fetchone()
            event_count = conn.execute(text("SELECT COUNT(*) FROM ops_case_events WHERE case_id = :case_id"), {"case_id": cards[0]["linked_case_id"]}).scalar()

        self.assertIsNotNone(case_row)
        self.assertEqual(case_row[0], "operations")
        self.assertEqual(case_row[1], "settlement_shortfall_review")
        self.assertEqual(case_row[2], "proactive")
        self.assertEqual(case_row[3], cards[0]["dedupe_key"])
        self.assertIn("settlement s_010", case_row[4].lower())
        self.assertGreaterEqual(int(event_count or 0), 1)

    def test_refresh_background_proactive_cards_heals_nested_evidence_ids_on_refresh(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE settlements (
                        settlement_id TEXT,
                        mid TEXT,
                        settlement_date TEXT,
                        settlement_status TEXT,
                        gross_amount REAL,
                        mdr_deducted REAL,
                        gst_on_mdr REAL,
                        tds_deducted REAL,
                        chargeback_deductions REAL,
                        reserve_held REAL,
                        adjustment_amount REAL,
                        net_settlement_amount REAL,
                        settlement_utr TEXT,
                        hold_reason TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO settlements (
                        settlement_id, mid, settlement_date, settlement_status, gross_amount,
                        mdr_deducted, gst_on_mdr, tds_deducted, chargeback_deductions,
                        reserve_held, adjustment_amount, net_settlement_amount, settlement_utr, hold_reason
                    ) VALUES (
                        's_020', 'm_001', '2026-02-16', 'PROCESSED', 28000.0,
                        700.0, 126.0, 0.0, 0.0,
                        174.0, 0.0, 27000.0, 'UTR020', 'Risk review'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE merchant_actions (
                        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mid TEXT,
                        category TEXT,
                        title TEXT,
                        description TEXT,
                        impact_rupees REAL,
                        confidence REAL,
                        priority_score REAL,
                        owner TEXT,
                        evidence TEXT,
                        status TEXT DEFAULT 'OPEN',
                        created_at TEXT
                    )
                    """
                )
            )

        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-01", "2026-03-01")), patch(
            "app.merchant_os.generate_insight_cards",
            return_value=[],
        ):
            first = refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)

        self.assertEqual(first["ops_case_intake"]["created_count"], 1)
        first_card = list_background_proactive_cards(engine, "m_001", limit=8)[0]
        case_id = first_card["linked_case_id"]

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE proactive_cards
                    SET evidence_ids = :evidence_ids
                    WHERE dedupe_key = :dedupe_key
                    """
                ),
                {
                    "dedupe_key": first_card["dedupe_key"],
                    "evidence_ids": json.dumps(
                        [
                            ["settlement:s_020", "shortfall:settlement:s_020"],
                            "['insight_card:payout_shortfall_s_020:2026-02-01:2026-03-01', 'window:2026-02-01:2026-03-01']",
                        ]
                    ),
                },
            )
            conn.execute(
                text(
                    """
                    UPDATE ops_cases
                    SET evidence_ids_json = :evidence_ids_json
                    WHERE case_id = :case_id
                    """
                ),
                {
                    "case_id": case_id,
                    "evidence_ids_json": json.dumps(
                        [
                            "['insight_card:payout_shortfall_s_020:2026-02-01:2026-03-01', 'window:2026-02-01:2026-03-01']",
                            ["settlement:s_020", "shortfall:settlement:s_020"],
                            '"[\\"settlement:s_020\\", \\"merchant:m_001\\"]"',
                        ]
                    ),
                },
            )

        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-01", "2026-03-01")), patch(
            "app.merchant_os.generate_insight_cards",
            return_value=[],
        ):
            second = refresh_background_proactive_cards(engine, "m_001", days=30, limit=8)

        self.assertEqual(second["ops_case_intake"]["refreshed_count"], 1)
        cards = list_background_proactive_cards(engine, "m_001", limit=8)
        self.assertEqual(len(cards), 1)
        self.assertEqual(
            cards[0]["evidence_ids"],
            [
                "insight_card:payout_shortfall_s_020:2026-02-01:2026-03-01",
                "window:2026-02-01:2026-03-01",
                "settlement:s_020",
                "shortfall:settlement:s_020",
            ],
        )

        with engine.connect() as conn:
            stored_evidence = conn.execute(
                text("SELECT evidence_ids_json FROM ops_cases WHERE case_id = :case_id"),
                {"case_id": case_id},
            ).scalar()
        self.assertEqual(
            json.loads(stored_evidence),
            [
                "insight_card:payout_shortfall_s_020:2026-02-01:2026-03-01",
                "window:2026-02-01:2026-03-01",
                "settlement:s_020",
                "shortfall:settlement:s_020",
                "merchant:m_001",
            ],
        )

    def test_ensure_background_proactive_refresh_updates_schedule_when_due(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        current_time = dt.datetime(2026, 3, 9, 10, 0, tzinfo=dt.timezone.utc)
        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-07", "2026-03-09")), patch(
            "app.merchant_os.generate_insight_cards",
            return_value=[
                {
                    "id": "success_rate_drop",
                    "icon": "📉",
                    "title": "Success rate down 2pp",
                    "type": "warning",
                    "confidence": 0.7,
                    "impact_rupees": 8000.0,
                    "actions": ["Check failures by hour"],
                    "body": "Success rate degraded in the last 24h.",
                }
            ],
        ):
            result = ensure_background_proactive_refresh(
                engine,
                "m_001",
                days=30,
                limit=8,
                min_interval_minutes=30,
                now=current_time,
            )

        self.assertTrue(result["refreshed"])
        self.assertEqual(result["reason"], "due")
        self.assertEqual(result["generated_count"], 1)
        self.assertIn("ops_case_intake", result)
        self.assertEqual(result["next_refresh_at"], "2026-03-09T10:30:00+00:00")

        status = get_background_refresh_status(
            engine,
            "m_001",
            days=30,
            min_interval_minutes=30,
            now=current_time + dt.timedelta(minutes=5),
        )
        self.assertFalse(status["due"])
        self.assertEqual(status["last_generated_count"], 1)

    def test_ensure_background_proactive_refresh_skips_when_not_due(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        first_time = dt.datetime(2026, 3, 9, 10, 0, tzinfo=dt.timezone.utc)
        second_time = first_time + dt.timedelta(minutes=10)
        with patch("app.merchant_os.default_window_from_max_date", return_value=("2026-02-07", "2026-03-09")), patch(
            "app.merchant_os.generate_insight_cards",
            return_value=[
                {
                    "id": "terminal_anomaly",
                    "icon": "🖥️",
                    "title": "Terminal anomaly",
                    "type": "warning",
                    "confidence": 0.8,
                    "impact_rupees": 5000.0,
                    "actions": ["Check terminal health"],
                    "body": "Terminal health degraded.",
                }
            ],
        ) as generate_cards:
            first = ensure_background_proactive_refresh(
                engine,
                "m_001",
                days=30,
                limit=8,
                min_interval_minutes=30,
                now=first_time,
            )
            second = ensure_background_proactive_refresh(
                engine,
                "m_001",
                days=30,
                limit=8,
                min_interval_minutes=30,
                now=second_time,
            )

        self.assertTrue(first["refreshed"])
        self.assertFalse(second["refreshed"])
        self.assertEqual(second["reason"], "not_due")
        self.assertEqual(generate_cards.call_count, 1)

    def test_rows_to_csv_serializes_rows(self):
        csv_bytes = rows_to_csv(
            [
                {"bucket": "CARD", "fail_txns": 4},
                {"bucket": "UPI", "fail_txns": 2},
            ]
        )

        csv_text = csv_bytes.decode("utf-8")
        self.assertIn("bucket,fail_txns", csv_text)
        self.assertIn("CARD,4", csv_text)
        self.assertIn("UPI,2", csv_text)


if __name__ == "__main__":
    unittest.main()

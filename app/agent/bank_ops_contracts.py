from __future__ import annotations

from typing import Final


BANK_AGENT_REQUIRED_SECTIONS: Final[tuple[str, ...]] = (
    "executive_summary",
    "key_findings",
    "next_best_action",
    "caveats",
)


BANK_AGENT_TOOL_FILTERS: Final[dict[str, tuple[str, ...]]] = {
    "generic_ops_case_copilot_agent": (
        "get_merchant_profile",
        "get_window_kpis",
        "get_failure_breakdown",
    ),
    "settlement_case_summary_agent": (
        "get_merchant_profile",
        "list_settlements",
        "get_settlement_detail",
        "get_hold_reason",
        "get_settlement_reconciliation",
        "get_settlement_cashflow_snapshot",
        "explain_settlement_shortfall",
    ),
    "reconciliation_investigation_agent": (
        "get_merchant_profile",
        "list_settlements",
        "get_settlement_detail",
        "get_deduction_breakdown",
        "get_settlement_reconciliation",
        "get_reconciliation_breaks",
        "explain_settlement_shortfall",
    ),
    "delayed_payout_agent": (
        "get_merchant_profile",
        "list_settlements",
        "get_settlement_timeline",
        "get_payout_delay_context",
        "get_hold_reason",
        "get_settlement_reconciliation",
    ),
    "settlement_approval_draft_agent": (
        "submit_settlement_intervention",
        "submit_reconciliation_review",
    ),
    "chargeback_review_agent": (
        "get_merchant_profile",
        "get_chargeback_summary",
        "list_chargebacks",
        "get_chargeback_detail",
        "get_customer_service_context",
        "draft_case_note",
        "draft_approval_request",
        "draft_merchant_update",
    ),
    "refund_exception_agent": (
        "get_merchant_profile",
        "get_refund_summary",
        "list_refunds",
        "get_refund_detail",
        "get_customer_service_context",
        "draft_case_note",
        "draft_approval_request",
        "draft_merchant_update",
    ),
    "payments_exception_agent": (
        "get_merchant_profile",
        "get_payment_mode_mix",
        "get_recent_transactions",
        "get_transaction_detail",
        "get_terminal_profile",
        "get_terminal_health_summary",
        "get_terminal_failure_breakdown",
        "retrieve_payments_knowledge",
        "draft_case_note",
        "draft_approval_request",
    ),
    "merchant_support_case_agent": (
        "get_merchant_profile",
        "get_support_case_history",
        "get_contact_and_escalation_context",
        "get_customer_service_context",
        "get_chargeback_detail",
        "get_refund_detail",
        "draft_merchant_update",
    ),
    "risk_triage_agent": (
        "get_merchant_profile",
        "get_risk_profile",
        "get_kyc_status",
        "get_velocity_anomalies",
        "get_dispute_risk_signals",
        "get_watchlist_hits",
        "retrieve_compliance_guidance",
        "get_policy_rule_explanation",
        "draft_case_note",
        "draft_approval_request",
    ),
    "aml_investigation_agent": (
        "get_merchant_profile",
        "get_watchlist_hits",
        "get_screening_results",
        "get_aml_case_context",
        "retrieve_compliance_guidance",
        "get_policy_rule_explanation",
        "draft_case_note",
        "draft_approval_request",
    ),
    "connector_supervisor_agent": (
        "get_merchant_profile",
        "get_case_timeline",
        "list_connector_runs",
        "get_sla_snapshot",
        "get_api_health",
        "get_monitoring_alerts",
        "get_job_failures",
        "draft_case_note",
        "draft_approval_request",
    ),
    "incident_response_agent": (
        "get_merchant_profile",
        "get_incident_context",
        "get_api_health",
        "get_monitoring_alerts",
        "get_job_failures",
        "get_data_quality_checks",
        "get_policy_rule_explanation",
        "draft_case_note",
        "draft_approval_request",
    ),
}


def tool_filter_for_agent(agent_name: str) -> list[str]:
    return list(BANK_AGENT_TOOL_FILTERS[agent_name])

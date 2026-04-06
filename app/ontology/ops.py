from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

LaneName = Literal["operations", "support", "risk"]
CaseType = Literal[
    "held_settlement",
    "processed_unsettled_payout",
    "settlement_shortfall_review",
    "reconciliation_mismatch",
    "delayed_payout_exception",
    "chargeback_review",
    "refund_exception",
    "merchant_support_case",
    "risk_triage",
    "kyc_review",
    "aml_investigation",
    "connector_follow_up",
    "background_refresh_issue",
    "incident_response",
    "manual_ops_review",
]
PriorityLevel = Literal["low", "medium", "high", "critical"]


@dataclass
class EvidenceRef:
    evidence_id: str
    label: str | None = None
    source_type: str | None = None


@dataclass
class WorkItemLink:
    link_type: str
    ref: str
    label: str | None = None


@dataclass
class SlaPolicy:
    name: str
    target_hours: int
    warning_hours: int


@dataclass
class RunbookStep:
    step_id: str
    title: str
    description: str
    action_type: str


@dataclass
class Runbook:
    code: str
    lane: LaneName
    title: str
    steps: list[RunbookStep] = field(default_factory=list)


@dataclass
class OpsTask:
    task_id: str
    case_id: str
    title: str
    description: str
    status: str
    owner: str | None = None
    priority: PriorityLevel = "medium"
    due_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CaseEvent:
    event_id: str
    case_id: str
    event_type: str
    actor_id: str
    actor_role: str
    body: str
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None


@dataclass
class CaseNote:
    note_id: str
    case_id: str
    body: str
    actor_id: str
    actor_role: str
    created_at: str | None = None


@dataclass
class ApprovalRequest:
    approval_id: str
    case_id: str
    action_type: str
    payload_summary: str
    status: str
    requested_by: str
    requested_role: str
    requested_at: str
    reviewed_by: str | None = None
    reviewed_role: str | None = None
    reviewed_at: str | None = None
    decision_notes: str | None = None
    receipt_ref: str | None = None


@dataclass
class OpsCase:
    case_id: str
    merchant_id: str
    lane: LaneName
    case_type: CaseType
    title: str
    summary: str
    status: str
    priority: PriorityLevel
    owner: str | None = None
    source: str = "manual"
    source_ref: str | None = None
    evidence: list[EvidenceRef] = field(default_factory=list)
    links: list[WorkItemLink] = field(default_factory=list)
    approval_state: str = "not_requested"
    runbook_code: str | None = None
    opened_at: str | None = None
    due_at: str | None = None
    resolved_at: str | None = None


DEFAULT_SLA_BY_PRIORITY: dict[PriorityLevel, SlaPolicy] = {
    "critical": SlaPolicy(name="critical", target_hours=4, warning_hours=2),
    "high": SlaPolicy(name="high", target_hours=8, warning_hours=4),
    "medium": SlaPolicy(name="medium", target_hours=24, warning_hours=12),
    "low": SlaPolicy(name="low", target_hours=48, warning_hours=24),
}


RUNBOOK_LIBRARY: dict[CaseType, Runbook] = {
    "held_settlement": Runbook(
        code="settlement_hold_review",
        lane="operations",
        title="Held Settlement Review",
        steps=[
            RunbookStep("verify_hold", "Verify hold state", "Confirm the settlement remains held and capture the latest status.", "VERIFY_STATE"),
            RunbookStep("inspect_deductions", "Inspect deductions and hold reason", "Check deductions, hold reason, and reconciliation notes.", "CHECK_DEDUCTIONS"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the key settlement rows and evidence IDs to the case.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_escalation", "Draft escalation", "Prepare the next escalation or intervention request for approval.", "DRAFT_ESCALATION"),
        ],
    ),
    "processed_unsettled_payout": Runbook(
        code="processed_unsettled_review",
        lane="operations",
        title="Processed but Unsettled Payout",
        steps=[
            RunbookStep("verify_processed", "Verify payout lifecycle", "Confirm expected date, processed flag, and settlement completion state.", "VERIFY_STATE"),
            RunbookStep("inspect_reconciliation", "Inspect reconciliation state", "Review reconciliation adjustments, pending postings, and partner notes.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach payout rows and proof of status drift.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_intervention", "Draft intervention", "Draft the intervention request for approval.", "DRAFT_ESCALATION"),
        ],
    ),
    "settlement_shortfall_review": Runbook(
        code="settlement_shortfall_review",
        lane="operations",
        title="Settlement Shortfall Review",
        steps=[
            RunbookStep("verify_shortfall", "Verify shortfall", "Confirm expected versus net settlement amounts.", "VERIFY_SHORTFALL"),
            RunbookStep("inspect_deductions", "Inspect deductions", "Break down MDR, GST, chargebacks, and other deductions.", "CHECK_DEDUCTIONS"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach all settlement and deduction evidence.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_escalation", "Draft escalation", "Prepare a queue intervention or escalation draft.", "DRAFT_ESCALATION"),
        ],
    ),
    "reconciliation_mismatch": Runbook(
        code="reconciliation_mismatch_review",
        lane="operations",
        title="Reconciliation Mismatch Review",
        steps=[
            RunbookStep("verify_mismatch", "Verify mismatch", "Confirm the mismatch across source and destination amounts.", "VERIFY_RECON"),
            RunbookStep("inspect_postings", "Inspect postings", "Review missing or duplicate postings and settlement references.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach reconciliation evidence and related records.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_resolution", "Draft resolution", "Prepare the next action for approval.", "DRAFT_ESCALATION"),
        ],
    ),
    "delayed_payout_exception": Runbook(
        code="delayed_payout_exception",
        lane="operations",
        title="Delayed Payout Exception",
        steps=[
            RunbookStep("verify_delay", "Verify delay", "Confirm the payout is delayed beyond the expected window.", "VERIFY_STATE"),
            RunbookStep("inspect_dependencies", "Inspect dependencies", "Review hold reasons, queue blocks, and partner dependencies.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the payout timeline and latest system state.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_escalation", "Draft escalation", "Prepare the escalation request for approval.", "DRAFT_ESCALATION"),
        ],
    ),
    "chargeback_review": Runbook(
        code="chargeback_review",
        lane="operations",
        title="Chargeback Review",
        steps=[
            RunbookStep("verify_chargeback", "Verify chargeback", "Confirm the chargeback status, due date, and reason code.", "VERIFY_STATE"),
            RunbookStep("inspect_exposure", "Inspect exposure", "Review open and overdue chargeback exposure for the merchant.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the chargeback row and merchant dispute context.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_follow_up", "Draft follow-up", "Prepare the response or escalation request for approval.", "DRAFT_ESCALATION"),
        ],
    ),
    "refund_exception": Runbook(
        code="refund_exception",
        lane="operations",
        title="Refund Exception Review",
        steps=[
            RunbookStep("verify_refund", "Verify refund", "Confirm the refund status, amount, and related transaction context.", "VERIFY_STATE"),
            RunbookStep("inspect_refund_pattern", "Inspect refund pattern", "Review refund counts and amount over the active window.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the refund rows and merchant context.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_follow_up", "Draft follow-up", "Prepare the refund follow-up request for approval.", "DRAFT_ESCALATION"),
        ],
    ),
    "merchant_support_case": Runbook(
        code="merchant_support_case",
        lane="support",
        title="Merchant Support Case",
        steps=[
            RunbookStep("verify_support_context", "Verify support context", "Confirm the merchant request, ticket reference, and latest customer-facing issue details.", "VERIFY_STATE"),
            RunbookStep("inspect_history", "Inspect support history", "Review recent support cases, dispute context, and any active escalation chain.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the current case context and any related support or dispute evidence.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_merchant_update", "Draft merchant update", "Prepare the next merchant-safe update from the current verified case context.", "DRAFT_ESCALATION"),
        ],
    ),
    "risk_triage": Runbook(
        code="risk_triage",
        lane="risk",
        title="Risk Triage",
        steps=[
            RunbookStep("verify_risk_profile", "Verify risk profile", "Confirm the latest risk band and score for the merchant.", "VERIFY_STATE"),
            RunbookStep("inspect_kyc", "Inspect KYC state", "Review KYC status and expiry pressure before escalating.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the risk and KYC evidence to the case.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_follow_up", "Draft follow-up", "Prepare the next review or approval request.", "DRAFT_ESCALATION"),
        ],
    ),
    "kyc_review": Runbook(
        code="kyc_review",
        lane="risk",
        title="KYC Review",
        steps=[
            RunbookStep("verify_kyc", "Verify KYC status", "Confirm the latest KYC status and expiry state.", "VERIFY_STATE"),
            RunbookStep("inspect_risk_profile", "Inspect risk profile", "Review risk band alongside KYC pressure.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the KYC and merchant evidence to the case.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_follow_up", "Draft follow-up", "Prepare the next approval or outreach request.", "DRAFT_ESCALATION"),
        ],
    ),
    "aml_investigation": Runbook(
        code="aml_investigation",
        lane="risk",
        title="AML Investigation",
        steps=[
            RunbookStep("verify_screening_signal", "Verify screening signal", "Confirm the latest watchlist or screening signal and the merchant identity in scope.", "VERIFY_STATE"),
            RunbookStep("inspect_case_context", "Inspect AML context", "Review case evidence, timeline, and any pinned AML entities before escalation.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach watchlist, screening, and case evidence to the review package.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_follow_up", "Draft follow-up", "Prepare the next analyst review or approval-backed escalation.", "DRAFT_ESCALATION"),
        ],
    ),
    "connector_follow_up": Runbook(
        code="connector_follow_up",
        lane="operations",
        title="Connector Follow-Up",
        steps=[
            RunbookStep("verify_connector_state", "Verify connector state", "Confirm the latest connector run status and response details.", "VERIFY_STATE"),
            RunbookStep("inspect_case_timeline", "Inspect case timeline", "Review the latest approval and timeline events around the connector run.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the connector payload, response, and related case context.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_follow_up", "Draft follow-up", "Prepare the next operator action or connector retry request.", "DRAFT_ESCALATION"),
        ],
    ),
    "background_refresh_issue": Runbook(
        code="background_refresh_issue",
        lane="operations",
        title="Background Refresh Issue",
        steps=[
            RunbookStep("verify_refresh_state", "Verify refresh state", "Confirm the latest background refresh schedule and due state.", "VERIFY_STATE"),
            RunbookStep("inspect_card_backlog", "Inspect card backlog", "Review stored proactive cards and backlog pressure.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach refresh status and case timeline evidence.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_follow_up", "Draft follow-up", "Prepare the next operator action or manual refresh decision.", "DRAFT_ESCALATION"),
        ],
    ),
    "incident_response": Runbook(
        code="incident_response",
        lane="operations",
        title="Incident Response",
        steps=[
            RunbookStep("verify_incident_state", "Verify incident state", "Confirm the current operational signals and scope of the incident.", "VERIFY_STATE"),
            RunbookStep("inspect_dependencies", "Inspect dependencies", "Review connector, refresh, and case-timeline dependencies.", "CHECK_RECON"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach current operational evidence and impact context.", "ATTACH_EVIDENCE"),
            RunbookStep("draft_follow_up", "Draft follow-up", "Prepare the next operational response or escalation.", "DRAFT_ESCALATION"),
        ],
    ),
    "manual_ops_review": Runbook(
        code="manual_ops_review",
        lane="operations",
        title="Manual Ops Review",
        steps=[
            RunbookStep("verify_context", "Verify context", "Confirm the issue scope and latest relevant evidence.", "VERIFY_STATE"),
            RunbookStep("attach_evidence", "Attach evidence", "Attach the evidence IDs and related references.", "ATTACH_EVIDENCE"),
            RunbookStep("recommend_action", "Recommend next action", "Draft the next best action for approval.", "DRAFT_ESCALATION"),
        ],
    ),
}


def sla_policy_for_priority(priority: str) -> SlaPolicy:
    normalized = str(priority or "medium").strip().lower()
    return DEFAULT_SLA_BY_PRIORITY.get(normalized, DEFAULT_SLA_BY_PRIORITY["medium"])


def runbook_for_case_type(case_type: str) -> Runbook:
    normalized = str(case_type or "manual_ops_review").strip().lower()
    return RUNBOOK_LIBRARY.get(normalized, RUNBOOK_LIBRARY["manual_ops_review"])


def case_type_from_source(source_type: str, payload: dict[str, Any] | None = None) -> CaseType:
    payload = payload or {}
    normalized = str(source_type or "manual").strip().lower()
    title = str(payload.get("title") or "").strip().lower()
    source_ref = str(payload.get("source_ref") or "").strip().lower()
    combined = f"{title} {source_ref}".strip()
    if "held" in combined:
        return "held_settlement"
    if "shortfall" in combined or "deduction" in combined:
        return "settlement_shortfall_review"
    if "reconciliation" in combined or "mismatch" in combined:
        return "reconciliation_mismatch"
    if "delay" in combined or "delayed" in combined:
        return "delayed_payout_exception"
    if "chargeback" in combined or "dispute" in combined:
        return "chargeback_review"
    if "refund" in combined:
        return "refund_exception"
    if "support" in combined or "ticket" in combined or "customer service" in combined:
        return "merchant_support_case"
    if "aml" in combined or "watchlist" in combined or "screening" in combined:
        return "aml_investigation"
    if "kyc" in combined:
        return "kyc_review"
    if "risk" in combined:
        return "risk_triage"
    if "refresh" in combined:
        return "background_refresh_issue"
    if "incident" in combined:
        return "incident_response"
    if "connector" in combined or "dispatch" in combined:
        return "connector_follow_up"
    if "processed" in combined and "unsettled" in combined:
        return "processed_unsettled_payout"
    if normalized == "proactive_card" and "settlement" in combined:
        return "settlement_shortfall_review"
    return "manual_ops_review"


def dataclass_to_dict(value: Any) -> dict[str, Any]:
    return asdict(value)

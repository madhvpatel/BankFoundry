# Bank Foundry Agent and MCP Tool Matrix

## Purpose

This document turns the platform vision into a concrete build matrix:

- which MCP tools exist or should exist
- which agents use them
- which lane they belong to
- whether they are read or write capabilities
- whether approval is required
- what the current build status is

For the live backlog of MCP tools that are still missing, use
`docs/BANK_FOUNDRY_REMAINING_MCP_PARALLEL_MAP.md`.

## Status legend

- `Implemented`: active now
- `Partial`: real code path exists, but limited or simulated
- `Planned`: not built yet

## Current implemented agents

| Agent | Purpose | Lane | Current status | File |
|---|---|---|---|---|
| `merchant_live_agent` | Merchant-facing chat runtime | Merchant | Implemented | `app/agent/service.py` |
| `generic_bank_case_copilot_agent` | Base bank case summary over MCP | Operations | Implemented | `app/agent/mcp_client.py` |
| `settlement_case_summary_agent` | Settlement case analysis | Operations | Implemented | `app/agent/bank_ops_agents.py` |
| `settlement_operator_note_agent` | Draft operator note from case evidence | Operations | Implemented | `app/agent/bank_ops_agents.py` |
| `settlement_approval_draft_agent` | Draft approval payload for settlement follow-through | Operations | Implemented | `app/agent/bank_ops_agents.py` |

## MCP tool matrix

### Case and workflow

| MCP tool | Purpose | Lane | Read/Write | Approval required | Primary agents | Status |
|---|---|---|---|---|---|---|
| `list_ops_queue` | List queue and queue summary | Operations, Support, Risk | Read | No | `queue_prioritization_agent` | Planned |
| `get_case_detail` | Read case, tasks, approvals, timeline, memory | All bank lanes | Read | No | all bank case agents | Planned |
| `get_case_timeline` | Timeline-only case view | All bank lanes | Read | No | all bank case agents | Planned |
| `get_case_tasks` | Task-only view | All bank lanes | Read | No | all bank case agents | Planned |
| `get_case_memory` | Read pinned context and saved summary | All bank lanes | Read | No | all bank case agents | Planned |
| `pin_case_context` | Explicitly pin settlement/window/evidence | All bank lanes | Write | No | operator-facing workflow, not autonomous | Planned |
| `update_case_memory` | Update saved case memory | All bank lanes | Write | No | operator-facing workflow, not autonomous | Planned |
| `create_case` | Open new work item | All bank lanes | Write | No | promotion/router workflows | Planned |
| `promote_case_from_chat` | Promote merchant/chat finding into case | Operations, Support | Write | No | promotion/router workflows | Planned |
| `promote_case_from_proactive` | Promote proactive signal into case | Operations, Risk | Write | No | promotion/router workflows | Planned |
| `assign_case` | Assign or reassign owner | All bank lanes | Write | No | `queue_prioritization_agent`, operator workflow | Planned |
| `add_case_note` | Persist note on case | All bank lanes | Write | No | note-drafting agents | Planned |
| `request_case_approval` | Open approval request | All bank lanes | Write | No | approval-draft agents | Planned |
| `decide_case_approval` | Approve or reject request | All bank lanes | Write | Yes, human actor | approval workflow | Planned |
| `resolve_case` | Resolve or close case | All bank lanes | Write | No | operator workflow | Planned |
| `get_sla_snapshot` | SLA timers, breach status, aging | All bank lanes | Read | No | `queue_prioritization_agent` | Planned |
| `list_connector_runs` | Connector execution history | All bank lanes | Read | No | all bank case agents | Planned |
| `link_related_case` | Link duplicates/related work items | All bank lanes | Write | No | operator workflow | Planned |

### Customer and merchant

| MCP tool | Purpose | Lane | Read/Write | Approval required | Primary agents | Status |
|---|---|---|---|---|---|---|
| `get_merchant_profile` | Merchant identity, KYC, risk snapshot | Merchant, Operations, Support | Read | No | merchant and bank agents | Implemented |
| `get_terminal_profile` | Terminal identity and current health state | Merchant, Operations | Read | No | `payments_exception_agent` | Planned |
| `get_window_kpis` | Attempts, success rate, GMV, avg ticket | Merchant, Operations | Read | No | merchant and generic bank agents | Implemented |
| `get_payment_mode_mix` | Mix and success by payment mode | Merchant, Operations, Growth | Read | No | `payments_exception_agent` | Planned |
| `get_recent_transactions` | Bounded transaction listing | Merchant, Operations, Support | Read | No | merchant and support agents | Planned |
| `get_transaction_detail` | Single transaction detail | Merchant, Operations, Support | Read | No | support and exception agents | Planned |
| `get_terminal_health_summary` | Fleet/device health metrics | Merchant, Operations | Read | No | `payments_exception_agent`, `incident_response_agent` | Planned |
| `get_terminal_failure_breakdown` | Terminal-wise decline/failure context | Merchant, Operations | Read | No | `payments_exception_agent` | Planned |
| `get_customer_service_context` | Merchant support history and related context | Support | Read | No | `merchant_support_case_agent` | Planned |

### Payments and settlement

| MCP tool | Purpose | Lane | Read/Write | Approval required | Primary agents | Status |
|---|---|---|---|---|---|---|
| `list_settlements` | Bounded settlement listing | Merchant, Operations | Read | No | merchant and settlement agents | Planned |
| `get_settlement_detail` | Single settlement plus reconciliation rows | Operations | Read | No | settlement agents | Implemented |
| `get_settlement_cashflow_snapshot` | Pending/settled amounts and backlog | Operations | Read | No | settlement agents | Implemented |
| `get_settlement_reconciliation` | Detailed reconciliation status and breaks | Operations | Read | No | `reconciliation_investigation_agent` | Planned |
| `get_settlement_timeline` | Settlement lifecycle timeline | Operations | Read | No | `delayed_payout_agent` | Planned |
| `explain_settlement_shortfall` | Shortfall attribution and recommendations | Operations | Read | No | settlement agents | Implemented |
| `get_deduction_breakdown` | MDR/GST/TDS/chargeback component view | Operations | Read | No | `settlement_case_summary_agent` | Planned |
| `get_hold_reason` | Explicit hold reason and hold metadata | Operations | Read | No | `settlement_case_summary_agent` | Planned |
| `get_payout_delay_context` | Expected-vs-actual payout delay context | Operations | Read | No | `delayed_payout_agent` | Planned |
| `get_reconciliation_breaks` | Broken/missing/duplicate posting review | Operations | Read | No | `reconciliation_investigation_agent` | Planned |
| `submit_settlement_intervention` | Submit intervention/escalation downstream | Operations | Write | Yes | approval-driven workflows | Planned |
| `submit_reconciliation_review` | Submit reconciliation review downstream | Operations | Write | Yes | approval-driven workflows | Planned |

### Disputes and support

| MCP tool | Purpose | Lane | Read/Write | Approval required | Primary agents | Status |
|---|---|---|---|---|---|---|
| `get_chargeback_summary` | Chargeback overview for merchant or case | Merchant, Support, Risk | Read | No | `chargeback_review_agent` | Planned |
| `list_chargebacks` | Bounded chargeback listing | Merchant, Support, Risk | Read | No | `chargeback_review_agent` | Planned |
| `get_chargeback_detail` | Single chargeback detail | Support, Risk | Read | No | `chargeback_review_agent` | Planned |
| `get_refund_summary` | Refund snapshot and trends | Merchant, Support | Read | No | `refund_exception_agent` | Planned |
| `list_refunds` | Bounded refund listing | Merchant, Support | Read | No | `refund_exception_agent` | Planned |
| `get_refund_detail` | Single refund detail | Support | Read | No | `refund_exception_agent` | Planned |
| `get_support_case_history` | Historical support context | Support | Read | No | `merchant_support_case_agent` | Planned |
| `get_contact_and_escalation_context` | Contact and escalation chain | Support | Read | No | `merchant_support_case_agent` | Planned |

### Risk and compliance

| MCP tool | Purpose | Lane | Read/Write | Approval required | Primary agents | Status |
|---|---|---|---|---|---|---|
| `get_risk_profile` | Merchant risk profile and scoring context | Risk, Operations | Read | No | `risk_triage_agent` | Planned |
| `get_kyc_status` | KYC state and document expiry | Risk, Support | Read | No | `risk_triage_agent` | Planned |
| `get_watchlist_hits` | Screening/watchlist hits | Risk, AML | Read | No | `aml_investigation_agent` | Planned |
| `get_screening_results` | Screening result details | Risk, AML | Read | No | `aml_investigation_agent` | Planned |
| `get_aml_case_context` | AML-specific case context | AML | Read | No | `aml_investigation_agent` | Planned |
| `get_policy_rule_explanation` | Explain triggered rules and policy basis | Risk, AML | Read | No | `risk_triage_agent`, `aml_investigation_agent` | Planned |
| `get_velocity_anomalies` | Velocity/risk anomaly signals | Risk | Read | No | `risk_triage_agent` | Planned |
| `get_dispute_risk_signals` | Risk clues linked to disputes | Risk | Read | No | `risk_triage_agent` | Planned |
| `create_risk_review_request` | Open a risk review action | Risk | Write | Yes | approval-driven workflows | Planned |

### Tech and ops

| MCP tool | Purpose | Lane | Read/Write | Approval required | Primary agents | Status |
|---|---|---|---|---|---|---|
| `run_verified_sql` | Bounded verified analytics query | Operations, Risk, Internal analytics | Read | No | generic bank agents | Implemented |
| `get_connector_health` | Connector status and last runs | Operations, Tech ops | Read | No | `connector_supervisor_agent` | Planned |
| `get_api_health` | API/runtime health | Tech ops | Read | No | `incident_response_agent` | Planned |
| `get_monitoring_alerts` | Alert feed from monitoring systems | Tech ops | Read | No | `incident_response_agent` | Planned |
| `get_incident_context` | Incident ticket and blast radius | Tech ops | Read | No | `incident_response_agent` | Planned |
| `get_job_failures` | Failed jobs and retries | Tech ops | Read | No | `incident_response_agent` | Planned |
| `get_data_quality_checks` | DQ failures and anomalies | Tech ops, Risk | Read | No | `incident_response_agent`, `risk_triage_agent` | Planned |

### Drafting and comms

| MCP tool | Purpose | Lane | Read/Write | Approval required | Primary agents | Status |
|---|---|---|---|---|---|---|
| `draft_case_note` | Structured operator note draft | All bank lanes | Read-like drafting | No | note-drafting agents | Planned |
| `draft_approval_request` | Structured approval draft | All bank lanes | Read-like drafting | No | approval-draft agents | Planned |
| `draft_bank_escalation` | Escalation message for downstream ops | Operations, Risk | Read-like drafting | No | settlement and risk agents | Planned |
| `draft_merchant_update` | Merchant-facing update draft | Support | Read-like drafting | No | `merchant_support_case_agent` | Planned |
| `summarize_case_timeline` | Compress long timeline into operator brief | All bank lanes | Read-like drafting | No | all bank case agents | Planned |
| `summarize_queue_state` | Queue/shift handoff summary | Operations, Support, Risk | Read-like drafting | No | `queue_prioritization_agent` | Planned |

### Knowledge and policy

| MCP tool | Purpose | Lane | Read/Write | Approval required | Primary agents | Status |
|---|---|---|---|---|---|---|
| `retrieve_payments_knowledge` | Payments and domain knowledge | Merchant, Operations | Read | No | merchant and settlement agents | Planned |
| `retrieve_ops_runbook` | Lane/runbook guidance | Operations, Support, Risk | Read | No | all bank case agents | Planned |
| `retrieve_policy_knowledge` | Internal policy and SOP lookup | Operations, Support, Risk, AML | Read | No | all bank case agents | Planned |
| `retrieve_compliance_guidance` | Regulatory and compliance guidance | Risk, AML, Reporting | Read | No | `aml_investigation_agent`, `regulatory_reporting_agent` | Planned |

## Agent matrix

| Agent | Lane | Main job | Key MCP tools | Status |
|---|---|---|---|---|
| `merchant_live_agent` | Merchant | Merchant-facing answers and follow-ups | current tool loop, later MCP bridge | Implemented |
| `generic_bank_case_copilot_agent` | Operations | Base case summary for non-specialist cases | `get_merchant_profile`, `get_window_kpis`, `get_failure_breakdown` | Implemented |
| `settlement_case_summary_agent` | Operations | Settlement-focused case analysis | `get_merchant_profile`, `get_settlement_detail`, `get_settlement_cashflow_snapshot`, `explain_settlement_shortfall` | Implemented |
| `settlement_operator_note_agent` | Operations | Draft note from settlement evidence | current settlement summary output, later `draft_case_note` | Implemented |
| `settlement_approval_draft_agent` | Operations | Draft approval payload for settlement actions | current settlement summary output, later `draft_approval_request` | Implemented |
| `payments_exception_agent` | Operations | Diagnose payment exceptions beyond settlements | payment mode, terminal, transaction, failure tools | Planned |
| `reconciliation_investigation_agent` | Operations | Investigate reconciliation mismatches | `get_settlement_reconciliation`, `get_reconciliation_breaks`, `run_verified_sql` | Planned |
| `delayed_payout_agent` | Operations | Explain payout delay and next action | `get_payout_delay_context`, `get_settlement_timeline`, `get_hold_reason` | Planned |
| `merchant_support_case_agent` | Support | Merchant support case resolution | merchant, refund, chargeback, support history tools | Planned |
| `chargeback_review_agent` | Support, Risk | Chargeback and dispute investigation | chargeback detail and risk signals | Planned |
| `refund_exception_agent` | Support | Refund failure and delay review | refund summary and detail tools | Planned |
| `risk_triage_agent` | Risk | Risk review and anomaly triage | risk profile, velocity, policy, DQ tools | Planned |
| `aml_investigation_agent` | AML | AML/watchlist investigation | watchlist, screening, AML case, compliance guidance | Planned |
| `regulatory_reporting_agent` | Reporting, AML | Reporting prep and evidence assembly | compliance guidance, policy, case timeline, data checks | Planned |
| `incident_response_agent` | Tech ops | Incident triage and ops debugging | alerts, API health, connector health, job failures | Planned |
| `queue_prioritization_agent` | Operations, Support, Risk | Queue ranking and shift handoff | queue, SLA, queue summary, case detail | Planned |
| `approval_reviewer_assistant` | All bank lanes | Explain pending approvals and likely impact | approval, case detail, evidence, connector runs | Planned |
| `connector_supervisor_agent` | Operations, Tech ops | Watch downstream connector execution | connector health, runs, incident context | Planned |

## Important boundary rule

These should stay deterministic workflows, not free-running AI agents:

- case creation
- assignment
- approval decision
- resolve/close
- connector dispatch
- SLA calculation
- direct write-back into external systems

Agents should focus on:

- investigation
- summarization
- drafting
- recommendation
- prioritization

## Recommended build order

### Wave 1: finish settlement operations

Build next:

- `get_settlement_reconciliation`
- `get_hold_reason`
- `get_payout_delay_context`
- `get_deduction_breakdown`
- `submit_settlement_intervention`
- `reconciliation_investigation_agent`
- `delayed_payout_agent`

### Wave 2: support and disputes

Build next:

- chargeback tools
- refund tools
- support case context tools
- `merchant_support_case_agent`
- `chargeback_review_agent`
- `refund_exception_agent`

### Wave 3: risk and compliance

Build next:

- risk profile and policy tools
- watchlist/screening tools
- AML case tools
- `risk_triage_agent`
- `aml_investigation_agent`

### Wave 4: tech ops and platform supervision

Build next:

- monitoring and connector health tools
- incident context tools
- `incident_response_agent`
- `connector_supervisor_agent`

### Wave 5: regulatory and full platform coverage

Build next:

- compliance guidance and reporting tools
- queue summarization tools
- `regulatory_reporting_agent`
- `queue_prioritization_agent`
- `approval_reviewer_assistant`

## What this means right now

Today the platform already has the right pattern:

- governed MCP tools
- specialist settlement agents
- separate merchant and bank agent paths
- deterministic workflow core

The remaining work is mostly:

- more MCP tools
- more specialist agents
- real connectors
- stronger release gates

That is the right growth curve for Bank Foundry.

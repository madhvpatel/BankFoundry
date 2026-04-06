# Bank Foundry Remaining MCP Parallel Map

## Purpose

This note answers one practical question:

- which MCP tools are still missing
- which agents should use each tool
- which parts can be built in parallel

It is based on the live code, not the older planning matrix.

## Current baseline

These MCP tools already exist in [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py):

- `get_merchant_profile`
- `get_risk_profile`
- `get_kyc_status`
- `get_background_refresh_health`
- `get_window_kpis`
- `get_failure_breakdown`
- `get_chargeback_summary`
- `list_chargebacks`
- `get_chargeback_detail`
- `get_refund_summary`
- `list_refunds`
- `get_refund_detail`
- `summarize_case_timeline`
- `get_policy_rule_explanation`
- `get_connector_health`
- `draft_case_note`
- `draft_approval_request`
- `get_settlement_detail`
- `get_settlement_reconciliation`
- `get_hold_reason`
- `get_payout_delay_context`
- `get_deduction_breakdown`
- `get_settlement_cashflow_snapshot`
- `explain_settlement_shortfall`
- `run_verified_sql`

These bank-side specialist agents already exist in [app/agent/bank_ops_agents.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_agents.py):

- `settlement_case_summary_agent`
- `reconciliation_investigation_agent`
- `delayed_payout_agent`
- `chargeback_review_agent`
- `refund_exception_agent`
- `risk_triage_agent`
- `connector_supervisor_agent`
- `incident_response_agent`
- `case_note_draft_agent`
- `approval_reviewer_assistant`
- `settlement_operator_note_agent`
- `settlement_approval_draft_agent`

The remaining work is mostly MCP expansion, plus a few new specialist agents.

## Parallel track summary

These tracks can move in parallel:

1. `Case/workflow intelligence`
2. `Payments and merchant diagnostics`
3. `Settlement completion and write-back`
4. `Risk, AML, and compliance`
5. `Tech-ops and monitoring`
6. `Drafting, knowledge, and communications`
7. `Optional agent-facing workflow writes`

## Track 1: Case and workflow intelligence

These tools strengthen the current bank case copilot and queue logic.

| MCP tool | Why it matters | Primary agents | Agent status | Notes |
|---|---|---|---|---|
| `list_ops_queue` | Lets agents reason over queue state instead of one case at a time | `queue_prioritization_agent`, `approval_reviewer_assistant` | `queue_prioritization_agent` planned, `approval_reviewer_assistant` implemented | High-value next read tool |
| `get_case_detail` | Normalized case read for agent use instead of relying on workflow-specific payloads | all bank case agents | most agents implemented | Useful as the shared case-read contract |
| `get_case_timeline` | Focused timeline read for long cases | all bank case agents, `approval_reviewer_assistant` | mixed | Complements `summarize_case_timeline` |
| `get_case_tasks` | Lets agents reason over open tasks and blockers | `queue_prioritization_agent`, `incident_response_agent`, generic bank copilot | generic copilot implemented, queue agent planned | Useful for next-best-action quality |
| `get_case_memory` | Makes pinned context explicitly tool-readable | all bank case agents | implemented agents benefit immediately | Case memory exists in the repo today, but not as an MCP tool |
| `get_sla_snapshot` | Needed for prioritization, breach review, and shift handoff | `queue_prioritization_agent`, `incident_response_agent`, `approval_reviewer_assistant` | queue agent planned, others implemented | Queue/SLA hardening makes this more valuable now |
| `list_connector_runs` | Lets agents explain execution history directly | `connector_supervisor_agent`, `incident_response_agent`, `approval_reviewer_assistant` | all three exist | Good low-risk next read tool |
| `link_related_case` | Duplicate suppression and related work linking | `queue_prioritization_agent` | planned | Lower priority than the read tools above |

## Track 2: Payments and merchant diagnostics

These tools broaden the current bank agent surface beyond settlements and disputes.

| MCP tool | Why it matters | Primary agents | Agent status | Notes |
|---|---|---|---|---|
| `get_terminal_profile` | Gives device-specific context for terminal or field issues | `payments_exception_agent`, `incident_response_agent` | `payments_exception_agent` planned, `incident_response_agent` implemented | Readable from current terminal data |
| `get_payment_mode_mix` | Explains mode-wise failure and success skew | `payments_exception_agent`, later merchant copilot bridge | planned / later | Good fit for the existing transaction repo |
| `get_recent_transactions` | Needed for support and exception review | `payments_exception_agent`, `merchant_support_case_agent`, `refund_exception_agent` | one planned, one implemented, one planned | Bounded list only |
| `get_transaction_detail` | Needed for single-transaction RCA | `payments_exception_agent`, `merchant_support_case_agent`, `chargeback_review_agent` | mixed | High-leverage debugging tool |
| `get_terminal_health_summary` | Helps connect device health to failure patterns | `payments_exception_agent`, `incident_response_agent` | one planned, one implemented | Reuses current terminal health data |
| `get_terminal_failure_breakdown` | Useful for terminal-specific failure attribution | `payments_exception_agent` | planned | Best paired with payment-mode mix |
| `get_customer_service_context` | Gives support history around the merchant or ticket | `merchant_support_case_agent` | planned | Likely needs CRM/ticket data later, but can start from local case history |

## Track 3: Settlement completion and write-back

Settlement is the deepest lane today. These are the remaining MCP gaps for a complete settlement agent surface.

| MCP tool | Why it matters | Primary agents | Agent status | Notes |
|---|---|---|---|---|
| `list_settlements` | Lets agents move between merchant-level settlement context and one pinned settlement | `settlement_case_summary_agent`, `delayed_payout_agent`, generic bank copilot | implemented agents benefit immediately | Good read-only addition |
| `get_settlement_timeline` | Needed for lifecycle reasoning across expected date, processed state, and connector follow-through | `delayed_payout_agent`, `connector_supervisor_agent` | both implemented | Distinct from raw detail and payout-delay context |
| `get_reconciliation_breaks` | More focused than general reconciliation summary | `reconciliation_investigation_agent` | implemented | High-value refinement tool |
| `submit_settlement_intervention` | Explicit action tool for post-approval write-back | `settlement_approval_draft_agent`, `approval_reviewer_assistant` | both implemented | Should stay approval-gated |
| `submit_reconciliation_review` | Explicit reconciliation action tool | `reconciliation_investigation_agent`, `approval_reviewer_assistant` | both implemented | Should stay approval-gated |

## Track 4: Risk, AML, and compliance

This is the largest MCP gap today. The current risk agent exists, but its capability surface is still narrow.

| MCP tool | Why it matters | Primary agents | Agent status | Notes |
|---|---|---|---|---|
| `get_watchlist_hits` | Core AML/watchlist signal | `aml_investigation_agent`, `risk_triage_agent` | `aml_investigation_agent` planned, `risk_triage_agent` implemented | Blocked on real watchlist data if not present locally |
| `get_screening_results` | Needed for deeper AML review | `aml_investigation_agent` | planned | Likely integration-backed later |
| `get_aml_case_context` | Gives AML-specific case evidence | `aml_investigation_agent` | planned | Case model exists; AML lane data does not yet |
| `get_velocity_anomalies` | Useful for merchant risk review | `risk_triage_agent` | implemented | Can likely start from local transaction data |
| `get_dispute_risk_signals` | Connects disputes to risk review | `risk_triage_agent`, `chargeback_review_agent` | both implemented | Good bridge tool between disputes and risk |
| `create_risk_review_request` | Opens an approval-backed risk action | `risk_triage_agent`, `approval_reviewer_assistant` | both implemented | Should remain deterministic and approval-gated |
| `retrieve_compliance_guidance` | Gives regulatory context to the risk/AML agents | `aml_investigation_agent`, `risk_triage_agent`, `regulatory_reporting_agent` | one planned, one implemented, one planned | Good MCP retrieval candidate |

## Track 5: Tech-ops and monitoring

Current incident tooling is mostly internal-state aware. These tools expand it into a fuller ops supervision surface.

| MCP tool | Why it matters | Primary agents | Agent status | Notes |
|---|---|---|---|---|
| `get_api_health` | Gives API/runtime health directly to the ops agents | `incident_response_agent`, `connector_supervisor_agent` | both implemented | Can start with internal health checks |
| `get_monitoring_alerts` | Brings external alert context into case reasoning | `incident_response_agent` | implemented | Best when external monitoring is connected |
| `get_incident_context` | Provides blast-radius and recent incident history | `incident_response_agent` | implemented | Can begin from local case/timeline data |
| `get_job_failures` | Lets agents inspect failed jobs and retries | `incident_response_agent` | implemented | Good fit if background jobs are persisted locally |
| `get_data_quality_checks` | Exposes DQ failures to ops and risk agents | `incident_response_agent`, `risk_triage_agent`, `regulatory_reporting_agent` | two implemented, one planned | High-value cross-cutting tool |

## Track 6: Drafting, knowledge, and communications

These tools make the existing agents more useful without needing new lanes first.

| MCP tool | Why it matters | Primary agents | Agent status | Notes |
|---|---|---|---|---|
| `draft_bank_escalation` | Gives operator-ready downstream escalation content | `settlement_approval_draft_agent`, `connector_supervisor_agent`, `risk_triage_agent` | all implemented | Strong next drafting tool |
| `draft_merchant_update` | Gives support-safe customer updates | `merchant_support_case_agent`, `refund_exception_agent`, `chargeback_review_agent` | one planned, two implemented | Useful before a full support lane exists |
| `summarize_queue_state` | Needed for shift handoff and manager summaries | `queue_prioritization_agent`, `approval_reviewer_assistant` | one planned, one implemented | Pair with `list_ops_queue` |
| `retrieve_payments_knowledge` | Adds policy/domain context to settlement and payment RCA | `payments_exception_agent`, `settlement_case_summary_agent`, later merchant live agent | mixed | Best for hybrid fact + knowledge answers |
| `retrieve_ops_runbook` | Gives runbook grounding directly to agents | all bank case agents | most implemented | Natural fit with the ontology layer |
| `retrieve_policy_knowledge` | Gives SOP/policy context outside one case type | `risk_triage_agent`, `connector_supervisor_agent`, `approval_reviewer_assistant` | all implemented | Useful even before AML is built |

## Track 7: Optional agent-facing workflow writes

These workflows already exist deterministically in the bank surface. They do not need to become MCP tools immediately, but this is the full list if we later want agents to invoke them through a governed write boundary.

| MCP tool | Why it matters | Primary agents | Agent status | Notes |
|---|---|---|---|---|
| `create_case` | Agent-created work item | promotion/router workflows | workflow-driven today | Lower priority |
| `assign_case` | Assignment or reassignment | `queue_prioritization_agent` | planned | Keep approval/policy checks outside the LLM |
| `add_case_note` | Commit note drafted by an agent | `case_note_draft_agent` | implemented | Good candidate after note drafting is stable |
| `request_case_approval` | Open approval request from draft | `approval_reviewer_assistant`, `settlement_approval_draft_agent` | implemented | Must stay policy-gated |
| `decide_case_approval` | Approve or reject | human actor only | workflow-driven today | Should remain human-gated |
| `resolve_case` | Resolve or close | operator workflow | workflow-driven today | Should remain deterministic |
| `pin_case_context` | Persist pinned settlement/window/evidence via MCP | all bank case agents | most implemented | Operator controls already exist in the workflow layer |
| `update_case_memory` | Persist latest memory snapshot via MCP | all bank case agents | most implemented | Lower priority because workflow persistence already works |

## Remaining agents to build

These are the major specialist agents still missing from the codebase.

| Agent | What unlocks it | Main MCP dependencies |
|---|---|---|
| `payments_exception_agent` | Broader payment exception handling beyond settlements | `get_payment_mode_mix`, `get_recent_transactions`, `get_transaction_detail`, `get_terminal_health_summary`, `get_terminal_failure_breakdown`, `retrieve_payments_knowledge` |
| `merchant_support_case_agent` | Merchant support resolution and updates | `get_customer_service_context`, `get_recent_transactions`, `get_transaction_detail`, `draft_merchant_update` |
| `aml_investigation_agent` | AML and screening investigations | `get_watchlist_hits`, `get_screening_results`, `get_aml_case_context`, `retrieve_compliance_guidance` |
| `regulatory_reporting_agent` | Reporting prep and evidence assembly | `get_data_quality_checks`, `retrieve_compliance_guidance`, `retrieve_policy_knowledge`, `summarize_case_timeline` |
| `queue_prioritization_agent` | Queue ranking and shift handoff | `list_ops_queue`, `get_sla_snapshot`, `summarize_queue_state`, `get_case_tasks` |

## Recommended parallel build order

If we want to expand MCP and agents in parallel without causing integration thrash, the cleanest split is:

### Workstream A: case intelligence and drafting

Build:

- `list_ops_queue`
- `get_case_detail`
- `get_case_timeline`
- `get_case_tasks`
- `get_case_memory`
- `get_sla_snapshot`
- `summarize_queue_state`
- `draft_bank_escalation`

Unblocks:

- `queue_prioritization_agent`
- stronger `approval_reviewer_assistant`
- stronger existing bank case agents

### Workstream B: payments diagnostics

Build:

- `get_payment_mode_mix`
- `get_recent_transactions`
- `get_transaction_detail`
- `get_terminal_profile`
- `get_terminal_health_summary`
- `get_terminal_failure_breakdown`
- `retrieve_payments_knowledge`

Unblocks:

- `payments_exception_agent`
- stronger settlement and merchant-side reasoning later

### Workstream C: settlement completion

Build:

- `list_settlements`
- `get_settlement_timeline`
- `get_reconciliation_breaks`
- `submit_settlement_intervention`
- `submit_reconciliation_review`

Unblocks:

- stronger `reconciliation_investigation_agent`
- stronger `delayed_payout_agent`
- cleaner approval-to-execution flow

### Workstream D: risk and compliance

Build:

- `get_watchlist_hits`
- `get_screening_results`
- `get_aml_case_context`
- `get_velocity_anomalies`
- `get_dispute_risk_signals`
- `retrieve_compliance_guidance`

Unblocks:

- stronger `risk_triage_agent`
- new `aml_investigation_agent`
- later `regulatory_reporting_agent`

### Workstream E: tech-ops and data quality

Build:

- `get_api_health`
- `get_monitoring_alerts`
- `get_incident_context`
- `get_job_failures`
- `get_data_quality_checks`

Unblocks:

- stronger `incident_response_agent`
- stronger `connector_supervisor_agent`

## Practical takeaway

The next MCP expansion should not be one giant backlog.

The clean parallel plan is:

1. `Case intelligence and drafting`
2. `Payments diagnostics`
3. `Settlement completion`
4. `Risk and compliance`
5. `Tech-ops and data quality`

That gets us more agent capability quickly without forcing early lane expansion.

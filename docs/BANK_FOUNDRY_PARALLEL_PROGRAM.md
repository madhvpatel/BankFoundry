# Bank Foundry Parallel Program

## Program objective

Build the full Bank Foundry agent and MCP surface area without turning the
platform into a collection of fragile one-off agents.

The operating model is:

- one shared platform spine
- multiple vertical pods running in parallel
- deterministic workflow writes separated from agentic reads and drafting
- every agent built on governed MCP interfaces, not direct ad hoc integrations

Pod kickoff controls:

- [BANK_FOUNDRY_MCP_CONTRACT.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_MCP_CONTRACT.md)
- [BANK_FOUNDRY_AGENT_CONTRACT.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_AGENT_CONTRACT.md)
- [BANK_FOUNDRY_WORKFLOW_BOUNDARY.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_WORKFLOW_BOUNDARY.md)
- [BANK_FOUNDRY_EXECUTION_BOARD.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_EXECUTION_BOARD.md)
- [BANK_FOUNDRY_BRANCH_REVIEW_RULES.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_BRANCH_REVIEW_RULES.md)

## Core rule

No agent gets to:

- query random tables directly
- call raw repositories directly
- invent its own write path
- bypass approval, audit, or policy checks

Agents consume MCP tools.
Workflow writes stay deterministic.

## Delivery model

### 1. Platform spine

This is the shared layer that every pod depends on.

It owns:

- MCP schemas and envelopes
- guardrails and policy checks
- tool registry conventions
- case/task/approval contracts
- queue and SLA contracts
- connector execution contracts
- audit and observability
- release gates and replay evals

Current code anchor points:

- [app/mcp_server/schemas.py](/Users/madhavpatel/New_demo copy/app/mcp_server/schemas.py)
- [app/mcp_server/guards.py](/Users/madhavpatel/New_demo copy/app/mcp_server/guards.py)
- [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)
- [app/application/kernel/request_models.py](/Users/madhavpatel/New_demo copy/app/application/kernel/request_models.py)
- [app/data/ops/repository.py](/Users/madhavpatel/New_demo copy/app/data/ops/repository.py)
- [app/application/workflows/ops_console.py](/Users/madhavpatel/New_demo copy/app/application/workflows/ops_console.py)

### 2. Vertical pods

Each pod owns one capability family end to end:

- MCP tools in its domain
- specialist agents in its domain
- repo additions needed for its domain
- eval cases for its domain

Pods do not create new platform patterns.
They use the spine.

## Recommended pod split

### Pod A: Case Intelligence and Drafting

Scope:

- `list_ops_queue`
- `get_case_detail`
- `get_case_timeline`
- `get_case_tasks`
- `get_case_memory`
- `get_sla_snapshot`
- `summarize_queue_state`
- `draft_bank_escalation`

Agent outputs:

- stronger `approval_reviewer_assistant`
- new `queue_prioritization_agent`
- better shared case reasoning for all existing bank agents

### Pod B: Payments Diagnostics

Scope:

- `get_payment_mode_mix`
- `get_recent_transactions`
- `get_transaction_detail`
- `get_terminal_profile`
- `get_terminal_health_summary`
- `get_terminal_failure_breakdown`
- `retrieve_payments_knowledge`

Agent outputs:

- new `payments_exception_agent`

### Pod C: Settlement Completion

Scope:

- `list_settlements`
- `get_settlement_timeline`
- `get_reconciliation_breaks`
- `submit_settlement_intervention`
- `submit_reconciliation_review`

Agent outputs:

- stronger `reconciliation_investigation_agent`
- stronger `delayed_payout_agent`
- stronger `settlement_approval_draft_agent`

### Pod D: Risk, AML, and Compliance

Scope:

- `get_watchlist_hits`
- `get_screening_results`
- `get_aml_case_context`
- `get_velocity_anomalies`
- `get_dispute_risk_signals`
- `retrieve_compliance_guidance`

Agent outputs:

- stronger `risk_triage_agent`
- new `aml_investigation_agent`
- later `regulatory_reporting_agent`

### Pod E: Tech Ops and Monitoring

Scope:

- `get_api_health`
- `get_monitoring_alerts`
- `get_incident_context`
- `get_job_failures`
- `get_data_quality_checks`

Agent outputs:

- stronger `incident_response_agent`
- stronger `connector_supervisor_agent`

## Deterministic vs agentic boundary

These remain deterministic:

- case creation
- assignment
- approval decision
- case resolution
- connector dispatch
- SLA calculation
- write-back into external systems
- memory persistence

These are agentic:

- investigation
- summarization
- drafting
- evidence selection
- prioritization recommendations
- next-best-action suggestions

## Build contract for every new MCP tool

Every tool must have:

- typed input schema
- typed output envelope
- merchant or case scope enforcement
- bounded window or bounded limit
- verification status
- evidence ids
- notes/error behavior
- focused tests

No exceptions.

## Build contract for every new agent

Every agent must:

- declare its allowed MCP tool set
- use MCP only, not direct repositories
- return structured sections, not loose prose only
- degrade correctly when verification is partial
- have replay-style tests for its main scenarios

If an agent needs a new capability, build the MCP first.

## Parallel execution rules

Pods can build in parallel only if they do not fork the spine.

Safe parallel work:

- new MCP tools inside an existing schema/envelope pattern
- new specialist agents using existing MCP filters
- new tests and evals per pod

Unsafe parallel work:

- each pod inventing new response shapes
- each pod adding raw repo access into agents
- each pod adding its own approval/write path
- each pod redefining case memory or queue semantics

## Release gates

No pod is considered complete until it has:

- MCP tool tests
- agent tests
- replay scenarios
- verification downgrade behavior
- no direct repo access from agent code

## Suggested execution order

Run these in parallel:

1. Pod A: Case Intelligence and Drafting
2. Pod B: Payments Diagnostics
3. Pod D: Risk, AML, and Compliance
4. Pod E: Tech Ops and Monitoring

Keep Pod C: Settlement Completion active as the reference pod for write-back and
approval patterns.

That gives Bank Foundry:

- one stable platform spine
- multiple pods shipping in parallel
- expanding MCP coverage
- expanding specialist agents
- without turning the platform into agent sprawl

## What is a real product issue vs noise

Real program risks:

- agents bypassing MCP and calling repos directly
- each pod inventing different output shapes
- write paths leaking into agent logic
- missing replay coverage per agent

Usually not product issues:

- Starlette `python_multipart` warning in tests
- Vite chunk-size warning in frontend build

# Bank Foundry Execution Tracker

## Purpose

This document turns the platform map and tool matrix into a practical build
tracker.

For the parallel delivery model, use
`docs/BANK_FOUNDRY_PARALLEL_PROGRAM.md`.

For pod kickoff controls, use:

- `docs/BANK_FOUNDRY_MCP_CONTRACT.md`
- `docs/BANK_FOUNDRY_AGENT_CONTRACT.md`
- `docs/BANK_FOUNDRY_WORKFLOW_BOUNDARY.md`
- `docs/BANK_FOUNDRY_EXECUTION_BOARD.md`

It is organized around four buckets:

- `Implemented`
- `In progress / partial`
- `Next`
- `Blocked by integration`

This is the shortest path from the current product to the full Bank Foundry
platform.

## Current baseline

The current system already has the foundation for Bank Foundry:

- shared control plane and canonical request path
- separate merchant and bank surfaces
- bank ops console with cases, tasks, approvals, and timeline
- settlement-first specialist bank agents
- governed MCP capability boundary
- persistent case-scoped memory

Relevant code:

- `app/application/control_plane/*`
- `app/application/kernel/*`
- `app/application/workflows/*`
- `app/data/*`
- `app/ontology/*`
- `app/mcp_server/*`
- `app/agent/bank_ops_agents.py`
- `app/agent/mcp_client.py`

## 1. Implemented

| Area | What exists now | Main files |
|---|---|---|
| Control plane | Canonical request routing and session-key model | `app/application/control_plane/router.py`, `app/application/control_plane/sessions.py`, `app/application/kernel/request_models.py` |
| Surface split | Merchant-facing and bank-facing surfaces are separated | `app/api/server.py`, `app/application/workflows/merchant_surface.py`, `app/application/workflows/bank_surface.py` |
| Bank ops console | Queue, case detail, tasks, approvals, connector history | `app/application/workflows/ops_console.py`, `frontend/src/components/OpsConsoleView.jsx` |
| Case store | Cases, tasks, approvals, events, connector runs | `app/data/ops/repository.py` |
| Case memory | Pinned settlement/window/evidence state survives refreshes | `app/data/ops/repository.py`, `app/agent/bank_ops_agents.py`, `app/agent/mcp_client.py` |
| MCP boundary | Typed, guarded MCP server and tool registry | `app/mcp_server/server.py`, `app/mcp_server/tool_registry.py`, `app/mcp_server/guards.py`, `app/mcp_server/schemas.py` |
| Settlement MCP tools | Settlement detail, cashflow snapshot, shortfall explanation | `app/mcp_server/tool_registry.py`, `app/data/settlements/repository.py` |
| Settlement agents | Summary, operator note, approval draft | `app/agent/bank_ops_agents.py` |
| Merchant copilot | Live merchant chat runtime remains active | `app/agent/service.py` |

## 2. In Progress / Partial

| Area | Current state | Why it is still partial |
|---|---|---|
| Settlement connector | Real seam exists, but dispatch is simulated | No real external core-banking or settlement-ops integration yet |
| Settlement lane depth | Specialist agents exist, but settlement toolset is still narrow | Missing reconciliation, hold, payout-delay, and deduction-specific MCP tools |
| Operator memory control | Memory is persisted automatically | Operators cannot explicitly pin/unpin/edit context yet |
| Drafting and comms | Operator note and approval draft exist for settlement cases | Broader bank comms, merchant updates, and lane-specific drafting are not built |
| Replay evals | Focused tests exist | No full seeded ops benchmark suite and release gates yet |
| Observability | Good local test coverage and connector records exist | No full agent + MCP + workflow observability layer or audit dashboard yet |

## 3. Next

These are the highest-value next steps.

### Phase H: operator-controlled case memory

Goal:
- let operators explicitly control pinned settlement, window, and evidence

Build:

- `pin_case_context` MCP tool
- `update_case_memory` MCP tool
- bank UI controls to:
  - pin settlement id
  - pin date window
  - pin/remove evidence ids
  - clear stale memory

Success signal:
- operator can override copilot context without editing raw case data

### Phase I: finish the settlement MCP toolset

Goal:
- make settlement operations genuinely strong before broadening lanes

Build next:

- `get_settlement_reconciliation`
- `get_hold_reason`
- `get_payout_delay_context`
- `get_deduction_breakdown`
- `submit_settlement_intervention`
- `submit_reconciliation_review`

Build next agents:

- `reconciliation_investigation_agent`
- `delayed_payout_agent`

Success signal:
- held settlement, shortfall, payout delay, and reconciliation cases can all be worked credibly inside the bank console

### Phase J: queue and SLA hardening

Goal:
- make the ops console usable as a daily operator surface

Build:

- SLA snapshot and breach tooling
- blocked-state semantics
- duplicate suppression
- related-case linking
- queue ranking and prioritization logic

Success signal:
- queues behave like operational work queues, not just lists of cases

### Phase K: richer bank case copilot actions

Goal:
- move from summary-only case copilot to workflow-driving copilot

Build:

- stronger approval drafts
- better case-note refinement
- escalation draft generation
- evidence curation and missing-evidence prompts

Success signal:
- operators rely on the case copilot for actual case handling, not just summaries

## 4. Blocked By Integration

These items should be designed now, but full completion depends on external
systems or credentials.

| Area | What is blocked | Likely dependency |
|---|---|---|
| Real settlement execution | Replace simulated connector with real dispatch | bank/core-banking API contract, auth, payloads, environments |
| Core banking status sync | Real payout lifecycle and hold-state readback | production integration access |
| Payment rail visibility | Real external settlement rail state | rail/provider APIs |
| Monitoring and incident context | Alert and incident ingestion | Splunk, Dynatrace, ServiceNow, etc. |
| Risk and AML data | Screening and watchlist integration | risk/AML systems and data access |
| Regulatory reporting | Formal reporting feeds and policy data | compliance system integration |

## 5. Later Waves

These should come after settlement operations is solid.

### Wave 2: support and disputes

Build:

- refund and chargeback MCP tools
- support history and escalation-context tools
- `merchant_support_case_agent`
- `chargeback_review_agent`
- `refund_exception_agent`

### Wave 3: risk and AML

Build:

- risk profile and anomaly tools
- watchlist/screening tools
- policy/compliance retrieval tools
- `risk_triage_agent`
- `aml_investigation_agent`

### Wave 4: tech ops and platform supervision

Build:

- monitoring and incident MCP tools
- connector-health tools
- `incident_response_agent`
- `connector_supervisor_agent`

### Wave 5: regulatory and multi-lane maturity

Build:

- queue summarization
- reporting and compliance drafting
- `regulatory_reporting_agent`
- `queue_prioritization_agent`
- `approval_reviewer_assistant`

## 6. Readiness Checklist For Full Bank Foundry

Bank Foundry is ready for the full version when all are true:

- merchant and bank surfaces share one stable control plane
- settlement operations run end-to-end with real downstream execution
- support and risk lanes are active on the same core
- MCP is the governed capability boundary for bank-side agents
- case memory is durable and operator-controlled
- approvals and connector actions are fully auditable
- replay evals protect both merchant and bank workflows
- release gates exist for evidence quality, approval correctness, and connector behavior

## 7. Recommended Execution Order

1. operator-controlled case memory
2. settlement MCP tool expansion
3. reconciliation and payout-delay agents
4. real settlement connector
5. queue and SLA hardening
6. support lane
7. risk and AML lane
8. ops replay eval suite
9. incident / tech ops lane
10. regulatory reporting lane

## 8. Real Issues vs Noise

Real product gaps:

- connector execution is still simulated
- settlement tool coverage is not complete
- operators cannot directly control memory yet
- multi-lane bank coverage is still narrow
- replay eval gates are still missing

Usually not product issues:

- Starlette `python_multipart` warning in tests
- Vite chunk-size warning during frontend build

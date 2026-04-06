# Bank Foundry Execution Board

## Purpose

This is the source-of-truth board for starting parallel execution.

Use the active sprint launcher in
[BANK_FOUNDRY_SPRINT_SLICES.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_SPRINT_SLICES.md)
to start one chat per pod.

## Pod board

| Pod | Sprint 1 scope | Hard dependencies | Entry gate | Exit gate |
|---|---|---|---|---|
| Platform Foundations | MCP contract, agent contract, eval harness, release gate | none | kickoff review complete | contract docs merged, harness green |
| Workflow & Case System | `get_case_detail`, `get_case_timeline`, `get_case_tasks`, `get_case_memory`, `get_sla_snapshot`, `list_ops_queue`, `list_connector_runs` | Platform Foundations | MCP contract frozen | tools implemented + tests |
| Settlement Ops | settlement completion MCPs and settlement agent refinements | Platform Foundations, Workflow & Case | settlement contracts agreed | read tools + agent evals green |
| Merchant & Payments | payments diagnostics MCPs and `payments_exception_agent` | Platform Foundations | transaction/terminal contract frozen | MCPs + agent tests green |
| Support & Disputes | support-context MCPs on top of existing disputes surface | Platform Foundations, Workflow & Case | case-read MCPs available or mocked | support tools + agent tests green |
| Risk & Compliance | AML/risk read MCPs and `aml_investigation_agent` prep | Platform Foundations | policy/evidence rules frozen | MCPs + stricter verification tests green |
| Tech Ops & Supervision | health/alerts/jobs/DQ MCPs and stronger ops agents | Platform Foundations, Workflow & Case | observability fixtures ready | MCPs + incident tests green |

## Shared merge gate

No pod should merge a new MCP or agent without:

- contract-compliant tests
- replay or scenario coverage
- no direct repo access from agent code
- evidence and verification behavior checked

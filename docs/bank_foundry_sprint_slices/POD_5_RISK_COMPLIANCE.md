# Pod 5: Risk and Compliance

## Sprint 1 objective

Expand beyond the current risk snapshot into real risk and AML MCPs while
keeping the evidence and approval bar higher than the other pods.

## In scope

- `get_watchlist_hits`
- `get_screening_results`
- `get_aml_case_context`
- `get_velocity_anomalies`
- `get_dispute_risk_signals`
- `retrieve_compliance_guidance`
- strengthen `risk_triage_agent`
- add `aml_investigation_agent`

## First implementation slice

1. build `get_velocity_anomalies` and `get_dispute_risk_signals` first from local data
2. use seeded fixtures for watchlist/screening/compliance where integration is blocked
3. add `aml_investigation_agent` only after the core read MCPs are live

## Likely files

- [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)
- [app/data/transactions/repository.py](/Users/madhavpatel/New_demo copy/app/data/transactions/repository.py)
- [app/agent/bank_ops_contracts.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_contracts.py)
- [app/agent/bank_ops_agents.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_agents.py)
- [tests/fixtures/bank_foundry/watchlist_hits.json](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/watchlist_hits.json)
- [tests/fixtures/bank_foundry/compliance_guidance.json](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/compliance_guidance.json)

## Out of scope

- autonomous AML write actions
- regulator reporting delivery
- new workflow boundary rules

## Exit gate

- risk/AML read MCPs implemented or fixture-backed
- `risk_triage_agent` improved
- `aml_investigation_agent` added with strict verification downgrade behavior

## Pod kickoff brief

Build risk and AML read MCPs with stronger evidence discipline than the other pods. Use fixtures where integrations are blocked, and do not add autonomous write paths.

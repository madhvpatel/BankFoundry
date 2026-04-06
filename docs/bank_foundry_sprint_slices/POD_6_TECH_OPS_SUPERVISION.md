# Pod 6: Tech Ops and Supervision

## Sprint 1 objective

Turn the current internal-state incident logic into a fuller tech-ops MCP
surface for incident and supervision agents.

## In scope

- `get_api_health`
- `get_monitoring_alerts`
- `get_incident_context`
- `get_job_failures`
- `get_data_quality_checks`
- strengthen `incident_response_agent`
- strengthen `connector_supervisor_agent`

## First implementation slice

1. build internal-state versions of API health, incident context, and job failure MCPs first
2. use seeded alert and connector-health fixtures where external monitoring is blocked
3. extend incident and connector supervision agent tests with degraded/blocked cases

## Likely files

- [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)
- [app/data/ops/repository.py](/Users/madhavpatel/New_demo copy/app/data/ops/repository.py)
- [app/data/connectors/__init__.py](/Users/madhavpatel/New_demo copy/app/data/connectors/__init__.py)
- [app/agent/bank_ops_contracts.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_contracts.py)
- [app/agent/bank_ops_agents.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_agents.py)
- [tests/fixtures/bank_foundry/monitoring_alerts.json](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/monitoring_alerts.json)
- [tests/fixtures/bank_foundry/connector_health.json](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/connector_health.json)

## Out of scope

- external observability platform integration overhaul
- incident-management UI redesign
- queue-prioritization logic

## Exit gate

- tech-ops MCPs live or fixture-backed
- incident and connector supervision agents strengthened
- degraded-state tests green

## Pod kickoff brief

Expand the MCP surface for runtime, monitoring, connector, and data-quality supervision. Keep the agents advisory and use fixture-backed inputs when real integrations are not ready.

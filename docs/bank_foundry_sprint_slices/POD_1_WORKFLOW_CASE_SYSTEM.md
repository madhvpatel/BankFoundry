# Pod 1: Workflow and Case System

## Sprint 1 objective

Ship the missing case-read MCP surface so specialist agents can reason over the
same case substrate through MCP instead of workflow payloads.

## In scope

- `get_case_detail`
- `get_case_timeline`
- `get_case_tasks`
- `get_case_memory`
- `get_sla_snapshot`
- `list_ops_queue`
- `list_connector_runs`

## First implementation slice

1. add case-scoped input/output handling in [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)
2. reuse [app/data/ops/repository.py](/Users/madhavpatel/New_demo copy/app/data/ops/repository.py) and [app/data/connectors/__init__.py](/Users/madhavpatel/New_demo copy/app/data/connectors/__init__.py) reads only
3. add focused MCP tests and one bank-agent contract test proving the new tools are compatible

## Likely files

- [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)
- [app/mcp_server/schemas.py](/Users/madhavpatel/New_demo copy/app/mcp_server/schemas.py)
- [app/data/ops/repository.py](/Users/madhavpatel/New_demo copy/app/data/ops/repository.py)
- [tests/test_mcp_server.py](/Users/madhavpatel/New_demo copy/tests/test_mcp_server.py)

## Out of scope

- queue-prioritization agent
- new write workflows
- lane-specific support/risk data

## Exit gate

- all seven read MCPs implemented
- tests green
- no direct repo access added to agent code

## Pod kickoff brief

Build the missing case/workflow read MCPs first. Keep them normalized, case-scoped, evidence-backed, and read-only.

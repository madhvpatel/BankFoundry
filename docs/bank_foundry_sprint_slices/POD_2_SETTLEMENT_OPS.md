# Pod 2: Settlement Ops

## Sprint 1 objective

Finish the remaining settlement MCP surface and tighten settlement follow-through
without changing the shared platform contracts.

## In scope

- `list_settlements`
- `get_settlement_timeline`
- `get_reconciliation_breaks`
- `submit_settlement_intervention`
- `submit_reconciliation_review`
- strengthen `reconciliation_investigation_agent`
- strengthen `delayed_payout_agent`
- strengthen `settlement_approval_draft_agent`

## First implementation slice

1. build the remaining settlement read MCPs
2. expose the write-intent MCPs as approval-gated wrappers only
3. extend settlement agent tests for reconciliation and payout-delay follow-through

## Likely files

- [app/data/settlements/repository.py](/Users/madhavpatel/New_demo copy/app/data/settlements/repository.py)
- [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)
- [app/agent/bank_ops_agents.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_agents.py)
- [tests/test_mcp_server.py](/Users/madhavpatel/New_demo copy/tests/test_mcp_server.py)
- [tests/test_bank_ops_agents.py](/Users/madhavpatel/New_demo copy/tests/test_bank_ops_agents.py)

## Out of scope

- new settlement lane redesign
- merchant chat migration
- external connector redesign

## Exit gate

- remaining settlement MCPs live
- settlement write-intent MCPs approval-gated
- settlement agent suite green

## Pod kickoff brief

Treat settlement as the reference lane. Finish the remaining settlement MCPs and strengthen settlement agents, but keep all writes deterministic and approval-gated.

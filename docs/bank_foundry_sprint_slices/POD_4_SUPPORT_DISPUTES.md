# Pod 4: Support and Disputes

## Sprint 1 objective

Extend the already-built chargeback/refund surface with support-context MCPs and
the first real `merchant_support_case_agent`.

## In scope

- `get_support_case_history`
- `get_contact_and_escalation_context`
- `get_customer_service_context`
- strengthen `chargeback_review_agent`
- strengthen `refund_exception_agent`
- add `merchant_support_case_agent`
- add `draft_merchant_update`

## First implementation slice

1. add support-context MCPs using local case history or seeded fixtures where integration is missing
2. build `draft_merchant_update`
3. add `merchant_support_case_agent` with a strict support-safe tool filter

## Likely files

- [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)
- [app/data/ops/repository.py](/Users/madhavpatel/New_demo copy/app/data/ops/repository.py)
- [app/agent/bank_ops_contracts.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_contracts.py)
- [app/agent/bank_ops_agents.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_agents.py)
- [tests/fixtures/bank_foundry/support_case_history.json](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/support_case_history.json)

## Out of scope

- CRM integration overhaul
- support-specific workflow writes
- merchant portal UX changes

## Exit gate

- support MCPs live
- `merchant_support_case_agent` live
- draft merchant update path tested

## Pod kickoff brief

Build support-context MCPs on top of the shared case substrate and seeded fixtures, then add `merchant_support_case_agent` and `draft_merchant_update` without introducing new platform patterns.

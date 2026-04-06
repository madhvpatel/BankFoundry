# Pod 0: Platform Foundations

## Sprint 1 objective

Stabilize the platform spine so every other pod can ship against one enforced
contract.

## In scope

- lock MCP schema conventions
- lock bank-agent tool filter conventions
- strengthen shared eval harness
- add program-level release checks
- add mock/fixture standards for blocked integrations

## First implementation slice

1. Extend [tests/bank_foundry_eval_harness.py](/Users/madhavpatel/New_demo copy/tests/bank_foundry_eval_harness.py) with helper checks for:
   - verification downgrade behavior
   - evidence presence
   - read/write classification
2. Add missing contract tests around:
   - unknown tool behavior
   - guard failures
   - agent output section completeness
3. Add a lightweight shared release command note to [BANK_FOUNDRY_PARALLEL_EXECUTION_READY.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_PARALLEL_EXECUTION_READY.md).

## Likely files

- [app/mcp_server/schemas.py](/Users/madhavpatel/New_demo copy/app/mcp_server/schemas.py)
- [app/mcp_server/server.py](/Users/madhavpatel/New_demo copy/app/mcp_server/server.py)
- [app/agent/bank_ops_contracts.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_contracts.py)
- [tests/bank_foundry_eval_harness.py](/Users/madhavpatel/New_demo copy/tests/bank_foundry_eval_harness.py)
- [tests/test_bank_foundry_program_controls.py](/Users/madhavpatel/New_demo copy/tests/test_bank_foundry_program_controls.py)

## Out of scope

- new lane-specific MCPs
- new specialist agents
- workflow writes

## Exit gate

- shared harness updated
- program-control tests green
- no contract drift introduced

## Pod kickoff brief

Use the shared MCP and agent contracts as hard constraints. Tighten the eval harness and release checks for Bank Foundry without adding lane-specific logic.

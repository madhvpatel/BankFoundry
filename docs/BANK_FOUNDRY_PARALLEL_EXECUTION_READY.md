# Bank Foundry Parallel Execution Ready

## What was missing

The platform already had the core code, but it was missing the execution
controls needed for parallel pod work.

That meant pods could have drifted on:

- MCP shapes
- agent behavior
- workflow boundaries
- review rules
- mock data for blocked integrations

## What was added

- frozen MCP contract in [BANK_FOUNDRY_MCP_CONTRACT.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_MCP_CONTRACT.md)
- frozen agent contract in [BANK_FOUNDRY_AGENT_CONTRACT.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_AGENT_CONTRACT.md)
- deterministic workflow boundary in [BANK_FOUNDRY_WORKFLOW_BOUNDARY.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_WORKFLOW_BOUNDARY.md)
- execution board in [BANK_FOUNDRY_EXECUTION_BOARD.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_EXECUTION_BOARD.md)
- branch/review rules in [BANK_FOUNDRY_BRANCH_REVIEW_RULES.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_BRANCH_REVIEW_RULES.md)
- shared bank-agent tool filters in [app/agent/bank_ops_contracts.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_contracts.py)
- shared eval harness in [tests/bank_foundry_eval_harness.py](/Users/madhavpatel/New_demo copy/tests/bank_foundry_eval_harness.py)
- seeded blocked-integration fixtures in [/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry](</Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry>)

## Pod 0 tightening

What was broken:

- MCP descriptors did not carry explicit read versus drafting classification.
- release checks did not prove that unknown tools and guard failures stayed on the same error envelope
- release checks only sampled one agent summary shape, so section drift could slip through on other routes

What changed:

- MCP descriptors now declare `classification` directly in the shared schema
- the shared harness now checks classification, evidence presence, and verification downgrade behavior
- program-control tests now cover unknown tool errors, guard failures, and summary-section completeness across the implemented routes

## Shared release command

Run this before merging a Bank Foundry pod branch:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_bank_foundry_program_controls.py tests/test_mcp_server.py tests/test_bank_ops_agents.py tests/test_mcp_client.py -q
```

## How to verify

The shared release command above should pass.

For a quick Pod 0-only check, run:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_bank_foundry_program_controls.py -q
```

## Real issues vs noise

Real issues:

- a new MCP descriptor missing `classification`
- a new MCP tool that does not use the shared envelope
- verified tool output with no evidence ids and no explanation
- a bank case summary staying `verified` after one of its tool calls dropped to partial or unverified
- an agent importing repositories directly
- an agent bypassing tool filters
- a write path moving into agent code

Usually noise:

- Starlette `python_multipart` pending deprecation warning in tests
- Vite chunk-size warning during frontend build

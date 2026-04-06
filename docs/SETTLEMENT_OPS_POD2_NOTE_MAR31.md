# Settlement Ops Pod 2 Note

## What was missing

- The settlement MCP surface still lacked:
  - `list_settlements`
  - `get_settlement_timeline`
  - `get_reconciliation_breaks`
  - `submit_settlement_intervention`
  - `submit_reconciliation_review`
- The reconciliation and delayed-payout agents were still relying on a narrower settlement toolset.
- Settlement approval drafting was building payloads locally instead of using a shared MCP wrapper for approval-gated settlement actions.

## What changed

- Added the missing settlement read tools to the MCP server.
- Added two settlement write-intent MCPs that do not execute writes.
  - They only prepare an approval-gated wrapper.
  - They expose approval state, downstream target, and idempotency expectations.
- Strengthened settlement agent behavior:
  - `reconciliation_investigation_agent` now uses reconciliation-break buckets and nearby settlement context.
  - `delayed_payout_agent` now uses settlement timeline data and nearby settlement context.
  - `settlement_approval_draft_agent` now prefers the new settlement write-intent wrappers.
- Extended settlement MCP and agent tests for the new read tools and approval-gated wrapper behavior.

## How to verify

Run:

```bash
python -m unittest tests.test_mcp_server tests.test_bank_ops_agents
python -m unittest tests.test_bank_foundry_program_controls
```

Expected outcome:

- settlement MCP tests pass
- settlement agent routing tests pass
- Bank Foundry program-control contract tests still pass

## Real issue vs noise

Real issues for this slice:

- a missing settlement MCP tool
- a settlement action path that could bypass approval semantics
- settlement agent outputs that did not use the richer settlement evidence now available

Usually noise during verification:

- Python 3.13 `sqlite3` date-adapter deprecation warnings during test execution
- SQLAlchemy `ResourceWarning` messages about unclosed in-memory SQLite connections in tests

Those warnings did not block the Pod 2 settlement changes, but they are worth separate test-hygiene cleanup later.

# Bank Settlement Agents Phase 1

## What changed

The bank-side case copilot now uses **dedicated settlement agents** instead of only the generic MCP summary path.

New pieces:

- `app/agent/bank_ops_agents.py`
  - routes settlement case types to specialist MCP-backed agents
  - creates:
    - settlement case summary
    - draft operator note
    - draft approval request
- `app/mcp_server/tool_registry.py`
  - now exposes settlement-safe MCP tools:
    - `get_settlement_detail`
    - `get_settlement_cashflow_snapshot`
    - `explain_settlement_shortfall`
- `app/api/server.py`
  - bank case copilot endpoint now uses the dedicated bank agent router
- `frontend/src/components/OpsConsoleView.jsx`
  - bank case detail now shows:
    - dedicated agents used
    - draft operator note
    - draft approval request
  - operators can reuse the note draft and submit the approval draft directly

## What was missing before

Before this change, the bank case copilot used the same small MCP toolset for all cases:

- merchant profile
- window KPIs
- failure breakdown

That was fine for generic summaries, but weak for settlement operations. A held-settlement case needs settlement detail, cashflow context, and shortfall reasoning, not just payment KPIs.

## What the new flow does

For settlement case types like:

- `held_settlement`
- `processed_unsettled_payout`
- `settlement_shortfall_review`
- `reconciliation_mismatch`
- `delayed_payout_exception`

the bank case copilot now:

1. loads merchant profile
2. loads settlement cashflow context
3. loads the pinned settlement detail when a settlement id is available
4. runs shortfall explanation when the case or prompt needs it
5. returns:
   - executive summary
   - key findings
   - next best action
   - caveats
   - MCP tool calls
   - evidence ids
   - dedicated agents used
   - draft operator note
   - draft approval request

Non-settlement cases still fall back to the older generic MCP copilot path.

## How to verify

Backend:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_mcp_server.py tests/test_bank_ops_agents.py tests/test_ops_api_server.py tests/test_mcp_client.py -q
python -m compileall app/agent/bank_ops_agents.py app/mcp_server app/agent/mcp_client.py
```

Frontend:

```bash
cd "/Users/madhavpatel/New_demo copy"
npm run lint --prefix frontend -- --max-warnings=0
npm run build --prefix frontend
```

Manual UI check:

1. open the bank surface
2. open or create a settlement case
3. open the `Case copilot` panel
4. confirm you see:
   - dedicated agents
   - draft operator note
   - draft approval request
5. click `Use as case note` or `Use approval draft`

## Warnings that are not product bugs

- Starlette `python_multipart` pending deprecation warning
  - framework noise from a dependency import path
- Vite chunk size warning during frontend build
  - build optimization noise, not a runtime product failure

## Real limitations still left

- settlement detail is only as good as the current settlement schema
- settlement id pinning is still heuristic if the case was opened without an explicit `settlement:<id>` evidence id
- approval drafts still flow into the current internal approval + connector pipeline, not a real external bank integration yet

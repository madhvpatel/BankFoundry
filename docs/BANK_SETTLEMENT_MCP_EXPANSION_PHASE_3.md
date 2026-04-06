# Bank Settlement MCP Expansion Phase 3

## What changed

This phase deepens the bank-side settlement copilot without changing the merchant chat path.

Added new settlement MCP tools:

- `get_settlement_reconciliation`
- `get_hold_reason`
- `get_payout_delay_context`
- `get_deduction_breakdown`

These tools now sit behind the existing governed MCP boundary and return structured envelopes with:

- verification status
- evidence ids
- normalized data
- notes when the schema is incomplete

Added new specialist bank agents:

- `reconciliation_investigation_agent`
- `delayed_payout_agent`

Routing now works like this:

- `held_settlement` -> `settlement_case_summary_agent`
- `settlement_shortfall_review` / `reconciliation_mismatch` -> `reconciliation_investigation_agent`
- `processed_unsettled_payout` / `delayed_payout_exception` -> `delayed_payout_agent`

## What was wrong before

The bank case copilot had only a narrow settlement toolset:

- settlement detail
- settlement cashflow snapshot
- shortfall explanation

That meant:

- reconciliation cases were not using a dedicated reconciliation tool
- delayed payouts were not using a dedicated delay tool
- hold reason and deduction logic were being inferred from broader tools instead of being first-class MCP capabilities

## What is better now

- Settlement cases are routed to more appropriate specialist agents.
- Reconciliation reviews use verified deduction and reconciliation tools instead of relying on broad summary logic.
- Delayed payout reviews use explicit delay-state and hold-reason tools.
- Approval drafts now distinguish payout-delay work from reconciliation work.

## How to verify

Backend:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_mcp_server.py tests/test_bank_ops_agents.py tests/test_ops_api_server.py tests/test_mcp_client.py -q
```

Frontend:

```bash
cd "/Users/madhavpatel/New_demo copy"
npm run lint --prefix frontend -- --max-warnings=0
npm run build --prefix frontend
```

Manual product check:

1. Start the backend and frontend.
2. Open the bank surface.
3. Create or open:
   - a `reconciliation_mismatch` case
   - a `delayed_payout_exception` case
4. Open the case copilot panel.
5. Confirm the first listed agent matches the case type:
   - `reconciliation_investigation_agent`
   - `delayed_payout_agent`

## Known warnings

These are not product bugs:

- Starlette `python_multipart` pending deprecation warning during tests
- Vite chunk-size warning during frontend build

These are environment/build warnings and do not block the feature.

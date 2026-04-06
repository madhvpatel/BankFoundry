# Bank Case Copilot MCP Pilot

## What was added

The bank-facing case workflow now has a small **case copilot** pilot that uses the new MCP capability boundary.

This pilot is only in the **bank operations** path.
It does **not** change the merchant chat path.

### New backend path

- `POST /api/v1/ops/cases/{case_id}/copilot`

This route:

1. loads the selected case
2. checks lane access
3. derives a bounded analysis window
4. calls the MCP boundary
5. returns a structured case summary

### MCP tools used in the pilot

The case copilot currently uses only:

- `get_merchant_profile`
- `get_window_kpis`
- `get_failure_breakdown`

It does **not** use the merchant chat tool loop.

## Why this matters

This is the first place where the MCP boundary is being used in a real product workflow instead of only in isolated tests.

That gives us a safe place to prove:

- filtered tool visibility
- typed tool outputs
- evidence IDs
- verification-aware summaries

before moving any of this into merchant chat.

## What the case copilot shows

Inside the bank case detail view, the new panel shows:

- executive summary
- key findings
- next best action
- caveats
- MCP tool calls used
- evidence IDs

## What is still limited

This is still a pilot.

It does **not** yet:

- hold durable multi-turn copilot conversation state
- answer arbitrary freeform case questions
- use settlement-specific MCP tools
- replace the existing runbook, timeline, or approval logic

So right now it is best understood as:

- a structured case summary assistant
- backed by the MCP boundary
- inside the bank-facing case workflow

## How to verify

Run:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_mcp_server.py tests/test_mcp_client.py tests/test_ops_api_server.py -q
npm run lint --prefix frontend
npm run build --prefix frontend
```

Then:

1. open the bank surface
2. open a case
3. check the **Case copilot** panel
4. confirm that the summary includes evidence IDs and MCP tool calls

## Real issues vs noise

Real issues for this phase would be:

- case copilot loading on the merchant surface
- missing evidence IDs in the copilot response
- no lane access enforcement
- case copilot claiming certainty when tool verification is partial

Not a product issue:

- the existing Starlette `python_multipart` pending deprecation warning in broader backend test runs

That warning is framework noise and unrelated to this bank case copilot pilot.

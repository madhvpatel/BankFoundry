# Bank Case Memory Phase 1

## What changed

The bank-side case copilot now persists **case-scoped memory** in the ops data layer.

That memory survives:

- case detail refreshes
- copilot refreshes
- later operator actions like notes, approvals, and resolve flows

## What is stored

Each case can now keep:

- `pinned_entities`
  - merchant id
  - case type
  - settlement id when available
- `active_window`
  - start date
  - end date
  - why that window was chosen
- `confirmed_evidence_ids`
- `latest_summary`
  - executive summary
  - verification
  - last updated time
- `latest_tool_calls`

This is stored in the new `ops_case_memory` table through [repository.py](/Users/madhavpatel/New_demo copy/app/data/ops/repository.py).

## What was broken before

Before this change, the bank case copilot rebuilt its working context from the current case payload every time.

That meant:

- pinned settlement context could fall back to heuristics
- derived windows could shift across runs
- the last verified evidence set was not saved with the case

So the copilot could summarize the same case correctly once, but had no durable memory of that context.

## What changed in behavior

- case detail now returns `memory`
- the bank case copilot prefers saved memory before falling back to heuristics
- every copilot run writes back a fresh `memory_snapshot`
- the bank console now shows a small `Pinned context` section in the case copilot panel

## How to verify

Backend:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_ops_repository.py tests/test_bank_ops_agents.py tests/test_ops_api_server.py tests/test_mcp_server.py tests/test_mcp_client.py -q
python -m compileall app/data/ops/repository.py app/application/workflows/ops_console.py app/agent/bank_ops_agents.py app/agent/mcp_client.py
```

Frontend:

```bash
cd "/Users/madhavpatel/New_demo copy"
npm run lint --prefix frontend -- --max-warnings=0
npm run build --prefix frontend
```

Manual check:

1. open the bank surface
2. open a settlement case
3. load the `Case copilot`
4. confirm the `Pinned context` section appears
5. refresh the case or perform an operator action
6. confirm the pinned settlement/window/evidence context is still present

## Warnings that are not product bugs

- Starlette `python_multipart` pending deprecation warning
  - dependency noise
- Vite chunk-size warning
  - build-size optimization noise, not a runtime bug

## Current limitation

The memory is updated from the copilot summary path. It is not yet an editable operator memory model with explicit pin/unpin controls in the UI.

# Bank Case Memory Operator Controls Phase 2

## What changed

Bank operators can now **directly edit pinned case context** from the bank case
panel.

New operator controls:

- pin or change `settlement_id`
- pin or change the active date window
- pin or replace the confirmed evidence list
- clear pinned context when it becomes stale

This is now a deterministic workflow, not an implicit side effect of the case
copilot.

## What was missing before

Before this phase:

- case memory existed
- the bank copilot saved memory automatically
- operators could see pinned context

But they could **not control it directly**.

That meant:

- wrong or stale pinned settlement context could not be fixed in the UI
- operators had to wait for the copilot to infer new context
- the system had no explicit workflow for operator-owned context updates

## What changed in the backend

New deterministic request path:

- `RequestType.ops_case_memory_update`
- `POST /api/v1/ops/cases/{case_id}/memory`

New repository behavior:

- `update_case_memory_context(...)` in [repository.py](/Users/madhavpatel/New_demo copy/app/data/ops/repository.py)
- updates pinned settlement/window/evidence
- preserves the latest summary and tool-call history
- writes an audited `memory_updated` event to the case timeline

## What changed in the UI

The bank case copilot panel now includes:

- `Edit pinned context`
  - settlement id input
  - start/end date inputs
  - evidence list textarea
- `Save pinned context`
- `Clear pinned context`

That means the operator can now override or remove stale case memory directly.

## How to verify

Backend:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_ops_repository.py tests/test_ops_api_server.py tests/test_bank_ops_agents.py tests/test_mcp_server.py tests/test_mcp_client.py -q
python -m compileall app/application/kernel/request_models.py app/data/ops/repository.py app/application/workflows/ops_console.py app/api/server.py
```

Frontend:

```bash
cd "/Users/madhavpatel/New_demo copy"
npm run lint --prefix frontend -- --max-warnings=0
npm run build --prefix frontend
```

Manual UI check:

1. open the bank surface
2. open a settlement case
3. go to `Case copilot`
4. change settlement id / window / evidence in `Edit pinned context`
5. click `Save pinned context`
6. confirm:
   - `Pinned context` updates
   - case detail still loads
   - timeline includes a `memory updated` event
7. click `Clear pinned context`
8. confirm the settlement/window/evidence pins are removed

## Real issues vs noise

Real product scope:

- this phase only adds operator control over memory
- it does not yet add explicit MCP write tools for memory updates
- it does not yet add richer settlement MCP tools like hold-reason or payout-delay views

Not a product bug:

- Starlette `python_multipart` warning in tests
- Vite chunk-size warning during frontend build

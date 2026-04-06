# Revenue Recovery v1 Scaffold

## What was added

This repo now has an isolated scaffold for the Merchant Revenue Recovery Copilot runtime in:

- `app/revenue_recovery/models.py`
- `app/revenue_recovery/sql_compiler.py`
- `app/revenue_recovery/graph_v1.py`
- `app/revenue_recovery/write_policy.py`
- `app/revenue_recovery/checkpoint_store.py`
- `app/revenue_recovery/graders.py`
- `app/revenue_recovery/eval_runner.py`
- `app/revenue_recovery/demo_service.py`

There is also a separate test frontend and API surface for this scaffold:

- `POST /api/v1/revenue-recovery/preview/ask`
- `frontend/revenue-recovery.html`
- `frontend/src/RevenueRecoveryApp.jsx`

The scaffold is designed around a typed investigation graph instead of a free-form agent loop.

Main changes:

- added a strict shared state model with timezone-aware timestamps
- added typed runtime control fields for clarification, replans, and approval waits
- removed raw SQL generation from the model contract and replaced it with typed `QuerySpec`
- removed free-form `ORDER BY` expressions in favor of typed order targets
- added per-node write allowlists so nodes can only update owned state fields
- added in-memory checkpoint and trace persistence for local verification
- cleaned up the separate frontend build config so lint and multi-page Vite builds both work under ESM
- cleaned up preview runtime node closing so tracing/checkpoint flow is less repetitive and easier to follow

## What is intentionally not live yet

This scaffold is **not** wired into the active `/api/v1/ask` path yet.

That is intentional. The goal of this pass is to make the runtime contract testable in isolation before replacing the live flow.

The new preview endpoint and frontend are a **test harness** for the scaffold. They are separate from the live merchant chat.

## What was risky before

The earlier draft had a few production risks:

- hidden runtime state for replans
- no real clarify branch even though parse could request clarification
- string-based SQL escape hatches through metric expressions and order-by clauses
- documented write ownership, but no enforcement in code

This scaffold closes those gaps.

## How to verify

Run:

```bash
PYTHONPATH=. pytest \
  tests/test_revenue_recovery_models.py \
  tests/test_revenue_recovery_sql_compiler.py \
  tests/test_revenue_recovery_graph.py \
  tests/test_revenue_recovery_runtime_support.py \
  tests/test_revenue_recovery_demo_service.py \
  tests/test_api_server.py
```

What should pass:

- model validation for timezone-aware timestamps and typed runtime control
- SQL compiler allowlist and parameterization checks
- graph routing for clarification, replans, and approval waits
- checkpoint, trace, and grader round-trip behavior
- preview runtime and preview API endpoint contract behavior
- frontend lint and build for the separate preview page

To build the separate frontend:

```bash
cd frontend
npm run build
```

To open it in dev:

- run the backend as usual
- run `npm run dev` inside `frontend`
- open `/revenue-recovery.html`

## Real issues vs noise

Real issues:

- any `ValueError` about non-timezone-aware datetimes means the runtime contract is being violated
- any `SQLCompileError` about forbidden tables, columns, or tokens is a real safety failure
- any `WriteViolationError` means a node tried to write outside its owned state

Usually not a product issue:

- `langgraph is not installed` only matters if you try to compile the optional graph object with `build_graph()`
- broad `git status` noise outside this workspace is unrelated to this scaffold
- the Starlette `python_multipart` pending deprecation warning during tests is framework noise here; it is not caused by the revenue recovery scaffold

Fixed during cleanup:

- the separate frontend used `__dirname` inside an ESM Vite config, which made `npm run lint` fail even though the build succeeded
- the preview runtime used `locals()` to decide whether coverage and consistency existed; it now initializes those values explicitly

## Next step

The next safe move is to build real node handlers against this contract and then connect them behind a feature flag before replacing the live ask runtime.

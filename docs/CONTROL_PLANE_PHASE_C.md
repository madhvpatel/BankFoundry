# Control Plane Phase C

## What changed

Phase C moves the mixed snapshot, report, and merchant-catalog helper logic out
of the FastAPI server and into:

- `app/application/workflows/live_context.py`

This new module now owns:

- merchant snapshot assembly
- report payload shaping
- merchant label derivation
- merchant option loading

The API file:

- `app/api/server.py`

still builds requests and dependency bundles, but it no longer contains those
helper implementations.

## What was previously wrong

After Phase B, the server no longer owned the main workflow handlers, but it
still owned a set of SQL-heavy and snapshot-heavy helper functions.

That meant the API layer still had too much knowledge about:

- merchant data lookup
- report shaping
- workspace snapshot assembly

So the API boundary was better than before, but not clean yet.

## What changed in code

`app/api/server.py` now builds:

- `LiveContextDeps`
- `LiveSurfaceDeps`

and passes them down into the workflow layer.

The new `live_context.py` module does the actual helper work. This keeps the
server focused on:

- HTTP input/output
- merchant ID resolution
- canonical request construction
- control-plane routing

## What did not change

This phase does **not** yet:

- move dashboard SQL into a lower data-layer package
- create explicit repository modules
- add durable session state
- change endpoint payloads
- change frontend behavior

This is still a structural cleanup of the live path.

## Why this matters

The live code is now split more cleanly:

- `server.py` is closer to a true ingress adapter
- `live_surface.py` owns live workflow behavior
- `live_context.py` owns workflow-support data shaping

That makes later moves easier:

- extracting repository/data access modules
- sharing the same workflow helpers across more surfaces
- reducing API-layer coupling further

## How to verify

```bash
PYTHONPATH=. pytest \
  tests/test_control_plane_phase_a.py \
  tests/test_control_plane_phase_b.py \
  tests/test_live_context.py \
  tests/test_api_server.py \
  tests/test_unified_agent_service.py \
  tests/test_merchant_os.py \
  tests/test_prompt_loader.py \
  tests/test_insight_cards.py \
  tests/test_engine_signal_refinement.py \
  tests/test_response_code_mapping.py
```

Expected result after this phase:

- all tests above pass

## Real issues vs noise

Real issue:

- if new helper logic starts creeping back into `server.py`, the API layer will
  become mixed again

Usually not a product issue:

- the Starlette `python_multipart` deprecation warning during tests is still
  framework noise

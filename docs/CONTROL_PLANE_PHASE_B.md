# Control Plane Phase B

## What changed

Phase B moves the live execution logic out of the FastAPI server file and into
the application workflow layer.

The main live workflow module is now:

- `app/application/workflows/live_surface.py`

The API file:

- `app/api/server.py`

is now thinner. It still:

- validates request models
- resolves merchant IDs
- builds canonical requests
- sends them through the control-plane router

But it no longer owns the live workflow logic directly.

## What was previously wrong

After Phase A, the codebase had a control-plane router boundary, but the server
was still also the place where the actual live work happened.

That meant:

- the API layer still contained chat workflow logic
- the API layer still contained proactive/action workflow logic
- the API layer still contained dashboard workflow logic

So the architecture looked cleaner on paper than it really was in code.

## What changed in code

`app/api/server.py` now delegates each live request type to the workflow layer
through `app/application/workflows/live_surface.py`.

To keep the live path stable, the server still builds the dependency bundle at
request time. This is important because:

- existing tests patch symbols on `app.api.server`
- those patches still need to affect live behavior

So the extraction improves the architecture without breaking the current test
and patching model.

## What did not change

This phase does **not** yet:

- move raw SQL helpers fully out of the server
- add durable session persistence
- add a separate kernel executor
- change endpoint responses
- change frontend behavior

This is still a refactor, not a product rewrite.

## Why this matters

The API layer is now closer to a real ingress adapter.

That makes the next steps safer:

- moving more mixed logic out of `server.py`
- reusing the same workflow layer from more than one surface
- introducing a more explicit kernel/workflow split later

## How to verify

```bash
PYTHONPATH=. pytest \
  tests/test_control_plane_phase_a.py \
  tests/test_control_plane_phase_b.py \
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

- if a live handler starts bypassing the workflow layer again, the control-plane
  boundary becomes cosmetic instead of real

Usually not a product issue:

- the Starlette `python_multipart` deprecation warning during tests is still
  framework noise

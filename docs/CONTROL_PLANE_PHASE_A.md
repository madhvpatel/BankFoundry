# Control Plane Phase A

## What changed

Phase A of the target control-plane architecture is now in the live codebase.

New application-layer modules were added:

- `app/application/kernel/request_models.py`
- `app/application/kernel/response_models.py`
- `app/application/control_plane/sessions.py`
- `app/application/control_plane/router.py`

These define:

- a canonical request model
- a canonical response model
- deterministic session key generation
- a single control-plane router contract

## What changed in the live path

The current FastAPI server in `app/api/server.py` now acts more like an ingress adapter.

It still serves the same live endpoints, but it now does this first:

1. normalize endpoint input into a canonical request
2. derive a session key
3. send the request through the control-plane router
4. return the router payload

This is a structural change, not a user-facing behavior rewrite.

## What did not change yet

This phase does **not** add:

- durable session persistence
- a separate kernel executor module
- workflow state snapshots
- capability registry enforcement
- event streaming

So the architecture is improved, but the server still contains the actual handler implementations for now.

## Why this matters

Before this phase, the API layer was also the execution hub.

After this phase, the codebase now has:

- a formal request boundary
- a formal session-key model
- a formal router boundary

That makes later work much safer:

- moving chat into a kernel
- moving proactive jobs into the same runtime
- moving action workflows into the same runtime
- adding durable session state later

## How to verify

```bash
PYTHONPATH=. pytest \
  tests/test_control_plane_phase_a.py \
  tests/test_api_server.py \
  tests/test_unified_agent_service.py \
  tests/test_merchant_os.py \
  tests/test_prompt_loader.py \
  tests/test_insight_cards.py \
  tests/test_engine_signal_refinement.py \
  tests/test_response_code_mapping.py
```

Current result after this phase:

- `51 passed`
- `1 warning`

## Real issues vs noise

Real issue:

- if two surfaces that should share state end up with different session keys, the session-key model needs refinement

Usually not a product issue:

- the Starlette `python_multipart` pending deprecation warning during tests is framework noise

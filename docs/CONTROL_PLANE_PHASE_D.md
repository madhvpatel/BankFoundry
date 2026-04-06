# Control Plane Phase D

## What changed

Phase D introduces the first explicit live-path data repositories under:

- `app/data/merchants/repository.py`
- `app/data/transactions/repository.py`

These now own the SQL for:

- merchant option loading
- dashboard KPI and payment-mode aggregation

## What was previously wrong

Even after earlier cleanup phases, two important live-path SQL reads were still
living above the data layer:

- merchant catalog lookup
- dashboard transaction aggregation

That meant the application workflow layer still knew too much about raw SQL and
table inspection.

## What changed in code

The live path now splits like this:

- `app/api/server.py`
  - ingress
  - request normalization
  - dependency wiring
- `app/application/workflows/live_surface.py`
  - live workflow behavior
- `app/application/workflows/live_context.py`
  - snapshot/report helper assembly
- `app/data/merchants/repository.py`
  - merchant option queries
- `app/data/transactions/repository.py`
  - dashboard metric queries

The live endpoint payloads did not change.

## Why this matters

This is the first point where the live path now has a real application-to-data
boundary instead of only internal refactoring inside the API layer.

That makes the next steps more straightforward:

- moving more SQL from `merchant_os.py` and `tools.py` into repositories
- testing data access separately from workflow behavior
- keeping the control plane and workflow layers free of raw query logic

## How to verify

```bash
PYTHONPATH=. pytest \
  tests/test_data_repositories.py \
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

- if new live features add raw SQL back into the API or workflow layers, the
  new data boundary will erode quickly

Usually not a product issue:

- the Starlette `python_multipart` deprecation warning during tests is still
  framework noise

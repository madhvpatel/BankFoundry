# Control Plane Phase E

## What changed

Phase E extracts the SQL-heavy merchant-operations read logic out of:

- `app/merchant_os.py`

and into a dedicated data-layer repository:

- `app/data/merchant_ops/repository.py`

This repository now owns:

- table/column inspection
- connected-system detection reads
- terminal-scoped metric reads
- terminal-scoped failure-driver reads
- operating-signal reads

## What was previously wrong

`merchant_os.py` was still doing too many jobs at once:

- workflow composition
- report shaping
- action wiring
- and raw SQL access

That made it one of the biggest mixed modules left on the live path.

## What changed in code

`app/merchant_os.py` still exposes the same functions, but the SQL-heavy ones
are now thin wrappers over `app/data/merchant_ops/repository.py`.

This keeps the current live callers and tests stable while moving the data
access below the application layer.

## Why this matters

The live architecture is now more consistent:

- API/control plane at the top
- workflow modules in the middle
- data repositories below

It also reduces the risk of adding more raw query logic back into workflow code.

## What did not change

This phase does **not** yet:

- split the rest of `merchant_os.py`
- extract action persistence reads/writes
- extract report composition
- split the SQL-heavy paths inside `app/copilot/tools.py`

So `merchant_os.py` is cleaner, but still mixed overall.

## How to verify

```bash
PYTHONPATH=. pytest \
  tests/test_merchant_ops_repository.py \
  tests/test_merchant_os.py \
  tests/test_data_repositories.py \
  tests/test_control_plane_phase_a.py \
  tests/test_control_plane_phase_b.py \
  tests/test_live_context.py \
  tests/test_api_server.py \
  tests/test_unified_agent_service.py \
  tests/test_prompt_loader.py \
  tests/test_insight_cards.py \
  tests/test_engine_signal_refinement.py \
  tests/test_response_code_mapping.py
```

Expected result after this phase:

- all tests above pass

## Real issues vs noise

Real issue:

- if `merchant_os.py` starts adding fresh SQL directly again, the new repository
  boundary will erode

Usually not a product issue:

- the Starlette `python_multipart` deprecation warning during tests is still
  framework noise

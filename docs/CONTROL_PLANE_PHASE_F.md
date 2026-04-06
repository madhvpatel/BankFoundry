# Control Plane Phase F

## What changed

Phase F finishes the largest remaining `merchant_os` split in the live path.

New modules were added:

- `app/data/actions/repository.py`
- `app/data/proactive/repository.py`
- `app/application/workflows/reporting.py`

These now own:

- merchant action persistence reads and updates
- proactive card persistence and refresh-schedule persistence
- report pack / report brief / CSV generation

## What was previously wrong

`app/merchant_os.py` still mixed together:

- application workflow logic
- report rendering logic
- raw SQL persistence logic
- proactive schedule storage
- merchant action updates

That made it the most overloaded live module left in the codebase.

## What changed in code

`app/merchant_os.py` is now much closer to a compatibility facade:

- reporting functions delegate to `app/application/workflows/reporting.py`
- action persistence functions delegate to `app/data/actions/repository.py`
- proactive persistence functions delegate to `app/data/proactive/repository.py`
- merchant-ops SQL reads already delegate to `app/data/merchant_ops/repository.py`

The public function names stayed the same, so the live path and tests did not
need a breaking API change.

## Why this matters

The live architecture now has a clearer shape:

- control plane and API ingress
- workflow modules
- data repositories
- ontology semantics

This is the point where `merchant_os.py` stops being a giant mixed service and
starts becoming a transition layer over cleaner boundaries.

## What did not change

This phase does **not** yet:

- split the SQL-heavy paths inside `app/copilot/tools.py`
- turn the chat runtime into a fully durable session kernel
- remove every compatibility wrapper from `merchant_os.py`

So there is still more cleanup available, but the biggest live-path mixed module
is now substantially reduced.

## How to verify

```bash
PYTHONPATH=. pytest \
  tests/test_reporting_workflow.py \
  tests/test_action_repository.py \
  tests/test_proactive_repository.py \
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

- if new features put fresh SQL or report-generation logic back into
  `merchant_os.py`, the layer split will drift quickly

Usually not a product issue:

- the Starlette `python_multipart` deprecation warning during tests is still
  framework noise

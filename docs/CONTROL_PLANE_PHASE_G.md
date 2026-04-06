# Control Plane Phase G

## What changed

Phase G finishes the major `app/copilot/tools.py` split for the live chat path.

New repository modules were added:

- `app/data/settlements/repository.py`
- `app/data/disputes/repository.py`
- `app/data/terminals/repository.py`

Existing repositories were expanded:

- `app/data/merchants/repository.py`
- `app/data/transactions/repository.py`
- `app/data/actions/repository.py`

These now own the read and write operations that used to live directly inside
`app/copilot/tools.py`.

## What was previously wrong

`app/copilot/tools.py` mixed together:

- tool wrapper behavior
- merchant-scoped business helpers
- raw SQL reads
- terminal analytics queries
- settlement and dispute lookups
- merchant action inserts

That made the live copilot path hard to test and easy to break when changing
data access.

## What changed in code

`app/copilot/tools.py` is now mostly a thin wrapper layer:

- it normalizes dates and merchant scope
- it preserves the public tool function signatures
- it delegates the actual data access to `app/data/*` repositories

The only intentionally direct SQL tool left in `tools.py` is `sql_database`,
because that tool is explicitly the guarded raw-SQL escape hatch.

## Why this matters

The live architecture is now much more consistent:

- control plane and API ingress
- workflow modules
- ontology semantics
- data repositories
- thin tool wrappers on top

This reduces hidden coupling in the live chat surface and makes the remaining
agent/runtime work much easier to reason about.

## How to verify

```bash
PYTHONPATH=. pytest \
  tests/test_copilot_tool_repositories.py \
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

- if new feature work adds fresh SQL directly back into `app/copilot/tools.py`,
  the live-path layer split will drift again quickly

Usually not a product issue:

- the Starlette `python_multipart` pending deprecation warning during tests is
  still framework noise

# Ontology Phase 1 Migration

## What changed

Phase 1 of the layered-architecture plan is now in place.

A new canonical ontology package was added:

- `app/ontology/`

It now contains:

- recommendation types
- response-code semantics
- failure-code playbooks
- signal-module import surface

## What stayed compatible

The old imports under `app/intelligence/` still work for now.

Those files were turned into compatibility shims for:

- `app/intelligence/type.py`
- `app/intelligence/response_codes.py`
- `app/intelligence/playbooks.py`

This means the live app can start importing from `app/ontology/...` without forcing a risky big-bang rewrite.

## What the live system now uses

The live runner and evidence aggregator now import the new ontology package first.

That means the ontology layer has started becoming the canonical semantic layer even though some engine implementations still physically live under `app/intelligence/engines/`.

## What did not change yet

This phase did **not** split the mixed large modules yet:

- `app/copilot/tools.py`
- `app/merchant_os.py`
- `app/intelligence/runner.py`

Those are still the next major structural cleanup targets.

## How to verify

```bash
PYTHONPATH=. pytest \
  tests/test_response_code_mapping.py \
  tests/test_engine_signal_refinement.py \
  tests/test_unified_agent_service.py \
  tests/test_api_server.py \
  tests/test_merchant_os.py \
  tests/test_prompt_loader.py \
  tests/test_insight_cards.py
```

Current result after this phase:

- `48 passed`
- `1 warning`

## Real issues vs noise

Real issue:

- if new imports from `app/ontology/...` fail in live modules, the migration is incomplete

Usually not a product issue:

- the Starlette `python_multipart` pending deprecation warning during tests is framework noise
- `tests/test_intelligence_fixes.py` is not part of the live verification suite and still imports archived `app.intelligence.experiments`

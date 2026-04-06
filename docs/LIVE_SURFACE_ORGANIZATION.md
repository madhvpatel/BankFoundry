# Live Surface Organization

## What changed

The repo was reorganized so the active root app surface mostly reflects the live demo path.

Kept in the active app surface:

- `app/api/`
- `app/agent/`
- `app/copilot/kb.py`
- `app/copilot/toolcalling.py`
- `app/copilot/tools.py`
- `app/copilot/validation_server.py`
- `app/merchant_os.py`
- the intelligence modules still imported by the live backend
- the current live frontend in `frontend/`

Moved out of the active surface:

- future revenue recovery runtime and preview frontend
- legacy copilot backend
- retired chat path
- scenario engine and redundant intelligence code
- experimental intelligence helpers not used by the live path
- non-live tests and exploratory runtime docs

## Where things live now

Future development:

- `future/revenue_recovery/`

Archived code:

- `archive/backend/`
- `archive/intelligence/`

Archived tests:

- `archive/tests/`

Archived reports and runtime exploration docs:

- `archive/docs/`

## Why this was done

Before this cleanup, the root app surface mixed:

- live demo code
- future runtime work
- legacy implementations
- experiments

That made it hard to tell what was actually powering the demo.

The new rule is simple:

- active root paths should correspond to the live demo
- future work should live under `future/`
- old or retired paths should live under `archive/`

## How to verify

Backend checks:

```bash
PYTHONPATH=. pytest \
  tests/test_api_server.py \
  tests/test_unified_agent_service.py \
  tests/test_merchant_os.py \
  tests/test_prompt_loader.py \
  tests/test_insight_cards.py \
  tests/test_engine_signal_refinement.py \
  tests/test_response_code_mapping.py
```

Current result after the reorganization:

- `46 passed`
- `1 warning`

Frontend checks:

```bash
cd frontend
npm run lint
npm run build
```

## Real issues vs noise

Real issue:

- any import error from files still under `app/` or `frontend/` means the live-path split missed a dependency

Usually not a product issue:

- older docs inside `archive/` may still mention old paths because they were preserved as historical references
- the Starlette `python_multipart` pending deprecation warning in tests is framework noise, not caused by this reorganization
- parent-level `git status` output may show `/Users/madhavpatel/New_demo copy` as one untracked folder because this workspace sits inside a larger Git repo rooted at `/Users/madhavpatel`

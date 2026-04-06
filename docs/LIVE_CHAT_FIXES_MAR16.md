# Live Chat Fixes

## What was broken

Two demo problems were showing up in the live chat path:

- month questions like "in February" were not being resolved deterministically before the model ran
- the table shown under an answer could come from the first tabular tool result, even when the answer was using different evidence

That is why the app could ask unnecessary date questions or show a table that did not fully match the answer text.

## What changed

The live runtime in `app/agent/service.py` now does two extra checks before returning the answer:

- it resolves common time phrases in code first
  - examples: `February`, `this month`, `last month`, `today`, `yesterday`, `last 30 days`
  - the resolved window is then used as the default tool window
- it only renders a structured table when the answer points to matching evidence IDs
  - the runtime now hydrates the table from the matching tool result instead of picking the first table-like result

The prompt contract in `app/agent/AGENTS.md` was also tightened so the composer:

- uses a provided normalized time window instead of asking calendar-boundary questions
- selects evidence IDs for tables instead of inventing rows

## How to verify

Backend:

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

Focused checks added in `tests/test_unified_agent_service.py`:

- named month prompts now resolve to the correct month window before tool execution
- structured results now bind to the evidence selected in the answer

## Real issues vs noise

Real issue:

- if the answer still shows a table that does not match the cited evidence, that is a product bug

Usually not a product issue:

- the Starlette `python_multipart` pending deprecation warning during tests is framework noise

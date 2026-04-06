# Live Demo Polish Phase 1

## What changed

This pass improves the **live demo experience** without changing the live
architecture.

It focused on:

- cleaner merchant-facing answer text
- validation-aware caveats
- more specific follow-up suggestions
- clearer chat rendering in the frontend

## What was previously wrong

The live path could answer correctly but still feel rough because:

- some answers leaked internal mechanics like query or table-fix wording
- validation warnings appeared after the answer, but did not change the answer tone
- follow-up suggestions were often too generic
- the chat UI rendered most responses as one paragraph plus a footer

## What changed in code

### Backend

`app/agent/service.py` now:

- strips internal query-mechanics language from merchant-facing answers
- softens the final answer when validation is partial or unverified
- derives answer sections:
  - executive summary
  - key findings
  - next best action
  - caveats
- chooses more specific follow-ups based on the tools actually used

`app/intelligence/chat_reasoning.py` now returns clearer validation notices:

- partial validation = "some details are directional"
- unverified = "review the supporting rows before acting on this"

### Frontend

`frontend/src/components/ChatView.jsx` now renders structured answer blocks:

- executive answer
- key findings
- next best action
- caveats

The old table, evidence, clarification, and trace sections still work.

## How to verify

### Backend

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

### Frontend

```bash
cd frontend
npm run lint
npm run build
```

## Expected result

- answers should no longer mention internal SQL or table-correction mechanics
- partial validation should produce caveated language
- follow-ups should be more specific to settlements, transactions, failures, terminals, or disputes
- the chat UI should show clearer sections instead of a single dense paragraph

## Real issues vs noise

Real product issues:

- if answers still mention internal query mechanics, the composer or sanitization
  rules need another pass
- if the UI shows duplicated or conflicting sections, the backend section payload
  and frontend rendering are out of sync

Usually not a product issue:

- the Starlette `python_multipart` pending deprecation warning during tests is
  still framework noise

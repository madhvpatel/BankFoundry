# Unified Ask Rebuild

## What was broken

- The app had two public chat paths:
  - `/api/v1/chat/ask`
  - `/api/v1/copilot/ask`
- They used different control models, different response shapes, and different trust rules.
- That made the product hard to reason about and hard to debug. A frontend or test could hit the wrong path and get a completely different behavior.

## What changed

- The active chat API is now only `POST /api/v1/ask`.
- The frontend now sends chat requests to that single endpoint.
- The new runtime is `app/agent/service.py`.
- One runtime now owns:
  - tool planning
  - tool execution
  - final answer composition
  - validation summary
  - one trace object per turn
- The server returns one consistent response contract with:
  - `answer`
  - `sources`
  - `structured_result`
  - `clarifying_question`
  - `trace`

## How to verify

1. Start the backend and frontend.
2. Ask a simple question like `Who are you?`
   Expected: direct answer, no tool-heavy trace.
3. Ask a data question like `Show my recent settlements`
   Expected: answer plus `sources`, and usually a `structured_result`.
4. Ask a diagnostic question like `Why was my settlement short?`
   Expected: tool calls inside `trace`, evidence IDs in `sources`, and a grounded answer.
5. Run:

```bash
pytest tests/test_api_server.py tests/test_unified_agent_service.py
```

## Console noise

- Real issue:
  - Any request still going to `/api/v1/chat/ask` or `/api/v1/copilot/ask`. Those routes were removed from the live API and should be updated.
- Usually not a product issue:
  - Browser extension warnings in the frontend console.
  - Hot-reload reconnect messages from Vite/FastAPI during local development.

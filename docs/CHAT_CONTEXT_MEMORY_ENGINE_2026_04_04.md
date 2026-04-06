# Chat Context And Memory Engine

## What was broken

The merchant chat had a session key model, but no real memory behind it.

That meant:

- the backend only used whatever short `history` array the browser sent
- follow-up questions like "show the rows again" or "that settlement" could lose the earlier entity or date window
- a page refresh or a new request from another client could drop useful context
- there was no durable record of what the chat had already learned

## What changed

We added a durable chat memory layer for the merchant copilot.

- Chat turns now persist in `control_plane_sessions` and `control_plane_session_turns`
- The backend stores structured memory, not just raw text:
  - selected entities like settlement, chargeback, refund, terminal, and transaction IDs
  - active date window
  - active topics
  - remembered follow-ups
  - compact verified facts
- Before each new turn, the chat workflow now reloads that memory using the existing control-plane `session_key`
- The agent now receives `memory_context` so it can resolve references like "that settlement" or "same period"
- The API response now includes:
  - `session_key`
  - `thread_scope`
  - `memory`
- The frontend now shows a small thread-memory panel and a `New thread` button

## How to verify

1. Start the backend and frontend as usual.
2. In chat, ask a question with a specific entity and window, for example:
   - `Why is settlement 261 held in March 2026?`
3. Ask a follow-up without repeating the details:
   - `Show the rows again for that settlement.`
4. Confirm the answer still stays on the same settlement and window.
5. Check the thread memory card in the left panel:
   - pinned entity should include settlement `261`
   - the active window should still show March 2026
6. Click `New thread`.
7. Ask the same vague follow-up again.
8. Confirm the old settlement context is no longer reused in the new thread.

## Warnings and noise

These are not product failures:

- Vite build warning about chunks larger than 500 kB
  - this is a bundle-size optimization warning, not a broken UI
- `PendingDeprecationWarning` from `starlette.formparsers`
  - this comes from a dependency path during tests, not from the new memory engine

These are real product issues if they appear:

- `POST /api/v1/ask` returns `500`
  - chat memory or the upstream agent runtime is failing
- the chat forgets the entity after a second turn in the same thread
  - session persistence or memory extraction is broken
- the chat keeps reusing old context after clicking `New thread`
  - thread scoping is broken

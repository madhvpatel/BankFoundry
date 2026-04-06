# Ops Console Phase 3: Connector Execution Pipeline

This phase moves approved settlement actions beyond internal approval state.

## What was missing

Before this change, the ops console could:

- request approval
- approve or reject
- update the case state

But approval stopped at internal records. There was no connector execution record showing what happened after approval.

## What changed

- A settlement connector adapter was added in:
  - [app/data/connectors/settlement_ops.py](/Users/madhavpatel/New_demo copy/app/data/connectors/settlement_ops.py)
- It records connector executions in `ops_connector_runs`.
- Approved settlement actions now dispatch through this adapter from the approval flow.
- The current connector mode is `simulated` by default, so it creates:
  - connector run records
  - receipt refs
  - external refs
  - success / failure / skipped status
- Case detail now includes connector run history.
- The ops console UI now shows connector execution status directly on the case.

## What this means right now

- You do **not** need external settlement-system credentials yet.
- The pipeline is now structurally real, but the downstream call is simulated.
- Later, a real settlement/core-banking integration can replace the adapter logic without changing the case/approval workflow.

## How to verify

Backend:

```bash
PYTHONPATH=. pytest tests/test_ops_repository.py tests/test_ops_api_server.py tests/test_control_plane_replay_suite.py tests/test_api_server.py -q
```

Frontend:

```bash
npm run lint --prefix frontend
npm run build --prefix frontend
```

Manual:

1. Open the `Ops Console`.
2. Open or auto-intake a settlement case.
3. Request approval.
4. Approve it.
5. Verify the case now shows connector execution, a receipt ref, and an external ref.

## Noise vs real issues

- The Starlette `python_multipart` pending deprecation warning remains framework noise.
- Real product issues would be:
  - approval succeeds but no connector run is recorded
  - connector status never appears on the case
  - receipt/external refs are missing after a simulated successful dispatch

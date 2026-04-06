# Ops Console Phase 2: Settlement Auto-Intake

This phase makes the first ops lane more useful without changing the merchant-facing path.

## What was missing

In Phase 1, ops cases existed, but a settlement issue still had to be promoted manually from:

- a proactive card
- a merchant action
- or a chat finding

That meant the background monitoring layer could detect a settlement problem, but the internal ops queue would still stay empty unless someone clicked into it.

## What changed

- The background proactive refresh now auto-intakes settlement operations cases after cards are persisted.
- Only settlement-style operations cards are auto-intaked in this phase:
  - payout shortfall
  - settlement shortfall review
  - held / delayed / reconciliation-style settlement exceptions
- If the same proactive source is still open, the case is refreshed instead of duplicated.
- Proactive cards now store a `linked_case_id` so the UI can show that an ops case already exists.
- The proactive inbox now shows the linked ops case and disables the duplicate `Create ops case` button once auto-intake has already linked the signal.

## How to verify

Backend:

```bash
PYTHONPATH=. pytest tests/test_merchant_os.py tests/test_proactive_repository.py tests/test_ops_repository.py tests/test_ops_api_server.py -q
```

Full focused verification:

```bash
PYTHONPATH=. pytest tests/test_merchant_os.py tests/test_proactive_repository.py tests/test_ops_repository.py tests/test_ops_api_server.py tests/test_control_plane_replay_suite.py tests/test_api_server.py tests/test_unified_agent_service.py -q
npm run lint --prefix frontend
npm run build --prefix frontend
```

Manual:

1. Open the merchant workspace.
2. Refresh proactive cards for a merchant with settlement shortfall / payout issues.
3. Switch to `Ops Console`.
4. Verify a settlement ops case already exists in the queue.
5. Go back to the proactive inbox and confirm the card shows an `ops case ...` tag and the create button is disabled.

## Noise vs real issues

- The Starlette `python_multipart` pending deprecation warning is still framework noise.
- Real issues would be:
  - settlement signals not creating any ops case
  - repeated refreshes creating duplicate active cases
  - proactive cards not showing the linked case after intake

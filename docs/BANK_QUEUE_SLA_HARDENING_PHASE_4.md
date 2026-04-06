# Bank Queue and SLA Hardening Phase 4

## What changed

This phase makes the bank ops queue behave more like a real daily work queue instead of a simple case list.

Backend changes:

- queue rows now include:
  - `attention_level`
  - `waiting_on`
  - `approval_pending`
  - `connector_status`
  - `connector_attention`
  - task counts and overdue task counts
  - unassigned state
- queue summary now includes:
  - blocked count
  - SLA warning count
  - unassigned count
  - connector-attention count
  - overdue-task case count
  - active high-priority count
- queue ordering is now based on real operator pressure:
  - SLA breach
  - connector failure/skip
  - blocked state
  - awaiting approval
  - priority
  - aging

State-transition hardening:

- requesting approval clears stale blocked reasons
- rejected approvals now block the case with a clear blocked reason
- approved actions that hit connector failure or skip now push the case back to `BLOCKED`
- resolving a case clears the blocked reason

Frontend changes:

- the bank queue now shows more summary cards
- queue rows show waiting state, connector status, overdue tasks, and attention level
- case detail shows blocked reason, due date, waiting-on state, and queue pressure metadata

## What was weak before

Before this phase:

- the queue mostly sorted by priority and recent activity
- connector failures did not strongly change case visibility
- blocked state existed, but it was not strong enough as a queue concept
- SLA warnings and overdue tasks were not surfaced clearly to operators

## How to verify

Backend:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_ops_repository.py tests/test_ops_api_server.py tests/test_bank_ops_agents.py tests/test_mcp_server.py tests/test_mcp_client.py tests/test_control_plane_replay_suite.py tests/test_api_server.py -q
```

Frontend:

```bash
cd "/Users/madhavpatel/New_demo copy"
npm run lint --prefix frontend -- --max-warnings=0
npm run build --prefix frontend
```

Manual check:

1. Open the bank surface.
2. Create cases that land in different states:
   - open and unassigned
   - awaiting approval
   - blocked
3. Confirm the queue shows:
   - stronger sorting
   - attention badges
   - waiting-on hints
   - richer queue summary cards

## Known warnings

These are not product bugs:

- Starlette `python_multipart` pending deprecation warning during tests
- Vite chunk-size warning during frontend build

Those are framework/build warnings and do not block the queue/SLA feature.

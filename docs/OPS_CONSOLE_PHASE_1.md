# Ops Console Phase 1

This phase adds the first internal operations surface without breaking the merchant demo path.

## What was missing

Before this change, AcquiGuru could answer merchant questions and persist merchant-facing actions, but it did not have:

- a real internal case store
- queue and case detail workflows for operators
- approvals attached to cases
- a separate ops console surface
- a clean way to promote chat findings, proactive cards, or action items into operator work

## What changed

- The control plane now understands ops requests, lanes, case IDs, work item IDs, and request sources.
- A new ops ontology was added for cases, tasks, approvals, runbooks, SLA policies, links, and evidence refs.
- A new `app/data/ops` repository creates and manages:
  - `ops_cases`
  - `ops_tasks`
  - `ops_case_events`
  - `ops_approvals`
- A new ops workflow layer handles:
  - queue listing
  - case detail
  - case creation
  - promotion from proactive cards, merchant actions, and chat findings
  - assignment
  - notes
  - approval requests
  - approval decisions
  - resolution
- The frontend now has an `Ops Console` surface in the same deployment.
- Merchant chat, proactive cards, and action center items can now be promoted into ops cases.

## How to verify

Backend:

```bash
PYTHONPATH=. pytest tests/test_ops_repository.py tests/test_ops_api_server.py tests/test_control_plane_replay_suite.py tests/test_api_server.py -q
```

Frontend:

```bash
cd frontend
npm run lint
npm run build
```

Manual check:

1. Open the app.
2. Switch from `Merchant Workspace` to `Ops Console`.
3. Select a merchant and the `Settlement Ops` lane.
4. Open a manual case, or create one from chat / proactive / action center.
5. Verify queue, case detail, timeline, runbook steps, and approval inbox all update.

## Noise vs real issues

- The Starlette `python_multipart` pending deprecation warning in tests is framework noise.
- A real product issue would be:
  - a case not appearing after promotion
  - a lane/role mismatch returning the wrong queue
  - approval decisions not updating the case state

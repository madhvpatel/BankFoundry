# Phase 1 Agentic Runtime (Demo)

## What is implemented

- Single `/ask` runtime with Router → deterministic agents → Advisor synthesis.
- Deterministic evidence logging with reproducible `evid_<sha1[:16]>` IDs.
- Abstain + clarify behavior when confidence is low or required fields are missing.
- Proactive Action Center card generation and persistence.
- Scenario and evidence endpoints for dashboard integration.

## Agent chain

1. `router` (LLM): route, subtasks, confidence, required fields.
2. `facts` (deterministic): merchant KPIs and breakdowns.
3. `diagnostics` (deterministic): failure mode/time/terminal breakdowns.
4. `compliance` (deterministic): compliance/liability/tax/eligibility rules.
5. `or` (deterministic): scenario recommendations from OR registry.
6. `advisor` (LLM): synthesis constrained by deterministic payload and evidence IDs.

## Grounding controls

- Advisor cannot introduce unseen numbers/dates; otherwise fallback summary is returned.
- Every non-trivial recommendation appends evidence IDs (`EVID:<id>`).
- `response_meta.grounding_status`: `strict_pass` or `strict_fallback`.

## New/extended APIs

- `POST /ask` (extended metadata):
  - `route` (`analytics | knowledge | query | intelligence | mixed`)
  - `agents_invoked`
  - `evidence_ids`
  - `abstained`
  - `clarification_question`
  - `grounding_status`
- `POST /action_center/cards`
- `POST /evidence/lookup`
- `POST /scenario/run`
- `POST /copilot/clarify`

## Persistence tables

- `evidence_log`
- `insight_cards`
- `agent_trace_log`

Created automatically on startup via `ensure_phase1_tables()`.

## Demo run script

Use:

```bash
python /Users/madhavpatel/payments-intelligence-demo/scripts/run_phase1_demo.py --base-url http://localhost:8000
```

Outputs:

- JSONL transcript log in `/Users/madhavpatel/payments-intelligence-demo/logs`
- Summary JSON containing:
  - ask responses + response metadata
  - action center payload
  - evidence lookup payload
  - scenario run payload

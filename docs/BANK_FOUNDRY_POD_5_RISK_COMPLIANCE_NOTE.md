# Pod 5 Risk And Compliance Note

## What was broken

Pod 5 only had a basic risk snapshot before this change.

That meant:

- risk reviews could see merchant risk and KYC, but not recent velocity or dispute signals
- AML review did not have its own specialist agent
- watchlist, screening, and compliance reads were missing even in fixture-backed form
- the agent layer could not clearly separate live evidence from seeded placeholder data

## What changed

- added new read MCPs for:
  - `get_velocity_anomalies`
  - `get_dispute_risk_signals`
  - `get_watchlist_hits`
  - `get_screening_results`
  - `get_aml_case_context`
  - `retrieve_compliance_guidance`
- upgraded `risk_triage_agent` so it now uses current transaction and dispute signals
- added `aml_investigation_agent` for AML-oriented cases
- added seeded fixtures for blocked AML integrations in:
  - [watchlist_hits.json](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/watchlist_hits.json)
  - [screening_results.json](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/screening_results.json)
  - [compliance_guidance.json](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/compliance_guidance.json)
- kept fixture-backed AML tools intentionally `unverified` so the agents do not over-claim certainty

## How to verify

Run:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_bank_foundry_program_controls.py tests/test_mcp_server.py tests/test_bank_ops_agents.py -q
```

Then check:

- risk cases mention velocity or dispute signals when local data exists
- AML cases route to `aml_investigation_agent`
- watchlist, screening, and compliance MCPs return data but stay `unverified`
- settlement and support routes still keep the shared bank-agent output contract

## Real issues vs noise

Real issues:

- a risk or AML tool returning `verified` when it is actually fixture-backed
- an agent skipping the MCP layer and reading repositories directly
- a new tool changing the shared envelope shape
- AML or risk cases routing to the generic agent instead of their specialist agent

Usually noise:

- fixture-backed AML tools showing `unverified`
- Starlette `python_multipart` deprecation warnings during tests
- frontend build chunk-size warnings that do not affect these backend MCP changes

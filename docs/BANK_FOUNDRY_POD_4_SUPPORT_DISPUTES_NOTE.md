# Bank Foundry Pod 4 Notes

## What Was Missing

- Support cases did not have MCP reads for related case history, contact context, or escalation context.
- There was no merchant-facing draft path for support-safe updates.
- The chargeback and refund agents could not use support context or return a merchant update draft.
- There was no dedicated `merchant_support_case_agent` on the shared case substrate.

## What Changed

- Added `get_support_case_history`, `get_contact_and_escalation_context`, `get_customer_service_context`, and `draft_merchant_update`.
- Added support-context repository helpers on top of local ops cases, approvals, and case memory.
- Added seeded fallback support context from [`tests/fixtures/bank_foundry/support_case_history.json`](/Users/madhavpatel/New_demo copy/tests/fixtures/bank_foundry/support_case_history.json) when local support history is missing.
- Added the `merchant_support_case` runbook and the `merchant_support_case_agent`.
- Updated the chargeback and refund agents to read customer-service context and expose a `merchant_update` draft.
- Kept dispute reviews `verified` when the core dispute evidence is solid, even if optional support history is sparse.

## How To Verify

- Run `pytest '/Users/madhavpatel/New_demo copy/tests/test_mcp_server.py' -q`
- Run `pytest '/Users/madhavpatel/New_demo copy/tests/test_bank_ops_agents.py' -q`
- Run `pytest '/Users/madhavpatel/New_demo copy/tests/test_bank_foundry_program_controls.py' -q`
- Manual check:
  Open a support-lane `merchant_support_case` and confirm `drafts.merchant_update` is present.
- Manual check:
  Use a merchant with only fixture-backed support history and confirm the support MCPs return `unverified` with a fixture note instead of pretending the data is live.

## Real Issues Vs Noise

- Real issue: `fixture_fallback` or `unverified` on the new support tools means the case is using seeded support data, not live local history.
- Real issue: a missing preferred contact channel note means the case is safe to review, but not ideal to send externally without checking the merchant contact path first.
- Usually noise: the Starlette `python_multipart` deprecation warning in tests.
- Usually noise: the Vite chunk-size warning during frontend builds.

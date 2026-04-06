# Bank Settlement Connector Phase 5

## What changed

The settlement connector is no longer just:

- `simulated`
- or `unsupported`

It now supports a real HTTP connector contract.

New behavior in [settlement_ops.py](/Users/madhavpatel/New_demo copy/app/data/connectors/settlement_ops.py):

- `real` and `http` connector modes now map to an HTTP dispatch path
- connector requests include:
  - `request_id`
  - `case_id`
  - `action_type`
  - `requested_by`
  - `requested_at`
  - `idempotency_key`
  - nested `payload`
- outbound connector payloads are now enriched from our DB with:
  - case context
  - settlement id and merchant id
  - settlement row snapshot
  - reconciliation snapshot
  - deduction breakdown
  - payout delay snapshot
- auth is config-driven:
  - `none`
  - `bearer`
  - `api_key`
- connector runs now persist more execution metadata:
  - endpoint URL
  - idempotency key
  - HTTP status code

## What this means now

- If connector mode is `simulated`, behavior stays the same.
- If connector mode is `real` or `http` and config is valid, Bank Foundry can dispatch a real HTTP request.
- If connector mode is `real` or `http` but config is incomplete, the case fails safely and moves back into a visible blocked state.

That means the platform is now ready for a real downstream connector contract even though we do not yet have a live bank integration configured.

## DB enrichment behavior

Before dispatch, the connector now looks up the active case and settlement context from our own DB.

It derives:

- case metadata from `ops_cases`
- pinned settlement context from `ops_case_memory` when available
- settlement detail from `settlements`
- reconciliation context from `reconciliation_records`

This means the downstream connector no longer depends only on the operator approval payload. It now sends a richer, evidence-backed settlement context automatically.

## New config knobs

Added in [config.py](/Users/madhavpatel/New_demo copy/config.py):

- `SETTLEMENT_OPS_CONNECTOR_BASE_URL`
- `SETTLEMENT_OPS_CONNECTOR_ENDPOINT`
- `SETTLEMENT_OPS_CONNECTOR_AUTH_MODE`
- `SETTLEMENT_OPS_CONNECTOR_BEARER_TOKEN`
- `SETTLEMENT_OPS_CONNECTOR_API_KEY`
- `SETTLEMENT_OPS_CONNECTOR_API_KEY_HEADER`
- `SETTLEMENT_OPS_CONNECTOR_IDEMPOTENCY_HEADER`
- `SETTLEMENT_OPS_CONNECTOR_TIMEOUT_SECONDS`
- `SETTLEMENT_OPS_CONNECTOR_VERIFY_SSL`
- `SETTLEMENT_OPS_CONNECTOR_PARTNER_ID`

## How to verify

Backend:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_ops_repository.py tests/test_ops_api_server.py tests/test_control_plane_replay_suite.py tests/test_api_server.py -q
```

Compile check:

```bash
cd "/Users/madhavpatel/New_demo copy"
python -m compileall app/data/connectors/settlement_ops.py config.py
```

## Current safe default

Until real connector config is provided, the safest mode remains:

- `SETTLEMENT_OPS_CONNECTOR_MODE=simulated`

If you switch to `real` without the required endpoint/auth config, the connector will fail safely and the case will return to a blocked state for manual follow-up.

## Known warnings

This warning is not a product bug:

- Starlette `python_multipart` pending deprecation warning during tests

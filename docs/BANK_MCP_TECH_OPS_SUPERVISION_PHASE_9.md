# Bank MCP Expansion: Tech Ops Supervision

## What was broken

Pod 6 still had only partial tech-ops MCP coverage.

- `incident_response_agent` was still built mostly around background refresh and generic case state.
- `connector_supervisor_agent` did not have a dedicated API health, monitoring alert, or job failure view.
- blocked observability paths had seeded fixtures in `tests/fixtures/bank_foundry/`, but the MCP layer was not using them.
- degraded and blocked supervision cases were not covered well enough in tests.

## What changed

This phase added a fuller tech-ops MCP surface and rewired the supervision agents to use it.

New MCP tools:

- `get_api_health`
- `get_monitoring_alerts`
- `get_incident_context`
- `get_job_failures`
- `get_data_quality_checks`

Agent changes:

- `connector_supervisor_agent` now reviews API health, monitoring alerts, and internal job failures in addition to connector history and SLA state.
- `incident_response_agent` now uses incident context, API health, monitoring alerts, job failures, and data quality checks.
- blocked monitoring cases now fall back to the seeded `monitoring_alerts.json` and `connector_health.json` fixtures instead of inventing observability state.

Tests:

- added MCP coverage for internal-state tech-ops tools
- added fixture-backed blocked-monitoring coverage
- added degraded connector and incident agent cases

## How to verify

```bash
cd "/Users/madhavpatel/New_demo copy"
find tests/__pycache__ -name '*.pyc' -delete 2>/dev/null || true
PYTHONPATH=. pytest tests/test_mcp_server.py tests/test_bank_ops_agents.py tests/test_ops_api_server.py tests/test_bank_foundry_program_controls.py tests/test_mcp_client.py -q
python -m compileall app/mcp_server app/agent/bank_ops_agents.py app/agent/bank_ops_contracts.py
```

Expected result:

- the targeted Bank Foundry MCP and agent suites pass
- compile checks pass

## Real issues vs noise

Real product issues:

- missing tech-ops MCP tools for API health, monitoring alerts, incident context, job failures, and data quality
- supervision agents not downgrading verification when monitoring is fixture-backed
- no degraded or blocked supervision test coverage

Usually noise:

- Starlette `python_multipart` pending deprecation warning during tests
- stale `tests/__pycache__` files showing old assertion source after test edits; clearing the cache fixes the runner output and does not reflect a product defect

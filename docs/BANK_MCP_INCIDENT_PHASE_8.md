# Bank MCP Expansion: Incident and Tech-Ops Signals

## What changed

This phase added the first Bank Foundry MCP tools and specialist agent for internal operational incidents.

New MCP tools:
- `get_background_refresh_health`

New case types and runbooks:
- `background_refresh_issue`
- `incident_response`

New specialist agent:
- `incident_response_agent`

## What this means

Bank Foundry can now reason about:
- whether the background proactive refresh is due or stale
- how many proactive cards are currently stored
- the latest case timeline state
- connector run history for the case
- the runbook and SLA policy attached to the case

This is not external observability yet. It is Bank Foundry’s own internal operational state exposed through the MCP boundary.

## Why this approach

The repo does not yet contain real Splunk, Datadog, or external monitoring integrations.

So this phase intentionally used only state we already own:
- proactive refresh schedule
- proactive card backlog
- case timeline
- connector run history

That keeps the incident agent evidence-backed and avoids fake monitoring abstractions.

## Files changed

- `app/mcp_server/schemas.py`
- `app/mcp_server/__init__.py`
- `app/mcp_server/tool_registry.py`
- `app/ontology/ops.py`
- `app/agent/bank_ops_agents.py`
- `tests/test_mcp_server.py`
- `tests/test_bank_ops_agents.py`
- `tests/test_ops_api_server.py`

## How to verify

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_mcp_server.py tests/test_bank_ops_agents.py tests/test_ops_api_server.py tests/test_mcp_client.py tests/test_control_plane_replay_suite.py tests/test_api_server.py tests/test_ops_repository.py -q
python -m compileall app/mcp_server app/agent/bank_ops_agents.py app/ontology/ops.py
```

Expected result:
- tests pass
- compile check passes

## Warnings

The remaining backend warning is:
- Starlette `python_multipart` pending deprecation warning

That is framework noise, not a Bank Foundry product problem.

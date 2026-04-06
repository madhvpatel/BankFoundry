# Bank MCP Expansion: Risk and Connector Intelligence

## What changed

This phase expanded the Bank Foundry MCP boundary again, this time for:
- merchant risk and KYC review
- connector execution follow-up

New MCP tools:
- `get_risk_profile`
- `get_kyc_status`
- `get_policy_rule_explanation`
- `get_connector_health`

New specialist agents:
- `risk_triage_agent`
- `connector_supervisor_agent`

## What this unlocks

Bank Foundry can now:
- review merchant risk and KYC state through governed MCP tools
- explain the case runbook, SLA, and approval state through MCP
- inspect connector run history for a case instead of treating connector follow-up as generic queue state
- route risk and connector follow-up cases to dedicated specialist agents

## Implementation notes

- Risk and KYC data comes from the existing merchant repository path.
- Connector health comes from the existing `ops_connector_runs` history.
- Draft note and approval output still go through the MCP drafting tools, so the agents stay bounded and auditable.
- Merchant chat was not changed.

## Files changed

- `app/mcp_server/tool_registry.py`
- `app/agent/bank_ops_agents.py`
- `app/ontology/ops.py`
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

That is framework noise, not a Bank Foundry product issue.

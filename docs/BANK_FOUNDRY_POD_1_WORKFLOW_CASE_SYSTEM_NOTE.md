# Bank Foundry Pod 1: Workflow and Case System

## What was broken

Specialist bank agents did not have a full MCP read surface for case and queue data.

That meant:

- there was no MCP tool for full case detail
- there was no MCP tool for raw case timeline, task list, memory, or SLA state
- there was no MCP tool for queue listing or full connector run history
- agents had to rely on workflow payloads or narrower summary tools for parts of case reasoning

## What changed

This pod added seven read-only MCP tools:

- `get_case_detail`
- `get_case_timeline`
- `get_case_tasks`
- `get_case_memory`
- `get_sla_snapshot`
- `list_ops_queue`
- `list_connector_runs`

The new tools reuse the existing ops repository and connector read paths and return the standard MCP envelope.

`get_case_detail` is shaped like the workflow case-detail substrate so future agent work can consume the same structure through MCP.

The `connector_supervisor_agent` was also switched to use the new case-read MCP surface for:

- case timeline
- connector run history
- SLA state

## How to verify

Run:

```bash
pytest -q tests/test_mcp_server.py
pytest -q tests/test_bank_ops_agents.py
pytest -q tests/test_ops_api_server.py
pytest -q tests/test_mcp_client.py
```

Expected result:

- all tests pass
- new tool descriptors appear in the MCP tool list
- connector supervisor summaries show the new MCP tool calls:
  `get_case_timeline`, `list_connector_runs`, `get_sla_snapshot`

## Real issue vs noise

Real product issues:

- a new case tool returns `status="error"`
- merchant and case scope do not match
- a required tool is missing from the MCP tool list
- connector supervisor summaries stop including MCP-backed tool calls

Usually noise:

- Starlette `python_multipart` pending deprecation warning during tests
- Vite chunk-size warning during frontend builds
- browser extension console noise if the UI is open in a local browser

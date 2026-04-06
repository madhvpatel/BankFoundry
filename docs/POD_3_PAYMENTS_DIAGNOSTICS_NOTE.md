# POD 3 Payments Diagnostics Note

## What was missing

Pod 3 needed a payments diagnostics MCP surface plus a bank-side specialist agent.

Before this change:

- MCP had settlement, dispute, risk, and ops tools, but not the bounded payment and terminal reads from the Pod 3 brief
- there was no `payments_exception_agent`
- transaction detail and recent-transaction reads were less tolerant of thinner table shapes

## What changed

- added these MCP tools:
  - `get_payment_mode_mix`
  - `get_recent_transactions`
  - `get_transaction_detail`
  - `get_terminal_profile`
  - `get_terminal_health_summary`
  - `get_terminal_failure_breakdown`
  - `retrieve_payments_knowledge`
- added a lightweight local payments knowledge lookup over `bank_kb/`
- added `payments_exception_agent` with a constrained tool filter
- routed payment exception case types to the new specialist agent
- added MCP coverage and one representative agent replay for terminal-linked payment failures

## How to verify

Run:

```bash
pytest tests/test_mcp_server.py tests/test_bank_ops_agents.py tests/test_mcp_client.py tests/test_copilot_tool_repositories.py -q
```

What to look for:

- the new MCP tools return normal `ToolEnvelope` responses
- `payments_exception_agent` is selected for `payment_exception` cases
- the replay mentions payment-mode skew, recent failed transactions, and terminal-linked context

## Real issue vs noise

Real product issues:

- MCP tool returns `status="error"` because the schema is missing required transaction columns
- `payments_exception_agent` cannot see one of its allowed tools
- terminal-scoped failure or health tools return empty results when test data clearly contains matching rows

Usually environment or build noise:

- Starlette `python_multipart` pending deprecation warning in tests
- Vite chunk-size warning during frontend build

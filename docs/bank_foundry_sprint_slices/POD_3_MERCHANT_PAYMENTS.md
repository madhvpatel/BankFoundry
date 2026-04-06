# Pod 3: Merchant and Payments

## Sprint 1 objective

Create the payments-diagnostics MCP surface that will power a new
`payments_exception_agent`.

## In scope

- `get_payment_mode_mix`
- `get_recent_transactions`
- `get_transaction_detail`
- `get_terminal_profile`
- `get_terminal_health_summary`
- `get_terminal_failure_breakdown`
- `retrieve_payments_knowledge`
- new `payments_exception_agent`

## First implementation slice

1. ship the transaction and terminal read MCPs first
2. wire a thin `payments_exception_agent` on top of those MCPs
3. add one representative agent replay for payment-mode skew or terminal-linked failures

## Likely files

- [app/data/transactions/repository.py](/Users/madhavpatel/New_demo copy/app/data/transactions/repository.py)
- [app/data/terminals/repository.py](/Users/madhavpatel/New_demo copy/app/data/terminals/repository.py)
- [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)
- [app/agent/bank_ops_contracts.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_contracts.py)
- [app/agent/bank_ops_agents.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_agents.py)

## Out of scope

- merchant live chat migration onto MCP
- support-lane customer history
- write workflows

## Exit gate

- payments-diagnostics MCPs implemented
- `payments_exception_agent` added with a constrained tool filter
- tests green

## Pod kickoff brief

Build the payments-diagnostics MCP layer first, then add a thin `payments_exception_agent` on top of it. Do not touch merchant chat yet.

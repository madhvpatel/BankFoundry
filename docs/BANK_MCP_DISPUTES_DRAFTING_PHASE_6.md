# Bank MCP Expansion: Disputes and Drafting

## What changed

This phase expanded the Bank Foundry MCP boundary beyond settlement-only work.

New MCP tools:
- `get_chargeback_summary`
- `list_chargebacks`
- `get_chargeback_detail`
- `get_refund_summary`
- `list_refunds`
- `get_refund_detail`
- `summarize_case_timeline`
- `draft_case_note`
- `draft_approval_request`

New specialist bank agents:
- `chargeback_review_agent`
- `refund_exception_agent`
- `case_note_draft_agent`
- `approval_reviewer_assistant`

The specialist agents were added in the bank case copilot path, not the merchant chat path.

## What was missing before

Before this phase, the MCP layer was strong for settlement investigations but weak for:
- chargeback and refund review
- internal case-note drafting
- approval-draft generation for non-settlement cases

That meant new bank-side case types would fall back to the generic ops copilot too often.

## How it works now

- Dispute and refund data comes from the existing repository layer in `app/data/disputes/repository.py`.
- The MCP server wraps that data in typed envelopes with verification, notes, and evidence IDs.
- The bank case router now sends:
  - `chargeback_review` cases to `chargeback_review_agent`
  - `refund_exception` cases to `refund_exception_agent`
- Note and approval drafts for those cases are generated through MCP tools, so they stay bounded and auditable.

## Files changed

- `app/data/disputes/repository.py`
- `app/data/disputes/__init__.py`
- `app/mcp_server/schemas.py`
- `app/mcp_server/__init__.py`
- `app/mcp_server/tool_registry.py`
- `app/ontology/ops.py`
- `app/agent/bank_ops_agents.py`
- `tests/test_mcp_server.py`
- `tests/test_bank_ops_agents.py`
- `tests/test_ops_api_server.py`

## How to verify

Run:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_mcp_server.py tests/test_bank_ops_agents.py tests/test_ops_api_server.py tests/test_mcp_client.py tests/test_control_plane_replay_suite.py tests/test_api_server.py tests/test_ops_repository.py -q
python -m compileall app/mcp_server app/agent/bank_ops_agents.py app/data/disputes/repository.py app/ontology/ops.py
```

Expected result:
- tests pass
- compile check passes

## Warnings

The remaining backend warning is:
- Starlette `python_multipart` pending deprecation warning

This is framework noise, not a Bank Foundry product bug.

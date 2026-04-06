# Bank Foundry Agent Contract

## Purpose

This is the frozen contract for bank-side specialist agents.

## Source of truth

- [app/agent/bank_ops_contracts.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_contracts.py)
- [app/agent/bank_ops_agents.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_agents.py)
- [app/agent/mcp_client.py](/Users/madhavpatel/New_demo copy/app/agent/mcp_client.py)

## Rules

Every bank agent must:

- use MCP only
- avoid direct repository imports
- avoid direct external writes
- respect its assigned tool filter
- degrade certainty when verification is partial

## Required output shape

Every bank case agent must return:

- `summary`
- `answer_sections.executive_summary`
- `answer_sections.key_findings`
- `answer_sections.next_best_action`
- `answer_sections.caveats`
- `verification`
- `tool_calls`
- `evidence_ids`

## Boundary

Agents may:

- investigate
- summarize
- draft
- recommend
- prioritize

Agents may not autonomously:

- approve actions
- dispatch connectors
- resolve cases
- compute SLA outside workflow code
- bypass approval gates

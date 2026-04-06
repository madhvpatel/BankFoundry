# Bank Foundry Workflow Boundary

## Purpose

This note locks the deterministic workflow boundary so pods do not leak writes
into agent code.

## Source of truth

- [app/application/workflows/ops_console.py](/Users/madhavpatel/New_demo copy/app/application/workflows/ops_console.py)
- [app/data/ops/repository.py](/Users/madhavpatel/New_demo copy/app/data/ops/repository.py)
- [app/data/connectors/settlement_ops.py](/Users/madhavpatel/New_demo copy/app/data/connectors/settlement_ops.py)

## Deterministic workflows

These stay deterministic:

- case creation and promotion
- assignment
- note persistence
- approval requests
- approval decisions
- case resolution
- memory persistence
- connector dispatch
- SLA calculation

## Agentic work

These stay agentic:

- investigation
- summarization
- drafting
- evidence assembly
- next-best-action suggestions
- prioritization recommendations

## Rule

If something changes state outside the response itself, it must go through
workflow or repository code, not directly through an agent.

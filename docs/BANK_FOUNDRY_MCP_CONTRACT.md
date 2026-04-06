# Bank Foundry MCP Contract

## Purpose

This is the frozen MCP contract for Bank Foundry pods.

## Source of truth

- [app/mcp_server/schemas.py](/Users/madhavpatel/New_demo copy/app/mcp_server/schemas.py)
- [app/mcp_server/guards.py](/Users/madhavpatel/New_demo copy/app/mcp_server/guards.py)
- [app/mcp_server/tool_registry.py](/Users/madhavpatel/New_demo copy/app/mcp_server/tool_registry.py)

## Required input rules

- every tool must use a typed input model
- merchant-scoped tools must require `merchant_id`
- case-scoped tools must require `merchant_id` and `case_id`
- list tools must enforce bounded limits
- windowed tools must enforce bounded dates

## Required output envelope

Every tool returns the same `ToolEnvelope` shape:

- `status`
- `verification`
- `tool_name`
- `merchant_id`
- optional `window`
- `data`
- `evidence_ids`
- `notes`
- optional `error_message`

## Required classifications

Every tool must be explicitly treated as one of:

- `read`
- `drafting`
- `write`

That classification must be present on the shared `MCPToolDescriptor`.

Write tools must also define:

- approval requirement
- downstream target
- idempotency expectations

## Verification rules

Allowed values:

- `verified`
- `unverified`
- `not_applicable`

Use `verified` only when the result is grounded in current tool evidence.

## Evidence rules

- every tool must emit `evidence_ids`
- if evidence is incomplete, say so in `notes`
- pods may add fields under `data`, not invent new top-level response shapes

## Error rules

- normalize tool failures into the same envelope
- do not leak raw provider or database exceptions through the agent boundary

# MCP Capability Boundary Phase 1

## What was added

We added a small internal MCP-style capability layer under:

- `/Users/madhavpatel/New_demo copy/app/mcp_server`
- `/Users/madhavpatel/New_demo copy/app/agent/mcp_client.py`

This phase now exposes four safe tools:

- `get_merchant_profile`
- `get_window_kpis`
- `get_failure_breakdown`
- `run_verified_sql`

Each tool now returns the same kind of structured envelope:

- `status`
- `verification`
- `tool_name`
- `merchant_id`
- `window` when relevant
- `data`
- `evidence_ids`
- `notes`

## Why this matters

Before this, tool access was mostly tied to the live merchant chat runtime.

Now there is a separate governed capability boundary that can be reused by:

- merchant chat later
- ops copilot later
- future bounded investigation runtimes

The important change is not "more tools".
The important change is "typed, guarded, verification-aware tool access".

## What is guarded

The MCP layer currently enforces:

- `merchant_id` is required
- date windows must be bounded
- failure breakdown limit is capped
- verified SQL must include merchant and date placeholders
- verified SQL is restricted to the configured transaction fact table
- verified SQL blocks joins, comments, and multi-statement queries
- only approved tools are visible to each client through `tool_filter`

This is why the internal client can be restricted to only the tools it should see.

## What is not implemented yet

This is still an internal prototype boundary.

It does **not** yet include:

- a networked MCP server process
- external MCP clients
- live routing from `/api/v1/ask`
- audit dashboards for MCP usage

So this phase proves the pattern without changing the live merchant surface.

## How to verify

Run:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_mcp_server.py tests/test_mcp_client.py -q
```

You should see both tests pass.

## Real issues vs noise

Real product issues for this phase would be:

- missing evidence IDs
- unbounded date windows getting through
- filtered clients being able to call hidden tools
- tools returning loose or inconsistent shapes

Not a product issue:

- the existing Starlette `python_multipart` pending deprecation warning in broader test runs

That warning is framework noise and unrelated to this MCP boundary work.

# AcquiGuru LangGraph SQL Design

## Goal

Use a project-specific LangGraph pipeline for SQL-backed evidence gathering while preserving:

- strict merchant scoping
- read-only query execution
- schema-aware SQL validation
- business-semantic reasoning in final answers

The graph is integrated as an optional runtime path (`SQL_LANGGRAPH_ENABLED=true`).

## Graph Nodes

1. `plan`
- Converts user question to business intent (metric/dimensions/window), not raw SQL.

2. `metadata`
- Loads schema catalog from curated registry + auto-discovered tables.

3. `select_views`
- Chooses best views/tables from approved catalog entries.

4. `generate_sql`
- Produces one read-only SQL query with required placeholders (`:mid`, `:d1`, `:d2`).

5. `check_sql`
- Deterministic validation for:
  - read-only semantics
  - tenant scoping (`:mid`)
  - selected-view allowlist
  - blocked keywords

6. `policy_gate`
- Flags risky/ambiguous queries.
- Optional human-review hold controlled by `SQL_GRAPH_REQUIRE_HUMAN_REVIEW`.

7. `execute_sql`
- Executes wrapped query on analytics DB with row limit (`SQL_GRAPH_MAX_ROWS`).

8. `analyze`
- Converts SQL rows into merchant-facing evidence summary + caveats + next actions.

9. `finalize`
- Emits structured result contract with:
  - `verified`
  - `summary`
  - `rows`
  - `evidence`
  - `assumptions`
  - `caveats`
  - `next_actions`

## Runtime Integration

When enabled, each lane (`operations`, `growth`) uses the SQL LangGraph pipeline instead of the generic tool loop.

This keeps one `/ask` endpoint but adds controlled SQL orchestration inside each lane.

## Table Growth Strategy

The catalog system supports future DB growth without prompt rewrites:

- Curated registry file: `app/copilot/sql_catalog.json`
- Auto discovery: enabled by default (`SQL_GRAPH_AUTO_DISCOVER_TABLES=true`)
- Allowlist override: `SQL_GRAPH_TABLE_ALLOWLIST`
- Discovery prefixes: `SQL_GRAPH_DISCOVERY_PREFIXES`

Adding new tables can be done in two ways:

1. Append table metadata to `sql_catalog.json`.
2. Let auto-discovery include new tables that match configured prefixes.

For production, keep execution constrained to curated analytics views and use allowlists in sensitive environments.

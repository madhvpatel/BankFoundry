# Bank Foundry Branch and Review Rules

## Branching

- use `codex/` prefixes for Codex-created branches
- keep pod work isolated
- avoid mixing unrelated pod changes in one branch

## Pull request rules

Every MCP or agent change should include:

- contract tests
- pod-specific tests
- a short owner-facing doc note
- evidence and verification behavior

## Review rules

Platform review is required when a change touches:

- MCP schemas
- agent contracts
- workflow boundaries
- connector execution contracts
- shared eval harness

Pod lead review is required when a change touches:

- pod MCP tools
- pod specialist agents
- pod fixtures and eval cases

## Not allowed

- direct repo access added to agent modules
- new top-level MCP response shapes
- agent-driven autonomous writes
- hidden write paths outside workflow code

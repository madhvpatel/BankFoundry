# Future Project Note: Bank API Simulation and API Discovery

## Purpose

Create a separate project later to do two things in parallel:

1. simulate bank-facing APIs for Bank Foundry development and testing
2. explore real developer/API services from ICICI Bank and HSBC for our target workflows

## Why this should be a separate project

The current Bank Foundry codebase already has:

- a bank console
- settlement case workflows
- approval flow
- a connector seam

But it does not yet have a real external banking integration. A separate project would let us:

- prototype bank connector behavior safely
- test request/response contracts without touching production systems
- model happy paths and failure paths
- compare our required workflows against real bank API capabilities

## Proposed project goals

### Track 1: Simulated bank APIs

Build a local bank API simulator that can support:

- payout status lookup
- settlement detail lookup
- hold reason lookup
- reconciliation state lookup
- intervention request submission
- status sync callbacks or polling
- success, delay, reject, timeout, and retry scenarios

The simulator should let us test:

- approval -> connector dispatch
- idempotency
- retries
- audit trail
- connector run state transitions
- delayed or partial downstream responses

### Track 2: Real bank API discovery

Investigate ICICI Bank and HSBC developer/API offerings for use cases like:

- settlement status
- payout delay investigation
- reconciliation visibility
- merchant/account status lookup
- approved intervention or escalation requests
- transaction-banking or account-reporting support

The goal is to understand:

- what is publicly available
- what is sandbox-only
- what requires partner onboarding
- what is read-only versus write-capable
- what can realistically power Bank Foundry workflows

## Initial Bank Foundry use cases to map

- held settlement review
- delayed payout exception
- reconciliation mismatch review
- settlement shortfall review
- approval-driven external intervention

## Deliverables for the future project

- a local bank API simulator
- a connector contract spec for Bank Foundry
- an ICICI feasibility summary
- an HSBC feasibility summary
- a gap analysis between our workflows and available bank APIs
- a recommendation for the first real connector target

## Suggested output structure

When this project starts, produce:

1. simulated API endpoints and payload schemas
2. auth and idempotency design
3. workflow-to-endpoint mapping
4. bank-by-bank feasibility matrix
5. recommendation on whether to build:
   - direct bank connector
   - middleware adapter
   - hybrid simulation plus manual-ops connector

## Note

This should be a new project, not a quick extension inside the current repo. The goal is to explore connector realism without destabilizing the live Bank Foundry application path.

# Merchant OS UX Spec

## Purpose

Define a single merchant-facing product experience that works for:

- merchants with no ERP or accounting software
- merchants with fragmented tools (Excel, Tally, billing app, POS software)
- merchants with ERP-lite systems
- enterprise merchants with full ERP and treasury/recon systems
- multi-outlet and franchise merchants
- API-first digital merchants

The product should give merchants transparent, explainable visibility into payments and connected business operations, while adapting depth and workflow complexity to their operating maturity.

## Product Promise

Give merchants a clear, explainable view of:

- what money moved
- what failed
- what settled
- what was deducted
- what is at risk
- what action to take next

Payments transparency is the default. Broader business transparency is shown wherever connected systems are available.

## Experience Principles

1. Show verified facts first.
2. Separate `Operations` and `Growth`.
3. Never hide uncertainty.
4. Prefer guided workflows over analyst dashboards for smaller merchants.
5. Prefer control-plane views, APIs, and drilldowns for larger merchants.
6. Keep the same product shell across segments, but vary depth, defaults, and complexity.

## Merchant Segments

### 1. No-System Merchants

Examples:

- small retailers
- parking operators
- pharmacies
- salons
- local restaurants

Characteristics:

- no ERP
- no accounting software or accountant-only usage
- mostly mobile-first
- high dependency on bank/acquirer for payments clarity

Product role:

- default operating dashboard for payments
- reconciliation assistant
- dispute/chargeback guide

### 2. Fragmented-Tool Merchants

Examples:

- merchants using Excel, Tally, billing software, WhatsApp, or a local POS vendor

Characteristics:

- some digital processes, no unified system
- moderate operations maturity
- frequent export/import needs

Product role:

- primary payments control plane
- lightweight business ops layer
- export and connector hub

### 3. ERP-Lite Merchants

Examples:

- merchants using vertical SaaS, POS back office, or custom internal dashboards

Characteristics:

- partial operational systems
- need integration more than replacement

Product role:

- payments intelligence layer
- reconciliation and settlement cockpit
- connected workflow system

### 4. Enterprise / Full ERP Merchants

Examples:

- chains
- large retailers
- fuel networks
- multi-location operators

Characteristics:

- SAP, Oracle, NetSuite, Dynamics, treasury or internal recon systems
- multiple roles, teams, outlets, MIDs, and terminals

Product role:

- acquiring control plane
- intelligence layer
- dispute and settlement workflow hub
- integration surface, not ERP replacement

### 5. Multi-Outlet / Franchise Merchants

Characteristics:

- hierarchy matters
- outlet-level, regional, and brand-level views needed

Product role:

- shared shell with hierarchy navigation
- cross-outlet diagnostics
- benchmark and exception management

### 6. API-First Digital Merchants

Characteristics:

- internal product/engineering teams
- strong need for APIs, webhooks, and exports

Product role:

- data and workflow platform
- event-driven payments operations layer

## Product Modes

The same application adapts by merchant maturity.

### Mode A: Guided Mode

Target:

- no-system merchants
- fragmented-tool merchants

Defaults:

- mobile-first
- task-first homepage
- simplified language
- guided explanations
- minimal chart density

### Mode B: Control Plane Mode

Target:

- ERP-lite merchants
- multi-outlet operators

Defaults:

- dashboard + workflow hybrid
- deeper filters
- exports and connectors visible
- richer drilldowns

### Mode C: Enterprise Mode

Target:

- full ERP merchants
- API-first merchants

Defaults:

- hierarchy filters
- bulk workflows
- advanced reporting
- connector and API surfaces
- workflow audit trail

## Core Navigation

Top-level navigation should be consistent across segments.

1. `Home`
2. `Operations`
3. `Growth`
4. `Money`
5. `Disputes`
6. `Terminals`
7. `Connected Systems`
8. `Reports`
9. `Settings`

## Home

Home is role-aware and segment-aware.

Primary blocks:

- `Today at a glance`
- `Money expected vs received`
- `Actions due`
- `Failures and risks`
- `Growth opportunities`
- `Verification status and data coverage`

### No-System Home

Prioritize:

- yesterday expected amount
- yesterday settled amount
- difference
- pending deductions
- refund and chargeback alerts
- simple next actions

### Enterprise Home

Prioritize:

- hierarchy-level KPI selector
- exceptions across outlets/MIDs
- aging disputes
- settlement delays
- top growth opportunities by impact

## Operations Agent

The Operations tab is a standalone agent experience.

Primary jobs:

- explain payout shortfalls
- show settlement status
- identify deductions
- track refunds
- manage chargeback workflows
- suggest next operational action

Core screen sections:

- `Summary`
- `Expected vs received`
- `Deduction breakdown`
- `Pending items`
- `Recommended actions`
- `Verification status`
- `Evidence IDs / audit trail`

### Operations Workflows

#### Reconciliation Workflow

Entry:

- “I expected X, received Y”
- date window selection

Steps:

1. show expected settlement amount
2. show actual received amount
3. show difference
4. classify difference into:
   - MDR/fees
   - refunds
   - chargebacks
   - holds
   - pending settlement
   - unknown difference
5. suggest next action

#### Settlement Workflow

Key views:

- by payout date
- by batch
- by UTR
- by status

States:

- processed
- pending
- held
- delayed
- failed

#### Chargeback Workflow

Key elements:

- open cases
- amount at risk
- due dates
- reason code explanation
- evidence checklist
- upload proof
- submit / escalate
- audit trail

#### Refund Workflow

Key elements:

- initiated refunds
- completed refunds
- failed refunds
- refund aging
- settlement adjustment linkage

## Growth Agent

The Growth tab is a standalone agent experience.

Primary jobs:

- identify failure concentration
- recommend acceptance uplift actions
- detect terminal or routing opportunities
- suggest product enablement opportunities
- surface working capital or service-fit nudges

Core screen sections:

- `Summary`
- `Failure concentration`
- `Success rate opportunities`
- `Terminal opportunities`
- `Recommended growth actions`
- `Verification status`
- `Evidence IDs / audit trail`

### Growth Workflows

#### Acceptance Optimization

Questions answered:

- which payment modes are underperforming
- where are failures concentrated
- which terminals have poor acceptance
- which response codes are dragging success

Outputs:

- ranked opportunity
- expected impact
- evidence used
- next action

#### Device and Terminal Opportunity

Use cases:

- additional POS terminal
- backup terminal for high-volume outlet
- routing or retry service
- smart terminal upgrade

#### Product Recommendation

Examples:

- DCC only when international evidence exists
- working capital eligibility only when evidence supports it
- settlement cycle options only when cashflow mismatch is visible

## Money

This is the merchant-facing money movement layer.

Screens:

- `Sales to settlement bridge`
- `Settlement calendar`
- `Payout history`
- `Deductions and fees`
- `Cashflow timeline`

For smaller merchants, this is a simple ledger-like experience.
For larger merchants, this is a recon and treasury view.

## Disputes

Dedicated dispute center.

Screens:

- `Open cases`
- `Due this week`
- `Recently resolved`
- `Evidence upload`
- `Reason code explanations`

Primary CTAs:

- `Review case`
- `Upload evidence`
- `Submit response`
- `Escalate to bank`

## Terminals

Dedicated terminal and acceptance operations center.

Screens:

- terminal fleet list
- success rate by terminal
- outage / anomaly indicators
- network and battery issues where available
- outlet terminal coverage

## Connected Systems

This section determines how far the app can extend from payment transparency into business transparency.

Connection types:

- ERP
- accounting software
- POS / billing software
- inventory system
- bank statement feed
- API / webhook connection

States:

- not connected
- connected
- partially synced
- error

### UX Requirement

Always show data coverage clearly:

- `Payments only`
- `Payments + accounting`
- `Payments + ERP`
- `Payments + POS + inventory`

This prevents overclaiming business transparency.

## Reports

Report surfaces differ by segment.

### Small Merchant

- daily summary
- settlement summary
- refund summary
- chargeback summary
- accountant export

### Enterprise

- outlet-level reports
- MID-level reports
- terminal-level reports
- scheduled exports
- API / webhook delivery

## Role-Based Views

### Merchant Owner

- business summary
- money movement
- actions due
- growth insights

### Operations Manager

- settlements
- reconciliation
- disputes
- terminal health

### Finance / Accountant

- payout history
- deductions
- exports
- audit trail

### Growth / Commercial

- acceptance optimization
- payment mode performance
- terminal and service recommendations

## Transparency and Trust Rules

Every insight must show:

- what happened
- why the system believes it happened
- what data was used
- whether it is verified or inferred
- what action can be taken next

Status model:

- `Verified`
- `Unverified (supported)`
- `Insufficient evidence`

## Notification Model

Use chat plus cards, but adapt by merchant maturity.

### Small Merchants

- WhatsApp/SMS/email nudges
- mobile push
- simple language

### Larger Merchants

- in-app queue
- workflow inbox
- export and webhook events

Notification categories:

- payout mismatch
- settlement delay
- refund spike
- chargeback deadline
- terminal anomaly
- growth opportunity

## Mobile vs Desktop

### Mobile

Primary for small merchants.

UX rules:

- show one action per screen
- use plain language
- keep key financial numbers above the fold
- make uploads and approvals easy

### Desktop

Primary for finance, ops, and enterprise users.

UX rules:

- support table drilldowns
- bulk actions
- filter-heavy views
- report exports

## Design Constraints

1. Never force analytics-first UX for small merchants.
2. Never force simplified UX on enterprise users.
3. Keep `Operations` and `Growth` separate everywhere.
4. Show data coverage before showing “business transparency” claims.
5. Keep all financial explanations auditable.

## MVP Scope

Phase 1 should support:

- `Operations` standalone agent
- `Growth` standalone agent
- home overview
- settlement and payout mismatch workflow
- refund and chargeback workflows
- terminal and failure-driver diagnostics
- simple reports and exports
- connected-system status

## Future Scope

Future merchant OS expansion can add:

- inventory
- customer and loyalty
- campaign management
- invoices and orders
- supplier payouts
- deeper accounting integrations
- multi-bank visibility

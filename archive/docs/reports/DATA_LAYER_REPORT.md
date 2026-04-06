# Data Layer Report

Date: 2026-03-10
Scope: Current data layer used by the merchant OS, including storage schema, ingestion assumptions, schema coupling, internal state tables, and the main improvement path.

## Executive Summary

The current data layer is good enough for:
- the internal payout-shortfall demo
- payment operations analysis
- proactive payment alerts
- merchant-scoped chat over payments data

It is not yet good enough for:
- a full merchant OS
- true DB portability
- bank API-first ingestion
- broad business transparency beyond payments

The main reason is simple:

the product is built on top of a specific demo payments schema, not yet on top of a stable canonical merchant data contract.

## Current Role Of The Data Layer

Today, the data layer does five jobs:

1. stores merchant payments facts
2. stores settlement, refund, and chargeback records
3. stores merchant profile and KYC context
4. stores app-owned workflow state
5. provides enough schema introspection for tools to adapt across a few schema variants

This is why the system already behaves like a payments operations platform, but not yet like a full merchant operating system.

## Current Live Data Footprint

Observed in the active database:

- `merchants`: `14`
- `transaction_features`: `9846`
- `settlements`: `321`
- `refunds`: `13`
- `chargebacks`: `6`
- `merchant_kyc_documents`: `14`
- `merchant_actions`: `159`
- `proactive_cards`: `69`

This is enough for the current payments demo story.
It is not broad enough for strong cross-merchant proactive coverage across all workflows.

## Core Business Tables

### 1. `transaction_features`

This is the main payments fact table.

Key fields currently present:
- `merchant_id`
- `terminal_id`
- `payment_mode`
- `status`
- `response_code`
- `amount_rupees`
- `p_date`
- `initiated_at`
- `completed_at`
- `hour_of_day`
- device/network/payment enrichment fields

Why it matters:
- almost all analytics depend on this table
- most growth, failure, and payment-health logic starts here

Current assessment:
- strongest table in the data layer
- serves as the product’s de facto canonical payments fact

### 2. `settlements`

This holds payout and deduction data.

Key fields currently present:
- `mid`
- `tid`
- `settlement_date`
- `gross_amount`
- `mdr_deducted`
- `gst_on_mdr`
- `tds_deducted`
- `chargeback_deductions`
- `reserve_held`
- `adjustment_amount`
- `net_settlement_amount`
- `settlement_status`
- `hold_reason`

Why it matters:
- this table powers the best internal-demo capability
- payout shortfall detection depends on it

Current assessment:
- strong enough for deterministic payout attribution in some cases
- still not abstracted into a merchant-level reconciliation model

### 3. `refunds`

Key fields currently present:
- `mid`
- `tid`
- `refund_amount`
- `refund_status`
- `refund_date`
- `settlement_id`
- `p_date`

Why it matters:
- refund analysis
- reconciliation support
- refund spike proactive cards

Current assessment:
- schema support is present
- current data volume is sparse

### 4. `chargebacks`

Key fields currently present:
- `mid`
- `tid`
- `chargeback_amount`
- `response_due_date`
- `chargeback_stage`
- `resolution_outcome`
- `chargeback_reason_code`
- `chargeback_reason_desc`
- `settlement_id`

Why it matters:
- dispute analysis
- overdue operational issues
- chargeback-driven deductions

Current assessment:
- usable
- still sparse in live data

### 5. `merchants`

Key fields currently present:
- `mid`
- `merchant_trade_name`
- `nature_of_business`
- `business_city`
- `merchant_risk_category`
- `merchant_status`
- `annual_turnover`

Why it matters:
- merchant identity and profile context
- business-facing summaries

Current assessment:
- enough for the current shell
- not yet a full merchant master

### 6. `merchant_kyc_documents`

Key fields currently present:
- `mid`
- `expiry_date`
- `kyc_status`
- verification fields

Why it matters:
- KYC-risk proactive cards

Current assessment:
- supported, but not a rich compliance layer

## App-Owned State Tables

These are not source-of-truth banking tables. They are product-state tables.

### `merchant_actions`
- queue of generated and managed tasks
- tracks:
  - category
  - title
  - description
  - impact
  - confidence
  - priority
  - owner
  - evidence
  - status

### `proactive_cards`
- stores background proactive signals
- tracks:
  - dedupe key
  - merchant
  - lane
  - verification status
  - evidence IDs
  - payload JSON
  - card state
  - notes
  - linked/converted action state

### `proactive_refresh_schedule`
- stores in-app auto-refresh scheduling metadata

Why this matters:
- even if raw merchant data comes from APIs later, these tables still need a persistent internal store

## Current Data Access Model

The current system is:
- database-first
- SQLAlchemy-backed
- Postgres-first
- SQLite-tolerant for tests and some fallback paths

The main runtime assumptions come from:
- [`/Users/madhavpatel/New_demo copy/config.py`](/Users/madhavpatel/New_demo copy/config.py)
- [`/Users/madhavpatel/New_demo copy/app/copilot/tools.py`](/Users/madhavpatel/New_demo copy/app/copilot/tools.py)
- [`/Users/madhavpatel/New_demo copy/app/merchant_os.py`](/Users/madhavpatel/New_demo copy/app/merchant_os.py)

Important config points:
- `DATABASE_URL`
- `QUERY_SOURCE_TABLE` defaulting to `transaction_features`
- `SQL_GRAPH_CATALOG_PATH`

## Schema Introspection Support

The system already has lightweight schema-awareness.

It uses:
- `information_schema.columns` for Postgres
- `PRAGMA table_info(...)` for SQLite

This exists in multiple places:
- `app/copilot/tools.py`
- `app/merchant_os.py`
- `app/copilot/sql_catalog.py`
- `app/intelligence/payout_shortfall_monitor.py`
- scenario baseline helpers

Why this matters:
- the system can adapt across some schema variants
- examples:
  - `mid` vs `merchant_id`
  - `settlement_status` vs `status`
  - `refund_status` vs `status`

Current assessment:
- useful
- still local and ad hoc
- not a true canonical repository abstraction

## Curated Schema Registry

There is already a curated SQL catalog:
- [`/Users/madhavpatel/New_demo copy/app/copilot/sql_catalog.json`](/Users/madhavpatel/New_demo copy/app/copilot/sql_catalog.json)

This is intended to support:
- LangGraph SQL orchestration
- safer SQL planning
- recommended views

Current weakness:
- the catalog is only partially aligned with the live DB schema

Example:
- the catalog lists settlement columns such as `merchant_id`, `status`, `expected_date`, `amount_rupees`
- the live schema uses `mid`, `settlement_status`, `settlement_date`, `net_settlement_amount`

This mismatch does not break the current product because many runtime tools are schema-aware.
But it does weaken trust in the SQL-catalog layer.

## Ingestion Layer Today

The system has demo-oriented ingestion and normalization tooling.

### What exists

1. terminal master ETL mapping
- [`/Users/madhavpatel/New_demo copy/docs/run_time_docs/TERMINAL_MASTER_ETL_MAPPING.md`](/Users/madhavpatel/New_demo copy/docs/run_time_docs/TERMINAL_MASTER_ETL_MAPPING.md)
- [`/Users/madhavpatel/New_demo copy/app/intelligence/etl_terminal_master.py`](/Users/madhavpatel/New_demo copy/app/intelligence/etl_terminal_master.py)

This maps source exports like:
- `postransactions`
- `upitransactions`
- `tidmaster`
- `midmaster`

into a `transaction_features`-compatible normalized format.

2. demo data generators and injectors
- [`/Users/madhavpatel/New_demo copy/app/intelligence/demo_activity_generator.py`](/Users/madhavpatel/New_demo copy/app/intelligence/demo_activity_generator.py)
- [`/Users/madhavpatel/New_demo copy/app/intelligence/demo_conditions_injector.py`](/Users/madhavpatel/New_demo copy/app/intelligence/demo_conditions_injector.py)

These are useful for:
- synthetic test data
- demo scenarios
- reproducibility

### What does not exist yet

- no real canonical ingestion service
- no bank API adapter layer
- no source lineage model flowing through all downstream insights
- no event-driven source normalization pipeline

## Strengths Of The Current Data Layer

1. strong payments fact base
- `transaction_features` is rich and usable

2. enough settlement detail for a strong shortfall demo

3. app-owned state is already modeled
- actions
- cards
- refresh schedule

4. some schema flexibility already exists
- `mid`/`merchant_id` handling
- Postgres/SQLite introspection

5. deterministic analytics are possible without relying on the model

## Weaknesses Of The Current Data Layer

### 1. Schema coupling is still high

The system still assumes a specific payments schema:
- `transaction_features`
- `settlements`
- `refunds`
- `chargebacks`
- `merchants`

This is visible throughout:
- engine SQL
- tool SQL
- merchant OS snapshot building
- SQL graph catalog

Practical consequence:
- easy to use the current DB
- not easy to swap to a different bank schema

### 2. No canonical merchant data contract yet

There is no stable logical interface for:
- transactions
- settlements
- deductions
- refunds
- disputes
- merchant profile
- connected-system profile

Practical consequence:
- business logic depends directly on physical table shapes

### 3. Reconciliation model is still shallow

You have enough settlement rows for payout shortfall explanation.
You do not yet have a full normalized reconciliation model that cleanly represents:
- expected payout
- actual payout
- component deductions
- unexplained residual
- merchant-facing reconciliation status

### 4. Source lineage is weak

The product can cite evidence IDs.
It does not yet preserve rich lineage such as:
- source system
- sync time
- transform version
- ingestion run

Practical consequence:
- enough for demo traceability
- not enough for a serious bank-grade evidence chain

### 5. Sparse non-transaction data

Live data today is thin for:
- refunds
- chargebacks
- KYC

Practical consequence:
- many merchants will show no proactive cards
- dispute/refund intelligence appears weaker than the schema suggests

### 6. Tooling and catalog are not fully aligned

The runtime tools often handle schema variants correctly.
The curated SQL catalog still reflects partly idealized fields.

Practical consequence:
- some advanced SQL-orchestration paths may be less reliable than the direct tools

### 7. No full business data layer

There is no first-class support yet for:
- inventory
- orders
- CRM
- accounting system state
- outlet/store hierarchy beyond limited terminal scoping

Practical consequence:
- this is still a payments operating system, not yet a complete merchant OS

## Layer Assessment

### For the internal demo

The data layer is sufficient.

The internal demo needs:
- merchant identity
- transactions
- settlements
- payout deductions
- proactive state
- task state

You already have that.

### For a pilot

The data layer is usable, but uneven.

Main pilot risks:
- schema mismatch drift
- sparse refunds/chargebacks for some merchants
- overpromising broad business visibility

### For the full merchant OS vision

The data layer is not sufficient yet.

The future system needs:
- canonical logical entities
- richer merchant profile state
- bank API ingestion
- external-system connectors
- source lineage
- less physical-table coupling

## Immediate Fixes

These are the highest-value short-term improvements before expanding feature scope further.

### 1. Align the SQL catalog with the live DB

Update:
- `app/copilot/sql_catalog.json`

So that curated table metadata matches real columns now in use.

### 2. Define a canonical logical contract

Create a small internal contract for:
- `payments_transactions`
- `merchant_profile`
- `settlement_payouts`
- `refund_events`
- `chargeback_cases`

Do this first as documentation and repository interfaces, not as a giant rewrite.

### 3. Introduce a normalized deduction model

For payout and reconciliation work, define a consistent internal structure for:
- expected amount
- received amount
- delta
- deduction components
- residual unexplained amount

### 4. Add coverage diagnostics

The system should be able to say:
- merchant has no cards because no trigger fired
- merchant has no cards because the supporting dataset is absent
- merchant has no cards because data volume is too low

### 5. Make source table assumptions explicit

Where the product still relies directly on physical tables, document it clearly so future rewrites do not guess.

## Future Changes

### 1. Canonical data layer

Move the business logic from physical table names to canonical repositories or views.

Target entities:
- transaction
- settlement
- refund
- chargeback
- merchant
- terminal
- merchant profile answers
- connected systems

### 2. Bank API-first ingestion

Do not let the product depend on querying operational source APIs at request time.

Instead:
- ingest APIs/files/webhooks
- normalize them
- store canonical state
- preserve lineage
- let engines query the normalized store

### 3. Source adapters

Add adapters for:
- bank transaction feeds
- settlement feeds
- dispute feeds
- refund feeds
- ERP/POS/accounting connectors

### 4. Merchant profile layer

Store structured merchant answers such as:
- goals
- current software stack
- operating model
- enabled bank services

This data will eventually be needed for better personalization.

### 5. Hierarchy-aware data model

You will eventually need:
- merchant
- region
- outlet
- MID
- terminal

Today you have limited terminal scoping, but not a complete hierarchy layer.

## Bottom Line

The current data layer is:
- strong enough for a payments demo
- good enough for shortfall explanation and merchant payment operations
- not yet abstracted enough for the full merchant OS vision

If the goal is to refine layers before moving on, the data layer should be treated as:
- `demo-ready for payments`
- `pilot-usable with caution`
- `not yet platform-ready`

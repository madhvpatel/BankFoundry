# Sprint 2

## Goal
Stabilize the merchant OS around operational truthfulness, queue usability, and proactive workflows.

Sprint 1 established:
- separate `Operations` and `Growth` agents
- merchant OS shell
- action center cleanup and metadata editing
- role-based report packs
- proactive background inbox

Sprint 2 should focus on the gaps surfaced by manual testing and the next obvious product seams.

## Manual Testing Findings

### 1. Action Center and Operations agent are not aligned
Observed behavior:
- legacy cleanup hid `58` old or duplicate actions
- Action Center then showed `No persisted actions are present for this merchant yet`
- Operations chat still surfaced real operational work such as open chargebacks and settlement review

Problem:
- actionable operational findings are discoverable in chat
- the working queue is not being populated from those findings

Impact:
- the merchant sees analysis but no durable task queue
- the OS feels inconsistent and incomplete

### 2. Chargeback rollup is inconsistent
Observed behavior:
- response said `3 open, Rs 1,850.00 total`
- the same response then listed `Chargeback 1 ... Rs 2,500.00`

Problem:
- chargeback summary amount and row-level amounts do not agree

Impact:
- breaks trust immediately
- makes dispute prioritization unsafe

### 3. Deduction explanation is too weak
Observed behavior:
- deduction explanation said:
  `Top settlement bucket is PROCESSED (7 record(s), amount 4769580.07); shortfall may be tied to this bucket.`

Problem:
- this is a status summary, not a causal explanation of a shortfall
- the answer implies attribution without proving fees, holds, disputes, refunds, or reversals

Impact:
- reconciliation answers sound confident without being sufficiently grounded

### 4. Operational outputs are not prioritized as decision-complete tasks
Observed behavior:
- the response surfaced useful evidence, but not as ranked merchant tasks

Problem:
- outputs are still too summary-heavy
- they should resolve into queue items like:
  - `Review overdue chargebacks`
  - `Investigate settlement shortfall`
  - `Check delayed settlement release`

Impact:
- merchants still need to translate analysis into work

### 5. Terminal-scoped growth answers are verified but still too weak
Observed behavior:
- terminal-scoped growth prompt returned a verified answer for `EP070270`
- tools correctly scoped to the terminal and surfaced:
  - `UNKNOWN` as the top failure driver
  - terminal success rate and attempts
- the final answer reduced that into a thin nudge such as `Resolve "UNKNOWN" Failure Driver`

Problem:
- recommendation quality is lagging behind verification quality
- `UNKNOWN` is treated like a direct merchant action instead of a diagnosis bucket
- the answer did not use terminal-performance context to explain whether this is a concentrated fix or a broader terminal problem
- the opportunity was not ranked or translated into a concrete remediation path

Impact:
- terminal-specific growth answers look formally correct but operationally shallow
- merchants still need to interpret the evidence themselves to decide what to do next

## Proposed Patch Backlog

### P0. Deterministic operational action generation
Source:
- manual testing

Patch:
- auto-create Action Center items from deterministic operational findings
- initial scope:
  - overdue chargebacks
  - delayed settlements
  - verified payout shortfalls
  - settlement detail review prompts

Implementation direction:
- build a deterministic action derivation step from settlement, chargeback, refund, and cashflow outputs
- persist only merchant-actionable operations items
- avoid generic narrative items

Acceptance criteria:
- if the Operations agent surfaces overdue chargebacks or delayed settlements, matching queue items appear in Action Center
- Action Center is not empty when deterministic operational issues are present
- duplicate queue items are still suppressed

Likely files:
- `app/merchant_os.py`
- `app/intelligence/action_center.py`
- `main.py`

### P0. Chargeback rollup correction
Source:
- manual testing

Patch:
- fix amount aggregation for chargeback summaries
- ensure rollup totals match listed row amounts and filtered status set

Implementation direction:
- audit `list_chargebacks` consumption and any intermediate summary logic
- align summary totals to the exact chargeback rows displayed
- add tests for mixed amounts and status filtering

Acceptance criteria:
- chargeback total equals sum of displayed open chargebacks
- no summary can be lower than an included individual chargeback amount

Likely files:
- `app/copilot/runtime.py`
- `app/merchant_os.py`
- chargeback tool / summarization path

### P0. Reconciliation-safe deduction explanations
Source:
- manual testing

Patch:
- do not present settlement bucket summaries as deduction explanations
- only claim a shortfall explanation when fee, hold, dispute, refund, or reversal attribution is actually computed
- otherwise downgrade to `Unverified (supported)`

Implementation direction:
- add a deduction attribution helper that attempts a concrete bridge:
  - expected settlement
  - actual settlement
  - known deductions
  - residual unexplained delta
- if attribution is incomplete, explicitly say so

Acceptance criteria:
- `Verified` deduction explanations contain concrete deduction components
- unsupported explanations are downgraded
- `PROCESSED` alone is never presented as the explanation

Likely files:
- `app/copilot/runtime.py`
- settlement/cashflow tools
- `app/merchant_os.py`

### P1. Queue-first operational response composition
Source:
- manual testing

Patch:
- prioritize ranked merchant tasks over prose summaries in Operations outputs

Implementation direction:
- convert top operational findings into:
  - title
  - why it matters
  - due/amount
  - next action
- make queue-style answers the default when operational issues exist

Acceptance criteria:
- top operational answer starts with explicit actions
- summaries support the tasks instead of replacing them

Likely files:
- `app/copilot/runtime.py`
- Operations lane prompt section in `AGENTS.md`

### P1. Evidence-ranked terminal growth response composition
Source:
- manual testing

Patch:
- improve growth answer composition for terminal-scoped prompts
- translate verified driver rankings into ranked merchant actions instead of generic nudges
- treat `UNKNOWN` as a diagnostic/instrumentation bucket unless supporting evidence proves a more specific growth move

Implementation direction:
- combine terminal-scoped `verify_failure_drivers` output with terminal performance context before composing the growth answer
- require growth answers to state:
  - top opportunity
  - why it matters (`failed_txns`, `failed_gmv`, concentration)
  - terminal context (`attempts`, `success_rate_pct`)
  - concrete next action
- avoid presenting `UNKNOWN` as a standalone recommendation; instead recommend diagnosis steps such as issuer/gateway/network mapping, retry-path review, or response-code capture fixes

Acceptance criteria:
- terminal-scoped growth answers use both failure-driver evidence and terminal-performance context when both are available
- `UNKNOWN` cannot appear as the final recommended action label by itself
- the answer ranks at least one concrete next step with supporting evidence
- recommendation wording stays distinct from verification wording

Likely files:
- `app/copilot/runtime.py`
- Growth lane prompt section in `AGENTS.md`
- terminal growth summarization path

### P1. Proactive inbox to real background refresh job
Source:
- identified during current implementation

Patch:
- move proactive card generation from snapshot-triggered refresh to an actual scheduled/background refresh path

Current limitation:
- cards are generated when the snapshot loads or when the sidebar refresh button is pressed
- this is useful, but not a real background job

Implementation direction:
- add a periodic refresh entry point
- only refresh when the merchant window changes or card inputs materially change
- keep same-window replacement semantics

Acceptance criteria:
- proactive cards stay current without manual refresh
- UI load does not become the orchestration trigger for background work

Likely files:
- `app/merchant_os.py`
- scheduler / automation layer
- `main.py`

### P1. Proactive card state management
Source:
- identified during current implementation

Patch:
- add per-card workflow state:
  - `new`
  - `acknowledged`
  - `dismissed`
  - `converted_to_action`

Problem:
- the inbox currently shows signals, but not merchant handling state

Acceptance criteria:
- merchants can clear, acknowledge, or convert cards into work
- dismissed cards do not reappear unchanged within the same window unless evidence changes

Likely files:
- proactive card storage schema
- `app/merchant_os.py`
- `main.py`

### P1. Proactive card to action bridge
Source:
- identified during current implementation

Patch:
- allow selected proactive cards to become action previews or queued work items

Examples:
- `chargeback deadline` card -> dispute action
- `settlement delay` card -> settlement investigation action
- `terminal anomaly` card -> device review action

Acceptance criteria:
- at least operations cards can be converted into queue items
- conversion preserves evidence IDs and source card ID

Likely files:
- `app/merchant_os.py`
- `main.py`
- `app/copilot/tools.py`

### P2. Hierarchy filters
Source:
- previously identified product gap

Patch:
- add hierarchy slicing starting with `merchant -> terminal`
- only add `outlet/store` when the schema actually supports it

Acceptance criteria:
- merchant can filter views, queue items, and reports by terminal
- no fake store hierarchy is shown when the data does not exist

Likely files:
- `main.py`
- `app/merchant_os.py`
- KPI/settlement/terminal query helpers

### P2. Report pack export upgrades
Source:
- identified after report-pack implementation

Patch:
- add printable/exportable report summaries by role
- future outputs:
  - finance brief
  - operations brief
  - growth brief

Acceptance criteria:
- merchant can export a concise role-specific summary, not just CSVs

Likely files:
- `app/merchant_os.py`
- `main.py`

## Latest Implemented Regression Cases

These should stay in Sprint 2 regression coverage even though the first implementation already shipped.

### Auto proactive refresh cadence
- When no schedule row exists for a merchant/window, opening the app should trigger one proactive refresh and persist `last_refresh_at` and `next_refresh_at`.
- When the merchant reloads the app before `next_refresh_at`, the refresh path should skip with reason `not_due` and must not regenerate cards again.
- Manual `Refresh proactive cards` should force a refresh through the same scheduling path and update the schedule row.

### Terminal-scoped chat/tooling
- `run_turn(... terminal_id='T1')` must propagate terminal focus into the lane payload and returned turn metadata.
- `compute_kpis` with `ToolContext(..., terminal_id='T1')` must return only T1 transaction aggregates and include terminal scope in evidence/scope metadata.
- `verify_failure_drivers` with `ToolContext(..., terminal_id='T1')` must rank only T1 failures and include terminal scope in evidence/scope metadata.
- Terminal-scoped proactive cards generated from chat must keep terminal-aware dedupe and payload metadata so OS filtering remains consistent.

### Terminal-scoped growth answer quality
- When a terminal-scoped growth answer is built from verified failure-driver evidence and terminal-performance evidence, the final answer must use both inputs in the explanation.
- If the top verified driver is `UNKNOWN`, the answer must frame it as a diagnosis/instrumentation issue and propose a concrete next step; it must not stop at `Resolve UNKNOWN`.
- The final growth answer must rank at least one terminal-specific action using verified counts/GMV from the tool outputs.

## Regression Watch
These were identified during manual testing and already patched in Sprint 1, but should remain on the regression checklist:
- integer ID coercion for detail tools (`get_settlement_detail`, related wrappers)
- Streamlit duplicate element keys in Action Center
- legacy low-signal and duplicate action cleanup
- action provenance display
- role-based reports replacing raw report dumps

## Recommended Sprint 2 Order
1. deterministic operational action generation
2. chargeback rollup correction
3. reconciliation-safe deduction explanations
4. queue-first operational response composition
5. proactive card state management and card-to-action bridge
6. true background refresh job
7. hierarchy filters

## Definition of Done
Sprint 2 should be considered complete when:
- operational issues found in chat reliably become queue items
- dispute and chargeback totals are internally consistent
- payout shortfall explanations do not overclaim causality
- proactive cards have usable merchant workflow state
- the OS feels like a working operations surface rather than a chat shell plus tables

## Future Platform Track

These are not Sprint 2 deliverables, but they should remain on the roadmap as the platform hardening track after the current product-quality work.

### F1. Canonical data contract and DB-agnostic access layer
Goal:
- make the system portable across different bank data stores and analytics backends without rewriting business logic

Problem today:
- the app is tied to the current payments schema (`transaction_features`, `settlements`, `chargebacks`, `refunds`, `merchants`)
- many read and write paths still assume PostgreSQL-style behavior or specific table names

Direction:
- define a canonical logical contract for:
  - transactions
  - settlements
  - chargebacks
  - refunds
  - merchants
  - terminals
- move raw SQL behind adapter/repository interfaces
- keep the merchant OS, agents, and report builders dependent on the canonical contract rather than direct table names

Acceptance criteria:
- switching from one supported backend to another does not require changes in agent/runtime logic
- business modules query canonical repositories or views, not backend-specific table names
- app-owned workflow tables are migrated through explicit backend adapters

### F2. Bank API-first ingestion layer
Goal:
- decouple the product from direct database dependence where a real bank exposes stable APIs or event feeds

Feasibility:
- yes, this is possible
- but the right architecture is not `query bank APIs directly from the UI on every request`
- the right architecture is:
  - ingest from bank APIs / files / event streams
  - normalize into a canonical internal model
  - persist into the product's analytics + workflow store
  - let the agents reason on the normalized store

Why:
- bank APIs may be rate-limited, delayed, incomplete, or split across systems
- analytics, reconciliation, and proactive workflows need stable historical state
- the product also owns internal entities such as actions, proactive cards, evidence, and refresh schedules

Initial integration targets:
- merchant profile / onboarding APIs
- transaction and payment event feeds
- settlement and payout APIs
- dispute / chargeback case APIs
- refund status APIs
- terminal/device inventory or health APIs

Acceptance criteria:
- ingestion supports both pull (scheduled API sync) and push (webhooks/events) where the bank provides them
- source-specific payloads are mapped into canonical entities with lineage metadata
- the merchant OS continues to work even if a source API is temporarily unavailable, using the last normalized state

### F3. Source adapters and lineage
Goal:
- support multiple upstream banks or bank systems without reworking the product core

Direction:
- add source adapters such as:
  - `postgres_source`
  - `bank_api_source`
  - future `warehouse_source`
- stamp normalized records with:
  - source system
  - sync timestamp
  - source record id
  - transform version

Acceptance criteria:
- evidence and actions can point back to source lineage
- operators can diagnose whether an issue came from source data, transform logic, or agent reasoning

### F4. Platform regression coverage
When this platform track starts, add regression coverage for:
- canonical contract compatibility across at least two backends
- API payload to canonical model mapping
- partial-source failure handling
- lineage preservation through insight generation and action creation

# Layer Refinement Master Plan

## Purpose
This document synthesizes the current state of the main product layers and turns them into a practical refinement plan.

It is based on these reports:
- [`/Users/madhavpatel/New_demo copy/docs/DATA_LAYER_REPORT.md`](/Users/madhavpatel/New_demo copy/docs/DATA_LAYER_REPORT.md)
- [`/Users/madhavpatel/New_demo copy/docs/ORCHESTRATION_LAYER_REPORT.md`](/Users/madhavpatel/New_demo copy/docs/ORCHESTRATION_LAYER_REPORT.md)
- [`/Users/madhavpatel/New_demo copy/docs/PROACTIVE_LAYER_REPORT.md`](/Users/madhavpatel/New_demo copy/docs/PROACTIVE_LAYER_REPORT.md)
- [`/Users/madhavpatel/New_demo copy/docs/WORKFLOW_ACTION_LAYER_REPORT.md`](/Users/madhavpatel/New_demo copy/docs/WORKFLOW_ACTION_LAYER_REPORT.md)
- [`/Users/madhavpatel/New_demo copy/docs/CONVERSATION_LAYER_REPORT.md`](/Users/madhavpatel/New_demo copy/docs/CONVERSATION_LAYER_REPORT.md)
- [`/Users/madhavpatel/New_demo copy/docs/UI_ALIGNMENT_REPORT.md`](/Users/madhavpatel/New_demo copy/docs/UI_ALIGNMENT_REPORT.md)

## Executive Summary
The system is now strong enough to support a focused internal demo, especially around:
- merchant payments overview
- growth opportunity analysis
- proactive monitoring
- payout shortfall detection and explanation
- converting insights into queue items

The system is not yet a complete merchant OS. The biggest remaining gaps are not in surface area. They are in:
- consistency of operational truth
- clarity of orchestration ownership
- breadth of proactive coverage
- workflow depth
- data-model portability

## Current Layer Status
### 1. Data Layer
Current status:
- strong enough for a payments-focused internal demo
- adequate for a pilot with caution
- not ready for full merchant-OS scale or source portability

Strengths:
- good payments transaction base
- sufficient settlement data for shortfall attribution
- app-owned state tables for actions and proactive cards

Weaknesses:
- schema coupling
- no canonical logical data contract
- reconciliation model still narrow
- weak source lineage and ingestion abstraction

### 2. Orchestration Layer
Current status:
- good enough for the demo path
- improved significantly from earlier rigid flows
- still not clean enough as long-term platform architecture

Strengths:
- bounded lane routing
- deterministic shortfall fast path
- verification guard
- lower latency than before

Weaknesses:
- too many orchestration modes
- heuristic routing still dominates
- proactive generation ownership is split

### 3. Proactive Layer
Current status:
- functionally real
- useful for demo and selective monitoring
- still narrow in trigger coverage

Strengths:
- deterministic signals
- stateful cards
- notes, dismissal, acknowledgment
- action conversion
- payout shortfall alerts are strong

Weaknesses:
- narrow trigger catalog
- non-segmented thresholds
- too many settlement-level cards instead of merchant-level summary events

### 4. Workflow / Action Layer
Current status:
- good enough to prove that the system is actionable
- not yet a mature operations workflow engine

Strengths:
- persisted actions
- preview-before-write support
- action metadata editing
- queue cleanup
- proactive-to-action conversion

Weaknesses:
- uneven upstream action quality still leaks into the queue
- no canonical action model
- no timeline/history model
- limited workflow depth

### 5. Conversation Layer
Current status:
- acceptable for the internal demo
- materially improved from the earlier report-style output
- still not fully natural by design

Strengths:
- greeting and out-of-scope handling
- lower tool budgets
- better tone and answer shape
- deterministic shortfall explanation path
- structured row rendering in the frontend

Weaknesses:
- no explicit intent model
- list/table responses are not first-class backend objects
- operations answer quality is still uneven outside the shortfall path

### 6. UI Alignment Layer
Current status:
- directionally aligned with the merchant OS story
- strong pilot shell
- still short of a polished product experience

Strengths:
- broad surface coverage
- scope controls
- proactive inbox and Action Center
- role-based reports

Weaknesses:
- React chat still uses shared message state across lanes
- some surfaces are still readout-heavy rather than workflow-first
- signal presentation is sometimes too granular

## Cross-Layer Strengths
These are the strongest things the product can currently do across layers:

1. Detect and explain a real payout shortfall.
- Data layer provides settlement and deduction fields.
- Orchestration layer provides a deterministic fast path.
- Proactive layer can surface the shortfall as an alert.
- Workflow layer can convert it into an action.
- Conversation layer can explain it in natural language.
- UI can display it in inbox, Action Center, and chat.

2. Produce a merchant payments control-plane experience.
- The product is no longer just a chat demo.
- It now has structured views for money, disputes, terminals, actions, and reports.

3. Keep trust anchors in place without fully collapsing into state machines.
- evidence ids
- deterministic verification where needed
- bounded tool use
- preview-before-write for actions

## Cross-Layer Bottlenecks
These are the main system-level bottlenecks that show up across multiple layers.

### 1. No canonical event model
The system has:
- chat answers
- proactive cards
- action objects
- runtime side effects
- background refresh outputs

But these do not yet converge cleanly into one canonical event model.

This causes:
- duplicated logic
- presentation inconsistencies
- multiple “owners” for the same real-world issue

### 2. Operational truth is stronger than general operations quality, but still localized
The payout-shortfall flow is strong because it has deterministic logic.
Other operations explanations are weaker because they still rely more on heuristics and summary-level reasoning.

This causes:
- uneven merchant trust
- strong demo paths but inconsistent broader ops behavior

### 3. Heuristic routing is still carrying too much weight
Intent detection is currently spread across:
- smalltalk shortcuts
- out-of-scope rules
- lane routing keywords
- broad overview detection
- shortfall detection

This works, but it is brittle and hard to evolve.

### 4. UI still mirrors backend granularity too directly
The backend may be technically correct while still creating a poor operator experience.
Example:
- multiple settlement-level shortfall cards for one merchant/window

### 5. The data layer is good enough for payments, not yet good enough for the full merchant OS vision
This affects every higher layer.
Until the system has a canonical data contract and better ingestion abstraction, the higher layers will remain more coupled than they should be.

## Demo-Critical Work
This is the work that most directly improves the internal demo.

### 1. Promote one payout-shortfall summary event
Goal:
- one merchant-level shortfall story
- optional settlement drilldown underneath

Why it matters:
- improves proactive layer
- improves UI clarity
- reduces card noise
- makes the demo cleaner

### 2. Align chat, proactive card, and Action Center on the same shortfall object
Goal:
- same settlement or merchant summary object
- same evidence ids
- same expected/received/difference values

Why it matters:
- prevents contradiction
- makes the system feel coherent and productized

### 3. Keep operations answers truthful outside the shortfall fast path
Goal:
- if deduction cause is not actually computed, say so
- do not let status summaries masquerade as explanations

Why it matters:
- protects trust during the demo

### 4. Improve demo ranking and presentation
Goal:
- make the highest-priority operational issue rise to the top
- suppress less relevant sibling signals during the demo flow

Why it matters:
- reduces operator intervention during the demo

### 5. Split React chat memory by lane
Goal:
- Operations and Growth actually feel like separate agents in the UI

Why it matters:
- improves product coherence immediately
- relatively small change with noticeable payoff

## Pilot-Critical Work
This is the work needed to move from a good demo shell to a more defensible pilot product.

### 1. Canonical action model
Needed fields:
- type
- status
- owner
- evidence ids
- workflow metadata
- history

### 2. Expand proactive coverage
Needed additions:
- overdue chargebacks
- payout mismatch summary
- refund exceptions
- merchant-segment-aware opportunity alerts

### 3. Introduce an explicit intent layer
At minimum:
- greeting
- out_of_scope
- overview
- list_request
- payout_shortfall
- dispute_status
- growth_opportunity

### 4. Make list responses first-class
The backend should return optional display structures for:
- transactions
- settlements
- chargebacks
- refunds

### 5. Reduce split ownership in proactive generation
Choose one canonical proactive event pipeline and make the other a consumer, not a peer.

## Future Platform Work
This is not needed for the internal demo, but it is necessary for the long-term merchant OS vision.

### 1. Canonical data contract
Create logical business models for:
- merchant
- terminal
- transaction
- settlement
- deduction
- refund
- chargeback
- action
- proactive event
- merchant profile

### 2. Bank API-first ingestion architecture
Use external APIs and feeds to normalize merchant data into an internal canonical model.
Do not make the UI or agents rely directly on raw bank APIs.

### 3. Source adapters and lineage
Every higher-layer signal should know:
- source system
- sync freshness
- transformation path

### 4. Workflow depth
Move from trackable actions to guided operational workflows.
Examples:
- representment package flow
- payout reconciliation flow
- refund exception resolution flow

### 5. Engine rationalization
Continue separating:
- active engines
- future integration candidates
- archived/redundant engines

## Recommended Sequencing
### Phase A: Internal Demo Hardening
1. Merchant-level payout shortfall summary event
2. Shortfall object alignment across chat/card/action
3. Lane-separated chat memory in React
4. Demo ranking and suppression of secondary noise

### Phase B: Pilot Readiness
1. Canonical action model
2. Expanded proactive trigger coverage
3. Intent layer
4. First-class structured list responses
5. Proactive pipeline consolidation

### Phase C: Platform Foundation
1. Canonical data contract
2. Bank API-first ingestion design
3. Source adapters and lineage
4. Workflow depth
5. DB-agnostic platforming

## Overall Assessment
### Internal demo
The system is close because the strongest story already exists:
- identify a payout shortfall
- explain it
- surface it proactively
- convert it into work

The remaining demo work is mostly coherence and presentation.

### Merchant OS vision
The system is directionally correct, but still early.
The shell is strong. The hard remaining work is in:
- canonical data and event models
- workflow depth
- proactive breadth
- architectural cleanup

## Practical Decision Rule
Before adding new features, ask:
- does this strengthen a current layer,
- or does it expand surface area without improving system coherence?

If it only expands surface area, it should probably wait.

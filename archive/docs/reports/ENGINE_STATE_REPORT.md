# Engine State Report

Date: 2026-03-10
Scope: Intelligence, proactive, copilot, and scenario engines in the current merchant OS codebase.

## Status Legend

- `ACTIVE`
  - wired into the current product path and materially used
- `ACTIVE_LIMITED`
  - wired and working, but narrow or uneven
- `PARTIAL`
  - implemented, but not central to the live product flow or not strong enough yet
- `EXPERIMENTAL`
  - optional/model-driven path, not a core trusted path
- `DORMANT`
  - present in the repo but not meaningfully wired into the current merchant OS flow
- `SUPPORTING`
  - helper engine, scorer, or aggregator that supports higher-level engines

## Executive Summary

The engine layer is not complete end-to-end.

What is solid:
- deterministic phase-1 recommendation engines
- phase-2 evidence collection
- proactive card engine
- payout shortfall engine
- health and impact scoring

What is not yet complete:
- broad proactive coverage
- merchant-wide operations truthfulness outside the shortfall path
- full scenario / merchant copilot integration into the main merchant OS path

For the internal payout-shortfall demo, the required engines are mostly in place.
For the full merchant OS vision, the engine layer is still partial.

## Orchestration Layer

### `run_intelligence`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/runner.py`
- Status: `ACTIVE`
- Role:
  - main intelligence orchestrator
  - runs deterministic phase-1 engines
  - collects phase-2 evidence
  - builds health and impact vectors
  - optionally runs phase-2 LLM reasoning
  - persists actions

### Core Logic
- resolve date window from `transaction_features`
- compute baseline KPIs and top failure codes
- run deterministic recommendation engines
- collect operational / reconciliation / dispute evidence
- score health and impact
- gate phase-2 LLM reasoning on materiality and signal complexity
- persist top recommendations as `merchant_actions`

### Current Assessment
- good as an orchestrator
- still recommendation-oriented rather than workflow-oriented
- more reliable for growth/performance than for full reconciliation truth

## Phase-1 Deterministic Recommendation Engines

### `lost_sales`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/lost_sales.py`
- Status: `ACTIVE`
- Role:
  - estimates recoverable revenue from failed payments
- Core Logic:
  - sums failed GMV from `transaction_features`
  - applies calibrated recovery rate
  - maps top failure codes into action playbooks
- Strength:
  - deterministic and materially useful
- Limitation:
  - still an estimate, not true causal attribution

### `anomaly`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/anomaly.py`
- Status: `ACTIVE`
- Role:
  - detects success-rate drop versus baseline
- Core Logic:
  - compares current success rate to a merchant-relative rolling baseline
  - applies volume and revenue-impact thresholds
  - ranks anomalies by success-rate drop and estimated value at risk
- Strength:
  - merchant-aware and impact-aware
- Limitation:
  - still heuristic, not predictive

### `payment_mode`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/payment_mode.py`
- Status: `ACTIVE`
- Role:
  - produces dominant payment-mode growth recommendation
- Core Logic:
  - identifies top successful payment mode
  - computes current and previous mode share
  - computes share trend and successful revenue
  - emits deterministic summary and actions
- Strength:
  - fast and deterministic
- Limitation:
  - simplistic growth framing
  - does not reason about merchant goals or current product enablement

### `peak_hour`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/peak_hour.py`
- Status: `ACTIVE`
- Role:
  - highlights strongest revenue hour
- Core Logic:
  - finds top `hour_of_day` by successful revenue
  - overlays failure rate and failed volume in the same peak hour
  - emits deterministic operational growth recommendation
- Strength:
  - direct and stable
- Limitation:
  - useful only when hourly distribution matters
  - can be shallow as a recommendation

## Phase-2 Evidence Engines

### `operational_signals`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/operational_signals.py`
- Status: `ACTIVE`
- Role:
  - produces raw payment-operations evidence
- Core Logic:
  - attempts / success / fail metrics
  - payment-mode breakdown
  - top failure codes
  - failure rate by hour and day
  - terminal health table
- Strength:
  - strongest raw evidence engine in the layer
- Limitation:
  - evidence-heavy, not explanation-heavy

### `reconciliation_signals`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/reconciliation_signals.py`
- Status: `ACTIVE`
- Role:
  - produces deduction-aware settlement and reconciliation evidence
- Core Logic:
  - computes window-scoped successful txn and GMV metrics
  - sums refunds and chargebacks
  - computes gross vs net settlement
  - computes known deductions and unexplained residual
  - compares actual MDR with expected MDR when `mdr_rates` is available
  - enriches from `reconciliation_records` when present
- Strength:
  - materially better for payout and settlement trust
- Limitation:
  - still depends on source-table completeness

### `dispute_signals`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/dispute_signals.py`
- Status: `ACTIVE`
- Role:
  - dispute and chargeback operational risk evidence
- Core Logic:
  - computes open, overdue, won, and lost counts and values
  - computes resolution rate
  - ranks reason codes by count and value
  - breaks disputes down by network and aging bucket
- Strength:
  - useful for merchant-visible dispute risk review
- Limitation:
  - dataset is sparse in the current DB

### `collect_phase2_evidence`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/evidence_aggragator.py`
- Status: `SUPPORTING`
- Role:
  - bundles `operational`, `reconciliation`, and `disputes`

## Health and Impact Scoring

### `health_engine`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/health_engine.py`
- Status: `SUPPORTING`
- Role:
  - converts evidence into health status and flags
- Core Logic:
  - scores performance, reconciliation, disputes, and data quality explicitly
  - exposes sub-scores and weights
  - flags unexplained residuals, overdue disputes, unknown failure share, and card-vs-UPI gap
  - returns:
    - `health_score`
    - `status`
    - `sub_scores`
    - `weights`
    - `flags`
    - positive/negative drivers
- Strength:
  - useful as a compact summary
- Limitation:
  - heuristic
  - depends on sparse reconciliation/dispute inputs

### `impact_engine_v2`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/impact_engine_v2.py`
- Status: `SUPPORTING`
- Role:
  - computes simple merchant-impact vectors
- Core Logic:
  - lost sales
  - unknown failure value
  - chargeback risk
  - reconciliation gap
  - overdue chargeback risk
  - explained vs unexplained reconciliation gap
- Strength:
  - helpful for ranking
- Limitation:
  - coarse, not causal

## Proactive Engines

### `insight_cards`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/insight_cards.py`
- Status: `ACTIVE_LIMITED`
- Role:
  - template-driven proactive card generation
- Core Logic:
  - loads fixed templates from `/Users/madhavpatel/New_demo copy/agent/CARDS`
  - computes merchant metrics from `transaction_features`, `refunds`, `chargebacks`, `settlements`, `merchant_kyc_documents`
  - evaluates trigger conditions
  - emits cards with impact and confidence
- Strength:
  - deterministic and fast
- Limitation:
  - narrow trigger catalog
  - mostly warning-driven
  - many merchants show no cards

### `payout_shortfall_monitor`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/payout_shortfall_monitor.py`
- Status: `ACTIVE`
- Role:
  - deterministic payout shortfall detection and card/action generation
- Core Logic:
  - scans `settlements`
  - finds rows where `gross_amount - net_settlement_amount >= threshold`
  - reuses deterministic shortfall explainer
  - creates:
    - proactive card payload
    - linked action payload
- Strength:
  - strongest internal-demo engine
  - grounded in real settlement fields
- Limitation:
  - settlement-centric, so one merchant can get multiple shortfall cards in one window

## Copilot and Scenario Engines

### `merchant_copilot`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/redundant/merchant_copilot.py`
- Status: `PARTIAL`
- Role:
  - alternate merchant Q&A layer with insight/recommendation/scenario handling
- Core Logic:
  - detects intent
  - may run scenario engine
  - may call LLM for narrative or experimental reasoning
- Current State:
  - implemented
  - not the main path for the current merchant OS UI/runtime
- Assessment:
  - useful conceptually
  - not central to current product behavior

### Scenario engine package
- Files:
  - `/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/planner.py`
  - `/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/assumptions.py`
  - `/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/baseline.py`
  - `/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/simulators.py`
  - `/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/narrator.py`
  - `/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/service.py`
- Status: `PARTIAL`
- Role:
  - deterministic what-if simulations with optional LLM planner/narrator
- Core Logic:
  - parse scenario intent
  - resolve assumptions/defaults
  - fetch deterministic baseline
  - simulate outcome
  - narrate result
- Strength:
  - coherent subsystem
- Limitation:
  - not part of the main merchant OS demo path

## Active Explainability Engines

### `attribution`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/attribution.py`
- Status: `ACTIVE`
- Role:
  - explains which dimensions contributed most to a KPI change across periods
- Core Logic:
  - compares current window to previous equal-length window
  - computes contribution-style attribution by:
    - payment mode
    - response code
    - hour of day
    - terminal
    - payer bank
  - ranks rows by impact and delta
- Current State:
  - active and wired into deterministic runner flow
  - used as contextual explainability, not causal inference
- Assessment:
  - useful for "what changed?" analysis
  - still intentionally shallow compared to full causal models

### `kpi_delta`
- File: `/Users/madhavpatel/New_demo copy/app/intelligence/engines/kpi_delta.py`
- Status: `ACTIVE`
- Role:
  - window-over-window KPI trend context for merchant and payment mode
- Core Logic:
  - compares current and previous equal-length windows
  - returns merchant-level and by-mode deltas for:
    - attempts
    - success txns
    - success GMV
    - success rate
    - average ticket
- Current State:
  - active and wired into deterministic runner flow
- Assessment:
  - useful context engine
  - not a standalone merchant-facing story by itself

## Engine Performance Assessment

### Engines performing well enough for the internal demo

- `payout_shortfall_monitor`
- deterministic shortfall explainer in the copilot tool layer
- `runner` phase-1 deterministic path
- `reconciliation_signals`
- `dispute_signals`
- `payment_mode`
- `peak_hour`
- `operational_signals`
- `attribution`
- `kpi_delta`
- proactive card persistence and action sync

### Engines that are implemented but still weak

- `insight_cards`
  - too narrow in trigger coverage
- `reconciliation_signals`
  - strong for settlement evidence, still limited by source-table completeness
- `dispute_signals`
  - materially deeper now, but current DB is sparse
- `anomaly`
  - improved, but still heuristic rather than predictive

### Engines not earning their keep yet

- `merchant_copilot`
- scenario engine in the main merchant OS product path

## Test Coverage Snapshot

Covered directly in tests:
- `run_intelligence`
- `reconciliation_signals`
- `dispute_signals`
- `health_engine`
- `impact_engine_v2`
- `attribution`
- `kpi_delta`
- `anomaly`
- `payment_mode`
- `peak_hour`
- `insight_cards`
- `scenario_engine`
- payout shortfall path through merchant OS tests

Main test files:
- `/Users/madhavpatel/New_demo copy/tests/test_engine_signal_refinement.py`
- `/Users/madhavpatel/New_demo copy/tests/test_intelligence_fixes.py`
- `/Users/madhavpatel/New_demo copy/tests/test_insight_cards.py`
- `/Users/madhavpatel/New_demo copy/tests/test_scenario_engine.py`
- `/Users/madhavpatel/New_demo copy/tests/test_merchant_os.py`

Coverage is strongest for:
- deterministic recommendation generation
- proactive cards
- shortfall path

Coverage is weakest for:
- full production wiring of merchant copilot

## What This Means

### For the internal demo

You have the engines you need.

The internal demo depends mainly on:
- shortfall attribution
- proactive shortfall surfacing
- action sync
- operations explanation

Those are largely present.

### For the full merchant OS vision

The engine layer is not complete.

Major gaps:
- more merchant-aware proactive engines
- broader operations truth engines
- better growth ranking and composition
- tighter integration of scenario and copilot subsystems

Archived redundant modules now live under:
- `/Users/madhavpatel/New_demo copy/app/intelligence/redundant`

## Recommended Next Actions

1. Keep the active demo engines and harden them.
- especially shortfall summary, card/action/chat alignment, and ops wording

2. Decide whether to deepen or simplify the remaining non-core engines.
- `merchant_copilot`
- scenario engine

3. Expand proactive coverage only after the demo path is stable.

4. Do not let experimental engines define the core product path yet.

## Bottom Line

Not all engines are in place and performing as they should.

But the engines required for the internal payout-shortfall demo are mostly in place and usable.

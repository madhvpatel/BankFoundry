# Merchant OS Capabilities Matrix

Date: 2026-03-10
Scope: Current application capabilities explained in business terms, with the engines behind each capability.

## How to Read This

This document answers four questions for each capability:
- what merchant problem the app solves
- what the app does today
- which engines make it work
- how mature that capability currently is

## Maturity Legend

- `Strong`
  - usable today and materially valuable
- `Usable`
  - works, but still uneven or narrow
- `Early`
  - present, but not strong enough for broad reliance
- `Future`
  - concept exists, but not part of the trusted product path yet

## Capability Matrix

| Merchant Problem | What the App Does | Engines Used | Current Maturity |
| --- | --- | --- | --- |
| “How is my payments business doing overall?” | Builds a merchant health snapshot across transactions, success rate, reconciliation, and dispute risk. | [`runner.py`](/Users/madhavpatel/New_demo copy/app/intelligence/runner.py), [`operational_signals.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/operational_signals.py), [`reconciliation_signals.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/reconciliation_signals.py), [`dispute_signals.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/dispute_signals.py), [`health_engine.py`](/Users/madhavpatel/New_demo copy/app/intelligence/health_engine.py), [`impact_engine_v2.py`](/Users/madhavpatel/New_demo copy/app/intelligence/impact_engine_v2.py) | `Strong` |
| “Am I losing money because payments are failing?” | Estimates recoverable value from failed payments and highlights likely failure-driven revenue leakage. | [`lost_sales.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/lost_sales.py), [`runner.py`](/Users/madhavpatel/New_demo copy/app/intelligence/runner.py) | `Strong` |
| “Did my success rate drop?” | Detects a meaningful success-rate drop versus baseline and raises a performance recommendation. | [`anomaly.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/anomaly.py), [`runner.py`](/Users/madhavpatel/New_demo copy/app/intelligence/runner.py) | `Usable` |
| “What is my main growth opportunity?” | Highlights the dominant payment mode and strongest revenue hour, then turns them into simple growth guidance. | [`payment_mode.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/payment_mode.py), [`peak_hour.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/peak_hour.py), [`runner.py`](/Users/madhavpatel/New_demo copy/app/intelligence/runner.py) | `Usable` |
| “What changed in my business compared to the previous period?” | Compares current vs previous window KPIs and explains which modes, codes, hours, terminals, or banks contributed most to the change. | [`kpi_delta.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/kpi_delta.py), [`attribution.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/attribution.py), [`runner.py`](/Users/madhavpatel/New_demo copy/app/intelligence/runner.py) | `Usable` |
| “Where are my payment failures concentrated?” | Breaks down failures by code, mode, hour, terminal, and merchant window. | [`operational_signals.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/operational_signals.py), copilot tool layer in [`tools.py`](/Users/madhavpatel/New_demo copy/app/copilot/tools.py), chat orchestration in [`runtime.py`](/Users/madhavpatel/New_demo copy/app/copilot/runtime.py) | `Strong` |
| “What is happening with chargebacks and disputes?” | Shows dispute counts, stages, reason codes, and networks; supports dispute-focused operational review. | [`dispute_signals.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/dispute_signals.py), [`reconciliation_signals.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/reconciliation_signals.py) | `Usable` |
| “Why do my settlements or payouts look wrong?” | Summarizes settlement, refund, and chargeback evidence; supports operational investigation. | [`reconciliation_signals.py`](/Users/madhavpatel/New_demo copy/app/intelligence/engines/reconciliation_signals.py), settlement tools in [`tools.py`](/Users/madhavpatel/New_demo copy/app/copilot/tools.py) | `Usable` |
| “I should have received X but received less. Why?” | Detects payout shortfalls, matches expected vs received payout, and explains known deduction components. | [`payout_shortfall_monitor.py`](/Users/madhavpatel/New_demo copy/app/intelligence/payout_shortfall_monitor.py), deterministic shortfall explainer in [`tools.py`](/Users/madhavpatel/New_demo copy/app/copilot/tools.py), chat flow in [`runtime.py`](/Users/madhavpatel/New_demo copy/app/copilot/runtime.py) | `Strong` |
| “Can the system alert me without me asking?” | Generates background proactive cards for failures, settlement issues, KYC risks, and shortfalls. | [`insight_cards.py`](/Users/madhavpatel/New_demo copy/app/intelligence/insight_cards.py), [`payout_shortfall_monitor.py`](/Users/madhavpatel/New_demo copy/app/intelligence/payout_shortfall_monitor.py), merchant OS orchestration in [`merchant_os.py`](/Users/madhavpatel/New_demo copy/app/merchant_os.py) | `Usable` |
| “Can the system turn an issue into a task?” | Creates and manages Action Center items from recommendations and proactive signals. | [`action_center.py`](/Users/madhavpatel/New_demo copy/app/intelligence/action_center.py), [`runner.py`](/Users/madhavpatel/New_demo copy/app/intelligence/runner.py), [`merchant_os.py`](/Users/madhavpatel/New_demo copy/app/merchant_os.py) | `Usable` |
| “Can I ask questions in plain English?” | Supports chat-driven analysis for operations and growth, including lists, summaries, and targeted drilldowns. | [`runtime.py`](/Users/madhavpatel/New_demo copy/app/copilot/runtime.py), [`toolcalling.py`](/Users/madhavpatel/New_demo copy/app/copilot/toolcalling.py), [`tools.py`](/Users/madhavpatel/New_demo copy/app/copilot/tools.py) | `Strong` |
| “Can I ask what-if questions?” | Supports scenario planning such as refund reduction, chargeback reduction, payment-mode shift, and success-rate uplift. | scenario engine: [`service.py`](/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/service.py), [`planner.py`](/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/planner.py), [`simulators.py`](/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/simulators.py), [`baseline.py`](/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/baseline.py), [`narrator.py`](/Users/madhavpatel/New_demo copy/app/intelligence/scenario_engine/narrator.py) | `Early` |
| “Can the system deeply reason like an analyst?” | Optional model-driven reasoning exists, but it is gated and not the primary trusted path. | [`agent_reasoning.py`](/Users/madhavpatel/New_demo copy/app/intelligence/agent_reasoning.py), phase-2 gating in [`runner.py`](/Users/madhavpatel/New_demo copy/app/intelligence/runner.py) | `Early` |
| “Can the app understand my whole business, not just payments?” | Only partially. Today it is strongest on payments, settlements, disputes, and payout operations. | Current engines are mostly payment-domain engines; no full ERP/customer/inventory engine layer yet. | `Future` |

## What the App Is Best At Today

The strongest current capabilities are:
- payment performance analysis
- failed-payment recovery insight
- payout shortfall explanation
- proactive payment operations alerts
- action creation and tracking
- chat-based merchant Q&A on payment operations

## What Is Still Narrow or Uneven

These capabilities exist, but are not broad enough yet:
- proactive alert coverage across all merchants
- terminal-specific growth explanation quality
- general reconciliation truth outside the payout-shortfall path
- dispute and refund intelligence in sparse-data merchants

## What Is Not Yet a Full Merchant OS Capability

These are still outside the current strong path:
- full business transparency beyond payments
- ERP/accounting/POS-native orchestration
- customer, catalog, and inventory intelligence
- broad predictive intelligence
- fully personalized merchant memory and goal-aware recommendations

## Internal Demo View

For the internal demo, the strongest story is:

1. detect a payout shortfall
2. explain the shortfall deterministically
3. surface it proactively
4. create the matching operational task
5. let the merchant ask follow-up questions in chat

The engines most relevant to that demo are:
- [`payout_shortfall_monitor.py`](/Users/madhavpatel/New_demo copy/app/intelligence/payout_shortfall_monitor.py)
- [`tools.py`](/Users/madhavpatel/New_demo copy/app/copilot/tools.py)
- [`runtime.py`](/Users/madhavpatel/New_demo copy/app/copilot/runtime.py)
- [`merchant_os.py`](/Users/madhavpatel/New_demo copy/app/merchant_os.py)
- [`action_center.py`](/Users/madhavpatel/New_demo copy/app/intelligence/action_center.py)

## Bottom Line

In plain language, the application already works as:
- a payments intelligence layer
- a payout and settlement operations assistant
- a proactive alert system for merchant payment issues
- a task-driven operating shell for payment operations

It does not yet fully work as:
- a complete business operating system across every merchant workflow

For that broader vision, more data integrations and more domain engines will be needed.

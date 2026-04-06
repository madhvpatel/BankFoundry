# Plug-and-Play Capability Architecture

This project now supports a plug-and-play architecture for analytics capabilities and operations-research (OR) models.

## Why this exists

- Keep `/ask` and `/analytics` stable while capabilities evolve.
- Add new analytical domains without editing endpoint logic.
- Add OR scenarios as independent modules (optimization-ready design).

## Core Components

- Capability contracts:
  - `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/contracts.py`
- Capability registry:
  - `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/registry.py`
- OR contracts:
  - `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/or_contracts.py`
- OR registry:
  - `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/or_registry.py`
- Bootstrap wiring:
  - `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/bootstrap.py`
- Runtime orchestrator:
  - `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/orchestrator.py`

Current default capability plugin:
- `legacy_analysis` at
  `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/capabilities/legacy_analysis.py`

New domain capability plugins:
- `settlement_recon` (`settlement_recon_insights`) at
  `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/capabilities/settlement_recon.py`
- `disputes` (`disputes_insights`) at
  `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/capabilities/disputes.py`
- `fraud_risk` (`fraud_risk_insights`) at
  `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/capabilities/fraud_risk.py`

Current default OR model:
- `card_success_lift_scenario` at
  `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/or_models/card_success_lift.py`

## Runtime flow

1. Router decides `route` and `analysis_type`.
2. Orchestrator resolves capability plugin from registry.
3. Plugin executes deterministic analytics and returns artifacts.
4. OR registry evaluates scenario models on returned metrics/data.
5. Response is composed and returned to `/ask` or `/analytics`.

## Adding a new capability plugin

1. Create a class implementing:
   - `name`
   - `supports(route)`
   - `execute(req)`
2. Register it in `ensure_bootstrapped()`:
   - `/Users/madhavpatel/payments-intelligence-demo/app/core/plugplay/bootstrap.py`
3. Return structured artifacts (plan/result/recommendations) so debug traces stay consistent.

## Adding a new OR model

1. Create a class implementing:
   - `name`
   - `applies(ctx)`
   - `run(ctx) -> List[ORRecommendation]`
2. Register it in `ensure_bootstrapped()`.
3. Use only deterministic metrics/data from `ctx` (no guessed numbers).
4. Include assumptions in `constraints`.

## Design guardrails

- Capabilities are deterministic first, narrative second.
- OR models are sidecar modules, not hardcoded in endpoint handlers.
- Endpoint layer should not be edited to add new capabilities or OR models.

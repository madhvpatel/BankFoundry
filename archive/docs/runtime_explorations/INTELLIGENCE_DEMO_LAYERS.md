# Intelligence Demo Layers

This demo layer is deterministic-first and designed for consultant-style merchant insights.

## Routing Model (Updated)

- `/ask` now uses LLM-first routing (via `AnalysisRouter`) for all questions.
- Deterministic marker-based short-circuit routes were removed from `/ask`.
- New route `intelligence` is selected by router for consultant-style asks (growth, compliance/liability posture, tax/accounting strategy, "what should I do").
- Metrics remain deterministic (database derived), while final reasoning on intelligence route is AI-generated from deterministic facts.
- Intelligence responses include numeric-safety validation (LLM numbers must come from deterministic facts; otherwise fallback is applied).

## New Backend Capabilities

1. `POST /intelligence/brief`
- Builds a full consultant brief from bank-side merchant data.
- Sections:
  - `ops_health`
  - `compliance`
  - `liabilities`
  - `product_eligibility`
  - `tax_accounting`
  - `predictive_intelligence`
  - `or_recommendations`
  - `proactive_cards`

2. `POST /intelligence/proactive`
- Returns ranked proactive cards only (compliance, liability, opportunity alerts).

3. `POST /ask` intelligent consultant route
- `intent=consultant_intelligence` now triggers full deterministic consultant brief generation.
- Typical prompts:
  - "What can I do to improve my business?"
  - "Give me compliance and liability risks for last 30 days"
  - "Increase revenue: what should I do next?"
- Response metadata exposes deterministic behavior:
  - `response_type=deterministic_intelligence_brief` (or fallback variant)
  - `llm_used=false`
  - `section_warnings` when data sections are unavailable.

## Conversational Proactive Nudges

- `/ask` now injects a proactive nudge periodically (every 3rd turn) for business/analytics intents.
- Nudge is deterministic and sourced from proactive cards.

## Determinism & Metadata

- All intelligence endpoints return deterministic outputs.
- `response_meta` includes:
  - `response_type`
  - `confidence`
  - `llm_used`
  - `section_warnings` for graceful degradation visibility.

## Robustness for Demo

- Intelligence brief assembly is now section-safe:
  - merchant profile
  - ops health
  - liabilities
  - compliance
  - tax/accounting
  - predictive
  - OR recommendations
- If any section fails (missing table/column/data issues), the endpoint still returns a usable brief with warnings.
- Debug SQL visibility now includes direct `Database.fetch_all` calls, so `/ask` debug output can show intelligence-layer queries too.

## Tool Registry

- Agent-callable tools are centralized in:
  - `app/core/tool_registry.py`
- Registry includes `use_when` and `why` guidance, injected into the system context.

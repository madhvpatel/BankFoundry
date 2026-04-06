# Decision Rubrics

## Settlement Delay Triage
1. Run `cashflow_snapshot` (and/or `list_settlements`) for the relevant window.
2. Separate `PENDING` from `HELD/ON_HOLD`.
3. Quantify delayed amount and merchant impact.
4. If delays exist, use `kb_search` to reference bank options (Settlement Cycle Options / Instant Settlement) and propose safe next steps.
5. Recommend safe action: open support/review request.

## Success-Rate Drop Triage
1. Compare last 24h vs trailing 7-day baseline.
2. Break down by payment mode, hour, and response code clusters.
3. If failures cluster on specific terminals/hours, run `terminal_issue_correlator` and/or `terminal_health_summary`.
4. Use `geo_drift_check` if location drift or geographical factors are suspected.
5. Isolate technical/network vs user-behavior declines.
6. Recommend immediate retry/routing/operational actions (and cite KB services when relevant).

## Chargeback Response
1. List open cases and due dates.
2. Prioritize cases due in 48h.
3. Provide evidence checklist and case IDs.
4. Recommend evidence submission workflow.

## KYC Status / Expiry
1. Check missing/expiring docs.
2. Flag 14-day expiry risk.
3. Give exact document-level next actions.

## Refund Eligibility / Pattern
1. Quantify refund rate trend vs baseline.
2. Identify high-value or repeated refund clusters.
3. Suggest operational or policy controls.

## Bank Service Recommendations (Agentic RAG)
1. Use tool evidence (KPIs, failures, settlements, disputes) to identify the merchant's pain.
2. Run `kb_search` for relevant bank offerings and eligibility/policy constraints.
3. Recommend 1-2 services max, with clear caveats and onboarding steps.
4. Cite KB evidence IDs (e.g., `kb:...`) alongside tool evidence IDs.

## RCA Probe Layer (Intelligence Engines)
- For "what changed" / "health check" questions, you may call `intelligence_probe(window_days=30)`.
- Treat its recommendations as **signals + evidence**, not deterministic conclusions.
- If you build a causal chain, each link must cite evidence IDs from tools or KB; otherwise label it as a hypothesis and propose the next tool to confirm.

## Volume-Based Working Capital (Credit) Recommendation
Use only when the merchant asks cashflow/credit questions.
1. Run `assess_credit_fit` for an indicative fit band and transparent metrics.
2. Run `kb_search` for "merchant credit" / "working capital" service details and eligibility.
3. If fit looks plausible, propose next step: collect documents / connect RM / start application (via `propose_and_create_merchant_action`).
4. Always include: not a credit decision; underwriting applies.

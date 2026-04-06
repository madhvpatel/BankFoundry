# Tool Catalog

## get_merchant_context
- Returns merchant profile, risk profile, and KYC snapshot.
- Use when user asks about account standing or KYC readiness.
- Always scope by `merchant_id`.

## list_transactions
- Returns paginated transaction list with status, amount, method, failure fields.
- Use for volume/failure drilldowns.
- Enforce row limits and tenant scoping.

## get_transaction_detail
- Returns a single transaction record (merchant-scoped).
- Use for dispute or failure investigations.

## compute_kpis
- Returns KPI aggregates over a date range and grouping.
- Use for trend checks, insight cards, and comparisons.

## list_settlements
- Returns settlement summaries in a date range.
- Use for payout or delay questions.

## get_settlement_detail
- Returns one settlement and (if available) reconciliation breakdown for that settlement.
- Use before proposing payout-related actions.

## list_chargebacks
- Returns chargeback list by status.
- Use for risk and due-date monitoring.

## get_chargeback_detail
- Returns one chargeback record.
- Use for evidence preparation.

## list_refunds
- Returns refund records and status.
- Use for refund-rate diagnostics.

## compare_kpis
- Compares KPI windows A vs B (two compute_kpis runs).
- Use for "last 7 days vs previous 7" style questions.

## terminal_performance
- Ranks terminals by attempts and success rate.
- Use for "is one device/terminal failing" questions.

## assess_credit_fit
- Computes an indicative fit assessment for a bank working-capital product using recent volume + stability + dispute/refund proxies.
- Use only when the merchant asks cashflow/credit questions.
- This is NOT an underwriting/approval decision; always include caveats.

## cashflow_snapshot
- Summarizes settlement timing + cashflow signals over a window.
- Includes counts/amounts by status, pending vs settled amount, and "past expected date" counts (best-effort across schemas).
- Use for payout timing/cashflow questions.

## terminal_health_summary
- Summarizes `terminal_health_snapshots` (battery/network/printer/etc.) for RCA.
- Use to support network/latency/device-health causal chain links.

## geo_drift_check
- Uses terminal health snapshot lat/long to quantify drift and deviation flags.
- Use to support geographical/location-factor causal chain links.

## terminal_issue_correlator
- Correlates terminal health flags (e.g., low_network_strength) with elevated payment failure rates by terminal.
- Use to identify terminals likely driving failures due to network/device issues.

## intelligence_probe
- Runs the intelligence runner (phase-1 engines by default) to produce evidence-first recommendations and impact estimates.
- Use as a probe layer for RCA and proactive insights (merchant health checks, what changed, lost sales).
- Keep output small; cite returned evidence IDs.

## end_to_end_analysis
- Runs an end-to-end health check bundle for the merchant.
- Includes: overall KPIs, payment modes, top failure codes, terminal health, and (best-effort) slices by card network / POS type / device type.
- Use for proactive weekly health checks.

## propose_and_create_merchant_action
- Two-step write tool with confirmation token.
- First call returns preview + token.
- Second call with token performs write to `merchant_actions` (if table exists).

## kb_search
- Searches the bank knowledge base in `bank_kb/*.md`.
- Use to answer questions about bank services and to recommend offerings (Instant Settlement, Smart Routing, Chargeback Assist, etc.).
- Include citations from returned `evidence` IDs.

## kb_reindex
- Rebuilds the local KB index from `bank_kb/*.md`.
- Use when KB content has changed and the index must be refreshed.

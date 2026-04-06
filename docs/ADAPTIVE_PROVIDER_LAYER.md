# Adaptive Provider Layer

## What was fragile

Before this change, many reads assumed:

- payments always came from `transaction_features`
- settlements always came from `settlements`
- the source schema always used the same column names

That meant a new database, a renamed table, or a bank-export schema could break merchant selection, coverage checks, or repository queries even when the source data was still usable.

## What changed

A small provider-resolution layer was added under `app/data/providers/`.

It does two things:

- detects the best available payments and settlements source table
- maps legacy or alternate column names into the app's canonical field names

Examples:

- payments: `merchant_id` or `mid`, `transaction_fact_id` or `transaction_id`, `payment_mode` or `mode`
- settlements: `merchant_id` or `mid`, `status` or `settlement_status`, `reference` or `settlement_utr`

The main repositories now use this layer first:

- merchant options
- merchant system coverage
- transaction repository reads
- settlement repository reads

So if the preferred primary table is missing, the app can fall back to a compatible alternate table instead of failing immediately.

This follow-up slice extends the same idea into the intelligence layer.

Before that, the signal engines and runner still assumed:

- intelligence windows always came from `transaction_features`
- settlement evidence always came from `settlements`
- retry-calibration, drift checks, KPI deltas, and payment-mode insights could read raw source columns directly

Now the intelligence path also resolves canonical sources first and then queries those canonical expressions.

That means these flows can now adapt to legacy or alternate schemas without hand-editing the engine SQL:

- operational signals
- reconciliation signals
- KPI deltas and attribution
- anomaly and lost-sales calculations
- payment-mode and peak-hour recommendations
- amount scaling and retry-recovery calibration
- payout shortfall monitoring
- the main `run_intelligence(...)` orchestration path

## How to verify

Run:

```bash
pytest -q \
  tests/test_adaptive_intelligence_layer.py \
  tests/test_engine_signal_refinement.py \
  tests/test_response_code_mapping.py \
  tests/test_adaptive_provider_layer.py \
  tests/test_data_repositories.py \
  tests/test_merchant_ops_repository.py \
  tests/test_copilot_tool_repositories.py
```

Optional spot check:

1. Point the app at a database without `transaction_features` but with a compatible alternate payments table.
2. Verify merchant options still load.
3. Verify transaction KPIs and transaction detail still return data.
4. Point the app at a database without `settlements` but with a compatible alternate settlement table.
5. Verify settlement listing, shortfall explanation, and connected-system coverage still work.

## Real issues vs noise

Real product issues:

- provider resolution cannot find a source with the canonical merchant/date fields
- a settlement detail flow needs `settlement_id`, but the source table does not expose any settlement identifier
- fallback succeeds, but returned rows are empty because the merchant scope does not match the source
- the source table exists, but it does not expose the fields needed for a specific intelligence feature such as `payment_mode`, `hour_of_day`, or settlement amount columns
- secondary code paths that still bypass the provider layer can remain brittle until they are migrated too; the main signal and runner flow is covered by this change, but not every auxiliary report path in the repo is

Usually not a product issue:

- a fallback note saying the repo used `payment_transactions` or `settlement_records`
- a fallback note inside intelligence output saying it used `payment_transactions` or `settlement_records`
- unrelated browser-extension console noise
- unrelated framework deprecation warnings elsewhere in the repo

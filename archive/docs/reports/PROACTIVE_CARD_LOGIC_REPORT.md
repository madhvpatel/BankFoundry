# Proactive Card Logic Report

Date: 2026-03-10
Scope: Current proactive card generation, persistence, and merchant coverage in the live demo database.

## Summary

The proactive card system is not hardcoded to a specific merchant.

What is hardcoded:
- the set of card templates
- the trigger conditions for those templates
- the shortfall monitor rule shape

What is dynamic:
- whether a merchant gets any cards
- which cards appear
- how many cards appear for a given window
- the evidence and impact shown on each card

At the moment, the system is a fixed scenario detector, not a broad proactive engine. That is why only a small subset of merchants currently show cards.

## Generation Flow

There are two background card generators:

1. Template-driven KPI cards
- Implemented in `/Users/madhavpatel/New_demo copy/app/intelligence/insight_cards.py`
- Entry point: `generate_insight_cards(...)`
- Uses metrics derived from `transaction_features`, `refunds`, `chargebacks`, `settlements`, and `merchant_kyc_documents`
- Loads card definitions from `/Users/madhavpatel/New_demo copy/agent/CARDS`

2. Deterministic payout shortfall cards
- Implemented in `/Users/madhavpatel/New_demo copy/app/intelligence/payout_shortfall_monitor.py`
- Entry point: `generate_payout_shortfall_alerts(...)`
- Scans settlement rows for `gross_amount - net_settlement_amount >= min_difference_rupees`
- Reuses the deterministic shortfall explainer to build the card and matching action payload

These are merged during refresh in `/Users/madhavpatel/New_demo copy/app/merchant_os.py`:
- `refresh_background_proactive_cards(...)`

## Template Card Logic

### Data Source

Template cards are generated from merchant-level metrics built in `_build_metrics(...)` in:
- `/Users/madhavpatel/New_demo copy/app/intelligence/insight_cards.py`

That metrics builder does the following:
- anchors the window to the merchant's latest `p_date` in `transaction_features`
- computes 30-day totals
- computes last-24h and previous-7d baselines
- computes derived impact values used by templates

### Current Template Set

Current card templates are stored in:
- `/Users/madhavpatel/New_demo copy/agent/CARDS/chargeback_deadline.md`
- `/Users/madhavpatel/New_demo copy/agent/CARDS/high_value_failed_txns.md`
- `/Users/madhavpatel/New_demo copy/agent/CARDS/kyc_expiry.md`
- `/Users/madhavpatel/New_demo copy/agent/CARDS/refund_rate_spike.md`
- `/Users/madhavpatel/New_demo copy/agent/CARDS/settlement_delay.md`
- `/Users/madhavpatel/New_demo copy/agent/CARDS/success_rate_drop.md`
- `/Users/madhavpatel/New_demo copy/agent/CARDS/terminal_anomaly.md`
- `/Users/madhavpatel/New_demo copy/agent/CARDS/upi_callback_delay_spike.md`

### Current Trigger Conditions

These are the exact active conditions:

- `chargeback_deadline`
  - `chargeback_due_48h_count >= 1`

- `high_value_failed_txns`
  - `high_value_failed_count >= 1`

- `kyc_expiry`
  - `kyc_expiring_14d_count >= 1 or kyc_overdue_count >= 1`

- `refund_rate_spike`
  - `refund_count_24h >= 2 and refund_rate_24h >= refund_rate_7d_avg * 1.5`

- `settlement_delay`
  - `settlement_delayed_count >= 1`

- `success_rate_drop`
  - `success_rate_drop_pp >= 1.5 and attempts_24h >= 50`

- `terminal_anomaly`
  - `top_terminal_attempts >= 20 and terminal_fail_ratio >= 3`

- `upi_callback_delay_spike`
  - `callback_delay_ratio >= 2.0 and callback_delay_p95_ms_today >= 500`

### Important Consequence

These triggers are narrow and warning-heavy.

That means a merchant will show no cards if:
- they have healthy recent behavior
- they have low volume
- they do not have refunds/chargebacks/KYC rows
- their issues do not fit the specific trigger shapes

## Payout Shortfall Logic

The payout shortfall path is separate from the templates.

### Detection Rule

Implemented in:
- `/Users/madhavpatel/New_demo copy/app/intelligence/payout_shortfall_monitor.py`

Logic:
- read `settlements`
- require:
  - merchant scope
  - date within current window
  - `gross_amount IS NOT NULL`
  - `net_settlement_amount IS NOT NULL`
  - `(gross_amount - net_settlement_amount) >= min_difference_rupees`
- default threshold:
  - `1000.0`
- default limit:
  - `3`

### Why One Merchant Can Have Multiple Shortfall Cards

The shortfall monitor is settlement-centric.

Each qualifying settlement becomes its own card:
- card id shape: `payout_shortfall_<settlement_id>`

So if one merchant has three qualifying settlement gaps in the same time window, the system will generate three cards. This is current design, not duplication.

## Persistence and Dedupe Logic

Cards are persisted into:
- `proactive_cards`

Relevant logic:
- `/Users/madhavpatel/New_demo copy/app/merchant_os.py`

Background refresh behavior:
- generate template cards
- generate shortfall cards
- merge them
- sort by `(impact_rupees, confidence)` descending
- keep top `limit`
- replace same-window background cards for the merchant

Dedupe key format:
- `bg:<merchant_id>:<lane>:<card_id>:<window_from>:<window_to>`

Implications:
- same merchant, same card id, same window -> one persisted card
- different settlement ids in the same window -> multiple shortfall cards
- same trigger in a new window -> new card

## Lane Assignment

Current lane routing is fixed by card type:

Operations:
- `chargeback_deadline`
- `settlement_delay`
- `kyc_expiry`
- `refund_rate_spike`
- `payout_shortfall_*`

Growth:
- `high_value_failed_txns`
- `success_rate_drop`
- `terminal_anomaly`
- `upi_callback_delay_spike`

This mapping lives in:
- `/Users/madhavpatel/New_demo copy/app/merchant_os.py`

## Required DB Support

The current DB does support the logic used by the proactive engine.

### Tables and fields used

`transaction_features`
- `merchant_id`
- `terminal_id`
- `payment_mode`
- `status`
- `response_code`
- `amount_rupees`
- `p_date`
- `initiated_at`
- `completed_at`

`settlements`
- `mid`
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

`refunds`
- `mid`
- `refund_amount`
- `refund_status`
- `refund_date`
- `settlement_id`
- `p_date`

`chargebacks`
- `mid`
- `chargeback_amount`
- `response_due_date`
- `resolution_outcome`
- `settlement_id`
- `p_date`

`merchant_kyc_documents`
- `mid`
- `expiry_date`
- `kyc_status`

### Live table coverage

Observed in the live DB:
- `merchants`: 14
- `transaction_features`: 7677
- `settlements`: 183
- `refunds`: 4
- `chargebacks`: 3
- `merchant_kyc_documents`: 8

### Practical consequence

The schema is sufficient, but the data is sparse for some card families:
- refund-related cards will rarely fire
- chargeback-related cards will rarely fire
- KYC cards depend on only 8 document rows total

So "no card" for a merchant often means "no qualifying data", not "broken logic".

## Live Merchant Coverage Snapshot

Merchant-by-merchant sample from the live DB:

| Merchant ID | Merchant | Template Cards | Shortfall Cards |
| --- | --- | --- | --- |
| 100000000121215 | Delhi Airport Parking | `high_value_failed_txns` | `payout_shortfall_9`, `payout_shortfall_5`, `payout_shortfall_7` |
| 903381252836376 | Acqui Grocery 02 | none | none |
| 925845324475537 | Acqui Kitchen 01 | none | none |
| 939562528976988 | Acqui Grocery 02 | none | none |
| 940998114800635 | Acqui Grocery 01 | none | none |
| 941406491142572 | Acqui Kitchen 01 | none | none |
| 952748394969596 | Acqui Grocery 01 | none | none |
| 953409687864599 | Acqui Hypermart 01 | none | none |
| 958495545061783 | Acqui Grocery 01 | `high_value_failed_txns` | none |
| 971061677663171 | Acqui Fuel 01 | none | none |
| 994090253704234 | Acqui Grocery 02 | none | none |
| 994296382567606 | Acqui Kitchen 01 | none | none |
| 995454842703327 | Acqui Grocery 02 | none | none |
| 997099161546568 | Acqui Grocery 01 | none | none |

## Why Other Merchants Often Show Nothing

Primary reasons:

1. Trigger coverage is narrow
- only 8 template scenarios
- most are risk/event spikes, not broad opportunity cards

2. Thresholds are relatively high
- examples:
  - 24h attempts must be at least 50 for success-rate-drop
  - top terminal needs at least 20 attempts for terminal anomaly
  - shortfall requires at least Rs 1,000 difference

3. Some source tables are sparse
- only 4 refunds and 3 chargebacks in the current DB

4. The engine is mostly warning-driven
- it does not yet generate many "healthy merchant but here is an opportunity" cards

## Current Design Limits

The current proactive system is good for:
- high-value failed payment alerts
- settlement shortfall alerts
- a few spike or deadline signals

It is weak for:
- small merchants
- healthy merchants
- broader growth opportunities
- merchant-segment-aware alerting
- one-summary-card-per-merchant behavior

## Recommended Improvements

### High priority

1. Add merchant-level summary cards
- example:
  - `3 payout shortfalls totaling Rs X`
  - with drilldown rows beneath

2. Add more operations triggers
- overdue chargebacks
- processed settlement shortfalls
- unresolved payout mismatches

3. Add more growth triggers
- low card acceptance opportunity
- terminal underperformance without full anomaly
- payment-mode concentration risk

4. Add merchant-segment-aware thresholds
- small merchants should not need the same thresholds as large merchants

### Medium priority

5. Add "opportunity" cards for healthy merchants
- not only warning cards

6. Add suppression and prioritization policy
- avoid showing multiple sibling shortfall cards without a parent summary

7. Add card-family coverage diagnostics
- explain why a merchant has no cards:
  - no qualifying events
  - insufficient volume
  - missing supporting data

## Bottom Line

The proactive cards are not hardcoded to one merchant.

The actual problem is:
- fixed scenario catalog
- narrow thresholds
- sparse supporting data for many merchants

That is why Delhi Airport Parking shows multiple cards and most other merchants show none.

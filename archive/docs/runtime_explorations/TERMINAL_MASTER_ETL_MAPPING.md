# Merchant Terminal Master ETL Mapping

## Purpose

This document defines a practical mapping from the source schema described in
`Merchant_Terminal_Transaction_Master.xlsx` into the runtime analytics fact model
used by this demo (`transaction_features`).

This is an ingestion contract for reliability and reproducibility, not a full
production data model.

## Source Assets Identified

The workbook contains dictionary metadata for these source tables:

- `postransactions`
- `upitransactions`
- `tidmaster`
- `midmaster`

The workbook itself does not contain raw transaction rows.

## Minimum Runtime Fields Needed

The current copilot/runtime tools are most sensitive to these normalized fields:

- `merchant_id`
- `terminal_id`
- `source_system`
- `source_txn_id`
- `invoice_nr`
- `payment_mode`
- `status`
- `response_code`
- `response_desc`
- `amount_rupees`
- `p_date`
- `initiated_at`
- `completed_at`
- `hour_of_day`
- `day_of_week`

## Field Mapping

### `postransactions` -> normalized

- `mid` -> `merchant_id`
- `tid` -> `terminal_id`
- `tran_id` (fallback `rrn`) -> `source_txn_id`
- `invoice_nr` -> `invoice_nr`
- `rsp_code` -> `response_code`
- `rsp_desc` -> `response_desc`
- `amount` (fallback `sale_amt`) -> `amount_rupees`
- `tran_date` -> `p_date`
- `request_datetime` (fallback `inserted_on`) -> `initiated_at`
- `rsp_datetime` -> `completed_at`
- `card_type` -> `card_type`
- `network_type` -> `card_network`
- `pos_entry_mode` -> `pos_entry_mode`
- constant -> `source_system = "POS"`
- constant -> `payment_mode = "CARD"`

Status inference for POS rows:

- success codes (`00`, `0`, `000`) -> `SUCCESS`
- otherwise -> `FAILED`

### `upitransactions` -> normalized

- `mid` -> `merchant_id`
- `tid` -> `terminal_id`
- `upitranlogid` (fallback `rrn`) -> `source_txn_id`
- `invoice_nr` -> `invoice_nr`
- `responsecode` (fallback `payer_respcode`, `payee_respcode`) -> `response_code`
- `txnstatus` -> `status` (normalized)
- `amount` (fallback `sale_amt`) -> `amount_rupees`
- `p_date` (fallback `txninitdate`, `txncompletiondate`) -> `p_date`
- `txninitdate` -> `initiated_at`
- `txncompletiondate` -> `completed_at`
- `payee_mcccode` -> `mcc`
- constant -> `source_system = "UPI"`
- constant -> `payment_mode = "UPI"`

Status normalization for UPI rows:

- values like `SUCCESS`, `APPROVED`, `COMPLETED` -> `SUCCESS`
- values like `FAILED`, `FAILURE`, `DECLINED`, `REVERSED`, `TIMEOUT` -> `FAILED`
- otherwise -> `UNKNOWN`

## Join-Ready Dimensions (Optional Enrichment)

These can be joined post-normalization for richer analytics:

- `tidmaster.mid` + `tidmaster.tid` for terminal attributes
- `midmaster.merchantid` for merchant attributes (MCC/profile limits, etc.)

## Reliability Rules

- Keep all amounts in INR rupees in the normalized output (`amount_rupees`).
- Keep `merchant_id` scoping intact for every transformed row.
- Derive `hour_of_day` and `day_of_week` from `initiated_at` (fallback `p_date`).
- Do not drop rows solely because optional fields are missing.
- Emit deterministic status normalization and preserve source `response_code`.

## Skeleton ETL Script

Reference implementation:

- `/Users/madhavpatel/New_demo copy/app/intelligence/etl_terminal_master.py`

It reads source CSV exports for POS/UPI and writes normalized JSONL suitable for
loading into downstream staging or directly into `transaction_features`-compatible
pipelines.

# Ops Refresh Evidence Normalization

## What was broken

Refreshing proactive cards could fail while updating `ops_cases` with an error like:

`psycopg2.errors.InternalError_: invalid memory alloc request size 1073741824`

The underlying problem was not the case title or summary. The real issue was `evidence_ids_json`.

Some refresh paths were treating nested lists or stringified list payloads as plain text. On the next refresh, that text was wrapped again and saved back to the case. Repeating that cycle made the evidence payload grow until PostgreSQL rejected the update.

When that refresh failed, the ops console and inbox looked empty because the refresh endpoint errored before the UI could return a normal payload.

## What changed

- Added a shared evidence normalizer in `app/data/evidence.py`.
- Flattened nested evidence arrays and stringified list payloads before saving or merging them.
- Applied the normalizer to proactive cards, ops case upserts, merchant actions, and settlement connector case context reads.
- Capped case-list and case-detail reads so an oversized `evidence_ids_json` field is treated as empty evidence instead of breaking the queue.
- Added a repair script at `scripts/repair_evidence_payloads.py` so corrupted stored evidence can be normalized in both `ops_cases` and `proactive_cards`.

## Operational note

- If refresh still throws the PostgreSQL memory allocation error after the code fix is present on disk, the API process may still be running older imported code.
- Restart the backend process and run the proactive refresh again.
- If older rows were already corrupted, run:

```bash
python scripts/repair_evidence_payloads.py
```

- The script rewrites malformed `evidence_ids_json`, `evidence_ids`, and embedded payload `evidence_ids` values into small flat arrays of strings.

## How to verify

1. Trigger the background proactive refresh for a merchant with payout shortfall cards.
2. Confirm the refresh returns successfully instead of raising the PostgreSQL memory allocation error.
3. Open the ops console and inbox and confirm cases/actions are visible again.
4. Inspect `ops_cases.evidence_ids_json` for the affected case and confirm it is a small flat JSON array of strings, not a nested or escaped list-of-lists blob.
5. If you ran the repair script, confirm it reports the updated row count and that re-running it immediately reports zero or fewer remaining malformed rows.

## Real issue vs noise

- Real product issue: the PostgreSQL `invalid memory alloc request size 1073741824` error during `UPDATE ops_cases`.
- Likely unrelated noise: browser extension warnings, DevTools messages, or generic frontend console chatter that does not mention the failed `ops_cases` update.

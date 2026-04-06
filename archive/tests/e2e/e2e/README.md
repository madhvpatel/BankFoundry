# E2E Frontend Test Suite (Merchant)

This suite drives the Streamlit UI like a merchant would and verifies that the expected backend tools are invoked (via the **Tool trace** expander).

## What it covers

It runs chat prompts that should trigger these tools:
- `get_merchant_context`
- `compute_kpis`
- `compare_kpis`
- `list_transactions`
- `get_transaction_detail`
- `list_settlements`
- `get_settlement_detail`
- `list_chargebacks`
- `get_chargeback_detail`
- `list_refunds`
- `terminal_performance`
- `end_to_end_analysis`
- `propose_and_create_merchant_action`

## Prereqs

1) Start Postgres with the demo DB available.

2) Start Ollama:
- `OLLAMA_BASE_URL=http://localhost:11434`
- `OLLAMA_MODEL=qwen2.5:8b` (or whatever you use)

3) Python environment: this codebase uses Python 3.10+ (it uses `X | None` typing in core modules). Ensure you run tests with the same interpreter you use to run `streamlit run main.py`.

4) Install test deps:
- `pip install pytest playwright`
- `playwright install chromium`

## Run

Run ONLY the e2e folder to avoid importing other test modules:

```bash
python3 -m pytest tests/e2e -m e2e
```

Optional env overrides:

```bash
export DATABASE_URL=postgresql://demo:demo@localhost:5433/payments_demo
export MERCHANT_ID=<merchant_id>   # optional; if omitted we read it from the UI
```

## Notes

- Tests start Streamlit automatically on a random free port.
- They are intentionally black-box: they assert tool usage from the UI’s **Tool trace**.
- If you later hide Tool trace for merchant UX, keep a `?debug=1` mode so tests can still validate tool calls.

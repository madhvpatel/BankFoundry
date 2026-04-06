# E2E Chat Suite

This suite validates `/ask` end-to-end behavior for chat capabilities:
- analytics routing and analysis type selection
- follow-up handling in the same session
- capability/profile/business summary routes
- debug trace coverage (`route`, `analysis_plan`, SQL count)

## Test Cases

Case definitions live at:
- `/Users/madhavpatel/payments-intelligence-demo/tests/e2e_chat_cases.json`

## Run as Script

Start API server first, then run:

```bash
python /Users/madhavpatel/payments-intelligence-demo/scripts/run_e2e_chat_suite.py
```

Optional flags:

```bash
python /Users/madhavpatel/payments-intelligence-demo/scripts/run_e2e_chat_suite.py \
  --base-url http://localhost:8000 \
  --cases /Users/madhavpatel/payments-intelligence-demo/tests/e2e_chat_cases.json \
  --log-dir /Users/madhavpatel/payments-intelligence-demo/logs \
  --timeout 120 \
  --fail-fast
```

## Run with Pytest

```bash
RUN_E2E_CHAT=1 E2E_BASE_URL=http://localhost:8000 pytest -q /Users/madhavpatel/payments-intelligence-demo/tests/test_e2e_chat_suite.py
```

## Output Logs

For each run:
- per-case log (JSONL): `/Users/madhavpatel/payments-intelligence-demo/logs/e2e_chat_results_<RUN_ID>.jsonl`
- summary log (JSON): `/Users/madhavpatel/payments-intelligence-demo/logs/e2e_chat_summary_<RUN_ID>.json`

Each case entry includes:
- request payload
- HTTP status
- validation errors
- answer preview
- debug route
- SQL statement count
- full response payload

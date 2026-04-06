# Sprint 1 Manual Validation Runbook

## Purpose
Use this runbook to execute the shipped Sprint 1 validation path with:
- `curl` for prompt-driven agent checks through the local test wrapper
- Streamlit for UI-only checks that depend on tabs, sidebar state, buttons, and downloads

## Local Test Wrapper
Start the local-only validation server in a separate shell:

```bash
python -m app.copilot.validation_server --host 127.0.0.1 --port 8765
```

Endpoint:
- `POST /test/ask`

Request JSON:
- `merchant_id`
- `prompt`
- `lane`: `operations|growth`
- `terminal_id`: nullable

Response JSON:
- `answer`
- `operations_section`
- `growth_section`
- `tool_calls`
- `tool_results`
- `evidence`
- `terminal_focus`

## Prompt-Driven Checks via curl
Run the reproducible shell harness:

```bash
./scripts/run_sprint1_prompt_checks.sh
```

Environment overrides:

```bash
BASE_URL=http://127.0.0.1:8765 \
MERCHANT_ID=100000000121215 \
TERMINAL_ID=EP070270 \
./scripts/run_sprint1_prompt_checks.sh
```

The harness writes:
- raw per-case responses under `artifacts/sprint1_manual_validation/cases/`
- aggregate JSON archive at `artifacts/sprint1_manual_validation/prompt_runs.json`

### Exact curl examples

```bash
curl -sS -X POST http://127.0.0.1:8765/test/ask \
  -H 'Content-Type: application/json' \
  --data '{
    "merchant_id": "100000000121215",
    "prompt": "I expected Rs 20,000 settlement but got Rs 19,000. Explain the shortfall.",
    "lane": "operations"
  }'
```

```bash
curl -sS -X POST http://127.0.0.1:8765/test/ask \
  -H 'Content-Type: application/json' \
  --data '{
    "merchant_id": "100000000121215",
    "prompt": "What are my top growth opportunities in the last 30 days?",
    "lane": "growth"
  }'
```

```bash
curl -sS -X POST http://127.0.0.1:8765/test/ask \
  -H 'Content-Type: application/json' \
  --data '{
    "merchant_id": "100000000121215",
    "prompt": "What are my top growth opportunities for this terminal in the last 30 days?",
    "lane": "growth",
    "terminal_id": "EP070270"
  }'
```

## Answer Review Rubric
Use this rubric for each prompt response:
- `Grounding`: every number traces to tool output
- `Verification`: verification status is present and not overstated
- `Evidence`: exact evidence IDs are present
- `Scope`: merchant vs terminal scope is correct
- `Actionability`: recommended actions are merchant-actionable
- `Quality`: response is concise and decision-complete, not generic filler
- `Issues`: exact defects with likely root cause

A case passes only if both tool behavior and answer quality are acceptable.

## UI-Only Checks in Streamlit
Run the checks in [`PRE_SPRINT_2_TESTS.md`](/Users/madhavpatel/New_demo copy/docs/PRE_SPRINT_2_TESTS.md):
- `TERMINAL_SCOPE_UI`
- `REPORT_BRIEFS_UI`
- `PROACTIVE_INBOX_UI`
- `ACTION_CENTER_UI`

These cannot be replaced by `curl` because they depend on Streamlit widget state and downloads.

## Reporting
Use [`SPRINT1_MANUAL_VALIDATION_REPORT_TEMPLATE.md`](/Users/madhavpatel/New_demo copy/docs/SPRINT1_MANUAL_VALIDATION_REPORT_TEMPLATE.md) to record:
- prompt cases first
- UI cases second
- final defects grouped into:
  - `Sprint 2 existing backlog`
  - `New findings`

Use [`SPRINT1_PROMPT_RESPONSE_ARCHIVE_TEMPLATE.json`](/Users/madhavpatel/New_demo copy/docs/SPRINT1_PROMPT_RESPONSE_ARCHIVE_TEMPLATE.json) as the reference schema for the JSON archive.

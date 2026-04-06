# Bank chargeback schema fix

## What was broken

The bank case copilot could open a chargeback case, but the copilot call failed
when it tried to read chargeback detail from the live database.

The reason was simple:

- the list path already handled multiple chargebacks schemas
- the detail path still assumed fixed columns like `status`, `opened_at`, and `due_by`

On the live database, the chargebacks table uses different column names, so the
copilot crashed with a database column error.

## What changed

In [app/data/disputes/repository.py](/Users/madhavpatel/New_demo copy/app/data/disputes/repository.py):

- `get_chargeback_detail()` now resolves the chargebacks schema dynamically
- it supports both newer and legacy/live column names for:
  - merchant scope
  - status / stage
  - opened date
  - due date
  - amount
  - reason code
  - network
  - transaction id

I also added a regression test in [tests/test_mcp_server.py](/Users/madhavpatel/New_demo copy/tests/test_mcp_server.py) that uses a legacy-style chargebacks table.

## How to verify

Run:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_mcp_server.py tests/test_bank_ops_agents.py tests/test_ops_api_server.py -q
```

Then retry the bank case copilot call for your chargeback case.

## Real issues vs noise

Real issue:

- `UndefinedColumn` errors from chargeback detail reads in the bank copilot

Usually noise:

- the `.zshrc` missing `openclaw.zsh` completion file warning
- Starlette `python_multipart` pending deprecation warning during tests

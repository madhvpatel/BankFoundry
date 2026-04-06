# Bank entity parser fix

## What was broken

The bank copilot could misread plain text like:

- `settlement delay`
- `chargeback review`
- `refund exception`

as if those words contained real entity ids.

That happened because the internal regex parser allowed zero separator
characters after words like `settlement` and `chargeback`.

So text like `settlement_delay` could be parsed as settlement id `_delay`.

## What changed

In [app/agent/bank_ops_agents.py](/Users/madhavpatel/New_demo copy/app/agent/bank_ops_agents.py):

- the entity regex patterns now require a real separator after the keyword
- extracted ids must start with an alphanumeric character

This prevents:

- `settlement_delay` from becoming `_delay`
- similar false ids from case type names or titles

I also added a regression test in [tests/test_bank_ops_agents.py](/Users/madhavpatel/New_demo copy/tests/test_bank_ops_agents.py) for a delayed payout case whose title contains only `settlement delay` text.

## How to verify

Run:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_bank_ops_agents.py tests/test_ops_api_server.py tests/test_mcp_server.py -q
```

Then retry the bank copilot call for the delayed payout or settlement-delay case.

## Real issues vs noise

Real issue:

- the copilot inferring fake entity ids like `_delay`

Usually noise:

- shell completion warnings from `.zshrc`
- Starlette `python_multipart` pending deprecation warning during tests

# Pre-Sprint 2 Tests

## Purpose
Manual smoke checks to run before Sprint 2 patch work begins. These checks cover the shipped Sprint 1.5 merchant OS behavior so later fixes do not regress the current workflow surface.

Prompt-driven agent checks can also be exercised through the local validation wrapper and `curl`; see [`SPRINT1_MANUAL_VALIDATION_RUNBOOK.md`](/Users/madhavpatel/New_demo copy/docs/SPRINT1_MANUAL_VALIDATION_RUNBOOK.md).

## Terminal Scope
- Open the app and load a merchant with more than one terminal.
- In the sidebar, change `Terminal focus` from `All terminals` to a specific terminal.
- Confirm the scope banner appears and explains what is terminal-scoped vs merchant-wide.
- Open `Terminals` and confirm only the selected terminal row and its health row remain.
- Open `Reports` and confirm each report pack includes a scope line for the selected terminal.
- Switch back to `All terminals` and confirm the full merchant-wide view returns.

## Report Briefs
- Open `Reports`.
- For each pack (`Finance`, `Operations`, `Growth`), confirm a `Briefing summary` section appears above the datasets.
- Download `email brief` and confirm it includes merchant name, window, scope, summary bullets, and dataset counts.
- Download `print brief` and confirm it opens as a print-friendly HTML page with the same role summary.

## Proactive Inbox
- Load a merchant and wait for the sidebar auto-refresh status to appear.
- Confirm the sidebar shows `Auto proactive refresh` with a next-due timestamp.
- Click `Refresh proactive cards`.
- Open `Operations Agent` and `Growth Agent`.
- Confirm proactive cards render without duplicate widget-key errors.
- On one proactive card, add a note and click `Acknowledge`.
- Click `Refresh proactive cards` again and confirm the card keeps the acknowledged state and note.
- On one proactive card, click `Preview action`, then `Create action from card`.
- Confirm the card shows a converted action id after refresh.
- Refresh the page again within the same interval and confirm the app does not regenerate cards on every load.

## Action Center
- Open `Action Center`.
- Click `Hide legacy and duplicate items`.
- Confirm generic legacy rows remain hidden after refresh.
- Edit one action and save `Owner`, `Notes`, `Blocked reason`, and `Follow-up date`.
- Confirm the saved metadata appears on the action card after refresh.

## Agent Tabs
- In `Operations Agent`, ask: `I expected Rs 20,000 settlement but got Rs 19,000. Explain the shortfall.`
- In `Growth Agent`, ask: `What are my top growth opportunities in the last 30 days?`
- If a terminal is selected, confirm the tab notes that supported analytics tools use terminal scope automatically while some payouts/dispute tools remain merchant-wide.
- With a terminal selected, ask in `Growth Agent`: `What are my top growth opportunities for this terminal in the last 30 days?`
- Confirm the answer/tool trace uses terminal-scoped KPI or failure-driver evidence where supported.

## Regression Watch
- `Action Center` should never show duplicate button-key errors.
- `Refresh proactive cards` should not wipe acknowledged state for unchanged same-window cards.
- Terminal scope should not pretend settlements, refunds, or chargebacks are terminal-filtered unless explicit terminal evidence exists.

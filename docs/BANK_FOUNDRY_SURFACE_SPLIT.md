# Bank Foundry Surface Split

## What changed

The live product is now presented as **Bank Foundry** instead of **AcquiGuru** on the active UI and runtime prompts.

We also made the merchant-facing and bank-facing surfaces explicit instead of treating them like one mixed shell with a mode switch.

### Frontend split

The app now uses separate shells:

- `/Users/madhavpatel/New_demo copy/frontend/src/components/MerchantWorkspaceShell.jsx`
- `/Users/madhavpatel/New_demo copy/frontend/src/components/BankOperationsShell.jsx`

The merchant-facing views still live under the merchant workspace.
The bank-facing case and queue work lives under the bank operations surface.

### Backend split

The control plane now routes through separate workflow entry modules:

- `/Users/madhavpatel/New_demo copy/app/application/workflows/merchant_surface.py`
- `/Users/madhavpatel/New_demo copy/app/application/workflows/bank_surface.py`

These wrap the existing merchant and bank-facing handlers so the product boundary is visible in the codebase too.

## Why this matters

Before this change:

- merchant and bank views shared one mixed shell
- the workflow names still read like an internal add-on (`ops_console`)
- the product brand was inconsistent with the new platform direction

After this change:

- merchant-facing screens are clearly separate from bank-facing screens
- bank operations has its own shell and navigation
- the workflow layer reflects the same boundary
- the active brand is now Bank Foundry

## What did not change

This phase does **not** change the live API endpoints or the case model.

Examples:

- merchant APIs still work the same
- ops APIs still work the same
- old internal workflow modules still exist for compatibility
- the legacy `surface=ops` URL still maps to the bank surface

So this is a surface and workflow separation change, not a backend protocol break.

## How to verify

Run:

```bash
cd "/Users/madhavpatel/New_demo copy"
PYTHONPATH=. pytest tests/test_control_plane_phase_a.py tests/test_control_plane_phase_b.py tests/test_mcp_server.py tests/test_mcp_client.py -q
npm run lint --prefix frontend
npm run build --prefix frontend
```

Then open:

- merchant surface: `http://127.0.0.1:5173/?surface=merchant`
- bank surface: `http://127.0.0.1:5173/?surface=bank`

## Real issues vs noise

Real issues for this phase would be:

- bank and merchant shells showing the wrong controls
- the bank surface not loading case queues
- the merchant surface losing workspace navigation
- prompt/runtime strings still surfacing the old product name in active flows

Not a product issue:

- the existing Starlette `python_multipart` pending deprecation warning in broader backend test runs

That warning is framework noise and unrelated to this surface split.

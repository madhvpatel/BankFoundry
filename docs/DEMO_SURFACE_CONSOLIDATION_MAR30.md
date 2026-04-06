# Demo Surface Consolidation

## What was wrong

The demo UI exposed too many surfaces at once:

- Home
- Money
- Disputes
- Terminals
- Connected Systems
- Reports
- Action Center
- Chat
- Inbox
- Bank Operations

That made the product feel wider than the stable demo path actually is.

It also exposed unfinished or transitional behavior:

- terminal filtering in the UI, even though some data flows are still merchant-wide
- hidden workflow dependencies, such as creating items for surfaces that were not needed in the demo
- a separate merchant/bank surface toggle that made the navigation feel larger than necessary

For a live demo, that creates risk. A confident demo is better when the visible surface area is tight.

## What changed

The frontend now exposes exactly four surfaces:

- Dashboard
- Chat
- Inbox
- Ops Console

The main UI changes are:

- removed the extra merchant-facing tabs from navigation
- removed the separate merchant/bank toggle from the sidebar
- removed the terminal selector from the demo shell
- wired `Dashboard` to the dedicated analytics dashboard endpoint
- kept `Inbox` focused on acknowledge, dismiss, refresh, and promote-to-ops behavior
- kept `Chat` focused on grounded answers and promote-to-ops behavior
- kept `Ops Console` as the only operational workflow surface shown in the demo

Important note:

- hidden surfaces were not deleted from the codebase
- they are simply no longer part of the live demo navigation

## How to verify

### 1. Build the frontend

From `frontend/`:

```bash
npm run build
```

Expected result:

- build completes successfully

### 2. Check the visible navigation

Run the app and confirm the sidebar shows only:

- Dashboard
- Chat
- Inbox
- Ops Console

There should be no visible tabs for:

- Money
- Disputes
- Terminals
- Connected Systems
- Reports
- Action Center

### 3. Check the top controls

For Dashboard, Chat, and Inbox:

- merchant selector should be visible
- terminal selector should not be visible

For Ops Console:

- merchant selector should be visible
- lane and role selectors should be visible

### 4. Check the cross-surface flow

- open `Chat` and create an ops case from a finding
- confirm the app moves into `Ops Console`
- open `Inbox` and create an ops case from a proactive card
- confirm the app moves into `Ops Console`

### 5. Check the dashboard path

- open `Dashboard`
- confirm KPI cards and payment-mode chart render from the analytics endpoint

## Real issues vs noise

### Real product issues

These matter for the demo:

- Dashboard stays empty because `/api/v1/analytics/dashboard` fails
- Inbox does not load because `/api/v1/merchant/snapshot` fails
- Chat returns API errors or blank answers
- promoting a finding or card does not open or refresh Ops Console

### Usually not a product issue

These are not demo blockers by themselves:

- Vite chunk-size warning during `npm run build`
- browser extension console errors
- dev-only hot reload reconnect messages

The current build warning about a large JS chunk is real technical debt, but not a product regression for this demo cut.

## Why this is safer

This change makes the demo easier to explain:

- Dashboard shows the operating summary
- Chat answers questions
- Inbox shows proactive signals
- Ops Console shows the operational queue

That is a complete story, and it avoids showing transitional surfaces that dilute confidence.

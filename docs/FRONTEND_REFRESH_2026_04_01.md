# Frontend Refresh Notes

## What felt broken

- The app looked flat and generic, so the first screen did not explain what mattered.
- The dashboard showed numbers, but not enough merchant context or settlement risk.
- Chat had weak onboarding, so it felt empty until you already knew what to ask.
- The inbox and ops console were usable, but visually dense and hard to scan quickly.

## What changed

- Rebuilt the shared visual system in the frontend with a lighter editorial style, stronger hierarchy, and responsive layouts.
- Upgraded the left navigation and top shell so each surface explains its purpose before the user interacts.
- Expanded the dashboard to show merchant context, live KPI cards, payment mix, settlement watch, and active signals in one view.
- Improved chat with starter prompts, a clearer brief panel, and better rendering for runtime failures.
- Added inbox summary cards so signal volume and handoff status are visible immediately.
- Added an ops hero block so queue health is visible before opening a case.
- Improved table formatting so money, rates, and counts are easier to read.

## Refinement pass

- Reduced copy in the sidebar, top bar, and scope strip so the shell feels quieter.
- Shortened navigation labels and removed the extra descriptive text under each nav item.
- Tightened the dashboard by cutting repeated helper text and trimming signal cards to the essentials.
- Simplified the inbox header and replaced the large summary cards with compact stat pills.
- Compressed chat onboarding so the rail is mostly thread controls and quick prompts instead of instructions.
- Kept the same functionality, but made the UI rely more on spacing, grouping, and labels than long sentences.

## Retro-future pass

- Shifted the visual system toward a sharper retro-future style with darker panels, neon accents, and more angular geometry.
- Reduced text scale across headings, pills, controls, tables, and chat surfaces so the UI feels tighter.
- Reworked the dashboard so the top section reads left-to-right on desktop instead of stacking major blocks vertically.
- Narrowed the chat rail and kept the conversation area dominant, so the page reads horizontally before it reads downward.
- Kept mobile layouts responsive by collapsing back to single-column only when screen width actually requires it.

## Inbox minimalism pass

- Rebuilt the merchant inbox as a split-pane mail view with a quiet left list and a focused preview on the right.
- Rebuilt the ops console to use the same email-style pattern, so approvals and cases now behave like an inbox instead of a stack of control cards.
- Reduced visual noise inside both views by replacing large hero sections and floating cards with dividers, slim metadata pills, and flatter rows.
- Kept the existing case actions, notes, approvals, and pinned context workflows, but moved them into a cleaner preview area.
- Left the surrounding shell mostly intact, so this pass changes the working surfaces first without forcing a full app-wide redesign.

## Scroll fix

- The inbox and ops panes had `overflow:auto`, but some parent flex/grid containers were still using default sizing rules that stopped those panes from shrinking into actual scroll regions.
- Fixed the layout so the main shell, view stack, sidebar list area, and preview body all use explicit `min-height: 0` and `flex: 1` where needed.
- The result is that long inbox lists and long ops case detail now scroll inside their own panes instead of getting clipped or refusing to move.

## How to verify

1. Start the backend:

```bash
python3 -m uvicorn app.api.server:app --host 127.0.0.1 --port 8000
```

2. Start the frontend:

```bash
cd frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

3. Open these views and confirm they render cleanly:

- `http://127.0.0.1:5173/?view=dashboard`
- `http://127.0.0.1:5173/?view=chat`
- `http://127.0.0.1:5173/?view=inbox`
- `http://127.0.0.1:5173/?view=ops_console`

For the inbox and ops console specifically, confirm:

- The page reads left to right on desktop: list on the left, selected item or case on the right.
- Rows feel flat and inbox-like, not like individual glass cards.
- The right pane keeps actions available without pushing long explanatory text above the fold.
- On narrower screens, the layout collapses to a single column without clipping content.

4. Confirm engineering checks still pass:

```bash
cd frontend
npm run build
npm run lint
```

## Console and runtime notes

- `POST /api/v1/ask` returning `500` with `Connection refused` is a real product/runtime issue. The frontend is working, but chat answers will fail until the upstream dependency behind the ask flow is reachable again.
- `Download the React DevTools for a better development experience...` is normal React dev-mode noise. It is not a product bug.
- The Vite production build warns that the main JavaScript bundle is larger than 500 kB. That is a real engineering warning worth fixing later, but it does not block the UI from working now.
- Browser extension warnings are not product issues unless they reproduce in a clean browser profile.

# Bank Knowledge Base (MD)

This folder is the **source of truth** for bank offerings + domain knowledge used by the copilot (agentic RAG).

- Add/update Markdown files here.
- Run `kb_reindex` (via tool or CLI) to refresh the local search index.

## Structure

- `services/` — bank products/offerings the agent can recommend (merchant-facing + RM notes)
- `domain/` — payments domain primers (UPI, cards, settlements, chargebacks, reconciliation)

## Authoring tips

- Keep each service in its own file.
- Include: overview, eligibility, pricing (high level), onboarding steps, and "when to recommend".
- Prefer plain text + headings; avoid huge PDFs for the demo.

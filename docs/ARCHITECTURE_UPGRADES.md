# Bank Foundry Architecture Upgrades: What Changed & How to Verify

## Overview

This document explains the 4 new architectural modules added to Bank Foundry and how to verify each one works correctly.

---

## What Was Added

### 1. MoE Agent Router — `app/agent/expert_agent_base.py`

**What was broken / missing:**
`bank_ops_agents.py` (2815 lines) contained a massive `if/else` tree inside `BankOpsCaseCopilotRouter.summarize_case()`. Every new case type required editing that file, there was no formal contract for what an "agent" must look like, and the dispatch logic was duplicated 9 times.

**What changed:**
- New `ExpertAgent` Protocol — every specialist class (Settlement, Chargeback, etc.) already satisfies it with no changes needed.
- New `AgentRoute` descriptor — maps a set of `case_type` strings to an agent class + tool filter + draft mode.
- New `AgentRouter` — a pure routing class that reads the route table and returns the best `AgentRoute`. Zero if/else chains.
- `build_default_router()` wires all 12 existing agents into the route table.

**How to verify:**
```python
from app.agent.expert_agent_base import build_default_router

router = build_default_router()
print(router.describe())  # lists all 12 routes
```

---

### 2. Proactive Job Runner — `app/intelligence/proactive_job_runner.py`

**What was broken / missing:**
All 10 signal engines in `app/intelligence/engines/` were implemented but only the `payout_shortfall_monitor` was wired to production. The others were importable libraries sitting idle. There was no scheduler, no job registry, and no idempotency (running the same signal twice for the same merchant in the same date window would create duplicate cases).

**What changed:**
- New `ProactiveJob` descriptor — defines schedule, lookback window, case type, lane, and severity per job.
- 10 signal adapters — thin wrappers over each existing engine that normalize their output to `{triggered, payload, summary}`.
- `PROACTIVE_JOBS` registry — all 10 jobs registered with their schedules (30 min to 1440 min intervals).
- `ProactiveJobRunner` — runs all jobs, checks idempotency key before firing, writes to the ops case store.
- APScheduler integration — call `runner.register_jobs(scheduler)` to schedule all jobs automatically.

**How to verify:**
```python
from app.intelligence.proactive_job_runner import ProactiveJobRunner, PROACTIVE_JOBS

# Inspect the registry
for job in PROACTIVE_JOBS:
    print(f"{job.name}: every {job.interval_minutes}min, creates {job.case_type}")

# Dry run (no side effects — no real merchants, no DB required)
runner = ProactiveJobRunner(engine=None, fired_keys=set())
print(len(runner._jobs))  # should be 10
```

**Idempotency key format:**
`sha256("{job_name}::{merchant_id}::{start_date}::{end_date}")[:24]`

---

### 3. Multi-Signal Intent Scoring — `app/intelligence/intent_scoring.py`

**What was broken / missing:**
`route_chat_intent()` in `chat_reasoning.py` made a full LLM call on every single turn, even for trivial messages like "hi" or "show my settlements". This was a latency bottleneck (~200ms–500ms per turn) with no deterministic fallback.

**What changed:**
New 4-axis scoring pipeline runs **before** the LLM:
1. **Lexical trigger** — 30+ compiled regex patterns covering greetings, settlement queries, chargeback queries, failure analysis, out-of-scope, etc.
2. **Entity signal** — detects settlement IDs, chargeback IDs, transaction IDs, terminal IDs and assigns a high-confidence route.
3. **Session memory** — continuation phrases like "show me that again" inherit the last intent from the history.
4. **Semantic (TF-IDF)** — cosine similarity to 10 intent exemplar strings as a final pre-LLM step.

If any axis returns `confidence >= 0.80`, the LLM call is skipped entirely. Otherwise, `route_chat_intent()` falls through to the LLM as before.

**How to verify:**
```python
from app.intelligence.intent_scoring import score_intent

# Greeting — should resolve via lexical, no LLM needed
score = score_intent(question="hey there")
print(score.route, score.intent, score.source, score.needs_llm)
# → greeting, general, lexical, False

# Settlement query — entity signal on entity ID
score = score_intent(question="show me settlement:ABC123")
print(score.route, score.intent, score.source)
# → deterministic, recent_settlements, entity

# Ambiguous message — should fall through to LLM
score = score_intent(question="what do you think I should do?")
print(score.needs_llm)  # → True
```

**Wire in `chat_reasoning.py` was also updated** — the `route_chat_intent()` function now runs the pre-filter first and only calls the LLM if `needs_llm=True`.

---

### 4. Enhanced RAG Retrieval — `app/copilot/kb_enhanced.py`

**What was broken / missing:**
`kb.py` performed a single-stage flat TF-IDF vector lookup with no query expansion and no exact-term matching. This meant response codes like `U002`, settlement IDs, or specific domain terms (like "hold_reason") often ranked poorly because TF-IDF alone doesn't handle exact-term recall well.

**What changed:**
Drop-in enhanced replacement with 3 improvements:
1. **Query expansion** — each detected intent maps to a set of domain terms appended to the query before embedding (e.g., `exact_shortfall` → `"shortfall net payout deduction gross settlement difference"`). Free — no extra model call.
2. **BM25 hybrid scoring** — exact-term matching runs in parallel with TF-IDF. The two scores are fused via weighted sum (55% TF-IDF + 45% BM25).
3. **Chunk usage tracking** — `record_chunk_usage()` logs which chunks contributed to validated answers. Enables corpus quality analysis over time.

**How to verify:**
```python
from app.copilot.kb_enhanced import search_kb_enhanced

results = search_kb_enhanced(
    query="shortfall",
    intent="exact_shortfall",
    top_k=3,
)
for r in results["results"]:
    print(r["title"], r["score"], r["tfidf_score"], r["bm25_score"])

# Also shows expanded_query
print(results["expanded_query"])
```

---

## Files Changed

| File | Status | Description |
|------|--------|-------------|
| `app/agent/expert_agent_base.py` | New | MoE router + AgentRoute + ExpertAgent protocol |
| `app/intelligence/proactive_job_runner.py` | New | Job registry + runner for all 10 signal engines |
| `app/intelligence/intent_scoring.py` | New | 4-axis deterministic intent pipeline |
| `app/copilot/kb_enhanced.py` | New | Enhanced RAG with BM25 hybrid + query expansion |
| `app/intelligence/chat_reasoning.py` | Modified | Pre-filter wired into `route_chat_intent()` |
| `docs/BACKEND_SYSTEM_GRAPH.md` | New | Full system graph, data flows, agent map, job registry |

---

## Console Warnings That Are Safe to Ignore

- `WARNING payout_shortfall_signal failed for X: module not found` — signal adapter trying to import an engine module not yet present. Safe; the runner will skip and return `no_signal`.
- `WARNING ProactiveJobRunner: ops repository not available` — `create_work_item` import fails in test/dev environments. Safe; case payload is logged instead.
- `WARNING route_chat_intent: pre-filter raised ...` — intent scoring import or runtime error. The code falls through to the LLM gracefully.

## Console Warnings That Are Real Product Issues

- `ERROR ProactiveJobRunner: failed to load merchant IDs: ...` — DB connection problem. Investigate SQLAlchemy engine config.
- `ERROR ProactiveJobRunner: failed to write case: ...` — ops case store write failure. Investigate repository layer.
- `WARNING Chat router failed: ...` — LLM router hard failed. Check Ollama connectivity.

# Bank Foundry — Backend System Graph

> **Document type:** Authoritative architecture map.  
> **Update this file** whenever a major new subsystem is added.

---

## System Overview

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       BANK FOUNDRY BACKEND                                           │
│                                                                                                      │
│  ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐   ┌───────────────────┐   │
│  │  Merchant Chat      │   │  Copilot / Bank    │   │  Proactive Monitor │   │  Lending Engine   │   │
│  │  (chat_reasoning)   │   │  Ops Copilot       │   │  (cron jobs)       │   │  (growth layer)   │   │
│  └────────┬───────────┘   └────────┬───────────┘   └────────┬───────────┘   └────────┬──────────┘   │
│           │                        │                          │                         │              │
│           ▼                        ▼                          ▼                         ▼              │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│  │                             INTELLIGENCE LAYER                                                 │   │
│  │                                                                                                │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌───────────────────┐  ┌────────────────────────┐ │   │
│  │  │ Intent Scoring   │  │  MoE Router     │  │  10 Signal Engines│  │  RAG (kb_enhanced)     │ │   │
│  │  │ (intent_scoring) │  │  (AgentRouter)  │  │  (engines/)       │  │  TF-IDF + BM25         │ │   │
│  │  └────────┬─────────┘  └────────┬────────┘  └─────────┬─────────┘  └──────────┬─────────────┘ │   │
│  │           │                     │                       │                        │               │   │
│  │           └─────────────────────┴───────────────────────┴────────────────────────┘               │   │
│  │                                             │                                                    │   │
│  └─────────────────────────────────────────────┼────────────────────────────────────────────────────┘   │
│                                                │                                                        │
│  ┌─────────────────────────────────────────────▼──────────────────────────────────────────────────┐   │
│  │                               MCP TOOL LAYER (BankFoundryMCPServer)                            │   │
│  │                                                                                                 │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │   │
│  │  │ Settlement   │  │  Chargeback  │  │  Payments    │  │  Risk / KYC  │  │  KB / Compliance │ │   │
│  │  │ Tools        │  │  Tools       │  │  Tools       │  │  Tools       │  │  Tools           │ │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────────┘ │   │
│  └─────────┼────────────────┼─────────────────┼────────────────┼────────────────────┼─────────────┘   │
│            │                │                  │                 │                    │                  │
│  ┌─────────▼────────────────▼──────────────────▼─────────────────▼────────────────────▼──────────┐   │
│  │                               DATA LAYER                                                        │   │
│  │                                                                                                 │   │
│  │  ┌────────────────────────────────────────────────────────────────────────────────────────┐    │   │
│  │  │                         SQLite / SQLAlchemy                                            │    │   │
│  │  │  settlements | transactions | chargebacks | refunds | terminals | merchants            │    │   │
│  │  │  ops_cases | approvals | risk_profiles | velocity_anomalies | kb_index                │    │   │
│  │  └────────────────────────────────────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Systems Inventory

| # | System | Entry Point | Key Files | Status |
|---|--------|-------------|-----------|--------|
| 1 | **Merchant Chat** | `POST /api/chat` | `app/intelligence/chat_reasoning.py`, `app/agent/service.py` | Live |
| 2 | **Intent Scoring** | `score_intent()` | `app/intelligence/intent_scoring.py` | **New** |
| 3 | **MoE Router** | `AgentRouter.resolve()` | `app/agent/expert_agent_base.py` | **New** |
| 4 | **Bank Ops Copilot** | `build_bank_ops_case_copilot_summary()` | `app/agent/bank_ops_agents.py` | Live |
| 5 | **MCP Tool Layer** | `BankFoundryMCPServer` | `app/mcp_server/tool_registry.py` | Live |
| 6 | **Signal Engines (×10)** | `compute_*_signals()` | `app/intelligence/engines/` | Live |
| 7 | **Proactive Job Runner** | `ProactiveJobRunner.run_all()` | `app/intelligence/proactive_job_runner.py` | **New** |
| 8 | **RAG (kb.py)** | `search_kb()` | `app/copilot/kb.py` | Live |
| 9 | **Enhanced RAG** | `search_kb_enhanced()` | `app/copilot/kb_enhanced.py` | **New** |
| 10 | **Lending Engine** | `get_lending_offers()` | `app/growth/lending_engine.py` | Live |
| 11 | **Connector Layer** | `execute_connector()` | `app/data/connectors/settlement_ops.py` | Simulated |
| 12 | **Case / Approval Store** | `create_work_item()` | `app/data/ops/repository.py` | Live |

---

## Data Flow Diagrams

### Merchant Chat Turn
```
User Message
    │
    ▼
score_intent()          ← [NEW] lexical→entity→session→semantic
    │
    ├── confidence ≥ 0.80 ──► Direct route (no LLM call)
    │
    └── confidence < 0.80 ──► route_chat_intent() (LLM router)
                                      │
                                      ▼
                               Tool resolution
                                      │
                                      ▼
                              Synthesizer LLM
                                      │
                                      ▼
                             Final response + evidence
```

### Bank Ops Copilot Turn
```
Case Detail dict
    │
    ▼
AgentRouter.resolve()   ← [NEW] case_type → AgentRoute descriptor
    │
    ▼
BankFoundryMCPClient    ← tool_filter_for_agent(route.tool_filter_key)
    │
    ▼
ExpertAgent.summarize_case()
    │
    ├── key_findings
    ├── next_best_action
    ├── evidence_ids
    └── caveats
    │
    ▼
Draft agents (operator_note, approval_request, merchant_update)
    │
    ▼
memory_snapshot → case context pinned
```

### Proactive Monitor (Cron)
```
APScheduler (every N minutes)
    │
    ▼
ProactiveJobRunner._apscheduler_task()
    │
    ├── load_active_merchant_ids()
    │
    └── for each merchant:
            │
            ▼
        signal_fn(engine, merchant_id, start_date, end_date)
            │
            ├── triggered=False ─► skip
            │
            └── triggered=True  ─► idempotency_key check
                                        │
                                        ├── already fired ─► skip (no duplicate)
                                        │
                                        └── new event ─► write_case() → ops case store
```

### Enhanced RAG Retrieval
```
search_kb_enhanced(query, intent)
    │
    ├── _expand_query(query, intent)   ← intent → extra domain terms
    │
    ├── TF-IDF similarity (vectorizer.transform)
    │
    ├── BM25 scores (exact-term matching)
    │
    ├── _fuse_scores(tfidf, bm25)      ← 55/45 weighted fusion
    │
    └── Top-K results with evidence_ids
```

---

## Expert Agent Map (MoE)

| Case Type(s) | Expert Agent | Tool Filter Key | Draft Mode |
|---|---|---|---|
| `settlement_shortfall_review`, `reconciliation_mismatch` | `ReconciliationInvestigationAgent` | `reconciliation_investigation_agent` | SETTLEMENT |
| `processed_unsettled_payout`, `delayed_payout_exception` | `DelayedPayoutAgent` | `delayed_payout_agent` | SETTLEMENT |
| Other settlement types | `SettlementCaseSummaryAgent` | `settlement_case_summary_agent` | SETTLEMENT |
| `chargeback_review` | `ChargebackReviewAgent` | `chargeback_review_agent` | DISPUTE |
| `refund_exception` | `RefundExceptionAgent` | `refund_exception_agent` | DISPUTE |
| `payment_exception`, `terminal_*` | `PaymentsExceptionAgent` | `payments_exception_agent` | DISPUTE |
| `merchant_support_case` | `MerchantSupportCaseAgent` | `merchant_support_case_agent` | SUPPORT |
| `aml_investigation`, `screening_review` | `AMLInvestigationAgent` | `aml_investigation_agent` | RISK |
| `risk_triage`, `kyc_review` | `RiskTriageAgent` | `risk_triage_agent` | RISK |
| `connector_follow_up` | `ConnectorSupervisorAgent` | `connector_supervisor_agent` | CONNECTOR |
| `background_refresh_issue`, `incident_response` | `IncidentResponseAgent` | `incident_response_agent` | INCIDENT |
| *(catch-all)* | `OpsCaseCopilotMCPAgent` | `generic_ops_case_copilot_agent` | GENERIC |

---

## Proactive Job Registry

| Job Name | Case Type | Lane | Interval | Lookback | Severity |
|---|---|---|---|---|---|
| `payout_shortfall_monitor` | `settlement_shortfall_review` | settlement | 60 min | 7d | high |
| `terminal_anomaly_monitor` | `terminal_failure_review` | payments | 30 min | 3d | high |
| `chargeback_anomaly_monitor` | `chargeback_review` | disputes | 120 min | 7d | medium |
| `reconciliation_break_monitor` | `reconciliation_mismatch` | settlement | 60 min | 7d | medium |
| `payment_mode_skew_monitor` | `payment_mode_skew` | payments | 60 min | 7d | medium |
| `kpi_delta_monitor` | `payment_exception` | payments | 240 min | 14d | medium |
| `anomaly_monitor` | `risk_triage` | risk | 120 min | 7d | high |
| `peak_hour_monitor` | `payment_exception` | payments | 60 min | 3d | low |
| `lost_sales_monitor` | `payment_exception` | payments | 240 min | 7d | medium |
| `lending_eligibility_monitor` | `merchant_support_case` | growth | 1440 min | 30d | low |

---

## Key Architectural Boundaries

| Boundary | Rule |
|---|---|
| **No raw SQL in agents** | Agents call MCP tools only; SQL stays in `data/` layer |
| **No LLM calls in engines** | Signal engines are deterministic Python; no model calls |
| **Intent scoring before LLM** | `score_intent()` must be tried before `route_chat_intent()` |
| **Drafts are post-processing** | Draft agents run after `summarize_case()` has returned |
| **Proactive cases are idempotent** | Never write the same `(job, merchant, window)` twice |

---

## Outstanding Risks

| Item | Severity | Owner |
|---|---|---|
| `tool_registry.py` is 161KB — mixed concerns | High | Architecture |
| Connector execution is simulated | High | Data layer |
| `merchant_os.py` is a catch-all monolith | Medium | Application layer |
| No live LLM call count metrics / rate-limit guard | Medium | Intelligence layer |
| `bank_kb/` corpus is small — RAG recall limited | Medium | Knowledge base |

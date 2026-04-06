# Layered Architecture Map

## Goal

Split the live codebase into three layers:

- `data`
- `ontology`
- `application`

This is not a rewrite plan. It is a safe migration map for the current live system.

## The target shape

```text
app/
  data/
    db.py
    merchants/
      repository.py
    transactions/
      repository.py
    settlements/
      repository.py
    chargebacks/
      repository.py
    terminals/
      repository.py
    integrations/
      repository.py
    knowledge/
      repository.py

  ontology/
    context.py
    evidence.py
    recommendations.py
    response_codes.py
    metrics/
      definitions.py
      calculators.py
    signals/
      anomaly.py
      attribution.py
      disputes.py
      kpi_delta.py
      lost_sales.py
      operational.py
      payment_mode.py
      peak_hour.py
      reconciliation.py
    playbooks/
      failure_codes.py
      settlement_ops.py
      terminal_ops.py
    cards/
      templates.py
      generator.py

  application/
    api/
      server.py
    agent/
      service.py
      toolcalling.py
      prompts.py
    merchant_os/
      snapshot_service.py
      reports_service.py
      proactive_service.py
      actions_service.py
    workflows/
      action_center.py
      approvals.py
```

## What each layer should own

### Data

Owns:

- raw database access
- repository functions
- table/column inspection
- source-system adapters
- read/write persistence helpers

Must not own:

- recommendation logic
- business narratives
- chat orchestration

### Ontology

Owns:

- business objects
- canonical metrics
- response-code semantics
- signals
- evidence models
- recommendation/playbook semantics

Must not own:

- raw SQL endpoint wiring
- frontend/API concerns
- direct FastAPI request handling

### Application

Owns:

- API routes
- chat orchestration
- user workflows
- approvals
- workspace composition
- frontend-facing payloads

Must not own:

- raw SQL logic
- duplicated metric semantics

## File-by-file mapping

### Keep in application

- `app/api/server.py`
  - keep as API boundary only
  - remove DB helpers and merchant lookup SQL over time
- `app/agent/service.py`
  - keep as live chat runtime
- `app/copilot/toolcalling.py`
  - move later to `app/application/agent/toolcalling.py`
  - still application, not ontology

### Move to ontology

- `app/intelligence/type.py`
  - move to `app/ontology/recommendations.py`
- `app/intelligence/response_codes.py`
  - move to `app/ontology/response_codes.py`
- `app/intelligence/playbooks.py`
  - move to `app/ontology/playbooks/failure_codes.py`
- `app/intelligence/engines/anomaly.py`
  - move to `app/ontology/signals/anomaly.py`
- `app/intelligence/engines/attribution.py`
  - move to `app/ontology/signals/attribution.py`
- `app/intelligence/engines/dispute_signals.py`
  - move to `app/ontology/signals/disputes.py`
- `app/intelligence/engines/kpi_delta.py`
  - move to `app/ontology/signals/kpi_delta.py`
- `app/intelligence/engines/lost_sales.py`
  - move to `app/ontology/signals/lost_sales.py`
- `app/intelligence/engines/operational_signals.py`
  - move to `app/ontology/signals/operational.py`
- `app/intelligence/engines/payment_mode.py`
  - move to `app/ontology/signals/payment_mode.py`
- `app/intelligence/engines/peak_hour.py`
  - move to `app/ontology/signals/peak_hour.py`
- `app/intelligence/engines/reconciliation_signals.py`
  - move to `app/ontology/signals/reconciliation.py`
- `app/intelligence/insight_cards.py`
  - split:
  - card template loading and trigger semantics -> ontology
  - card delivery/workspace placement -> application
- `app/intelligence/constants.py`
  - split:
  - semantic constants -> ontology
  - app wiring constants -> application

### Move to data

- `app/copilot/kb.py`
  - move to `app/data/knowledge/repository.py`
- raw query portions of `app/copilot/tools.py`
  - split into repositories by domain
- raw query portions of `app/merchant_os.py`
  - split into domain repositories
- raw query portions of `app/intelligence/runner.py`
  - split into repository/calculator dependencies
- merchant selection helpers in `app/api/server.py`
  - move to `app/data/merchants/repository.py`
- `app/copilot/validation_server.py`
  - split:
  - merchant default resolution -> data
  - json-safe response helper -> application/util

### Split across layers

- `app/copilot/tools.py`
  - current role: mixed data + ontology + application
  - target split:
    - query/repository calls -> data
    - semantic output shapes and evidence conventions -> ontology
    - LangChain tool wrappers -> application

- `app/merchant_os.py`
  - current role: mixed data + ontology + application
  - target split:
    - snapshot queries -> data
    - snapshot object semantics / card semantics / report object semantics -> ontology
    - proactive refresh / reports / actions services -> application

- `app/intelligence/runner.py`
  - current role: mixed data + ontology + application
  - target split:
    - metric retrieval -> data
    - signal computation / recommendation semantics -> ontology
    - orchestration of the runner for live use -> application

- `app/intelligence/action_center.py`
  - current role: ontology + application + persistence
  - target split:
    - action eligibility rules -> ontology
    - merchant action persistence -> data
    - action-center workflow service -> application

## The practical layer ownership of current live modules

### Mostly data today

- `app/copilot/kb.py`
- SQL-heavy parts of `app/copilot/tools.py`
- SQL-heavy parts of `app/merchant_os.py`
- SQL-heavy parts of `app/intelligence/runner.py`

### Mostly ontology today

- `app/intelligence/type.py`
- `app/intelligence/response_codes.py`
- `app/intelligence/playbooks.py`
- `app/intelligence/engines/*`
- semantic parts of `app/intelligence/insight_cards.py`

### Mostly application today

- `app/api/server.py`
- `app/agent/service.py`
- `app/copilot/toolcalling.py`
- workflow parts of `app/merchant_os.py`
- workflow parts of `app/intelligence/action_center.py`

## Safe migration order

### Phase 1

No behavior change. Create folders and move only pure semantic modules.

- move `type.py`
- move `response_codes.py`
- move `playbooks.py`
- move `engines/*`

### Phase 2

Extract repositories from mixed services.

- merchant repository
- transaction repository
- settlement repository
- chargeback repository
- terminal repository

### Phase 3

Split `merchant_os.py`.

- snapshot service
- proactive service
- reports service
- actions service

### Phase 4

Split `tools.py`.

- repositories in data
- semantic result mappers in ontology
- tool wrappers in application

### Phase 5

Split `runner.py`.

- repository calls move down
- signal semantics stay in ontology
- orchestration stays in application

## The blunt assessment

Right now the architecture is usable, but the middle layer is weak.

The live app already behaves like:

`data access -> semantic interpretation -> workspace/chat application`

The problem is that those concerns are still packed into a few large mixed modules.

The highest-value structural cleanup is:

- split `tools.py`
- split `merchant_os.py`
- split `runner.py`

That will make the `ontology` layer real instead of implied.

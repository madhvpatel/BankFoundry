# Control Plane Target Architecture

## Purpose

This document maps the abstract gateway/control-plane architecture onto the current AcquiGuru codebase.

It is meant to answer one practical question:

**How do we evolve the current live system into a multi-surface banking and merchant-operations platform with one consistent execution model?**

This is an application-layer target architecture.

It sits on top of:

- the `data` layer
- the `ontology` layer

It does **not** replace those layers.

## The target application shape

```text
app/
  application/
    control_plane/
      router.py
      policy.py
      sessions.py
      events.py
      auth.py
      registry.py
    kernel/
      executor.py
      request_models.py
      response_models.py
      context_loader.py
      state_store.py
      trace_store.py
    ingress/
      http_api.py
      web_chat.py
      proactive_jobs.py
      operator_actions.py
      external_api.py
    delivery/
      chat.py
      workspace.py
      action_center.py
      reports.py
      webhooks.py
    capabilities/
      tool_registry.py
      workflow_registry.py
      approval_registry.py
```

## What the control plane should be

In AcquiGuru, the control plane should become the source of truth for:

- request routing
- session lookup
- context loading
- policy enforcement
- workflow selection
- trace persistence
- event streaming
- approval pauses

### Current partial control plane

Today this role is split across:

- `app/api/server.py`
- `app/agent/service.py`
- `app/merchant_os.py`

That is why the system works, but does not yet feel like one coherent runtime.

### Target control plane responsibilities

The new control plane should answer these questions for every request:

1. who is calling
2. what surface they came from
3. what tenant / merchant / workspace they are in
4. what session key applies
5. what workflow or agent path should run
6. what tools and actions are allowed
7. where state and traces should be persisted
8. how the output should be delivered

## Canonical request schema

Every major surface should normalize into one request contract before execution.

## Target request model

```python
class CanonicalRequest(TypedDict):
    request_id: str
    request_type: Literal[
        "chat_turn",
        "workspace_refresh",
        "proactive_job",
        "action_preview",
        "action_confirm",
        "report_build",
        "external_api_call",
    ]
    surface: Literal[
        "web_chat",
        "workspace",
        "proactive_inbox",
        "action_center",
        "reports",
        "api",
        "scheduler",
    ]
    actor: dict
    tenant: dict
    workspace: dict
    session: dict
    payload: dict
    policy_context: dict
    delivery: dict
    debug: bool
```

## AcquiGuru examples

### Chat turn

```json
{
  "request_type": "chat_turn",
  "surface": "web_chat",
  "workspace": {
    "merchant_id": "MID123",
    "terminal_id": "GS288699",
    "workspace_kind": "merchant_workspace"
  },
  "session": {
    "session_key": "merchant:mid123:web_chat:terminal:GS288699",
    "thread_id": "chat:active"
  },
  "payload": {
    "prompt": "Why did failures increase this week?"
  }
}
```

### Proactive refresh

```json
{
  "request_type": "proactive_job",
  "surface": "scheduler",
  "workspace": {
    "merchant_id": "MID123",
    "workspace_kind": "merchant_workspace"
  },
  "session": {
    "session_key": "merchant:mid123:proactive:30d"
  },
  "payload": {
    "job_name": "refresh_proactive_cards",
    "days": 30
  }
}
```

### Action preview

```json
{
  "request_type": "action_preview",
  "surface": "action_center",
  "workspace": {
    "merchant_id": "MID123",
    "workspace_kind": "merchant_workspace"
  },
  "session": {
    "session_key": "merchant:mid123:actions"
  },
  "payload": {
    "action_type": "investigate_settlement_shortfall",
    "input": {
      "settlement_id": "261"
    }
  }
}
```

## Session key design

This is one of the most important missing pieces in the current system.

Today:

- chat history is passed in request payloads
- workspace state is rebuilt ad hoc
- entity choices are not durably tracked across all surfaces

### Rule

Session identity should be based on business scope, not transport details.

## Proposed session key templates

### Merchant workspace session

```text
merchant:{merchant_id}:workspace
```

### Merchant + terminal scoped session

```text
merchant:{merchant_id}:terminal:{terminal_id}:workspace
```

### Chat thread session

```text
merchant:{merchant_id}:chat:{surface}:{thread_scope}
```

Examples:

- `merchant:M123:chat:web_chat:default`
- `merchant:M123:chat:web_chat:terminal:GS288699`

### Proactive job session

```text
merchant:{merchant_id}:job:{job_name}:{window_label}
```

### Action-center session

```text
merchant:{merchant_id}:actions
```

### Report session

```text
merchant:{merchant_id}:reports:{report_kind}:{window_label}
```

## What the state layer should hold

For each session key, store:

- current workspace scope
- recent transcript or event history
- selected entities
- active date window
- active comparison window
- open approvals
- latest evidence summary
- trace references

### For chat sessions specifically

Store structured state like:

```python
{
  "merchant_id": "MID123",
  "terminal_id": "GS288699",
  "selected_entities": {
    "settlement_id": "261",
    "chargeback_id": None,
    "action_id": None
  },
  "active_window": {
    "from_date": "2026-03-01",
    "to_date": "2026-03-08"
  },
  "active_dimension": "response_code"
}
```

That is the foundation for fixing cross-turn drift later.

## Ingress adapters for AcquiGuru

These should all be thin adapters that convert external input into the canonical request.

### Current live surfaces

- web chat
- merchant workspace refresh
- proactive inbox refresh
- action preview
- action confirm
- report generation

### Proposed ingress mapping

- `frontend chat -> application/ingress/web_chat.py`
- `FastAPI endpoints -> application/ingress/http_api.py`
- `proactive refresh jobs -> application/ingress/proactive_jobs.py`
- `action center mutations -> application/ingress/operator_actions.py`
- `future external partner API -> application/ingress/external_api.py`

## Delivery adapters for AcquiGuru

Today delivery is mostly implicit in the API response shape.

That is fine for now, but the target should make delivery explicit.

### Delivery types

- chat reply payload
- workspace snapshot payload
- proactive inbox update payload
- action-preview payload
- report-pack payload
- future webhook/ticket payload

### Proposed delivery modules

- `delivery/chat.py`
- `delivery/workspace.py`
- `delivery/action_center.py`
- `delivery/reports.py`
- `delivery/webhooks.py`

## Capability registry

The current system exposes tools directly from `app/copilot/tools.py`.

That works, but it is not yet a control-plane capability system.

### The target model

Capabilities should register as explicit contracts:

- capability name
- scope requirements
- allowed surfaces
- read vs write
- approval required or not
- execution function

## Example contract

```python
Capability(
    name="get_settlement_detail",
    category="payments_data",
    requires_scope=["merchant_id"],
    allowed_surfaces=["web_chat", "workspace", "api"],
    mode="read",
    approval_required=False,
)
```

### Capability groups in AcquiGuru

- merchant profile and context
- transaction analytics
- settlements and reconciliation
- disputes and chargebacks
- terminal health and device analytics
- reports
- action proposals
- action confirmations
- knowledge retrieval

## Execution kernel

This is where all major requests should converge.

### Target kernel stages

1. validate canonical request
2. load policy
3. derive session key
4. load session state
5. load workspace context
6. choose workflow
7. execute workflow
8. persist state and trace
9. hand off to delivery adapter

### For chat

The chat path should become just one workflow inside the kernel:

- `chat_turn -> chat workflow -> delivery/chat`

### For proactive refresh

- `proactive_job -> proactive workflow -> delivery/workspace`

### For action confirmation

- `action_confirm -> approval workflow -> delivery/action_center`

## Workflow selection

Do not think of everything as one agent.

Think of the kernel routing into bounded workflows such as:

- chat investigation workflow
- merchant snapshot build workflow
- proactive-card generation workflow
- action preview workflow
- action confirmation workflow
- report build workflow

The LLM can still be used inside these workflows, but the workflow itself should be selected in code.

## Mapping current files to the target control-plane architecture

### Current file -> target home

- `app/api/server.py`
  - split into:
  - `application/ingress/http_api.py`
  - `application/control_plane/router.py`

- `app/agent/service.py`
  - split into:
  - `application/kernel/executor.py`
  - `application/workflows/chat.py`

- `app/copilot/toolcalling.py`
  - move to:
  - `application/capabilities/tool_registry.py`
  - `application/workflows/chat_tool_loop.py`

- `app/merchant_os.py`
  - split into:
  - `application/workflows/workspace_snapshot.py`
  - `application/workflows/proactive.py`
  - `application/workflows/reports.py`
  - `application/workflows/actions.py`

## Migration path from the live system

### Phase A

Define contracts only.

- canonical request schema
- canonical response schema
- session key builder
- capability registry schema

No behavior change.

### Phase B

Introduce the control plane boundary without changing endpoints.

- FastAPI endpoints still exist
- they now normalize into canonical requests
- they call one router

### Phase C

Move chat execution into the kernel.

- `/api/v1/ask` becomes:
  - ingress -> control plane -> kernel -> chat workflow -> delivery

### Phase D

Move proactive and action flows into the same kernel model.

- proactive refresh
- action preview
- action confirm
- report generation

### Phase E

Add durable session state.

- structured state store
- selected entities
- active windows
- approval pauses
- trace snapshots

### Phase F

Add new surfaces.

- scheduled jobs
- external workflow APIs
- ticketing/webhooks
- operator console

## The blunt assessment

Can this architecture be used here?

Yes.

But only if it is understood correctly:

- `data` and `ontology` remain foundational layers
- this reference architecture becomes the application-layer operating model

The current system is already close enough to start:

- there is already one main chat runtime
- there are already multiple user-facing surfaces
- there are already capability-like tools
- there are already workflow-like operations

What is missing is the thing that ties them together:

- one canonical request model
- one control plane
- one session model
- one kernel

That should be the next major architectural step after the ontology work.

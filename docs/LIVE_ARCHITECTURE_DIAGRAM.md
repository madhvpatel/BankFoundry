# Live Architecture Diagram

## Purpose

This diagram explains the **current live architecture** after the control-plane,
workflow, data-layer, and ontology cleanup work.

It is intentionally focused on the live path, not the future runtime.

## Diagram

```mermaid
flowchart TD
  subgraph Surfaces["User Surfaces"]
    Web["Frontend workspace and chat<br/>frontend/src/App.jsx"]
    Ops["Reports, proactive inbox, action center"]
  end

  subgraph Ingress["Ingress and Control Plane"]
    API["FastAPI ingress<br/>app/api/server.py"]
    Router["Control-plane router<br/>app/application/control_plane/router.py"]
    Session["Session-key builder<br/>app/application/control_plane/sessions.py"]
    Contracts["Canonical contracts<br/>app/application/kernel/request_models.py<br/>app/application/kernel/response_models.py"]
  end

  subgraph Workflows["Application Workflows"]
    LiveSurface["Live surface handlers<br/>app/application/workflows/live_surface.py"]
    LiveContext["Workspace context builder<br/>app/application/workflows/live_context.py"]
    Reporting["Reporting workflow<br/>app/application/workflows/reporting.py"]
    Agent["Live chat runtime<br/>app/agent/service.py"]
    Toolcalling["Bounded tool loop<br/>app/copilot/toolcalling.py"]
  end

  subgraph ToolLayer["Tool and Compatibility Layer"]
    Tools["Tool wrappers<br/>app/copilot/tools.py"]
    MerchantOS["Compatibility facade<br/>app/merchant_os.py"]
  end

  subgraph Ontology["Ontology and Semantics"]
    Recos["Recommendations<br/>app/ontology/recommendations.py"]
    Codes["Response-code semantics<br/>app/ontology/response_codes.py"]
    Signals["Signals and playbooks<br/>app/ontology/signals/*<br/>app/ontology/playbooks/*"]
  end

  subgraph Data["Data Repositories"]
    Merchants["Merchants<br/>app/data/merchants/repository.py"]
    Transactions["Transactions<br/>app/data/transactions/repository.py"]
    Settlements["Settlements<br/>app/data/settlements/repository.py"]
    Disputes["Disputes and refunds<br/>app/data/disputes/repository.py"]
    Terminals["Terminals and merchant ops<br/>app/data/terminals/repository.py<br/>app/data/merchant_ops/repository.py"]
    Actions["Actions and proactive storage<br/>app/data/actions/repository.py<br/>app/data/proactive/repository.py"]
  end

  DB["Merchant and payments database"]

  Web --> API
  Ops --> API

  API --> Contracts
  API --> Session
  API --> Router

  Router --> LiveSurface
  LiveSurface --> LiveContext
  LiveSurface --> Reporting
  LiveSurface --> Agent
  LiveSurface --> MerchantOS

  Agent --> Toolcalling
  Toolcalling --> Tools
  MerchantOS --> LiveContext

  Tools --> Merchants
  Tools --> Transactions
  Tools --> Settlements
  Tools --> Disputes
  Tools --> Terminals
  Tools --> Actions

  LiveContext --> Merchants
  LiveContext --> Transactions
  LiveContext --> Actions
  LiveContext --> Terminals
  Reporting --> Transactions
  Reporting --> Actions

  Agent -.uses validation and intelligence semantics.-> Recos
  Agent -.uses response-code and signal meaning.-> Codes
  Agent -.uses response-code and signal meaning.-> Signals
  Tools -.intelligence and evidence meaning.-> Recos
  Tools -.intelligence and evidence meaning.-> Signals

  Merchants --> DB
  Transactions --> DB
  Settlements --> DB
  Disputes --> DB
  Terminals --> DB
  Actions --> DB
```

## How to read this

The live system now has four practical layers:

1. **Ingress and control plane**
   - `app/api/server.py` accepts requests
   - requests are normalized into canonical models
   - session keys and routing are decided before execution

2. **Application workflows**
   - `live_surface.py` handles the live product behaviors
   - `live_context.py` builds merchant workspace context
   - `reporting.py` builds report payloads
   - `app/agent/service.py` handles the live chat turn

3. **Tool and compatibility layer**
   - `app/copilot/tools.py` is now mostly a thin wrapper layer
   - `app/merchant_os.py` still exists as a compatibility facade for the live path

4. **Ontology and data**
   - `app/ontology/*` owns business meaning, signals, and recommendation semantics
   - `app/data/*` owns raw reads and writes against the database

## What is better now

Before this cleanup, too much logic lived in:

- `app/api/server.py`
- `app/merchant_os.py`
- `app/copilot/tools.py`

Now the live path is much clearer:

- API and routing live at the top
- workflows sit in the middle
- tools are thin wrappers
- data access lives in repositories
- business semantics live in ontology modules

## What is still true

This is still the **live demo architecture**, not the future graph runtime.

That means:

- the live chat path still runs through `app/agent/service.py`
- the future revenue-recovery runtime is still separate
- `app/merchant_os.py` still exists as a transition layer

## Real issues vs noise

Real issue:

- if new feature work adds fresh SQL directly back into `server.py`,
  `merchant_os.py`, or `tools.py`, the layer split will drift again

Usually not a product issue:

- the Starlette `python_multipart` warning seen in tests is still framework
  noise

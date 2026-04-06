# Bank Foundry Sprint Slices

## Purpose

This is the active sprint launcher for Bank Foundry.

Use one chat per pod, with one pod brief as the working scope for that chat.

## Read before any pod starts

- [BANK_FOUNDRY_MCP_CONTRACT.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_MCP_CONTRACT.md)
- [BANK_FOUNDRY_AGENT_CONTRACT.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_AGENT_CONTRACT.md)
- [BANK_FOUNDRY_WORKFLOW_BOUNDARY.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_WORKFLOW_BOUNDARY.md)
- [BANK_FOUNDRY_EXECUTION_BOARD.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_EXECUTION_BOARD.md)
- [BANK_FOUNDRY_BRANCH_REVIEW_RULES.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_BRANCH_REVIEW_RULES.md)
- [BANK_FOUNDRY_REMAINING_MCP_PARALLEL_MAP.md](/Users/madhavpatel/New_demo copy/docs/BANK_FOUNDRY_REMAINING_MCP_PARALLEL_MAP.md)

## Active pod briefs

- [Pod 0: Platform Foundations](/Users/madhavpatel/New_demo copy/docs/bank_foundry_sprint_slices/POD_0_PLATFORM_FOUNDATIONS.md)
- [Pod 1: Workflow and Case System](/Users/madhavpatel/New_demo copy/docs/bank_foundry_sprint_slices/POD_1_WORKFLOW_CASE_SYSTEM.md)
- [Pod 2: Settlement Ops](/Users/madhavpatel/New_demo copy/docs/bank_foundry_sprint_slices/POD_2_SETTLEMENT_OPS.md)
- [Pod 3: Merchant and Payments](/Users/madhavpatel/New_demo copy/docs/bank_foundry_sprint_slices/POD_3_MERCHANT_PAYMENTS.md)
- [Pod 4: Support and Disputes](/Users/madhavpatel/New_demo copy/docs/bank_foundry_sprint_slices/POD_4_SUPPORT_DISPUTES.md)
- [Pod 5: Risk and Compliance](/Users/madhavpatel/New_demo copy/docs/bank_foundry_sprint_slices/POD_5_RISK_COMPLIANCE.md)
- [Pod 6: Tech Ops and Supervision](/Users/madhavpatel/New_demo copy/docs/bank_foundry_sprint_slices/POD_6_TECH_OPS_SUPERVISION.md)

## How to use this

Each pod chat should take exactly one pod brief as its sprint scope.

The pod chat should:

- stay within that scope
- use the shared contracts
- ship MCPs first, then agents
- update tests and a short docs note
- stop when the pod exit gate is met

## Real issue vs noise

Real issues:

- pods redefining MCP shapes
- agents importing repositories directly
- write workflows moving into agent code
- missing pod tests or replay checks

Usually noise:

- Starlette `python_multipart` pending deprecation warning in tests
- Vite chunk-size warning during frontend build

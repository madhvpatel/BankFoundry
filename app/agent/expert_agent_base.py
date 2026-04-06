"""
expert_agent_base.py — MoE (Mixture of Experts) agent contract and router.

Defines:
  - ExpertAgent: base protocol that all specialist agents satisfy.
  - AgentRoute: descriptor wiring a case-type set to an agent class + tool filter + draft mode.
  - AgentRouter: replaces the if/else dispatch tree in BankOpsCaseCopilotRouter.

All existing agent classes in bank_ops_agents.py already satisfy ExpertAgent.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from app.agent.mcp_client import BankFoundryMCPClient
    from app.mcp_server import BankFoundryMCPServer

logger = logging.getLogger("expert_agent.router")


# ---------------------------------------------------------------------------
# ExpertAgent protocol
# ---------------------------------------------------------------------------

class ExpertAgent(Protocol):
    """
    Contract that every specialist case agent must satisfy.

    All existing agents (SettlementCaseSummaryAgent, ChargebackReviewAgent, etc.)
    already implement this method — no changes needed to existing code.
    """

    def summarize_case(
        self,
        *,
        case_detail: dict[str, Any],
        prompt: str | None = None,
    ) -> dict[str, Any]:
        """Run the expert analysis and return the structured summary dict."""
        ...


# ---------------------------------------------------------------------------
# DraftMode — controls which draft agents are attached
# ---------------------------------------------------------------------------

class DraftMode:
    SETTLEMENT = "settlement"   # operator_note + approval_request (settlement tools)
    DISPUTE    = "dispute"      # operator_note + approval_request + merchant_update
    SUPPORT    = "support"      # merchant_update only
    RISK       = "risk"         # operator_note + approval_request
    CONNECTOR  = "connector"    # operator_note + approval_request
    INCIDENT   = "incident"     # operator_note + approval_request
    GENERIC    = "generic"      # generic fallback drafts


# ---------------------------------------------------------------------------
# AgentRoute descriptor
# ---------------------------------------------------------------------------

@dataclass
class AgentRoute:
    """
    Maps a set of case_type strings to an expert agent class + metadata.

    Fields
    ------
    case_types     : set of case_type strings that activate this route.
                     Empty set means "catch-all" (used for the generic fallback).
    agent_cls      : callable that takes (client: BankFoundryMCPClient) → ExpertAgent.
    tool_filter_key: the agent name key used in tool_filter_for_agent().
    draft_mode     : DraftMode constant controlling which draft agents run.
    lane_match     : optional lane string that must also match (e.g. "support").
                     Only checked when case_type is "manual_ops_review".
    label          : human-readable name for observability.
    """

    case_types: set[str]
    agent_cls: Callable[..., Any]
    tool_filter_key: str
    draft_mode: str
    label: str
    lane_match: str | None = None


# ---------------------------------------------------------------------------
# AgentRouter
# ---------------------------------------------------------------------------

class AgentRouter:
    """
    Pure routing layer: given a case_detail, returns the best AgentRoute.

    Separation of concerns
    ----------------------
    - BankOpsCaseCopilotRouter owns the "run the agent + attach drafts" logic.
    - AgentRouter owns only the routing decision.
    - New expert agents are wired in by appending to the ROUTES table.
    """

    def __init__(self, routes: list[AgentRoute]) -> None:
        self._routes = routes

    def resolve(self, case_detail: dict[str, Any]) -> AgentRoute:
        """Return the best matching AgentRoute for this case."""
        from app.agent.bank_ops_agents import _case_type, _lane

        case_type = _case_type(case_detail)
        lane = _lane(case_detail)

        for route in self._routes:
            # Catch-all route: empty case_types set
            if not route.case_types:
                logger.debug("AgentRouter: catch-all route → %s", route.label)
                return route

            if case_type in route.case_types:
                # Extra lane guard — used for support manual_ops_review
                if route.lane_match is not None:
                    if lane == route.lane_match:
                        logger.debug(
                            "AgentRouter: matched %s (lane=%s) → %s",
                            case_type, lane, route.label,
                        )
                        return route
                    # case_type matched but lane didn't — keep searching
                    continue

                logger.debug("AgentRouter: matched %s → %s", case_type, route.label)
                return route

        # Fallback: last route is always the catch-all
        logger.debug("AgentRouter: no match for %s → falling back", case_type)
        return self._routes[-1]

    def describe(self) -> list[dict[str, Any]]:
        """Introspection helper for diagnostics / system graph."""
        return [
            {
                "label": r.label,
                "case_types": sorted(r.case_types),
                "tool_filter": r.tool_filter_key,
                "draft_mode": r.draft_mode,
                "lane_match": r.lane_match,
            }
            for r in self._routes
        ]


# ---------------------------------------------------------------------------
# Route table factory
# ---------------------------------------------------------------------------

def build_default_router() -> AgentRouter:
    """
    Build the canonical route table for Bank Foundry.

    Importing agent classes here (not at module level) avoids circular imports
    because bank_ops_agents.py imports from this module.
    """
    from app.agent.bank_ops_agents import (
        AMLInvestigationAgent,
        ChargebackReviewAgent,
        ConnectorSupervisorAgent,
        DelayedPayoutAgent,
        IncidentResponseAgent,
        MerchantSupportCaseAgent,
        PaymentsExceptionAgent,
        ReconciliationInvestigationAgent,
        RefundExceptionAgent,
        RiskTriageAgent,
        SettlementCaseSummaryAgent,
    )
    from app.agent.mcp_client import OpsCaseCopilotMCPAgent

    routes: list[AgentRoute] = [
        # ── Settlement ──────────────────────────────────────────────────────
        AgentRoute(
            case_types={"settlement_shortfall_review", "reconciliation_mismatch"},
            agent_cls=ReconciliationInvestigationAgent,
            tool_filter_key="reconciliation_investigation_agent",
            draft_mode=DraftMode.SETTLEMENT,
            label="Reconciliation Investigation",
        ),
        AgentRoute(
            case_types={"processed_unsettled_payout", "delayed_payout_exception"},
            agent_cls=DelayedPayoutAgent,
            tool_filter_key="delayed_payout_agent",
            draft_mode=DraftMode.SETTLEMENT,
            label="Delayed Payout",
        ),
        AgentRoute(
            case_types={"held_settlement", "settlement_shortfall_review"},
            agent_cls=SettlementCaseSummaryAgent,
            tool_filter_key="settlement_case_summary_agent",
            draft_mode=DraftMode.SETTLEMENT,
            label="Settlement Case Summary",
        ),
        # Broader settlement catch — any remaining SETTLEMENT_CASE_TYPES
        AgentRoute(
            case_types={
                "held_settlement",
                "processed_unsettled_payout",
                "settlement_shortfall_review",
                "reconciliation_mismatch",
                "delayed_payout_exception",
            },
            agent_cls=SettlementCaseSummaryAgent,
            tool_filter_key="settlement_case_summary_agent",
            draft_mode=DraftMode.SETTLEMENT,
            label="Settlement Case Summary (fallback)",
        ),
        # ── Disputes ────────────────────────────────────────────────────────
        AgentRoute(
            case_types={"chargeback_review"},
            agent_cls=ChargebackReviewAgent,
            tool_filter_key="chargeback_review_agent",
            draft_mode=DraftMode.DISPUTE,
            label="Chargeback Review",
        ),
        AgentRoute(
            case_types={"refund_exception"},
            agent_cls=RefundExceptionAgent,
            tool_filter_key="refund_exception_agent",
            draft_mode=DraftMode.DISPUTE,
            label="Refund Exception",
        ),
        # ── Payments ────────────────────────────────────────────────────────
        AgentRoute(
            case_types={"payment_exception", "payment_mode_skew", "terminal_linked_failures", "terminal_failure_review"},
            agent_cls=PaymentsExceptionAgent,
            tool_filter_key="payments_exception_agent",
            draft_mode=DraftMode.DISPUTE,
            label="Payments Exception",
        ),
        # ── Support ─────────────────────────────────────────────────────────
        AgentRoute(
            case_types={"merchant_support_case"},
            agent_cls=MerchantSupportCaseAgent,
            tool_filter_key="merchant_support_case_agent",
            draft_mode=DraftMode.SUPPORT,
            label="Merchant Support Case",
        ),
        # Manual ops review with support lane
        AgentRoute(
            case_types={"manual_ops_review"},
            agent_cls=MerchantSupportCaseAgent,
            tool_filter_key="merchant_support_case_agent",
            draft_mode=DraftMode.SUPPORT,
            label="Manual Ops Review (support lane)",
            lane_match="support",
        ),
        # ── Risk & AML ──────────────────────────────────────────────────────
        AgentRoute(
            case_types={"aml_investigation", "aml_review", "screening_review", "watchlist_review"},
            agent_cls=AMLInvestigationAgent,
            tool_filter_key="aml_investigation_agent",
            draft_mode=DraftMode.RISK,
            label="AML Investigation",
        ),
        AgentRoute(
            case_types={"risk_triage", "kyc_review"},
            agent_cls=RiskTriageAgent,
            tool_filter_key="risk_triage_agent",
            draft_mode=DraftMode.RISK,
            label="Risk Triage",
        ),
        # ── Connector & Incident ─────────────────────────────────────────────
        AgentRoute(
            case_types={"connector_follow_up"},
            agent_cls=ConnectorSupervisorAgent,
            tool_filter_key="connector_supervisor_agent",
            draft_mode=DraftMode.CONNECTOR,
            label="Connector Supervisor",
        ),
        AgentRoute(
            case_types={"background_refresh_issue", "incident_response"},
            agent_cls=IncidentResponseAgent,
            tool_filter_key="incident_response_agent",
            draft_mode=DraftMode.INCIDENT,
            label="Incident Response",
        ),
        # ── Catch-all ────────────────────────────────────────────────────────
        AgentRoute(
            case_types=set(),          # empty = catch-all
            agent_cls=OpsCaseCopilotMCPAgent,
            tool_filter_key="generic_ops_case_copilot_agent",
            draft_mode=DraftMode.GENERIC,
            label="Generic Ops Copilot (catch-all)",
        ),
    ]

    return AgentRouter(routes)

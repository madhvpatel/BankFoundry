from __future__ import annotations

from typing import Callable, Literal, Mapping

from .models import InvestigationState, ReplanAction, RunStatus

END = "__end__"

V1_NODES = [
    "initialize_run",
    "parse_intent",
    "clarify_or_continue",
    "build_initial_plan",
    "resolve_data_requirements",
    "collect_evidence",
    "grade_evidence",
    "replan_if_needed",
    "synthesize_diagnosis",
    "map_recommendations",
    "approval_gate",
    "compose_response",
    "checkpoint_and_finish",
]


def route_after_parse(state: InvestigationState) -> Literal["build_initial_plan", "compose_response"]:
    if state.runtime_control.clarification_needed:
        return "compose_response"
    return "build_initial_plan"


def route_after_replan(state: InvestigationState) -> Literal["resolve_data_requirements", "synthesize_diagnosis", "compose_response"]:
    decision = state.runtime_control.last_replan_action
    if decision == ReplanAction.replan:
        return "resolve_data_requirements"
    if decision == ReplanAction.stop_insufficient_evidence:
        return "compose_response"
    return "synthesize_diagnosis"


def route_after_approval(state: InvestigationState) -> Literal["checkpoint_and_finish", "compose_response"]:
    if state.execution.status == RunStatus.waiting_for_approval:
        return "checkpoint_and_finish"
    return "compose_response"


LANGGRAPH_V1_ADJACENCY = {
    "initialize_run": ["parse_intent"],
    "parse_intent": ["clarify_or_continue"],
    "clarify_or_continue": ["build_initial_plan", "compose_response"],
    "build_initial_plan": ["resolve_data_requirements"],
    "resolve_data_requirements": ["collect_evidence"],
    "collect_evidence": ["grade_evidence"],
    "grade_evidence": ["replan_if_needed"],
    "replan_if_needed": ["resolve_data_requirements", "synthesize_diagnosis", "compose_response"],
    "synthesize_diagnosis": ["map_recommendations"],
    "map_recommendations": ["approval_gate"],
    "approval_gate": ["compose_response", "checkpoint_and_finish"],
    "compose_response": ["checkpoint_and_finish"],
    "checkpoint_and_finish": [END],
}

try:
    from langgraph.graph import END as LANGGRAPH_END
    from langgraph.graph import START, StateGraph
except Exception:  # pragma: no cover - optional import for scaffold assembly
    LANGGRAPH_END = END
    START = "__start__"
    StateGraph = None


def build_graph(node_handlers: Mapping[str, Callable[[dict], dict]]):
    if StateGraph is None:  # pragma: no cover - exercised only when langgraph is missing
        raise RuntimeError("langgraph is not installed")
    missing = [node for node in V1_NODES if node not in node_handlers]
    if missing:
        raise ValueError(f"Missing handlers for nodes: {', '.join(missing)}")

    graph = StateGraph(dict)
    for node_name in V1_NODES:
        graph.add_node(node_name, node_handlers[node_name])

    graph.add_edge(START, "initialize_run")
    graph.add_edge("initialize_run", "parse_intent")
    graph.add_edge("parse_intent", "clarify_or_continue")
    graph.add_conditional_edges(
        "clarify_or_continue",
        route_after_parse,
        {
            "build_initial_plan": "build_initial_plan",
            "compose_response": "compose_response",
        },
    )
    graph.add_edge("build_initial_plan", "resolve_data_requirements")
    graph.add_edge("resolve_data_requirements", "collect_evidence")
    graph.add_edge("collect_evidence", "grade_evidence")
    graph.add_edge("grade_evidence", "replan_if_needed")
    graph.add_conditional_edges(
        "replan_if_needed",
        route_after_replan,
        {
            "resolve_data_requirements": "resolve_data_requirements",
            "synthesize_diagnosis": "synthesize_diagnosis",
            "compose_response": "compose_response",
        },
    )
    graph.add_edge("synthesize_diagnosis", "map_recommendations")
    graph.add_edge("map_recommendations", "approval_gate")
    graph.add_conditional_edges(
        "approval_gate",
        route_after_approval,
        {
            "compose_response": "compose_response",
            "checkpoint_and_finish": "checkpoint_and_finish",
        },
    )
    graph.add_edge("compose_response", "checkpoint_and_finish")
    graph.add_edge("checkpoint_and_finish", LANGGRAPH_END)
    return graph.compile()

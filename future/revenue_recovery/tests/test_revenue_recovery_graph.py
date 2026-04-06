from datetime import datetime, timezone

import pytest

from app.revenue_recovery.graph_v1 import V1_NODES, route_after_approval, route_after_parse, route_after_replan
from app.revenue_recovery.models import (
    ActionLevel,
    CheckpointState,
    ExecutionState,
    InvestigationState,
    ReplanAction,
    RunStatus,
    UserRole,
)
from app.revenue_recovery.write_policy import WriteViolationError, apply_node_writes


def _state() -> InvestigationState:
    now = datetime.now(timezone.utc)
    return InvestigationState(
        run_id="run_001",
        user_id="user_001",
        session_id="session_001",
        user_question="Why did failures increase?",
        user_role=UserRole.ops,
        requested_action_level=ActionLevel.read_only,
        execution=ExecutionState(current_node="parse_intent", status=RunStatus.running),
        checkpoint=CheckpointState(last_persisted_at=now),
    )


def test_graph_nodes_include_clarify_and_approval_gate():
    assert "clarify_or_continue" in V1_NODES
    assert "approval_gate" in V1_NODES


def test_route_after_parse_branches_to_compose_for_clarification():
    state = _state()
    state.runtime_control.clarification_needed = True

    assert route_after_parse(state) == "compose_response"


def test_route_after_replan_uses_typed_runtime_control():
    state = _state()
    state.runtime_control.last_replan_action = ReplanAction.replan
    assert route_after_replan(state) == "resolve_data_requirements"

    state.runtime_control.last_replan_action = ReplanAction.stop_insufficient_evidence
    assert route_after_replan(state) == "compose_response"


def test_route_after_approval_stops_for_waiting_runs():
    state = _state()
    state.execution.status = RunStatus.waiting_for_approval

    assert route_after_approval(state) == "checkpoint_and_finish"


def test_apply_node_writes_allows_only_owned_paths():
    state = _state()
    updated = apply_node_writes(
        state,
        "parse_intent",
        {
            "runtime_control.parse_confidence": 0.92,
            "runtime_control.clarification_needed": False,
        },
    )

    assert updated.runtime_control.parse_confidence == 0.92


def test_apply_node_writes_blocks_disallowed_paths():
    state = _state()

    with pytest.raises(WriteViolationError, match="not allowed to write"):
        apply_node_writes(
            state,
            "parse_intent",
            {
                "response.executive_summary": "illegal overwrite",
            },
        )

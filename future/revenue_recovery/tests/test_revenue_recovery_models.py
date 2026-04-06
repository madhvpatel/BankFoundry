from datetime import datetime, timezone

import pytest

from app.revenue_recovery.models import (
    ActionLevel,
    CheckpointState,
    ColumnRef,
    ExecutionState,
    InvestigationState,
    MetricSpec,
    QuerySpec,
    QueryShape,
    ReplanAction,
    RunStatus,
    TableRef,
    UserRole,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _state() -> InvestigationState:
    now = _now()
    return InvestigationState(
        run_id="run_001",
        user_id="user_001",
        session_id="session_001",
        user_question="Why did failures increase?",
        user_role=UserRole.ops,
        requested_action_level=ActionLevel.read_only,
        execution=ExecutionState(current_node="initialize_run", status=RunStatus.running),
        checkpoint=CheckpointState(last_persisted_at=now),
    )


def test_investigation_state_requires_timezone_aware_timestamps():
    with pytest.raises(ValueError, match="timezone-aware"):
        InvestigationState(
            run_id="run_001",
            user_id="user_001",
            session_id="session_001",
            user_question="Why did failures increase?",
            user_role=UserRole.ops,
            requested_action_level=ActionLevel.read_only,
            execution=ExecutionState(current_node="initialize_run", status=RunStatus.running),
            checkpoint=CheckpointState(last_persisted_at=datetime.utcnow()),
        )


def test_metric_spec_blocks_semantic_and_aggregate_mix():
    with pytest.raises(ValueError, match="either semantic_metric_id or aggregation"):
        MetricSpec(
            name="bad_metric",
            alias="bad_metric",
            semantic_metric_id="success_rate",
            column=ColumnRef(table_alias="t0", column="amount"),
        )


def test_metric_spec_requires_semantic_metric_or_aggregate_pair():
    with pytest.raises(ValueError, match="requires semantic_metric_id or aggregation"):
        MetricSpec(name="missing_metric", alias="missing_metric")


def test_query_spec_accepts_typed_structure():
    spec = QuerySpec(
        query_id="q_001",
        shape=QueryShape.aggregate,
        base_table=TableRef(name="transactions", alias="t0"),
        metrics=[MetricSpec(name="amount_sum", alias="amount_sum", aggregation="SUM", column=ColumnRef(table_alias="t0", column="amount"))],
    )

    assert spec.base_table.name == "transactions"
    assert spec.metrics[0].alias == "amount_sum"


def test_runtime_state_tracks_replan_action_explicitly():
    state = _state()
    state.runtime_control.last_replan_action = ReplanAction.replan

    assert state.runtime_control.last_replan_action == ReplanAction.replan

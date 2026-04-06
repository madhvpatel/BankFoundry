from datetime import datetime, timezone

from app.revenue_recovery.checkpoint_store import InMemoryCheckpointStore
from app.revenue_recovery.eval_runner import EvalCase, replay_eval_case
from app.revenue_recovery.graders import grade_query_plan_from_trace
from app.revenue_recovery.models import (
    ActionLevel,
    CheckpointState,
    ColumnRef,
    EvidenceRequirement,
    ExecutionState,
    InvestigationPlan,
    InvestigationState,
    MetricSpec,
    PlanStep,
    QueryShape,
    QuerySpec,
    RunStatus,
    TableRef,
    UserRole,
)
from app.revenue_recovery.sql_compiler import compile_query_spec


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _state() -> InvestigationState:
    now = _now()
    return InvestigationState(
        run_id="run_001",
        user_id="user_001",
        session_id="session_001",
        user_question="Why did card failures increase?",
        user_role=UserRole.ops,
        requested_action_level=ActionLevel.read_only,
        execution=ExecutionState(current_node="resolve_data_requirements", status=RunStatus.running),
        checkpoint=CheckpointState(last_persisted_at=now),
    )


def _query_spec() -> QuerySpec:
    return QuerySpec(
        query_id="q_001",
        shape=QueryShape.aggregate,
        base_table=TableRef(name="transactions", alias="t0"),
        metrics=[
            MetricSpec(
                name="amount_sum",
                alias="amount_sum",
                aggregation="SUM",
                column=ColumnRef(table_alias="t0", column="amount"),
            )
        ],
    )


def test_checkpoint_store_round_trips_state_trace_and_query_specs():
    store = InMemoryCheckpointStore()
    state = _state()
    query_spec = _query_spec()

    input_ref = store.persist_state(state.run_id, "resolve_data_requirements", state)
    traced_state = state.model_copy(
        update={
            "query_specs": [query_spec],
            "compiled_queries": [compile_query_spec(query_spec)],
        }
    )
    output_ref = store.persist_state(state.run_id, "collect_evidence", traced_state)
    store.persist_query_specs(state.run_id, "resolve_data_requirements", [query_spec])
    trace = store.persist_trace(
        run_id=state.run_id,
        node_name="resolve_data_requirements",
        input_state_ref=input_ref,
        output_state_ref=output_ref,
        context_manifest_version="resolve_data_requirements_v1",
    )

    reloaded_state = store.load_state(output_ref)
    reloaded_trace = store.list_traces(state.run_id)[0]
    reloaded_specs = store.load_query_specs(state.run_id, "resolve_data_requirements")

    assert reloaded_state.query_specs[0].query_id == "q_001"
    assert reloaded_trace.node_name == trace.node_name
    assert reloaded_trace.context_manifest_version == "resolve_data_requirements_v1"
    assert reloaded_specs[0].base_table.name == "transactions"


def test_grade_query_plan_from_trace_returns_pass_for_allowlisted_query_plan():
    store = InMemoryCheckpointStore()
    state_before = _state()
    query_spec = _query_spec()
    state_after = state_before.model_copy(
        update={
            "plan": InvestigationPlan(
                required_evidence=[
                    EvidenceRequirement(
                        requirement_id="req_001",
                        name="overall_amount",
                        description="Overall transaction amount",
                    )
                ],
                steps=[
                    PlanStep(
                        step_id="step_001",
                        node_name="resolve_data_requirements",
                        purpose="Map evidence requirement to query spec",
                    )
                ],
            ),
            "query_specs": [query_spec],
        }
    )
    input_ref = store.persist_state(state_before.run_id, "resolve_data_requirements", state_before)
    output_ref = store.persist_state(state_after.run_id, "resolve_data_requirements", state_after)
    trace = store.persist_trace(
        run_id=state_after.run_id,
        node_name="resolve_data_requirements",
        input_state_ref=input_ref,
        output_state_ref=output_ref,
        context_manifest_version="resolve_data_requirements_v1",
    )

    grade = grade_query_plan_from_trace(trace, state_before, state_after, [query_spec])

    assert grade.label.value == "pass"
    assert grade.metrics["query_spec_count"] == 1
    assert grade.metrics["no_forbidden_tables"] is True


def test_eval_runner_repackages_runtime_outputs():
    case = EvalCase(
        case_id="case_001",
        question="Why did card failures increase?",
        expected_slice="issuer timeout spike",
    )

    def _run_case(eval_case: EvalCase):
        state = _state().model_copy(update={"user_question": eval_case.question})
        store = InMemoryCheckpointStore()
        input_ref = store.persist_state(state.run_id, "initialize_run", state)
        output_ref = store.persist_state(state.run_id, "checkpoint_and_finish", state)
        trace = store.persist_trace(
            run_id=state.run_id,
            node_name="checkpoint_and_finish",
            input_state_ref=input_ref,
            output_state_ref=output_ref,
            context_manifest_version="checkpoint_and_finish_v1",
        )
        return state, [trace], []

    result = replay_eval_case(case=case, run_case=_run_case)

    assert result.case_id == "case_001"
    assert len(result.trace_manifests) == 1
    assert result.node_grades == []

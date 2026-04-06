from __future__ import annotations

from .models import GradeLabel, InvestigationState, NodeGrade, PersistedTraceManifest, QuerySpec, TraceRef
from .sql_compiler import ALLOWED_TABLES


def grade_query_plan_from_trace(
    trace: PersistedTraceManifest,
    state_before: InvestigationState,
    state_after: InvestigationState,
    query_specs: list[QuerySpec],
) -> NodeGrade:
    plan = state_after.plan
    if plan is None:
        raise ValueError("state_after.plan is required for query-plan grading")

    unresolved = len(state_after.evidence_store.missing_data)
    no_forbidden_tables = True
    join_budget_ok = True

    for query_spec in query_specs:
        if query_spec.base_table.name not in ALLOWED_TABLES:
            no_forbidden_tables = False
        if len(query_spec.joins) > 2:
            join_budget_ok = False

    all_required = len(plan.required_evidence) >= 1
    score = sum([all_required, no_forbidden_tables, join_budget_ok]) / 3.0
    label = GradeLabel.pass_ if score == 1.0 else GradeLabel.warn if score >= 0.66 else GradeLabel.fail

    return NodeGrade(
        run_id=trace.run_id,
        node_name=trace.node_name,
        grader_name="grade_query_plan_from_trace",
        label=label,
        score=score,
        rationale="Checks mapping completeness, allowlisted tables, and join budget.",
        trace_ref=TraceRef(
            run_id=trace.run_id,
            node_name=trace.node_name,
            checkpoint_ref=trace.checkpoint_ref,
            llm_call_ref=trace.llm_call_ref,
            tool_call_refs=trace.tool_call_refs,
        ),
        metrics={
            "unresolved_requirements_count": unresolved,
            "no_forbidden_tables": no_forbidden_tables,
            "join_budget_ok": join_budget_ok,
            "plan_required_evidence_count": len(plan.required_evidence),
            "query_spec_count": len(query_specs),
        },
    )

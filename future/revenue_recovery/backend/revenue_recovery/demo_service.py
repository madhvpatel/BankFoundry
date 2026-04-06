from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.engine import Engine

from .checkpoint_store import InMemoryCheckpointStore
from .models import (
    ActionLevel,
    ApprovalRequest,
    CheckpointState,
    ClarificationRequest,
    ComposeResponseOutput,
    ConflictItem,
    ConflictSeverity,
    ContextBudget,
    Diagnosis,
    DriverAssessment,
    DriverImpact,
    EvidenceBundle,
    EvidenceProvenance,
    EvidenceQuality,
    EvidenceRequirement,
    EvidenceSourceType,
    EvidenceStore,
    ExecutionState,
    ExplanationQuality,
    Fact,
    FilterOp,
    FilterSpec,
    Finding,
    Grain,
    InvestigationIntent,
    InvestigationPlan,
    InvestigationState,
    MetricSpec,
    MissingDataItem,
    MissingDataSeverity,
    OrderBySpec,
    OrderTargetKind,
    PlanStep,
    QueryShape,
    QuerySpec,
    ReplanAction,
    Recommendation,
    RunStatus,
    StopPolicy,
    SupportLevel,
    TableRef,
    TaskType,
    TimeWindow,
    UserResponse,
    UserRole,
    ColumnRef,
)
from .sql_compiler import SQLCompileError, compile_query_spec
from .write_policy import apply_node_writes


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _trace_node(
    store: InMemoryCheckpointStore,
    node_name: str,
    state_before: InvestigationState,
    state_after: InvestigationState,
) -> None:
    input_ref = store.persist_state(state_before.run_id, f"{node_name}:input", state_before)
    output_ref = store.persist_state(state_after.run_id, f"{node_name}:output", state_after)
    store.persist_trace(
        run_id=state_after.run_id,
        node_name=node_name,
        input_state_ref=input_ref,
        output_state_ref=output_ref,
        context_manifest_version=f"{node_name}_v1",
    )


def _close_node(
    store: InMemoryCheckpointStore,
    node_name: str,
    state_before: InvestigationState,
    state_after: InvestigationState,
) -> InvestigationState:
    finalized = _leave_node(state_after, node_name)
    _trace_node(store, node_name, state_before, finalized)
    return finalized


def _enter_node(state: InvestigationState, node_name: str) -> InvestigationState:
    execution = state.execution.model_copy(update={"current_node": node_name})
    return state.model_copy(update={"execution": execution})


def _leave_node(state: InvestigationState, node_name: str) -> InvestigationState:
    completed = list(state.execution.completed_nodes)
    if node_name not in completed:
        completed.append(node_name)
    execution = state.execution.model_copy(update={"current_node": node_name, "completed_nodes": completed})
    return state.model_copy(update={"execution": execution})


def _extract_days(prompt: str) -> int:
    text = (prompt or "").lower()
    if "yesterday" in text:
        return 1
    if "last week" in text or "this week" in text:
        return 7
    match = re.search(r"last\s+(\d+)\s+days?", text)
    if match:
        return max(1, min(int(match.group(1)), 90))
    return 30


def _window_pair(now: datetime, days: int) -> tuple[TimeWindow, TimeWindow]:
    current_end = now
    current_start = now - timedelta(days=days)
    baseline_end = current_start
    baseline_start = baseline_end - timedelta(days=days)
    return (
        TimeWindow(start=current_start, end=current_end),
        TimeWindow(start=baseline_start, end=baseline_end),
    )


def _derive_task_type(prompt: str) -> TaskType:
    text = (prompt or "").lower()
    if "chargeback" in text or "dispute" in text:
        return TaskType.chargeback_review
    if "priorit" in text or "which merchant" in text or "what should we do first" in text:
        return TaskType.prioritization
    if any(token in text for token in ("why", "drop", "fall", "increase", "leak", "failing", "failure")):
        return TaskType.root_cause_analysis
    return TaskType.metric_explanation


def _derive_metric(prompt: str) -> str:
    text = (prompt or "").lower()
    if "success rate" in text:
        return "success_rate"
    if "gmv" in text or "revenue" in text:
        return "failed_gmv"
    if "chargeback" in text:
        return "chargebacks"
    return "failed_attempts"


def _clarification_for_prompt(prompt: str) -> ClarificationRequest | None:
    text = re.sub(r"\s+", " ", str(prompt or "").strip().lower())
    if len(text.split()) >= 4 and any(token in text for token in ("fail", "failure", "success", "gmv", "revenue", "chargeback", "terminal", "payment")):
        return None
    return ClarificationRequest(
        question="Which metric should I investigate, and over what time window?",
        reason="The preview runtime needs a specific payments metric to build a plan.",
        choices=[
            "Why did failures increase in the last 30 days?",
            "Where is revenue leaking this week?",
            "Which terminals are driving failed GMV?",
        ],
    )


def _build_intent(prompt: str, merchant_id: str, now: datetime) -> tuple[InvestigationIntent, float, ClarificationRequest | None]:
    days = _extract_days(prompt)
    current_window, baseline_window = _window_pair(now, days)
    clarification = _clarification_for_prompt(prompt)
    intent = InvestigationIntent(
        task_type=_derive_task_type(prompt),
        metric=_derive_metric(prompt),
        current_window=current_window,
        baseline_window=baseline_window,
        compare_required=True,
        dimensions_to_check=["response_code", "payment_mode", "terminal_id"],
        entity_scope={"merchant_id": merchant_id},
    )
    return intent, (0.42 if clarification else 0.86), clarification


def _hypotheses_for_prompt(prompt: str) -> list[dict[str, Any]]:
    text = (prompt or "").lower()
    hypotheses = [
        {
            "hypothesis_id": "hyp_response_codes",
            "driver_type": "response_code_concentration",
            "statement": "A concentrated response-code cluster is driving most of the failure delta.",
            "priority": 1,
            "falsification_criteria": ["Response-code breakdown shows diffuse failures with no dominant code."],
        },
        {
            "hypothesis_id": "hyp_terminal_concentration",
            "driver_type": "terminal_concentration",
            "statement": "A small number of terminals account for a disproportionate share of failures.",
            "priority": 2,
            "falsification_criteria": ["Failure distribution is flat across terminals."],
        },
        {
            "hypothesis_id": "hyp_payment_mode_mix",
            "driver_type": "payment_mode_mix_shift",
            "statement": "Payment mode mix shifted toward a weaker-performing mode.",
            "priority": 3,
            "falsification_criteria": ["Payment mode failure mix is stable versus baseline."],
        },
    ]
    if "chargeback" in text:
        hypotheses[0]["driver_type"] = "chargeback_pressure"
        hypotheses[0]["statement"] = "Chargeback concentration is driving the current operational issue."
    return hypotheses


def _build_plan(prompt: str) -> InvestigationPlan:
    return InvestigationPlan(
        hypotheses=_hypotheses_for_prompt(prompt),
        required_evidence=[
            EvidenceRequirement(requirement_id="req_current_overview", name="current_overview", description="Current window overview", critical=True, tags=["overview"]),
            EvidenceRequirement(requirement_id="req_baseline_overview", name="baseline_overview", description="Baseline overview", critical=True, tags=["overview"]),
            EvidenceRequirement(requirement_id="req_response_code_breakdown", name="response_code_breakdown", description="Top failure response codes", critical=False, tags=["breakdown"]),
            EvidenceRequirement(requirement_id="req_terminal_breakdown", name="terminal_breakdown", description="Top failing terminals", critical=False, tags=["breakdown"]),
            EvidenceRequirement(requirement_id="req_payment_mode_breakdown", name="payment_mode_breakdown", description="Payment mode failure mix", critical=False, tags=["breakdown"]),
        ],
        steps=[
            PlanStep(step_id="step_overview", node_name="resolve_data_requirements", purpose="Map core overview queries"),
            PlanStep(step_id="step_collect", node_name="collect_evidence", purpose="Collect overview and breakdown evidence"),
            PlanStep(step_id="step_diagnosis", node_name="synthesize_diagnosis", purpose="Rank likely drivers"),
        ],
        max_tool_calls=5,
        max_replans=1,
        stop_conditions=StopPolicy(),
    )


def _merchant_filters(merchant_id: str, window: TimeWindow) -> list[FilterSpec]:
    return [
        FilterSpec(column=ColumnRef(table_alias="t0", column="merchant_id"), op=FilterOp.eq, value=merchant_id),
        FilterSpec(column=ColumnRef(table_alias="t0", column="p_date"), op=FilterOp.between, value=window.start, value_to=window.end),
    ]


def _overview_query(query_id: str, merchant_id: str, window: TimeWindow) -> QuerySpec:
    return QuerySpec(
        query_id=query_id,
        shape=QueryShape.aggregate,
        base_table=TableRef(name="transaction_features", alias="t0"),
        metrics=[
            MetricSpec(name="total_attempts", alias="total_attempts", semantic_metric_id="total_attempts"),
            MetricSpec(name="failed_attempts", alias="failed_attempts", semantic_metric_id="failed_attempts"),
            MetricSpec(name="failed_gmv", alias="failed_gmv", semantic_metric_id="failed_gmv"),
            MetricSpec(name="success_rate", alias="success_rate", semantic_metric_id="success_rate"),
        ],
        filters=_merchant_filters(merchant_id, window),
        limit=1,
        time_column=ColumnRef(table_alias="t0", column="p_date"),
    )


def _breakdown_query(query_id: str, merchant_id: str, window: TimeWindow, dimension: str) -> QuerySpec:
    return QuerySpec(
        query_id=query_id,
        shape=QueryShape.breakdown,
        base_table=TableRef(name="transaction_features", alias="t0"),
        metrics=[
            MetricSpec(name="failed_attempts", alias="failed_attempts", semantic_metric_id="failed_attempts"),
            MetricSpec(name="failed_gmv", alias="failed_gmv", semantic_metric_id="failed_gmv"),
        ],
        dimensions=[ColumnRef(table_alias="t0", column=dimension)],
        filters=_merchant_filters(merchant_id, window),
        order_by=[OrderBySpec(target_kind=OrderTargetKind.metric_alias, target="failed_attempts")],
        limit=5,
        time_column=ColumnRef(table_alias="t0", column="p_date"),
    )


def _resolve_queries(prompt: str, merchant_id: str, intent: InvestigationIntent) -> tuple[list[QuerySpec], list[MissingDataItem]]:
    query_specs = [
        _overview_query("current_overview", merchant_id, intent.current_window),
        _overview_query("baseline_overview", merchant_id, intent.baseline_window or intent.current_window),
        _breakdown_query("response_code_breakdown", merchant_id, intent.current_window, "response_code"),
        _breakdown_query("terminal_breakdown", merchant_id, intent.current_window, "terminal_id"),
        _breakdown_query("payment_mode_breakdown", merchant_id, intent.current_window, "payment_mode"),
    ]
    missing_data: list[MissingDataItem] = []
    if "issuer" in (prompt or "").lower():
        missing_data.append(
            MissingDataItem(
                key="issuer_dimension_unavailable",
                description="Issuer-level diagnosis is not available in the local preview schema.",
                severity=MissingDataSeverity.non_critical,
                source="transaction_features",
            )
        )
    return query_specs, missing_data


def _execute_compiled_query(engine: Engine, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    connection = engine.raw_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params)
        if cursor.description is None:
            return []
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        connection.close()


def _grain_for_query(query_id: str) -> Grain:
    if "terminal" in query_id:
        return Grain.terminal
    if "payment_mode" in query_id:
        return Grain.payment_mode
    if "response_code" in query_id:
        return Grain.day
    return Grain.merchant


def _window_for_query(query_id: str, intent: InvestigationIntent) -> TimeWindow:
    if query_id == "baseline_overview" and intent.baseline_window is not None:
        return intent.baseline_window
    return intent.current_window


def _bundle_summary(query_id: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return f"{query_id} returned no rows."
    if len(rows) == 1:
        row = rows[0]
        highlights = []
        for key in ("failed_attempts", "failed_gmv", "success_rate", "total_attempts"):
            if key in row:
                highlights.append(f"{key}={row[key]}")
        return f"{query_id}: " + ", ".join(highlights[:4])
    first = rows[0]
    first_key = next(iter(first.keys()), "dimension")
    return f"{query_id}: top slice {first.get(first_key)} with {first.get('failed_attempts', 0)} failed attempts."


def _bundle_facts(rows: list[dict[str, Any]]) -> list[Fact]:
    if not rows:
        return []
    first = rows[0]
    return [
        Fact(name=str(key), value=value)
        for key, value in first.items()
        if isinstance(value, (str, int, float, bool)) and value is not None
    ][:8]


def _make_bundle(query_id: str, rows: list[dict[str, Any]], tables_used: list[str], intent: InvestigationIntent) -> EvidenceBundle:
    return EvidenceBundle(
        evidence_id=query_id,
        source_type=EvidenceSourceType.sql,
        source_ref=query_id,
        tags=[query_id],
        grain=_grain_for_query(query_id),
        window=_window_for_query(query_id, intent),
        facts=_bundle_facts(rows),
        summary=_bundle_summary(query_id, rows),
        quality=EvidenceQuality(
            completeness=1.0 if rows else 0.0,
            freshness=1.0,
            conflict_risk=0.1 if rows else 0.4,
        ),
        provenance=EvidenceProvenance(query_id=query_id, table_names=tables_used, generated_at=_utcnow()),
    )


def _first_row(rows_by_query: dict[str, list[dict[str, Any]]], query_id: str) -> dict[str, Any]:
    rows = rows_by_query.get(query_id) or []
    return rows[0] if rows else {}


def _pct_delta(current_value: float, baseline_value: float) -> float | None:
    if baseline_value == 0:
        return None
    return ((current_value - baseline_value) / baseline_value) * 100.0


def _grade_evidence(state: InvestigationState, rows_by_query: dict[str, list[dict[str, Any]]]) -> tuple[float, float, list[ConflictItem], bool]:
    required = state.plan.required_evidence if state.plan else []
    available = 0
    for requirement in required:
        if rows_by_query.get(requirement.name):
            available += 1
    coverage = available / len(required) if required else 1.0
    current_exists = bool(rows_by_query.get("current_overview"))
    baseline_exists = bool(rows_by_query.get("baseline_overview"))
    consistency = 1.0 if current_exists and baseline_exists else 0.6
    conflicts: list[ConflictItem] = []
    if current_exists:
        current_row = _first_row(rows_by_query, "current_overview")
        failed_attempts = float(current_row.get("failed_attempts") or 0)
        total_attempts = float(current_row.get("total_attempts") or 0)
        if failed_attempts > total_attempts:
            conflicts.append(
                ConflictItem(
                    key="failed_attempts_exceed_total",
                    description="Failed attempts exceed total attempts in current overview.",
                    severity=ConflictSeverity.blocking,
                    evidence_ids=["current_overview"],
                )
            )
    proceed = current_exists and not any(conflict.severity == ConflictSeverity.blocking for conflict in conflicts)
    return coverage, consistency, conflicts, proceed


def _build_diagnosis(rows_by_query: dict[str, list[dict[str, Any]]]) -> Diagnosis:
    current = _first_row(rows_by_query, "current_overview")
    baseline = _first_row(rows_by_query, "baseline_overview")
    response_rows = rows_by_query.get("response_code_breakdown") or []
    terminal_rows = rows_by_query.get("terminal_breakdown") or []
    payment_mode_rows = rows_by_query.get("payment_mode_breakdown") or []

    top_dimension = None
    top_row: dict[str, Any] = {}
    driver_type = "response_code_concentration"
    evidence_ids = ["current_overview", "baseline_overview"]

    if response_rows:
        top_row = response_rows[0]
        top_dimension = f"response code {top_row.get('response_code') or 'unknown'}"
        evidence_ids.append("response_code_breakdown")
    elif terminal_rows:
        top_row = terminal_rows[0]
        top_dimension = f"terminal {top_row.get('terminal_id') or 'unknown'}"
        driver_type = "terminal_concentration"
        evidence_ids.append("terminal_breakdown")
    elif payment_mode_rows:
        top_row = payment_mode_rows[0]
        top_dimension = f"payment mode {top_row.get('payment_mode') or 'unknown'}"
        driver_type = "payment_mode_mix_shift"
        evidence_ids.append("payment_mode_breakdown")

    current_failed = float(current.get("failed_attempts") or 0)
    baseline_failed = float(baseline.get("failed_attempts") or 0)
    current_failed_gmv = float(current.get("failed_gmv") or 0)
    baseline_failed_gmv = float(baseline.get("failed_gmv") or 0)

    confidence = 0.82 if response_rows else 0.68 if top_dimension else 0.45
    explanation_quality = ExplanationQuality.strong if response_rows else ExplanationQuality.moderate if top_dimension else ExplanationQuality.weak

    driver = DriverAssessment(
        driver_type=driver_type,
        impact_estimate=DriverImpact(
            attempts_delta=int(current_failed - baseline_failed),
            gmv_delta=current_failed_gmv - baseline_failed_gmv,
        ),
        evidence_ids=evidence_ids,
        confidence=confidence,
        support_level=SupportLevel.correlative,
        falsifiers_considered=["No dominant slice in the current breakdowns."],
    )

    if top_dimension:
        driver.falsifiers_considered.append(f"Top visible driver is {top_dimension}.")

    return Diagnosis(
        ranked_drivers=[driver],
        confidence=confidence,
        explanation_quality=explanation_quality,
    )


def _map_recommendations(diagnosis: Diagnosis, action_level: ActionLevel, run_id: str) -> tuple[list[Recommendation], ApprovalRequest | None]:
    if not diagnosis.ranked_drivers:
        return [], None

    top_driver = diagnosis.ranked_drivers[0]
    action = "Review the dominant failure cluster and investigate the top failing terminals."
    owner = "ops"
    urgency = "high"

    if top_driver.driver_type == "response_code_concentration":
        action = "Review the dominant response-code cluster and escalate with the bank or acquirer if it is timeout-heavy."
        owner = "bank"
    elif top_driver.driver_type == "terminal_concentration":
        action = "Inspect or temporarily suppress the worst-performing terminals."
        owner = "ops"
    elif top_driver.driver_type == "payment_mode_mix_shift":
        action = "Review payment mode mix and steer traffic toward the healthier mode where possible."
        owner = "growth"

    recommendation = Recommendation(
        recommendation_id="rec_001",
        action=action,
        owner=owner,
        urgency=urgency,
        rationale="Mapped from the top-ranked preview diagnosis.",
        expected_benefit="Reduce failed attempts and recover a portion of failed GMV.",
        risk_caveat="Preview recommendations are based on current local schema coverage, not the full semantic layer.",
        evidence_ids=top_driver.evidence_ids,
        approval_required=action_level != ActionLevel.read_only,
    )

    if action_level == ActionLevel.read_only:
        return [recommendation], None

    approval_request = ApprovalRequest(
        approval_id=f"approval_{uuid.uuid4().hex[:10]}",
        requested_by_run=run_id,
        action_type="draft_bank_escalation",
        payload_summary=recommendation.action,
        risk_level="medium",
        expires_at=_utcnow() + timedelta(hours=24),
    )
    return [recommendation], approval_request


def _compose_response(
    prompt: str,
    state: InvestigationState,
    rows_by_query: dict[str, list[dict[str, Any]]],
) -> ComposeResponseOutput:
    clarification = state.runtime_control.clarification_request
    if clarification is not None:
        return ComposeResponseOutput(
            response=UserResponse(
                executive_summary=clarification.question,
                findings=[],
                caveats=[clarification.reason],
                evidence_ids=[],
            )
        )

    diagnosis = state.diagnosis or Diagnosis(ranked_drivers=[], confidence=0.0, explanation_quality=ExplanationQuality.weak)
    current = _first_row(rows_by_query, "current_overview")
    baseline = _first_row(rows_by_query, "baseline_overview")

    current_failed = float(current.get("failed_attempts") or 0)
    baseline_failed = float(baseline.get("failed_attempts") or 0)
    current_failed_gmv = float(current.get("failed_gmv") or 0)
    delta_pct = _pct_delta(current_failed, baseline_failed)
    delta_text = "from a zero or missing baseline" if delta_pct is None else f"{delta_pct:.1f}% versus the prior window"

    top_driver = diagnosis.ranked_drivers[0] if diagnosis.ranked_drivers else None
    driver_text = "No dominant driver was visible in the current preview data."
    evidence_ids = ["current_overview", "baseline_overview"]
    if top_driver is not None:
        driver_text = f"The strongest visible driver is {top_driver.driver_type.replace('_', ' ')}."
        evidence_ids = list(dict.fromkeys(top_driver.evidence_ids))

    findings = [
        Finding(
            title="Failure delta",
            summary=f"Failed attempts are {current_failed:.0f} in the current window, {delta_text}.",
            evidence_ids=["current_overview", "baseline_overview"],
        )
    ]
    if top_driver is not None:
        findings.append(
            Finding(
                title="Top visible driver",
                summary=driver_text,
                evidence_ids=top_driver.evidence_ids,
            )
        )

    caveats = [
        "This preview runtime uses deterministic parsing and the local demo schema, not the final semantic layer.",
    ]
    for missing in state.evidence_store.missing_data:
        caveats.append(missing.description)

    summary = (
        f"Preview result for '{prompt}': failed attempts are {current_failed:.0f} {delta_text}. "
        f"{driver_text} Current failed GMV is approximately ₹{current_failed_gmv:,.0f}."
    )

    return ComposeResponseOutput(
        response=UserResponse(
            executive_summary=summary,
            findings=findings,
            caveats=caveats,
            evidence_ids=evidence_ids,
        )
    )


def run_preview_turn(
    engine: Engine | None,
    *,
    merchant_id: str,
    prompt: str,
    user_role: UserRole = UserRole.ops,
    requested_action_level: ActionLevel = ActionLevel.read_only,
) -> dict[str, Any]:
    now = _utcnow()
    store = InMemoryCheckpointStore()
    rows_by_query: dict[str, list[dict[str, Any]]] = {}
    coverage_score = 0.0
    consistency_score = 0.0

    state = InvestigationState(
        run_id=f"rr_{uuid.uuid4().hex[:12]}",
        user_id="preview_user",
        session_id=f"preview_{uuid.uuid4().hex[:8]}",
        user_question=prompt,
        user_role=user_role,
        requested_action_level=requested_action_level,
        context_budget=ContextBudget(),
        evidence_store=EvidenceStore(),
        execution=ExecutionState(current_node="initialize_run", status=RunStatus.running),
        checkpoint=CheckpointState(last_persisted_at=now),
    )
    state = _close_node(store, "initialize_run", state, state)

    node_name = "parse_intent"
    before = _enter_node(state, node_name)
    intent, parse_confidence, clarification = _build_intent(prompt, merchant_id, now)
    after = apply_node_writes(
        before,
        node_name,
        {
            "intent": intent,
            "runtime_control.parse_confidence": parse_confidence,
            "runtime_control.clarification_needed": clarification is not None,
            "runtime_control.clarification_request": clarification,
        },
    )
    state = _close_node(store, node_name, before, after)

    node_name = "clarify_or_continue"
    before = _enter_node(state, node_name)
    state = _close_node(store, node_name, before, before)

    if state.runtime_control.clarification_needed:
        node_name = "compose_response"
        before = _enter_node(state, node_name)
        composed = _compose_response(prompt, before, rows_by_query)
        after = apply_node_writes(before, node_name, {"response": composed.response})
        state = _close_node(store, node_name, before, after)
    else:
        node_name = "build_initial_plan"
        before = _enter_node(state, node_name)
        after = apply_node_writes(before, node_name, {"plan": _build_plan(prompt)})
        state = _close_node(store, node_name, before, after)

        node_name = "resolve_data_requirements"
        before = _enter_node(state, node_name)
        query_specs, unresolved = _resolve_queries(prompt, merchant_id, state.intent)
        compiled_queries = [compile_query_spec(spec) for spec in query_specs]
        store.persist_query_specs(state.run_id, node_name, query_specs)
        after = apply_node_writes(
            before,
            node_name,
            {
                "query_specs": query_specs,
                "compiled_queries": compiled_queries,
                "evidence_store.missing_data": list(before.evidence_store.missing_data) + unresolved,
            },
        )
        state = _close_node(store, node_name, before, after)

        node_name = "collect_evidence"
        before = _enter_node(state, node_name)
        bundles: list[EvidenceBundle] = []
        missing_data = list(before.evidence_store.missing_data)
        if engine is None:
            raise ValueError("Preview runtime requires a database engine for evidence collection")
        for compiled in before.compiled_queries:
            try:
                rows = _execute_compiled_query(engine, compiled.sql, compiled.params)
                rows_by_query[compiled.query_id] = rows
                if rows:
                    bundles.append(_make_bundle(compiled.query_id, rows, compiled.tables_used, before.intent))
                else:
                    missing_data.append(
                        MissingDataItem(
                            key=f"{compiled.query_id}_empty",
                            description=f"{compiled.query_id} returned no rows for the selected merchant window.",
                            severity=MissingDataSeverity.critical if "overview" in compiled.query_id else MissingDataSeverity.non_critical,
                            source=compiled.query_id,
                        )
                    )
            except SQLCompileError as exc:
                missing_data.append(
                    MissingDataItem(
                        key=f"{compiled.query_id}_compile_error",
                        description=str(exc),
                        severity=MissingDataSeverity.critical,
                        source=compiled.query_id,
                    )
                )
            except Exception as exc:  # pragma: no cover - exercised via live preview
                missing_data.append(
                    MissingDataItem(
                        key=f"{compiled.query_id}_execution_error",
                        description=str(exc),
                        severity=MissingDataSeverity.critical if "overview" in compiled.query_id else MissingDataSeverity.non_critical,
                        source=compiled.query_id,
                    )
                )
        after = apply_node_writes(
            before,
            node_name,
            {
                "evidence_store.bundles": bundles,
                "evidence_store.missing_data": missing_data,
            },
        )
        state = _close_node(store, node_name, before, after)

        node_name = "grade_evidence"
        before = _enter_node(state, node_name)
        coverage_score, consistency_score, conflicts, proceed = _grade_evidence(before, rows_by_query)
        after = apply_node_writes(before, node_name, {"evidence_store.conflicts": conflicts})
        state = _close_node(store, node_name, before, after)

        node_name = "replan_if_needed"
        before = _enter_node(state, node_name)
        replan_action = ReplanAction.proceed if proceed else ReplanAction.stop_insufficient_evidence
        stop_reason = None if proceed else "Critical overview evidence was unavailable in the preview runtime."
        after = apply_node_writes(
            before,
            node_name,
            {
                "runtime_control.last_replan_action": replan_action,
                "runtime_control.stop_reason": stop_reason,
            },
        )
        state = _close_node(store, node_name, before, after)

        if state.runtime_control.last_replan_action == ReplanAction.proceed:
            node_name = "synthesize_diagnosis"
            before = _enter_node(state, node_name)
            diagnosis = _build_diagnosis(rows_by_query)
            after = apply_node_writes(before, node_name, {"diagnosis": diagnosis})
            state = _close_node(store, node_name, before, after)

            node_name = "map_recommendations"
            before = _enter_node(state, node_name)
            recommendations, approval_request = _map_recommendations(
                before.diagnosis,
                before.requested_action_level,
                before.run_id,
            )
            after = apply_node_writes(
                before,
                node_name,
                {
                    "recommendations": recommendations,
                    "runtime_control.approval_required": approval_request is not None,
                    "runtime_control.approval_reason": "Operational action requested." if approval_request else None,
                    "checkpoint.approval_request": approval_request,
                },
            )
            state = _close_node(store, node_name, before, after)

            node_name = "approval_gate"
            before = _enter_node(state, node_name)
            next_status = RunStatus.waiting_for_approval if state.runtime_control.approval_required else state.execution.status
            after = apply_node_writes(
                before,
                node_name,
                {
                    "execution.status": next_status,
                    "checkpoint.resumable_from_node": "approval_gate" if next_status == RunStatus.waiting_for_approval else None,
                },
            )
            state = _close_node(store, node_name, before, after)

        node_name = "compose_response"
        before = _enter_node(state, node_name)
        composed = _compose_response(prompt, before, rows_by_query)
        after = apply_node_writes(before, node_name, {"response": composed.response})
        state = _close_node(store, node_name, before, after)

    node_name = "checkpoint_and_finish"
    before = _enter_node(state, node_name)
    final_status = RunStatus.completed if before.execution.status == RunStatus.running else before.execution.status
    after = apply_node_writes(
        before,
        node_name,
        {
            "checkpoint": before.checkpoint.model_copy(update={"last_persisted_at": _utcnow(), "resumable_from_node": before.checkpoint.resumable_from_node}),
            "execution.status": final_status,
        },
    )
    state = _close_node(store, node_name, before, after)

    return {
        "run_id": state.run_id,
        "merchant_id": merchant_id,
        "prompt": prompt,
        "answer": state.response.executive_summary if state.response else "",
        "status": state.execution.status.value,
        "response": state.response.model_dump(mode="python") if state.response else None,
        "clarification_request": state.runtime_control.clarification_request.model_dump(mode="python") if state.runtime_control.clarification_request else None,
        "coverage_score": coverage_score,
        "consistency_score": consistency_score,
        "state": state.model_dump(mode="python"),
        "traces": [trace.model_dump(mode="python") for trace in store.list_traces(state.run_id)],
    }

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Dict, List, Literal, Optional

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator


def _validate_aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime values must be timezone-aware")
    return value


AwareDatetime = Annotated[datetime, AfterValidator(_validate_aware_datetime)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class UserRole(str, Enum):
    ops = "ops"
    growth = "growth"
    support = "support"
    admin = "admin"


class ActionLevel(str, Enum):
    read_only = "read_only"
    draft_operational = "draft_operational"
    approval_required = "approval_required"


class TaskType(str, Enum):
    root_cause_analysis = "root_cause_analysis"
    metric_explanation = "metric_explanation"
    anomaly_investigation = "anomaly_investigation"
    prioritization = "prioritization"
    chargeback_review = "chargeback_review"


class RunStatus(str, Enum):
    running = "running"
    waiting_for_approval = "waiting_for_approval"
    waiting_for_resume = "waiting_for_resume"
    completed = "completed"
    failed = "failed"
    stopped_insufficient_evidence = "stopped_insufficient_evidence"


class SupportLevel(str, Enum):
    direct = "direct"
    indirect = "indirect"
    correlative = "correlative"


class ExplanationQuality(str, Enum):
    strong = "strong"
    moderate = "moderate"
    weak = "weak"


class EvidenceSourceType(str, Enum):
    sql = "sql"
    semantic_metric = "semantic_metric"
    lookup = "lookup"
    health_log = "health_log"


class Grain(str, Enum):
    merchant = "merchant"
    terminal = "terminal"
    issuer = "issuer"
    payment_mode = "payment_mode"
    day = "day"
    txn = "txn"


class QueryShape(str, Enum):
    aggregate = "aggregate"
    breakdown = "breakdown"
    timeseries = "timeseries"
    topk = "topk"
    detail = "detail"


class JoinType(str, Enum):
    inner = "INNER"
    left = "LEFT"


class FilterOp(str, Enum):
    eq = "="
    ne = "!="
    gt = ">"
    gte = ">="
    lt = "<"
    lte = "<="
    in_ = "IN"
    not_in = "NOT IN"
    between = "BETWEEN"
    like = "LIKE"
    is_null = "IS NULL"
    is_not_null = "IS NOT NULL"


class AggregationOp(str, Enum):
    sum = "SUM"
    count = "COUNT"
    count_distinct = "COUNT_DISTINCT"
    avg = "AVG"
    min = "MIN"
    max = "MAX"


class OrderDirection(str, Enum):
    asc = "ASC"
    desc = "DESC"


class OrderTargetKind(str, Enum):
    metric_alias = "metric_alias"
    dimension = "dimension"


class MissingDataSeverity(str, Enum):
    critical = "critical"
    non_critical = "non_critical"


class ConflictSeverity(str, Enum):
    blocking = "blocking"
    non_blocking = "non_blocking"


class ApprovalActionType(str, Enum):
    draft_bank_escalation = "draft_bank_escalation"
    draft_terminal_intervention_queue = "draft_terminal_intervention_queue"
    draft_merchant_alert = "draft_merchant_alert"
    trigger_ticket_creation = "trigger_ticket_creation"


class ReplanAction(str, Enum):
    proceed = "proceed"
    replan = "replan"
    stop_insufficient_evidence = "stop_insufficient_evidence"


class TimeWindow(StrictModel):
    start: AwareDatetime
    end: AwareDatetime

    @field_validator("end")
    @classmethod
    def validate_window(cls, value: datetime, info):
        start = info.data.get("start")
        if start and value <= start:
            raise ValueError("end must be after start")
        return value


class EntityScope(StrictModel):
    merchant_id: Optional[str] = None
    tids: List[str] = Field(default_factory=list)
    issuers: List[str] = Field(default_factory=list)
    payment_modes: List[str] = Field(default_factory=list)


class Hypothesis(StrictModel):
    hypothesis_id: str
    driver_type: str
    statement: str
    priority: int = Field(ge=1, le=10)
    falsification_criteria: List[str] = Field(default_factory=list)
    status: Literal["active", "rejected", "stale", "confirmed"] = "active"


class EvidenceRequirement(StrictModel):
    requirement_id: str
    name: str
    description: str
    critical: bool = True
    tags: List[str] = Field(default_factory=list)


class PlanStep(StrictModel):
    step_id: str
    node_name: str
    purpose: str
    consumes: List[str] = Field(default_factory=list)
    produces: List[str] = Field(default_factory=list)


class StopPolicy(StrictModel):
    min_coverage_score: float = Field(default=0.80, ge=0.0, le=1.0)
    min_consistency_score: float = Field(default=0.90, ge=0.0, le=1.0)
    min_top_driver_confidence: float = Field(default=0.70, ge=0.0, le=1.0)
    max_tool_calls: int = Field(default=12, ge=1)
    max_replans: int = Field(default=2, ge=0)
    min_replan_gain: float = Field(default=0.10, ge=0.0, le=1.0)


class ContextBudget(StrictModel):
    max_prompt_tokens_per_node: int = Field(default=4000, ge=256)
    max_rows_per_result: int = Field(default=200, ge=1)
    max_tables_per_step: int = Field(default=3, ge=1)
    max_evidence_bundles_per_node: int = Field(default=8, ge=1)


class Fact(StrictModel):
    name: str
    value: str | int | float | bool
    unit: Optional[str] = None


class MissingDataItem(StrictModel):
    key: str
    description: str
    severity: MissingDataSeverity
    source: Optional[str] = None


class ConflictItem(StrictModel):
    key: str
    description: str
    severity: ConflictSeverity
    evidence_ids: List[str] = Field(default_factory=list)


class EvidenceProvenance(StrictModel):
    query_id: Optional[str] = None
    table_names: List[str] = Field(default_factory=list)
    generated_at: AwareDatetime


class EvidenceQuality(StrictModel):
    completeness: float = Field(ge=0.0, le=1.0)
    freshness: float = Field(ge=0.0, le=1.0)
    conflict_risk: float = Field(ge=0.0, le=1.0)


class EvidenceBundle(StrictModel):
    evidence_id: str
    source_type: EvidenceSourceType
    source_ref: str
    tags: List[str] = Field(default_factory=list)
    grain: Grain
    window: TimeWindow
    facts: List[Fact] = Field(default_factory=list)
    summary: str
    quality: EvidenceQuality
    provenance: EvidenceProvenance


class ToolCallRecord(StrictModel):
    tool_name: str
    started_at: AwareDatetime
    finished_at: Optional[AwareDatetime] = None
    status: Literal["ok", "error", "timeout"]
    request_ref: Optional[str] = None
    response_ref: Optional[str] = None
    error_message: Optional[str] = None


class LLMCallRecord(StrictModel):
    node_name: str
    started_at: AwareDatetime
    finished_at: Optional[AwareDatetime] = None
    prompt_template_version: str
    context_manifest_version: str
    input_ref: Optional[str] = None
    output_ref: Optional[str] = None
    token_in: Optional[int] = None
    token_out: Optional[int] = None


class DriverImpact(StrictModel):
    attempts_delta: Optional[int] = None
    gmv_delta: Optional[float] = None
    success_rate_delta_pct_points: Optional[float] = None


class DriverAssessment(StrictModel):
    driver_type: str
    impact_estimate: DriverImpact
    evidence_ids: List[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    support_level: SupportLevel
    falsifiers_considered: List[str] = Field(default_factory=list)


class Finding(StrictModel):
    title: str
    summary: str
    evidence_ids: List[str] = Field(default_factory=list)


class Recommendation(StrictModel):
    recommendation_id: str
    action: str
    owner: Literal["merchant", "bank", "ops", "support", "growth"]
    urgency: Literal["low", "medium", "high"]
    rationale: str
    expected_benefit: str
    risk_caveat: Optional[str] = None
    evidence_ids: List[str] = Field(default_factory=list)
    approval_required: bool = False


class Diagnosis(StrictModel):
    ranked_drivers: List[DriverAssessment] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    explanation_quality: ExplanationQuality


class UserResponse(StrictModel):
    executive_summary: str
    findings: List[Finding] = Field(default_factory=list)
    caveats: List[str] = Field(default_factory=list)
    evidence_ids: List[str] = Field(default_factory=list)


class ClarificationRequest(StrictModel):
    question: str
    reason: str
    choices: List[str] = Field(default_factory=list)


class ApprovalRequest(StrictModel):
    approval_id: str
    requested_by_run: str
    action_type: ApprovalActionType
    payload_summary: str
    risk_level: Literal["low", "medium", "high"]
    expires_at: AwareDatetime


class CheckpointState(StrictModel):
    last_persisted_at: AwareDatetime
    resumable_from_node: Optional[str] = None
    approval_request: Optional[ApprovalRequest] = None


class ExecutionState(StrictModel):
    current_node: str
    completed_nodes: List[str] = Field(default_factory=list)
    tool_calls: List[ToolCallRecord] = Field(default_factory=list)
    llm_calls: List[LLMCallRecord] = Field(default_factory=list)
    replan_count: int = Field(default=0, ge=0)
    status: RunStatus = RunStatus.running


class InvestigationIntent(StrictModel):
    task_type: TaskType
    metric: Optional[str] = None
    entity_scope: EntityScope = Field(default_factory=EntityScope)
    current_window: TimeWindow
    baseline_window: Optional[TimeWindow] = None
    dimensions_to_check: List[str] = Field(default_factory=list)
    compare_required: bool = True


class TableRef(StrictModel):
    name: str
    alias: Optional[str] = None


class ColumnRef(StrictModel):
    table_alias: Optional[str] = None
    column: str


class MetricSpec(StrictModel):
    name: str
    alias: str
    semantic_metric_id: Optional[str] = None
    aggregation: Optional[AggregationOp] = None
    column: Optional[ColumnRef] = None

    @model_validator(mode="after")
    def validate_metric_spec(self) -> "MetricSpec":
        has_semantic_metric = bool(self.semantic_metric_id)
        has_aggregate = self.aggregation is not None or self.column is not None
        if has_semantic_metric and has_aggregate:
            raise ValueError("MetricSpec must use either semantic_metric_id or aggregation+column, not both")
        if not has_semantic_metric and not (self.aggregation and self.column):
            raise ValueError("MetricSpec requires semantic_metric_id or aggregation+column")
        return self


class FilterSpec(StrictModel):
    column: ColumnRef
    op: FilterOp
    value: Optional[Any] = None
    value_to: Optional[Any] = None


class JoinSpec(StrictModel):
    right_table: TableRef
    join_type: JoinType
    on_left: ColumnRef
    on_right: ColumnRef


class OrderBySpec(StrictModel):
    target_kind: OrderTargetKind
    target: str
    direction: OrderDirection = OrderDirection.desc


class QuerySpec(StrictModel):
    query_id: str
    shape: QueryShape
    base_table: TableRef
    joins: List[JoinSpec] = Field(default_factory=list)
    metrics: List[MetricSpec] = Field(default_factory=list)
    dimensions: List[ColumnRef] = Field(default_factory=list)
    filters: List[FilterSpec] = Field(default_factory=list)
    order_by: List[OrderBySpec] = Field(default_factory=list)
    limit: int = Field(default=100, ge=1, le=1000)
    time_column: Optional[ColumnRef] = None


class CompiledSQL(StrictModel):
    query_id: str
    sql: str
    params: List[Any] = Field(default_factory=list)
    tables_used: List[str] = Field(default_factory=list)
    estimated_shape: QueryShape


class SQLGuardrailResult(StrictModel):
    allowed: bool
    violations: List[str] = Field(default_factory=list)


class InvestigationPlan(StrictModel):
    hypotheses: List[Hypothesis] = Field(default_factory=list)
    required_evidence: List[EvidenceRequirement] = Field(default_factory=list)
    steps: List[PlanStep] = Field(default_factory=list)
    max_tool_calls: int = Field(default=12, ge=1)
    max_replans: int = Field(default=2, ge=0)
    stop_conditions: StopPolicy = Field(default_factory=StopPolicy)


class EvidenceStore(StrictModel):
    bundles: List[EvidenceBundle] = Field(default_factory=list)
    facts: List[Fact] = Field(default_factory=list)
    missing_data: List[MissingDataItem] = Field(default_factory=list)
    conflicts: List[ConflictItem] = Field(default_factory=list)


class RuntimeControl(StrictModel):
    parse_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    clarification_needed: bool = False
    clarification_request: Optional[ClarificationRequest] = None
    last_replan_action: Optional[ReplanAction] = None
    stop_reason: Optional[str] = None
    approval_required: bool = False
    approval_reason: Optional[str] = None


class InvestigationState(StrictModel):
    run_id: str
    parent_run_id: Optional[str] = None
    user_id: str
    session_id: str

    user_question: str
    user_role: UserRole
    requested_action_level: ActionLevel = ActionLevel.read_only

    intent: Optional[InvestigationIntent] = None
    plan: Optional[InvestigationPlan] = None
    query_specs: List[QuerySpec] = Field(default_factory=list)
    compiled_queries: List[CompiledSQL] = Field(default_factory=list)
    context_budget: ContextBudget = Field(default_factory=ContextBudget)
    evidence_store: EvidenceStore = Field(default_factory=EvidenceStore)
    execution: ExecutionState
    runtime_control: RuntimeControl = Field(default_factory=RuntimeControl)

    diagnosis: Optional[Diagnosis] = None
    recommendations: List[Recommendation] = Field(default_factory=list)
    response: Optional[UserResponse] = None

    checkpoint: CheckpointState


class InitializeRunInput(StrictModel):
    run_id: str
    user_id: str
    session_id: str
    user_question: str
    user_role: UserRole
    requested_action_level: ActionLevel = ActionLevel.read_only


class InitializeRunOutput(StrictModel):
    state: InvestigationState


class ParseIntentInput(StrictModel):
    user_question: str
    user_role: UserRole
    now: AwareDatetime


class ParseIntentOutput(StrictModel):
    intent: InvestigationIntent
    parse_confidence: float = Field(ge=0.0, le=1.0)
    clarification_needed: bool = False
    clarification_request: Optional[ClarificationRequest] = None


class BuildInitialPlanInput(StrictModel):
    intent: InvestigationIntent
    schema_semantics_ref: str
    tool_catalog_ref: str
    policy_constraints_ref: str


class BuildInitialPlanOutput(StrictModel):
    plan: InvestigationPlan


class ResolveDataRequirementsInput(StrictModel):
    intent: InvestigationIntent
    required_evidence: List[EvidenceRequirement]
    semantic_layer_ref: str


class ResolveDataRequirementsOutput(StrictModel):
    query_specs: List[QuerySpec] = Field(default_factory=list)
    unresolved_requirements: List[MissingDataItem] = Field(default_factory=list)


class CollectEvidenceInput(StrictModel):
    query_specs: List[QuerySpec]
    max_rows_per_result: int


class CollectEvidenceOutput(StrictModel):
    evidence_bundles: List[EvidenceBundle] = Field(default_factory=list)
    missing_data: List[MissingDataItem] = Field(default_factory=list)
    tool_errors: List[str] = Field(default_factory=list)


class GradeEvidenceInput(StrictModel):
    intent: InvestigationIntent
    required_evidence: List[EvidenceRequirement]
    evidence_bundles: List[EvidenceBundle]
    missing_data: List[MissingDataItem]
    conflicts: List[ConflictItem] = Field(default_factory=list)


class GradeEvidenceOutput(StrictModel):
    coverage_score: float = Field(ge=0.0, le=1.0)
    consistency_score: float = Field(ge=0.0, le=1.0)
    unresolved_conflicts: List[ConflictItem] = Field(default_factory=list)
    insufficient_areas: List[str] = Field(default_factory=list)
    proceed: bool
    replan_needed: bool


class ReplanInput(StrictModel):
    plan: InvestigationPlan
    grade: GradeEvidenceOutput
    missing_data: List[MissingDataItem] = Field(default_factory=list)
    conflicts: List[ConflictItem] = Field(default_factory=list)
    replan_count: int = Field(ge=0)


class ReplanOutput(StrictModel):
    action: ReplanAction
    updated_plan: Optional[InvestigationPlan] = None
    reason: str


class SynthesizeDiagnosisInput(StrictModel):
    intent: InvestigationIntent
    evidence_bundles: List[EvidenceBundle]
    conflicts: List[ConflictItem] = Field(default_factory=list)
    missing_data: List[MissingDataItem] = Field(default_factory=list)
    canonical_metric_definitions_ref: str


class SynthesizeDiagnosisOutput(StrictModel):
    diagnosis: Diagnosis


class MapRecommendationsInput(StrictModel):
    diagnosis: Diagnosis
    playbooks_ref: str
    requested_action_level: ActionLevel
    user_role: UserRole


class MapRecommendationsOutput(StrictModel):
    recommendations: List[Recommendation] = Field(default_factory=list)
    approval_required: bool = False
    approval_request: Optional[ApprovalRequest] = None


class ComposeResponseInput(StrictModel):
    intent: Optional[InvestigationIntent] = None
    diagnosis: Optional[Diagnosis] = None
    recommendations: List[Recommendation] = Field(default_factory=list)
    caveats: List[str] = Field(default_factory=list)
    clarification_request: Optional[ClarificationRequest] = None


class ComposeResponseOutput(StrictModel):
    response: UserResponse


class CheckpointAndFinishInput(StrictModel):
    state: InvestigationState


class CheckpointAndFinishOutput(StrictModel):
    state: InvestigationState


class GradeLabel(str, Enum):
    pass_ = "pass"
    warn = "warn"
    fail = "fail"


class TraceRef(StrictModel):
    run_id: str
    node_name: str
    attempt: int = 1
    checkpoint_ref: Optional[str] = None
    llm_call_ref: Optional[str] = None
    tool_call_refs: List[str] = Field(default_factory=list)


class NodeGrade(StrictModel):
    run_id: str
    node_name: str
    grader_name: str
    label: GradeLabel
    score: float = Field(ge=0.0, le=1.0)
    rationale: str
    trace_ref: TraceRef
    metrics: Dict[str, float | int | str | bool] = Field(default_factory=dict)


class PersistedTraceManifest(StrictModel):
    run_id: str
    node_name: str
    checkpoint_ref: str
    input_state_ref: str
    output_state_ref: str
    context_manifest_version: str
    prompt_template_version: Optional[str] = None
    tool_call_refs: List[str] = Field(default_factory=list)
    llm_call_ref: Optional[str] = None
    created_at: AwareDatetime
    started_at: Optional[AwareDatetime] = None
    finished_at: Optional[AwareDatetime] = None


class ParseIntentGrade(StrictModel):
    parseable: bool
    task_type_valid: bool
    windows_normalized: bool
    confidence_threshold_met: bool


class QueryPlanGrade(StrictModel):
    all_required_evidence_mapped: bool
    unresolved_requirements_count: int
    no_forbidden_tables: bool
    join_budget_ok: bool


class CollectEvidenceGrade(StrictModel):
    evidence_count: int
    tool_error_count: int
    provenance_complete: bool
    evidence_quality_avg: float


class GradeEvidenceGrade(StrictModel):
    coverage_score: float
    consistency_score: float
    blocking_conflicts_count: int
    proceed_decision_valid: bool


class DiagnosisGrade(StrictModel):
    drivers_ranked: bool
    evidence_attached: bool
    unsupported_claim_count: int
    confidence_calibrated: bool


class RecommendationGrade(StrictModel):
    all_recommendations_grounded: bool
    approval_routing_correct: bool
    unsupported_action_count: int


class ResponseGrade(StrictModel):
    schema_valid: bool
    executive_summary_present: bool
    findings_grounded: bool
    caveats_present_when_needed: bool
    evidence_ids_present: bool

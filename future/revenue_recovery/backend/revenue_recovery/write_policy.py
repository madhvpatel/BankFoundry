from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from .models import InvestigationState


class WriteViolationError(ValueError):
    pass


NODE_WRITE_ALLOWLIST: dict[str, set[str]] = {
    "initialize_run": {
        "run_id",
        "parent_run_id",
        "user_id",
        "session_id",
        "user_question",
        "user_role",
        "requested_action_level",
        "context_budget",
        "execution",
        "runtime_control",
        "checkpoint",
    },
    "parse_intent": {
        "intent",
        "runtime_control.parse_confidence",
        "runtime_control.clarification_needed",
        "runtime_control.clarification_request",
    },
    "build_initial_plan": {"plan"},
    "resolve_data_requirements": {"query_specs", "compiled_queries", "evidence_store.missing_data"},
    "collect_evidence": {"evidence_store.bundles", "evidence_store.missing_data"},
    "grade_evidence": {"evidence_store.conflicts"},
    "replan_if_needed": {"plan", "runtime_control.last_replan_action", "runtime_control.stop_reason"},
    "synthesize_diagnosis": {"diagnosis"},
    "map_recommendations": {
        "recommendations",
        "runtime_control.approval_required",
        "runtime_control.approval_reason",
        "checkpoint.approval_request",
    },
    "compose_response": {"response"},
    "approval_gate": {"execution.status", "checkpoint.resumable_from_node"},
    "checkpoint_and_finish": {"checkpoint", "execution.status"},
}


def validate_node_write_paths(node_name: str, paths: list[str]) -> None:
    allowed = NODE_WRITE_ALLOWLIST.get(node_name)
    if allowed is None:
        raise WriteViolationError(f"Unknown node: {node_name}")
    violations = [path for path in paths if not any(path == prefix or path.startswith(f"{prefix}.") for prefix in allowed)]
    if violations:
        raise WriteViolationError(f"Node '{node_name}' is not allowed to write: {', '.join(sorted(violations))}")


def _set_path(data: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cursor = data
    for key in parts[:-1]:
        cursor = cursor.setdefault(key, {})
    cursor[parts[-1]] = value


def apply_node_writes(state: InvestigationState, node_name: str, updates: Mapping[str, Any]) -> InvestigationState:
    paths = sorted(str(path) for path in updates.keys())
    validate_node_write_paths(node_name, paths)
    raw_state = deepcopy(state.model_dump(mode="python"))
    for path, value in updates.items():
        payload = value.model_dump(mode="python") if hasattr(value, "model_dump") else value
        _set_path(raw_state, str(path), payload)
    return InvestigationState.model_validate(raw_state)

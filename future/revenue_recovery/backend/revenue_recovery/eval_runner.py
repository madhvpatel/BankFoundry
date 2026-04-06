from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .models import InvestigationState, NodeGrade, PersistedTraceManifest


@dataclass(frozen=True)
class EvalCase:
    case_id: str
    question: str
    expected_slice: str


@dataclass(frozen=True)
class EvalResult:
    case_id: str
    node_grades: list[NodeGrade]
    trace_manifests: list[PersistedTraceManifest]


def replay_eval_case(
    *,
    case: EvalCase,
    run_case: Callable[[EvalCase], tuple[InvestigationState, list[PersistedTraceManifest], list[NodeGrade]]],
) -> EvalResult:
    _state, traces, grades = run_case(case)
    return EvalResult(case_id=case.case_id, node_grades=grades, trace_manifests=traces)

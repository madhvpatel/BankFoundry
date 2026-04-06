from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class ScenarioSpec:
    scenario_type: str
    knobs: Dict[str, Any]
    missing: List[str]


@dataclass
class ResolvedScenario:
    scenario_type: str
    knobs: Dict[str, Any]
    assumptions: List[Dict[str, Any]]
    questions: List[Dict[str, Any]]


@dataclass
class ScenarioResult:
    baseline: Dict[str, Any]
    projections: Dict[str, Any]
    assumptions: List[Dict[str, Any]]
    narrative: str

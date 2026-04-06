"""Scenario engine package for merchant what-if simulations."""

from .service import run_scenario
from .types import ScenarioResult, ScenarioSpec, ResolvedScenario

__all__ = ["run_scenario", "ScenarioResult", "ScenarioSpec", "ResolvedScenario"]

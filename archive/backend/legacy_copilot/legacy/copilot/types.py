from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class ToolCall:
    name: str
    args: dict[str, Any]


@dataclass
class ToolResult:
    name: str
    ok: bool
    output: Any
    error: str | None = None


@dataclass
class CopilotTurn:
    answer: str
    tool_calls: list[ToolCall]
    tool_results: list[ToolResult]
    intent: str
    evidence: list[str]
    operations_section: dict[str, Any] = field(default_factory=dict)
    growth_section: dict[str, Any] = field(default_factory=dict)
    primary_lane: str = "operations"
    secondary_lane: str = "growth"
    active_lane: str | None = None
    terminal_focus: str | None = None
    proactive_cards: list[dict[str, Any]] = field(default_factory=list)


Intent = Literal[
    "kyc",
    "transactions",
    "settlements",
    "chargebacks",
    "refunds",
    "general",
]

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field

from app.application.kernel.request_models import KernelModel, RequestType, Surface


class ResponseStatus(str, Enum):
    ok = "ok"
    error = "error"


class CanonicalResponse(KernelModel):
    request_id: str
    request_type: RequestType
    surface: Surface
    session_key: str
    status: ResponseStatus
    payload: dict[str, Any] = Field(default_factory=dict)
    trace: dict[str, Any] = Field(default_factory=dict)
    work_item: dict[str, Any] | None = None
    queue_summary: dict[str, Any] | None = None
    timeline: list[dict[str, Any]] = Field(default_factory=list)
    approval_state: dict[str, Any] | None = None
    runbook_steps: list[dict[str, Any]] = Field(default_factory=list)

from __future__ import annotations

from typing import Any, Callable, Mapping

from app.application.kernel.request_models import CanonicalRequest
from app.application.kernel.response_models import CanonicalResponse, ResponseStatus

Handler = Callable[[CanonicalRequest], dict[str, Any]]


class ControlPlaneRouter:
    def __init__(self, handlers: Mapping[str, Handler]):
        self._handlers = dict(handlers)

    def handle(self, request: CanonicalRequest) -> CanonicalResponse:
        handler = self._handlers.get(request.request_type.value)
        if handler is None:
            raise LookupError(f"No control-plane handler registered for {request.request_type.value}")

        payload = handler(request)
        return CanonicalResponse(
            request_id=request.request_id,
            request_type=request.request_type,
            surface=request.surface,
            session_key=request.session.session_key,
            status=ResponseStatus.ok,
            payload=payload if isinstance(payload, dict) else {"value": payload},
            trace={
                "request_type": request.request_type.value,
                "surface": request.surface.value,
                "session_key": request.session.session_key,
            },
            work_item=payload.get("work_item") if isinstance(payload, dict) else None,
            queue_summary=payload.get("queue_summary") if isinstance(payload, dict) else None,
            timeline=payload.get("timeline") if isinstance(payload, dict) and isinstance(payload.get("timeline"), list) else [],
            approval_state=payload.get("approval_state") if isinstance(payload, dict) else None,
            runbook_steps=payload.get("runbook_steps") if isinstance(payload, dict) and isinstance(payload.get("runbook_steps"), list) else [],
        )

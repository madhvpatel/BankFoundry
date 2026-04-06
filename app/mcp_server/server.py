from __future__ import annotations

from typing import Any, Iterable

from app.mcp_server.guards import MCPGuardError
from app.mcp_server.schemas import MCPToolCallResult, MCPToolContent, MCPToolDescriptor, ToolEnvelope, ToolStatus, VerificationStatus
from app.mcp_server.tool_registry import TOOLS


class BankFoundryMCPServer:
    def __init__(self, engine: Any):
        self._engine = engine

    def list_tools(self, *, tool_filter: Iterable[str] | None = None) -> list[MCPToolDescriptor]:
        allowed = set(tool_filter or [])
        descriptors = [descriptor for name, (descriptor, _handler) in TOOLS.items() if not allowed or name in allowed]
        return descriptors

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> MCPToolCallResult:
        registry_entry = TOOLS.get(name)
        if registry_entry is None:
            envelope = ToolEnvelope(
                status=ToolStatus.error,
                verification=VerificationStatus.not_applicable,
                tool_name=name,
                merchant_id=str((arguments or {}).get("merchant_id") or ""),
                evidence_ids=[],
                notes=[],
                error_message=f"Unknown tool: {name}",
            )
            return MCPToolCallResult(tool_name=name, is_error=True, content=[MCPToolContent(json=envelope.model_dump(mode="json"))])

        _descriptor, handler = registry_entry
        try:
            envelope = handler(self._engine, arguments or {})
        except MCPGuardError as exc:
            envelope = ToolEnvelope(
                status=ToolStatus.error,
                verification=VerificationStatus.not_applicable,
                tool_name=name,
                merchant_id=str((arguments or {}).get("merchant_id") or ""),
                evidence_ids=[],
                notes=[],
                error_message=str(exc),
            )
            return MCPToolCallResult(tool_name=name, is_error=True, content=[MCPToolContent(json=envelope.model_dump(mode="json"))])
        return MCPToolCallResult(
            tool_name=name,
            is_error=envelope.status == ToolStatus.error,
            content=[MCPToolContent(json=envelope.model_dump(mode="json"))],
        )

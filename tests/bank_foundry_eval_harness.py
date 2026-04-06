from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.agent.bank_ops_contracts import BANK_AGENT_REQUIRED_SECTIONS
from app.mcp_server.schemas import MCPToolDescriptor, ToolClassification, ToolEnvelope, ToolStatus, VerificationStatus


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "bank_foundry"


def load_blocked_integration_fixture(name: str) -> dict[str, Any]:
    path = FIXTURES_DIR / name
    return json.loads(path.read_text())


def assert_tool_descriptor_contract(name: str, descriptor: MCPToolDescriptor) -> None:
    assert descriptor.name == name
    assert descriptor.description.strip()
    assert isinstance(descriptor.input_schema, dict)
    assert descriptor.input_schema.get("type") == "object"
    assert isinstance(descriptor.input_schema.get("properties"), dict)


def assert_tool_classification_contract(descriptor: MCPToolDescriptor) -> None:
    assert descriptor.classification in set(ToolClassification)
    if descriptor.classification == ToolClassification.write:
        assert descriptor.downstream_target and descriptor.downstream_target.strip()
        assert descriptor.idempotency_expectation and descriptor.idempotency_expectation.strip()


def assert_tool_envelope_contract(envelope: ToolEnvelope) -> None:
    assert envelope.tool_name.strip()
    assert envelope.merchant_id is not None
    assert isinstance(envelope.data, dict)
    assert isinstance(envelope.evidence_ids, list)
    assert isinstance(envelope.notes, list)


def assert_tool_evidence_contract(envelope: ToolEnvelope) -> None:
    if envelope.status == ToolStatus.ok and envelope.verification == VerificationStatus.verified:
        assert envelope.evidence_ids, f"{envelope.tool_name} is verified but emitted no evidence ids"
        return
    if envelope.evidence_ids:
        return
    assert envelope.notes or envelope.error_message, f"{envelope.tool_name} must explain missing evidence"


def assert_agent_summary_contract(summary: dict[str, Any]) -> None:
    answer_sections = summary.get("answer_sections")
    assert isinstance(answer_sections, dict)
    for key in BANK_AGENT_REQUIRED_SECTIONS:
        assert key in answer_sections
    assert isinstance(answer_sections.get("executive_summary"), str)
    assert isinstance(answer_sections.get("key_findings"), list)
    assert isinstance(answer_sections.get("next_best_action"), str)
    assert isinstance(answer_sections.get("caveats"), list)
    assert isinstance(summary.get("summary"), str)
    assert isinstance(summary.get("tool_calls"), list)
    assert isinstance(summary.get("evidence_ids"), list)
    assert isinstance(summary.get("verification"), str)


def assert_agent_verification_downgrade_contract(summary: dict[str, Any]) -> None:
    tool_calls = summary.get("tool_calls")
    assert isinstance(tool_calls, list)
    has_partial_tool = any(
        str(item.get("verification") or "") != VerificationStatus.verified.value
        for item in tool_calls
        if isinstance(item, dict)
    )
    if has_partial_tool:
        assert summary.get("verification") == VerificationStatus.unverified.value
    elif tool_calls:
        assert summary.get("verification") == VerificationStatus.verified.value

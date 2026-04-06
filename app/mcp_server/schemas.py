from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class MCPModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ToolStatus(str, Enum):
    ok = "ok"
    error = "error"


class VerificationStatus(str, Enum):
    verified = "verified"
    unverified = "unverified"
    not_applicable = "not_applicable"


class ToolClassification(str, Enum):
    read = "read"
    drafting = "drafting"
    write = "write"


class DateWindow(MCPModel):
    start_date: date
    end_date: date


class MerchantScopedInput(MCPModel):
    merchant_id: str


class ComplianceGuidanceInput(MerchantScopedInput):
    topic: str = Field(default="merchant_screening", min_length=1)


class SettlementDetailInput(MerchantScopedInput):
    settlement_id: str


class ChargebackDetailInput(MerchantScopedInput):
    chargeback_id: str


class RefundDetailInput(MerchantScopedInput):
    refund_id: str


class MerchantWindowInput(MCPModel):
    merchant_id: str
    start_date: date
    end_date: date


class SettlementListInput(MerchantWindowInput):
    limit: int = Field(default=25, ge=1, le=100)


class MerchantDaysInput(MCPModel):
    merchant_id: str
    days: int = Field(default=30, ge=1, le=90)


class ChargebackListInput(MerchantWindowInput):
    status: Literal["open", "closed", "all"] = "open"
    limit: int = Field(default=25, ge=1, le=100)


class RefundListInput(MerchantWindowInput):
    limit: int = Field(default=25, ge=1, le=100)


class FailureBreakdownInput(MerchantWindowInput):
    dimension: Literal["response_code", "payment_mode"] = "response_code"
    limit: int = Field(default=5, ge=1, le=20)


class MerchantWindowTerminalInput(MerchantWindowInput):
    terminal_id: str | None = None


class PaymentModeMixInput(MerchantWindowTerminalInput):
    limit: int = Field(default=10, ge=1, le=20)


class RecentTransactionsInput(MerchantWindowTerminalInput):
    status: Literal["SUCCESS", "FAILURE", "ALL"] = "ALL"
    payment_mode: str = "ALL"
    limit: int = Field(default=25, ge=1, le=100)


class TransactionDetailInput(MerchantScopedInput):
    tx_id: str


class TerminalProfileInput(MerchantScopedInput):
    terminal_id: str


class TerminalHealthSummaryInput(MerchantWindowTerminalInput):
    group_by: Literal["tid", "hour", "tid_hour"] = "tid"
    limit: int = Field(default=25, ge=1, le=100)


class TerminalFailureBreakdownInput(MerchantWindowInput):
    terminal_id: str
    dimension: Literal["response_code", "payment_mode"] = "response_code"
    limit: int = Field(default=5, ge=1, le=20)


class PaymentsKnowledgeInput(MerchantScopedInput):
    query: str
    top_k: int = Field(default=3, ge=1, le=10)


class OpsQueueListInput(MerchantScopedInput):
    lane: str | None = None
    status: str | None = "ACTIVE"
    owner: str | None = None
    limit: int = Field(default=25, ge=1, le=100)


class SettlementShortfallInput(MerchantWindowInput):
    expected_amount: float | None = None
    received_amount: float | None = None
    limit: int = Field(default=20, ge=1, le=100)


class VerifiedSQLInput(MerchantWindowInput):
    query: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=100, ge=1, le=200)


class CaseScopedInput(MerchantScopedInput):
    case_id: str


class SettlementCaseActionInput(CaseScopedInput):
    settlement_id: str | None = None
    evidence_ids: list[str] = Field(default_factory=list)
    recommended_action: str | None = None
    payload_summary: str | None = None


class CaseScopedLimitInput(CaseScopedInput):
    limit: int = Field(default=10, ge=1, le=25)


class ToolEnvelope(MCPModel):
    status: ToolStatus
    verification: VerificationStatus
    tool_name: str
    merchant_id: str
    window: DateWindow | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    evidence_ids: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    error_message: str | None = None


class MCPToolDescriptor(MCPModel):
    name: str
    description: str
    input_schema: dict[str, Any]
    classification: ToolClassification
    approval_required: bool = False
    downstream_target: str | None = None
    idempotency_expectation: str | None = None


class MCPToolContent(MCPModel):
    type: Literal["json"] = "json"
    json_payload: dict[str, Any] = Field(alias="json")


class MCPToolCallResult(MCPModel):
    tool_name: str
    is_error: bool = False
    content: list[MCPToolContent] = Field(default_factory=list)

    def envelope(self) -> ToolEnvelope:
        if not self.content:
            raise ValueError("MCP tool result has no content")
        return ToolEnvelope.model_validate(self.content[0].json_payload)

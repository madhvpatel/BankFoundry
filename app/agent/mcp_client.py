from __future__ import annotations

import datetime as dt
from typing import Any, Iterable

from app.mcp_server import BankFoundryMCPServer, MCPToolDescriptor, ToolEnvelope, VerificationStatus


class BankFoundryMCPClient:
    def __init__(self, server: BankFoundryMCPServer, *, tool_filter: Iterable[str] | None = None):
        self._server = server
        self._tool_filter = tuple(tool_filter or ())

    def list_tools(self) -> list[MCPToolDescriptor]:
        return self._server.list_tools(tool_filter=self._tool_filter)

    def call_tool(self, name: str, arguments: dict[str, Any]) -> ToolEnvelope:
        if self._tool_filter and name not in self._tool_filter:
            raise PermissionError(f"Tool {name} is not visible to this client")
        return self._server.call_tool(name, arguments).envelope()


class FailureDiagnosticsMCPAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def analyze_failure_increase(
        self,
        *,
        merchant_id: str,
        start_date: str,
        end_date: str,
        dimension: str = "response_code",
    ) -> dict[str, Any]:
        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        kpis = self._client.call_tool(
            "get_window_kpis",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date},
        )
        failures = self._client.call_tool(
            "get_failure_breakdown",
            {
                "merchant_id": merchant_id,
                "start_date": start_date,
                "end_date": end_date,
                "dimension": dimension,
                "limit": 5,
            },
        )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id)
        breakdown = failures.data.get("breakdown") if isinstance(failures.data.get("breakdown"), list) else []
        top_driver = breakdown[0] if breakdown else None
        top_driver_label = str(top_driver.get("driver")) if isinstance(top_driver, dict) and top_driver.get("driver") is not None else None
        top_driver_count = int(top_driver.get("failed_txns") or 0) if isinstance(top_driver, dict) else 0

        verification = VerificationStatus.verified
        if any(item.verification != VerificationStatus.verified for item in (kpis, failures)):
            verification = VerificationStatus.unverified

        if top_driver_label:
            answer = (
                f"{trade_name} shows {int(kpis.data.get('kpis', {}).get('fail_txns') or 0)} failed transactions in this window. "
                f"The top {dimension.replace('_', ' ')} driver is {top_driver_label} with {top_driver_count} failures."
            )
        else:
            answer = (
                f"{trade_name} shows {int(kpis.data.get('kpis', {}).get('fail_txns') or 0)} failed transactions in this window, "
                "but no dominant failure driver was found."
            )

        evidence_ids: list[str] = []
        for item in (profile, kpis, failures):
            evidence_ids.extend(item.evidence_ids)

        notes: list[str] = []
        for item in (profile, kpis, failures):
            notes.extend(item.notes)

        return {
            "answer": answer,
            "verification": verification.value,
            "tool_calls": [
                {"tool_name": "get_merchant_profile", "verification": profile.verification.value},
                {"tool_name": "get_window_kpis", "verification": kpis.verification.value},
                {"tool_name": "get_failure_breakdown", "verification": failures.verification.value},
            ],
            "evidence_ids": list(dict.fromkeys(evidence_ids)),
            "notes": notes,
        }


def _dedupe_text(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text_value = str(value or "").strip()
        if text_value and text_value not in seen:
            seen.add(text_value)
            out.append(text_value)
    return out


def _parse_iso_datetime(value: Any) -> dt.datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _derive_case_window(case_detail: dict[str, Any]) -> tuple[str, str, str]:
    memory = case_detail.get("memory") if isinstance(case_detail.get("memory"), dict) else {}
    active_window = memory.get("active_window") if isinstance(memory.get("active_window"), dict) else {}
    memory_start = str(active_window.get("start_date") or "").strip()
    memory_end = str(active_window.get("end_date") or "").strip()
    if memory_start and memory_end:
        return memory_start, memory_end, "case_memory_window"

    work_item = case_detail.get("work_item") if isinstance(case_detail.get("work_item"), dict) else {}
    source_payload = work_item.get("source_payload") if isinstance(work_item.get("source_payload"), dict) else {}
    start_candidates = [
        source_payload.get("window_from"),
        (source_payload.get("window") or {}).get("from") if isinstance(source_payload.get("window"), dict) else None,
        source_payload.get("start_date"),
    ]
    end_candidates = [
        source_payload.get("window_to"),
        (source_payload.get("window") or {}).get("to") if isinstance(source_payload.get("window"), dict) else None,
        source_payload.get("end_date"),
    ]

    start_date = next((str(item).strip() for item in start_candidates if str(item or "").strip()), "")
    end_date = next((str(item).strip() for item in end_candidates if str(item or "").strip()), "")
    if start_date and end_date:
        return start_date, end_date, "case_source_window"

    opened_at = _parse_iso_datetime(work_item.get("opened_at"))
    anchor = (opened_at or dt.datetime.now(dt.timezone.utc)).date()
    derived_end = anchor + dt.timedelta(days=1)
    derived_start = derived_end - dt.timedelta(days=30)
    return derived_start.isoformat(), derived_end.isoformat(), "opened_at_30d_window"


class OpsCaseCopilotMCPAgent:
    def __init__(self, client: BankFoundryMCPClient):
        self._client = client

    def summarize_case(self, *, case_detail: dict[str, Any], prompt: str | None = None) -> dict[str, Any]:
        work_item = case_detail.get("work_item") if isinstance(case_detail.get("work_item"), dict) else {}
        merchant_id = str(work_item.get("merchant_id") or "").strip()
        if not merchant_id:
            raise ValueError("case detail is missing merchant_id")

        start_date, end_date, window_reason = _derive_case_window(case_detail)
        profile = self._client.call_tool("get_merchant_profile", {"merchant_id": merchant_id})
        kpis = self._client.call_tool(
            "get_window_kpis",
            {"merchant_id": merchant_id, "start_date": start_date, "end_date": end_date},
        )

        kpi_payload = kpis.data.get("kpis") if isinstance(kpis.data.get("kpis"), dict) else {}
        fail_txns = int(kpi_payload.get("fail_txns") or 0)
        should_check_failures = fail_txns > 0 or any(
            token in str(prompt or "").lower() for token in ("failure", "decline", "response code", "issuer")
        )

        failures: ToolEnvelope | None = None
        if should_check_failures:
            failures = self._client.call_tool(
                "get_failure_breakdown",
                {
                    "merchant_id": merchant_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "dimension": "response_code",
                    "limit": 5,
                },
            )

        merchant = profile.data.get("merchant") if isinstance(profile.data.get("merchant"), dict) else {}
        risk = profile.data.get("risk_profile") if isinstance(profile.data.get("risk_profile"), dict) else {}
        trade_name = str(merchant.get("merchant_trade_name") or merchant_id)
        city = str(merchant.get("business_city") or "").strip()
        case_title = str(work_item.get("title") or "Ops case")
        case_summary = str(work_item.get("summary") or "").strip()
        case_status = str(work_item.get("status") or "OPEN").replace("_", " ").lower()

        findings: list[str] = []
        if city:
            findings.append(f"Merchant context: {trade_name} in {city}.")
        elif trade_name:
            findings.append(f"Merchant context: {trade_name}.")
        if risk:
            findings.append(f"Risk profile is {str(risk.get('band') or 'unknown').lower()} ({risk.get('score')}).")
        findings.append(
            f"Recent payments window {start_date} to {end_date}: {int(kpi_payload.get('attempts') or 0)} attempts, "
            f"{float(kpi_payload.get('success_rate_pct') or 0.0):.2f}% success rate, "
            f"{fail_txns} failed transactions."
        )

        if failures is not None:
            breakdown = failures.data.get("breakdown") if isinstance(failures.data.get("breakdown"), list) else []
            top_driver = breakdown[0] if breakdown else None
            if isinstance(top_driver, dict) and top_driver.get("driver") is not None:
                findings.append(
                    f"Top recent failure driver is response code {top_driver.get('driver')} with "
                    f"{int(top_driver.get('failed_txns') or 0)} failures."
                )

        next_best_action = ""
        approval_state = case_detail.get("approval_state") if isinstance(case_detail.get("approval_state"), dict) else {}
        if str(approval_state.get("status") or "").lower() == "pending":
            next_best_action = "Review the pending approval so the case can move to connector execution."
        else:
            runbook_steps = case_detail.get("runbook_steps") if isinstance(case_detail.get("runbook_steps"), list) else []
            pending_step = next((step for step in runbook_steps if str(step.get("status") or "").upper() != "DONE"), None)
            if isinstance(pending_step, dict):
                next_best_action = f"{pending_step.get('title')}: {pending_step.get('description')}"
            else:
                next_best_action = "Review the latest timeline event and close the case if all runbook work is complete."

        caveats: list[str] = []
        if profile.verification != VerificationStatus.verified:
            caveats.append("Merchant profile context is incomplete in the current data sources.")
        if kpis.verification != VerificationStatus.verified:
            caveats.append("Recent KPI context needs verification before being treated as final.")
        if failures is not None and failures.verification != VerificationStatus.verified:
            caveats.append("Failure-driver context is directional and should be confirmed before escalation.")

        verification = VerificationStatus.verified
        tool_calls = [
            {"tool_name": "get_merchant_profile", "verification": profile.verification.value},
            {"tool_name": "get_window_kpis", "verification": kpis.verification.value},
        ]
        evidence_ids = list(work_item.get("evidence_ids") or [])
        evidence_ids.extend(profile.evidence_ids)
        evidence_ids.extend(kpis.evidence_ids)
        notes = list(profile.notes) + list(kpis.notes)

        if failures is not None:
            tool_calls.append({"tool_name": "get_failure_breakdown", "verification": failures.verification.value})
            evidence_ids.extend(failures.evidence_ids)
            notes.extend(failures.notes)
            if failures.verification != VerificationStatus.verified:
                verification = VerificationStatus.unverified

        if any(item["verification"] != VerificationStatus.verified.value for item in tool_calls):
            verification = VerificationStatus.unverified

        executive_summary = (
            f"{case_title} is currently {case_status} for {trade_name}. "
            f"{case_summary or 'The case remains active based on the current evidence and runbook state.'}"
        )

        return {
            "summary": executive_summary,
            "answer_sections": {
                "executive_summary": executive_summary,
                "key_findings": findings,
                "next_best_action": next_best_action,
                "caveats": caveats,
            },
            "verification": verification.value,
            "tool_calls": tool_calls,
            "evidence_ids": _dedupe_text([str(item) for item in evidence_ids]),
            "notes": _dedupe_text(notes),
            "window": {
                "start_date": start_date,
                "end_date": end_date,
                "reason": window_reason,
            },
        }


def build_ops_case_copilot_summary(engine: Any, case_detail: dict[str, Any], *, prompt: str | None = None) -> dict[str, Any]:
    server = BankFoundryMCPServer(engine)
    client = BankFoundryMCPClient(
        server,
        tool_filter=[
            "get_merchant_profile",
            "get_window_kpis",
            "get_failure_breakdown",
        ],
    )
    return OpsCaseCopilotMCPAgent(client).summarize_case(case_detail=case_detail, prompt=prompt)


AcquiGuruMCPClient = BankFoundryMCPClient

from __future__ import annotations

import html
import json
from typing import Any


def rows_to_csv(rows: list[dict[str, Any]]) -> bytes:
    if not rows:
        return b""
    columns: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if key not in columns:
                columns.append(str(key))

    lines = [",".join(json.dumps(col)[1:-1] for col in columns)]
    for row in rows:
        if not isinstance(row, dict):
            continue
        cells = []
        for col in columns:
            value = row.get(col)
            if value is None:
                cells.append("")
            else:
                text_value = str(value).replace('"', '""')
                if any(ch in text_value for ch in [",", "\n", '"']):
                    cells.append(f'"{text_value}"')
                else:
                    cells.append(text_value)
        lines.append(",".join(cells))
    return ("\n".join(lines)).encode("utf-8")


def _report_window_text(snapshot: dict[str, Any]) -> str:
    window = snapshot.get("window", {}) if isinstance(snapshot.get("window"), dict) else {}
    from_date = str(window.get("from") or "").strip()
    to_date = str(window.get("to") or "").strip()
    if from_date and to_date:
        return f"{from_date} to {to_date}"
    return "current window"


def _report_scope_text(snapshot: dict[str, Any]) -> str:
    scope = snapshot.get("scope", {}) if isinstance(snapshot.get("scope"), dict) else {}
    label = str(scope.get("label") or "All terminals").strip()
    return label or "All terminals"


def _report_merchant_name(snapshot: dict[str, Any]) -> str:
    merchant = snapshot.get("merchant_profile", {}).get("merchant", {}) if isinstance(snapshot.get("merchant_profile"), dict) else {}
    name = str(merchant.get("merchant_trade_name") or snapshot.get("merchant_id") or "Merchant").strip()
    return name or "Merchant"


def _report_dataset_overview(pack: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for dataset in pack.get("datasets") or []:
        if not isinstance(dataset, dict):
            continue
        title = str(dataset.get("title") or dataset.get("key") or "Dataset").strip()
        rows = dataset.get("rows") if isinstance(dataset.get("rows"), list) else []
        lines.append(f"{title}: {len(rows):,} row(s)")
    return lines


def _brief_text_bytes(text_value: str) -> bytes:
    return str(text_value or "").encode("utf-8")


def build_report_briefs(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    packs = build_report_packs(snapshot)
    merchant_name = _report_merchant_name(snapshot)
    window_text = _report_window_text(snapshot)
    scope_text = _report_scope_text(snapshot)

    briefs: list[dict[str, Any]] = []
    for pack in packs:
        if not isinstance(pack, dict):
            continue
        pack_id = str(pack.get("id") or "report")
        title = str(pack.get("title") or "Report Pack")
        summary_lines = [str(line) for line in (pack.get("summary_lines") or []) if str(line).strip()]
        dataset_lines = _report_dataset_overview(pack)
        subject = f"{title} | {merchant_name} | {window_text}"

        text_lines = [
            subject,
            f"Scope: {scope_text}",
            "",
            "Summary",
        ]
        text_lines.extend([f"- {line}" for line in summary_lines])
        if dataset_lines:
            text_lines.append("")
            text_lines.append("Included datasets")
            text_lines.extend([f"- {line}" for line in dataset_lines])
        text_body = "\n".join(text_lines).strip()

        html_summary = "".join(f"<li>{html.escape(line)}</li>" for line in summary_lines)
        html_datasets = "".join(f"<li>{html.escape(line)}</li>" for line in dataset_lines)
        html_body = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>{html.escape(subject)}</title>
  <style>
    body {{ font-family: Georgia, 'Times New Roman', serif; margin: 32px; color: #1f1f1f; }}
    h1 {{ font-size: 28px; margin-bottom: 8px; }}
    .meta {{ color: #555; font-size: 14px; margin-bottom: 24px; }}
    h2 {{ font-size: 18px; margin-top: 24px; }}
    ul {{ margin-top: 8px; }}
    li {{ margin-bottom: 6px; }}
    @media print {{ body {{ margin: 18px; }} }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <div class="meta">{html.escape(merchant_name)} | {html.escape(window_text)} | Scope: {html.escape(scope_text)}</div>
  <h2>Summary</h2>
  <ul>{html_summary}</ul>
  <h2>Included datasets</h2>
  <ul>{html_datasets}</ul>
</body>
</html>
"""

        briefs.append(
            {
                "id": pack_id,
                "title": title,
                "subject": subject,
                "summary_lines": summary_lines,
                "dataset_lines": dataset_lines,
                "email_text": text_body,
                "email_bytes": _brief_text_bytes(text_body),
                "print_html": html_body,
                "print_html_bytes": _brief_text_bytes(html_body),
            }
        )
    return briefs


def build_report_packs(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    summary = snapshot.get("summary", {}) if isinstance(snapshot.get("summary"), dict) else {}
    cashflow = snapshot.get("cashflow", {}) if isinstance(snapshot.get("cashflow"), dict) else {}
    amounts = cashflow.get("amounts", {}) if isinstance(cashflow.get("amounts"), dict) else {}
    past_expected = cashflow.get("past_expected", {}) if isinstance(cashflow.get("past_expected"), dict) else {}
    payment_mode_rows = snapshot.get("kpi_by_mode") if isinstance(snapshot.get("kpi_by_mode"), list) else []
    response_code_rows = snapshot.get("failure_drivers", {}).get("response_code", {}).get("rows") or []
    payment_driver_rows = snapshot.get("failure_drivers", {}).get("payment_mode", {}).get("rows") or []
    growth_tasks = snapshot.get("growth_tasks") if isinstance(snapshot.get("growth_tasks"), list) else []
    operations_tasks = snapshot.get("operations_tasks") if isinstance(snapshot.get("operations_tasks"), list) else []
    scope = snapshot.get("scope", {}) if isinstance(snapshot.get("scope"), dict) else {}
    scope_level = str(scope.get("level") or "merchant").lower()
    scope_label = str(scope.get("label") or "All terminals")

    top_mode = payment_driver_rows[0] if payment_driver_rows and isinstance(payment_driver_rows[0], dict) else {}
    finance_pack = {
        "id": "finance",
        "title": "Finance Pack",
        "summary_lines": [
            f"Settled amount in window: Rs {float(amounts.get('settled_amount') or 0.0):,.2f}.",
            f"Pending amount in window: Rs {float(amounts.get('pending_amount') or 0.0):,.2f}.",
            f"Open chargebacks: {int(summary.get('open_chargebacks') or 0):,}.",
            f"Refund rows in window: {int(summary.get('refund_count') or 0):,}.",
        ],
        "datasets": [
            {"key": "finance_settlement_timing", "title": "Settlement timing", "rows": cashflow.get("recent") or []},
            {"key": "finance_settlements", "title": "Settlements", "rows": snapshot.get("settlements", {}).get("rows") or []},
            {"key": "finance_chargebacks", "title": "Chargebacks", "rows": snapshot.get("chargebacks", {}).get("rows") or []},
            {"key": "finance_refunds", "title": "Refunds", "rows": snapshot.get("refunds", {}).get("rows") or []},
        ],
    }
    if scope_level == "terminal":
        finance_pack["summary_lines"].insert(0, f"Scope: {scope_label}. Finance datasets remain merchant-wide in this sprint.")
    operations_pack = {
        "id": "operations",
        "title": "Operations Pack",
        "summary_lines": [
            f"Success rate: {float(summary.get('success_rate_pct') or 0.0):.2f}% across {int(summary.get('attempts') or 0):,} attempts.",
            f"Past-expected settlements: {int(past_expected.get('past_expected_count') or 0):,} for Rs {float(past_expected.get('past_expected_amount') or 0.0):,.2f}.",
            f"Open operational tasks available: {len(operations_tasks):,}.",
            f"Terminal rows observed: {int(summary.get('terminal_count') or 0):,}.",
        ],
        "datasets": [
            {"key": "ops_settlements", "title": "Settlements", "rows": snapshot.get("settlements", {}).get("rows") or []},
            {"key": "ops_chargebacks", "title": "Chargebacks", "rows": snapshot.get("chargebacks", {}).get("rows") or []},
            {"key": "ops_terminals", "title": "Terminal performance", "rows": snapshot.get("terminals", {}).get("rows") or []},
            {"key": "ops_failure_codes", "title": "Failure drivers by response code", "rows": response_code_rows},
        ],
    }
    if scope_level == "terminal":
        operations_pack["summary_lines"].insert(
            0,
            f"Scope: {scope_label}. Terminal-backed diagnostics are filtered; settlement and dispute datasets remain merchant-wide.",
        )
    growth_pack = {
        "id": "growth",
        "title": "Growth Pack",
        "summary_lines": [
            f"Success GMV in window: Rs {float(summary.get('success_gmv') or 0.0):,.2f}.",
            (
                f"Top failed mode: {top_mode.get('driver')} with {int(top_mode.get('failed_txns') or 0):,} failed txns."
                if top_mode
                else "No verified top failed payment mode in the active window."
            ),
            f"Growth task candidates available: {len(growth_tasks):,}.",
            f"Payment modes observed: {len(payment_mode_rows):,}.",
        ],
        "datasets": [
            {"key": "growth_payment_modes", "title": "Payment mode KPIs", "rows": payment_mode_rows},
            {"key": "growth_failure_modes", "title": "Failure drivers by payment mode", "rows": payment_driver_rows},
            {"key": "growth_failure_codes", "title": "Failure drivers by response code", "rows": response_code_rows},
            {"key": "growth_terminals", "title": "Terminal performance", "rows": snapshot.get("terminals", {}).get("rows") or []},
        ],
    }
    if scope_level == "terminal":
        growth_pack["summary_lines"].insert(0, f"Scope: {scope_label}. Growth metrics below are filtered to the selected terminal.")
    return [finance_pack, operations_pack, growth_pack]

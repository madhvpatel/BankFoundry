from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class LiveContextDeps:
    engine: Any
    json_safe: Callable[[Any], Any]
    ensure_background_proactive_refresh: Callable[..., Any]
    get_merchant_os_snapshot: Callable[..., dict[str, Any]]
    terminal_scope_options: Callable[[dict[str, Any]], list[str]]
    scope_snapshot_to_terminal: Callable[..., dict[str, Any]]
    get_background_refresh_status: Callable[..., Any]
    build_report_briefs: Callable[[dict[str, Any]], list[dict[str, Any]]]
    build_report_packs: Callable[[dict[str, Any]], list[dict[str, Any]]]


def build_merchant_snapshot(
    merchant_id: str,
    terminal_id: str | None,
    *,
    days: int = 30,
    refresh: bool = True,
    deps: LiveContextDeps,
) -> dict[str, Any]:
    if refresh:
        deps.ensure_background_proactive_refresh(deps.engine, merchant_id, days=days, force=False)
    base_snapshot = deps.get_merchant_os_snapshot(deps.engine, merchant_id, days=days)
    terminal_options = deps.terminal_scope_options(base_snapshot)
    snapshot = deps.scope_snapshot_to_terminal(deps.engine, base_snapshot, terminal_id) if terminal_id else base_snapshot
    snapshot = dict(snapshot)
    snapshot["terminal_options"] = terminal_options
    snapshot["selected_terminal_id"] = terminal_id
    snapshot["refresh_status"] = deps.get_background_refresh_status(deps.engine, merchant_id, days=days)
    return snapshot


def build_report_payload(snapshot: dict[str, Any], *, deps: LiveContextDeps) -> dict[str, Any]:
    briefs = []
    for brief in deps.build_report_briefs(snapshot):
        if not isinstance(brief, dict):
            continue
        briefs.append(
            {
                "id": brief.get("id"),
                "title": brief.get("title"),
                "subject": brief.get("subject"),
                "summary_lines": brief.get("summary_lines") or [],
                "dataset_lines": brief.get("dataset_lines") or [],
                "email_text": brief.get("email_text") or "",
                "print_html": brief.get("print_html") or "",
            }
        )

    packs = []
    for pack in deps.build_report_packs(snapshot):
        if not isinstance(pack, dict):
            continue
        packs.append(
            {
                "id": pack.get("id"),
                "title": pack.get("title"),
                "summary_lines": pack.get("summary_lines") or [],
                "datasets": pack.get("datasets") or [],
            }
        )
    return {"packs": deps.json_safe(packs), "briefs": deps.json_safe(briefs)}


def merchant_label(snapshot: dict[str, Any], merchant_id: str) -> str:
    merchant = snapshot.get("merchant_profile", {}).get("merchant", {}) if isinstance(snapshot.get("merchant_profile"), dict) else {}
    name = str(merchant.get("merchant_trade_name") or "").strip()
    return name or merchant_id

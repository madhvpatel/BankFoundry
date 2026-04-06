from __future__ import annotations

import ast
import datetime as dt
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.intelligence.constants import FAILED_STATUS_SQL
from app.intelligence.money import get_amount_scale, scale_inr
from app.project_paths import repo_path
from config import Config

logger = logging.getLogger("insight_cards")

DEFAULT_CARDS_DIR = repo_path("agent", "CARDS")


@dataclass
class CardTemplate:
    card_id: str
    name: str
    trigger_tool: str
    condition: str
    title: str
    explanation: str
    actions: list[str] = field(default_factory=list)
    severity: str = "info"
    icon: str = "🔎"
    impact_metric: str | None = None
    confidence_metric: str | None = None


def _strip_quotes(value: str) -> str:
    raw = value.strip()
    if len(raw) >= 2 and ((raw[0] == '"' and raw[-1] == '"') or (raw[0] == "'" and raw[-1] == "'")):
        return raw[1:-1]
    return raw


def _parse_card_file(path: Path) -> CardTemplate | None:
    text_blob = path.read_text(encoding="utf-8")
    lines = text_blob.splitlines()
    if not lines:
        return None

    match = re.match(r"^#\s*CARD:\s*(.+)$", lines[0].strip())
    if not match:
        logger.warning("Skipping invalid card template (missing CARD heading): %s", path)
        return None

    name = match.group(1).strip()
    sections: dict[str, Any] = {"trigger": {}, "copy": {}, "actions": []}
    top_level: dict[str, str] = {}
    current_section = ""

    for raw_line in lines[1:]:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if stripped.endswith(":") and ":" not in stripped[:-1]:
            current_section = stripped[:-1].strip().lower()
            continue

        if current_section == "actions" and stripped.startswith("- "):
            sections["actions"].append(_strip_quotes(stripped[2:].strip()))
            continue

        if ":" not in stripped:
            continue

        key, value = stripped.split(":", 1)
        key = key.strip().lower()
        value = _strip_quotes(value.strip())

        if current_section in {"trigger", "copy"}:
            sections[current_section][key] = value
        elif current_section == "actions" and stripped.startswith("- "):
            sections["actions"].append(_strip_quotes(stripped[2:].strip()))
        else:
            top_level[key] = value

    trigger = sections.get("trigger") or {}
    copy = sections.get("copy") or {}

    required = {
        "tool": trigger.get("tool"),
        "condition": trigger.get("condition"),
        "title": copy.get("title"),
        "explanation": copy.get("explanation"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        logger.warning("Skipping invalid card template %s (missing: %s)", path, ", ".join(missing))
        return None

    return CardTemplate(
        card_id=path.stem,
        name=name,
        trigger_tool=str(trigger.get("tool") or "").strip(),
        condition=str(trigger.get("condition") or "").strip(),
        title=str(copy.get("title") or "").strip(),
        explanation=str(copy.get("explanation") or "").strip(),
        actions=[str(item).strip() for item in sections.get("actions") or [] if str(item).strip()],
        severity=str(top_level.get("severity") or "info").strip().lower(),
        icon=str(top_level.get("icon") or "🔎").strip(),
        impact_metric=(str(top_level.get("impact_metric")).strip() or None) if top_level.get("impact_metric") else None,
        confidence_metric=(str(top_level.get("confidence_metric")).strip() or None) if top_level.get("confidence_metric") else None,
    )


def load_card_templates(cards_dir: Path | None = None) -> list[CardTemplate]:
    base = cards_dir or DEFAULT_CARDS_DIR
    if not base.exists():
        logger.warning("Insight cards directory not found: %s", base)
        return []

    templates: list[CardTemplate] = []
    for path in sorted(base.glob("*.md")):
        try:
            parsed = _parse_card_file(path)
            if parsed:
                templates.append(parsed)
        except Exception as exc:
            logger.warning("Failed to parse card template %s: %s", path, exc)

    return templates


_ALLOWED_AST_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.BinOp,
    ast.UnaryOp,
    ast.Compare,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Mod,
    ast.Pow,
    ast.USub,
    ast.UAdd,
    ast.Gt,
    ast.GtE,
    ast.Lt,
    ast.LtE,
    ast.Eq,
    ast.NotEq,
)


def evaluate_trigger(condition: str, metrics: dict[str, Any]) -> bool:
    if not condition:
        return False

    try:
        parsed = ast.parse(condition, mode="eval")
    except SyntaxError:
        logger.warning("Invalid card trigger condition syntax: %s", condition)
        return False

    for node in ast.walk(parsed):
        if isinstance(node, ast.Call):
            logger.warning("Function calls are not allowed in trigger conditions: %s", condition)
            return False
        if not isinstance(node, _ALLOWED_AST_NODES):
            logger.warning("Unsupported token in trigger condition: %s", condition)
            return False

    local_vars: dict[str, Any] = {}
    for node in ast.walk(parsed):
        if isinstance(node, ast.Name):
            local_vars[node.id] = metrics.get(node.id, 0)

    try:
        return bool(eval(compile(parsed, "<card_trigger>", "eval"), {"__builtins__": {}}, local_vars))
    except Exception:
        logger.warning("Trigger condition evaluation failed: %s", condition)
        return False


class _FormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _render_text(template: str, metrics: dict[str, Any]) -> str:
    try:
        return template.format_map(_FormatDict(metrics))
    except Exception:
        return template


def _confidence_from_volume(n: int) -> float:
    if n < 50:
        return 0.45
    if n < 200:
        return 0.65
    return 0.8


def _to_num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_date(value: Any) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return dt.date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _build_metrics(engine, merchant_id: str, window_days: int) -> dict[str, Any]:
    table = Config.QUERY_SOURCE_TABLE
    amount_scale = get_amount_scale(engine)

    metrics: dict[str, Any] = {}

    with engine.connect() as conn:
        max_date_raw = conn.execute(
            text(f"SELECT MAX(p_date) FROM {table} WHERE merchant_id = :mid"),
            {"mid": merchant_id},
        ).scalar()
        max_date = _to_date(max_date_raw)
        if not max_date:
            return {}

        end_date = max_date + dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=window_days)
        today_start = max_date
        today_end = max_date + dt.timedelta(days=1)
        prev7_start = max_date - dt.timedelta(days=7)
        prev7_end = max_date

        params = {
            "mid": merchant_id,
            "start_date": start_date,
            "end_date": end_date,
            "today_start": today_start,
            "today_end": today_end,
            "prev7_start": prev7_start,
            "prev7_end": prev7_end,
        }

        base = conn.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS attempts,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_txns,
                    SUM(CASE WHEN status IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS fail_txns,
                    SUM(CASE WHEN status = 'SUCCESS' THEN amount_rupees ELSE 0 END) AS success_gmv,
                    SUM(CASE WHEN status IN {FAILED_STATUS_SQL} THEN amount_rupees ELSE 0 END) AS failed_gmv,
                    AVG(CASE WHEN status = 'SUCCESS' THEN amount_rupees END) AS avg_ticket_success,
                    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS success_rate_pct
                FROM {table}
                WHERE merchant_id = :mid
                  AND p_date >= :start_date
                  AND p_date < :end_date
                """
            ),
            params,
        ).mappings().first()

        if not base:
            return {}

        attempts_total = _to_int(base.get("attempts"), 0)
        success_txns_total = _to_int(base.get("success_txns"), 0)
        fail_txns_total = _to_int(base.get("fail_txns"), 0)
        success_revenue = scale_inr(base.get("success_gmv"), amount_scale)
        failed_gmv = scale_inr(base.get("failed_gmv"), amount_scale)
        avg_ticket_success = scale_inr(base.get("avg_ticket_success"), amount_scale)
        success_rate = _to_num(base.get("success_rate_pct"), 0.0)

        daily = conn.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS attempts_24h,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_24h,
                    SUM(CASE WHEN status IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS fail_24h,
                    SUM(CASE WHEN status IN {FAILED_STATUS_SQL}
                              AND (response_code IS NULL OR TRIM(response_code) = '' OR UPPER(TRIM(response_code)) = 'UNKNOWN')
                              AND UPPER(COALESCE(TRIM(payment_mode), '')) = 'UPI'
                             THEN 1 ELSE 0 END) AS upi_unmapped_fail_24h,
                    SUM(CASE WHEN status IN {FAILED_STATUS_SQL}
                              AND UPPER(COALESCE(TRIM(payment_mode), '')) = 'UPI'
                             THEN amount_rupees ELSE 0 END) AS upi_failed_gmv_24h
                FROM {table}
                WHERE merchant_id = :mid
                  AND p_date >= :today_start
                  AND p_date < :today_end
                """
            ),
            params,
        ).mappings().first()

        prev7 = conn.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS attempts_prev7,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_prev7,
                    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS success_rate_prev7
                FROM {table}
                WHERE merchant_id = :mid
                  AND p_date >= :prev7_start
                  AND p_date < :prev7_end
                """
            ),
            params,
        ).mappings().first()

        attempts_24h = _to_int((daily or {}).get("attempts_24h"), 0)
        success_24h = _to_int((daily or {}).get("success_24h"), 0)
        fail_24h = _to_int((daily or {}).get("fail_24h"), 0)
        success_rate_24h = round((100.0 * success_24h / attempts_24h), 2) if attempts_24h else 0.0
        success_rate_7d_avg = _to_num((prev7 or {}).get("success_rate_prev7"), 0.0)
        success_rate_drop_pp = max(0.0, success_rate_7d_avg - success_rate_24h)

        upi_unmapped_fail_24h = _to_int((daily or {}).get("upi_unmapped_fail_24h"), 0)
        upi_failed_gmv_24h = scale_inr((daily or {}).get("upi_failed_gmv_24h"), amount_scale)

        top_failures = conn.execute(
            text(
                f"""
                SELECT
                    CASE
                        WHEN response_code IS NULL OR TRIM(response_code) = '' OR UPPER(TRIM(response_code)) = 'UNKNOWN'
                            THEN CASE WHEN UPPER(COALESCE(TRIM(payment_mode), '')) = 'UPI' THEN 'UPI_FAILURE' ELSE 'UNMAPPED_FAILURE' END
                        ELSE UPPER(TRIM(response_code))
                    END AS response_code,
                    COUNT(*) AS c
                FROM {table}
                WHERE merchant_id = :mid
                  AND p_date >= :start_date
                  AND p_date < :end_date
                  AND status IN {FAILED_STATUS_SQL}
                GROUP BY 1
                ORDER BY c DESC
                LIMIT 3
                """
            ),
            params,
        ).fetchall()

        top_failure_codes = ", ".join([f"{row[0]} ({row[1]})" for row in top_failures if row[0]]) or "none"

        callback_delay_today = 0.0
        callback_delay_prev7 = 0.0
        try:
            callback_delay_today = _to_num(
                conn.execute(
                    text(
                        f"""
                        SELECT percentile_cont(0.95)
                               WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - initiated_at)) * 1000)
                        FROM {table}
                        WHERE merchant_id = :mid
                          AND UPPER(COALESCE(payment_mode, '')) = 'UPI'
                          AND initiated_at IS NOT NULL
                          AND completed_at IS NOT NULL
                          AND p_date >= :today_start
                          AND p_date < :today_end
                        """
                    ),
                    params,
                ).scalar(),
                0.0,
            )
            callback_delay_prev7 = _to_num(
                conn.execute(
                    text(
                        f"""
                        SELECT percentile_cont(0.95)
                               WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - initiated_at)) * 1000)
                        FROM {table}
                        WHERE merchant_id = :mid
                          AND UPPER(COALESCE(payment_mode, '')) = 'UPI'
                          AND initiated_at IS NOT NULL
                          AND completed_at IS NOT NULL
                          AND p_date >= :prev7_start
                          AND p_date < :prev7_end
                        """
                    ),
                    params,
                ).scalar(),
                0.0,
            )
        except Exception:
            logger.debug("Callback delay percentile not available for this backend.")

        callback_delay_ratio = (
            callback_delay_today / callback_delay_prev7
            if callback_delay_prev7 > 0
            else 0.0
        )

        refunds_today = conn.execute(
            text(
                """
                SELECT COUNT(*) AS refund_count, COALESCE(SUM(refund_amount), 0) AS refund_gmv
                FROM refunds
                WHERE mid = :mid
                  AND p_date >= :today_start
                  AND p_date < :today_end
                """
            ),
            params,
        ).mappings().first() or {}

        refunds_prev7 = conn.execute(
            text(
                """
                SELECT COUNT(*) AS refund_count, COALESCE(SUM(refund_amount), 0) AS refund_gmv
                FROM refunds
                WHERE mid = :mid
                  AND p_date >= :prev7_start
                  AND p_date < :prev7_end
                """
            ),
            params,
        ).mappings().first() or {}

        refund_count_24h = _to_int(refunds_today.get("refund_count"), 0)
        refund_gmv_24h = scale_inr(refunds_today.get("refund_gmv"), amount_scale)
        refund_count_prev7 = _to_int(refunds_prev7.get("refund_count"), 0)
        attempts_prev7 = _to_int((prev7 or {}).get("attempts_prev7"), 0)

        refund_rate_24h = (100.0 * refund_count_24h / attempts_24h) if attempts_24h else 0.0
        refund_rate_7d_avg = (100.0 * refund_count_prev7 / attempts_prev7) if attempts_prev7 else 0.0

        due_end = max_date + dt.timedelta(days=2)
        chargeback_due = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) AS due_count,
                    COALESCE(SUM(chargeback_amount), 0) AS due_gmv
                FROM chargebacks
                WHERE mid = :mid
                  AND response_due_date IS NOT NULL
                  AND response_due_date >= :today_start
                  AND response_due_date <= :due_end
                  AND (
                    resolution_outcome IS NULL
                    OR TRIM(resolution_outcome) = ''
                    OR UPPER(TRIM(resolution_outcome)) IN ('OPEN', 'PENDING', 'IN_PROGRESS')
                  )
                """
            ),
            {"mid": merchant_id, "today_start": today_start, "due_end": due_end},
        ).mappings().first() or {}

        delayed_cutoff = max_date - dt.timedelta(days=2)
        settlements_delayed = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) AS delayed_count,
                    SUM(CASE WHEN UPPER(COALESCE(settlement_status, '')) IN ('HELD', 'ON_HOLD', 'HOLD') THEN 1 ELSE 0 END) AS held_count,
                    COALESCE(SUM(net_settlement_amount), 0) AS delayed_amount
                FROM settlements
                WHERE mid = :mid
                  AND UPPER(COALESCE(settlement_status, '')) IN ('PENDING', 'HELD', 'ON_HOLD', 'HOLD')
                  AND settlement_date <= :delayed_cutoff
                """
            ),
            {"mid": merchant_id, "delayed_cutoff": delayed_cutoff},
        ).mappings().first() or {}

        p95_success_ticket = 0.0
        try:
            p95_success_ticket = scale_inr(
                conn.execute(
                    text(
                        f"""
                        SELECT percentile_cont(0.95)
                               WITHIN GROUP (ORDER BY amount_rupees)
                        FROM {table}
                        WHERE merchant_id = :mid
                          AND status = 'SUCCESS'
                          AND p_date >= :start_date
                          AND p_date < :end_date
                        """
                    ),
                    params,
                ).scalar(),
                amount_scale,
            )
        except Exception:
            p95_success_ticket = avg_ticket_success * 2 if avg_ticket_success else 0.0

        high_value_threshold = p95_success_ticket if p95_success_ticket > 0 else (avg_ticket_success * 2 if avg_ticket_success else 0)
        high_value_failed = conn.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS failed_count,
                    COALESCE(SUM(amount_rupees), 0) AS failed_gmv
                FROM {table}
                WHERE merchant_id = :mid
                  AND status IN {FAILED_STATUS_SQL}
                  AND p_date >= :start_date
                  AND p_date < :end_date
                  AND amount_rupees >= :high_value_threshold
                """
            ),
            {**params, "high_value_threshold": high_value_threshold / (amount_scale or 1.0)},
        ).mappings().first() or {}

        top_terminal = conn.execute(
            text(
                f"""
                SELECT
                    terminal_id,
                    COUNT(*) AS attempts,
                    SUM(CASE WHEN status IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) AS fail_txns,
                    ROUND(100.0 * SUM(CASE WHEN status IN {FAILED_STATUS_SQL} THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS fail_rate_pct
                FROM {table}
                WHERE merchant_id = :mid
                  AND terminal_id IS NOT NULL
                  AND TRIM(terminal_id) <> ''
                  AND p_date >= :start_date
                  AND p_date < :end_date
                GROUP BY terminal_id
                HAVING COUNT(*) >= 20
                ORDER BY fail_rate_pct DESC, attempts DESC
                LIMIT 1
                """
            ),
            params,
        ).mappings().first() or {}

        merchant_fail_rate_pct = round((100.0 * fail_txns_total / attempts_total), 2) if attempts_total else 0.0
        top_terminal_fail_rate = _to_num(top_terminal.get("fail_rate_pct"), 0.0)
        terminal_fail_ratio = top_terminal_fail_rate / merchant_fail_rate_pct if merchant_fail_rate_pct > 0 else 0.0

        kyc_end = max_date + dt.timedelta(days=14)
        kyc_expiry = conn.execute(
            text(
                """
                SELECT
                    SUM(CASE WHEN expiry_date >= :today_start AND expiry_date <= :kyc_end THEN 1 ELSE 0 END) AS expiring_14d_count,
                    SUM(CASE WHEN expiry_date < :today_start THEN 1 ELSE 0 END) AS overdue_count
                FROM merchant_kyc_documents
                WHERE mid = :mid
                  AND expiry_date IS NOT NULL
                  AND UPPER(COALESCE(kyc_status, 'PENDING')) <> 'REJECTED'
                """
            ),
            {"mid": merchant_id, "today_start": today_start, "kyc_end": kyc_end},
        ).mappings().first() or {}

        metrics.update(
            {
                "merchant_id": merchant_id,
                "window_start": str(start_date),
                "window_end": str(end_date - dt.timedelta(days=1)),
                "attempts_total": attempts_total,
                "success_txns_total": success_txns_total,
                "fail_txns_total": fail_txns_total,
                "success_rate_total": success_rate,
                "success_revenue": success_revenue,
                "avg_ticket_success": avg_ticket_success,
                "failed_gmv_total": failed_gmv,
                "attempts_24h": attempts_24h,
                "fail_txns_24h": fail_24h,
                "success_rate_24h": success_rate_24h,
                "success_rate_7d_avg": success_rate_7d_avg,
                "success_rate_drop_pp": round(success_rate_drop_pp, 2),
                "success_rate_change_pp": round(success_rate_24h - success_rate_7d_avg, 2),
                "top_failure_codes": top_failure_codes,
                "upi_unmapped_fail_24h": upi_unmapped_fail_24h,
                "upi_failed_gmv_24h": upi_failed_gmv_24h,
                "callback_delay_p95_ms_today": round(callback_delay_today, 2),
                "callback_delay_p95_ms_7d_avg": round(callback_delay_prev7, 2),
                "callback_delay_ratio": round(callback_delay_ratio, 2),
                "refund_count_24h": refund_count_24h,
                "refund_gmv_24h": refund_gmv_24h,
                "refund_rate_24h": round(refund_rate_24h, 2),
                "refund_rate_7d_avg": round(refund_rate_7d_avg, 2),
                "chargeback_due_48h_count": _to_int(chargeback_due.get("due_count"), 0),
                "chargeback_due_48h_gmv": scale_inr(chargeback_due.get("due_gmv"), amount_scale),
                "settlement_delayed_count": _to_int(settlements_delayed.get("delayed_count"), 0),
                "settlement_held_count": _to_int(settlements_delayed.get("held_count"), 0),
                "settlement_delayed_amount": scale_inr(settlements_delayed.get("delayed_amount"), amount_scale),
                "high_value_ticket_threshold": round(high_value_threshold, 2),
                "high_value_failed_count": _to_int(high_value_failed.get("failed_count"), 0),
                "high_value_failed_gmv": scale_inr(high_value_failed.get("failed_gmv"), amount_scale),
                "merchant_fail_rate_pct": merchant_fail_rate_pct,
                "top_terminal_id": str(top_terminal.get("terminal_id") or "N/A"),
                "top_terminal_attempts": _to_int(top_terminal.get("attempts"), 0),
                "top_terminal_fail_txns": _to_int(top_terminal.get("fail_txns"), 0),
                "top_terminal_fail_rate_pct": top_terminal_fail_rate,
                "terminal_fail_ratio": round(terminal_fail_ratio, 2),
                "kyc_expiring_14d_count": _to_int(kyc_expiry.get("expiring_14d_count"), 0),
                "kyc_overdue_count": _to_int(kyc_expiry.get("overdue_count"), 0),
                "signal_confidence": _confidence_from_volume(attempts_total),
            }
        )

        # Derived impact helpers used by card templates.
        metrics["impact_sr_drop_revenue"] = round(
            attempts_24h * (metrics["success_rate_drop_pp"] / 100.0) * avg_ticket_success,
            2,
        )
        metrics["impact_callback_delay_revenue"] = round(upi_failed_gmv_24h * 0.35, 2)
        metrics["impact_refund_reduction"] = round(refund_gmv_24h * 0.3, 2)
        metrics["impact_chargeback_at_risk"] = round(metrics["chargeback_due_48h_gmv"], 2)
        metrics["impact_settlement_blocked"] = round(metrics["settlement_delayed_amount"], 2)
        metrics["impact_high_value_failed"] = round(metrics["high_value_failed_gmv"], 2)
        metrics["impact_terminal_anomaly"] = round(
            _to_num(metrics["top_terminal_fail_txns"], 0) * avg_ticket_success * 0.25,
            2,
        )

    return metrics


def generate_insight_cards(
    engine,
    merchant_id: str,
    window_days: int = 30,
    cards_dir: Path | None = None,
) -> list[dict[str, Any]]:
    templates = load_card_templates(cards_dir)
    if not templates:
        return []

    metrics = _build_metrics(engine, merchant_id, window_days)
    if not metrics:
        return []

    cards: list[dict[str, Any]] = []
    for template in templates:
        if template.trigger_tool != "compute_kpis":
            continue
        if not evaluate_trigger(template.condition, metrics):
            continue

        impact = None
        if template.impact_metric:
            impact = _to_num(metrics.get(template.impact_metric), 0.0)
            if impact <= 0:
                impact = None

        confidence = _to_num(metrics.get(template.confidence_metric), metrics.get("signal_confidence", 0.6))
        confidence = max(0.0, min(1.0, confidence))

        actions = [_render_text(a, metrics) for a in template.actions]
        card = {
            "id": template.card_id,
            "icon": template.icon,
            "title": _render_text(template.title, metrics),
            "type": template.severity,
            "confidence": confidence,
            "impact_rupees": impact,
            "drivers": [
                f"Window: {metrics.get('window_start')} to {metrics.get('window_end')}",
                f"Attempts: {int(metrics.get('attempts_total') or 0):,}",
                f"Top failure codes: {metrics.get('top_failure_codes')}",
            ],
            "actions": actions,
            "body": _render_text(template.explanation, metrics),
        }
        cards.append(card)

    return cards

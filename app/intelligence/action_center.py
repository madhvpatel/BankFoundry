from sqlalchemy import text
import json
import logging

from app.data.evidence import normalize_evidence_ids

logger = logging.getLogger("action_center")

LOW_SIGNAL_TITLES = {
    "improve data quality",
    "foster collaboration",
    "monitor performance metrics",
}

ACTIVE_STATUSES = {"OPEN", "REQUESTED", "IN_PROGRESS"}


def _normalize_text(value) -> str:
    return str(value or "").strip().lower()


def _is_low_signal_action(action) -> bool:
    title = _normalize_text(action.get("title"))
    return title in LOW_SIGNAL_TITLES


def _is_action_eligible(action) -> bool:
    if _is_low_signal_action(action):
        return False

    title = str(action.get("title") or "").strip()
    description = str(action.get("description") or "").strip()
    if not title or len(description) < 12:
        return False

    try:
        confidence = float(action.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    try:
        priority = float(action.get("priority_score") or 0.0)
    except Exception:
        priority = 0.0
    try:
        impact = float(action.get("impact_rupees") or 0.0)
    except Exception:
        impact = 0.0
    evidence_ids = normalize_evidence_ids(action.get("evidence_ids"))
    workflow_steps = action.get("workflow_steps") or action.get("actions") or []
    has_workflow = any(str(step.get("text") if isinstance(step, dict) else step).strip() for step in workflow_steps)

    if confidence < 0.35:
        return False
    if priority <= 0.0 and impact <= 0.0:
        return False
    if not evidence_ids and not has_workflow and impact <= 0.0:
        return False
    return True


def _evidence_payload(action) -> dict:
    return {
        "source": str(action.get("source") or "unknown"),
        "evidence_ids": normalize_evidence_ids(action.get("evidence_ids")),
        "workflow_steps": list(action.get("workflow_steps") or action.get("actions") or []),
        "payload": action.get("evidence", {}),
    }


def create_action(engine, mid, action):
    if not _is_action_eligible(action):
        return None

    try:
        with engine.connect() as conn:
            existing = conn.execute(
                text(
                    """
                    SELECT action_id
                    FROM merchant_actions
                    WHERE mid = :mid
                      AND LOWER(COALESCE(category, '')) = :category
                      AND LOWER(COALESCE(title, '')) = :title
                      AND LOWER(COALESCE(description, '')) = :description
                      AND UPPER(COALESCE(status, 'OPEN')) IN ('OPEN', 'REQUESTED', 'IN_PROGRESS')
                    ORDER BY action_id DESC
                    LIMIT 1
                    """
                ),
                {
                    "mid": mid,
                    "category": _normalize_text(action.get("category")),
                    "title": _normalize_text(action.get("title")),
                    "description": _normalize_text(action.get("description")),
                },
            ).fetchone()
            if existing:
                return existing[0]

            result = conn.execute(text("""

            INSERT INTO merchant_actions
            (mid, category, title, description,
             impact_rupees, confidence, priority_score,
             owner, evidence)

            VALUES
            (:mid, :category, :title, :description,
             :impact, :confidence, :priority,
             :owner, :evidence)

            """), {

                "mid": mid,
                "category": action.get("category"),
                "title": action.get("title"),
                "description": action.get("description"),
                "impact": action.get("impact_rupees"),
                "confidence": action.get("confidence"),
                "priority": action.get("priority_score"),
                "owner": action.get("owner"),
                "evidence": json.dumps(_evidence_payload(action), default=str)

            })

            conn.commit()
            try:
                return result.lastrowid
            except Exception:
                return None

    except Exception as e:

        logger.error(f"action creation error {e}")
        return None

def get_actions(engine, mid):

    with engine.connect() as conn:

        rows = conn.execute(text("""

        SELECT action_id,
               title,
               category,
               impact_rupees,
               confidence,
               status,
               created_at

        FROM merchant_actions

        WHERE mid = :mid

        ORDER BY priority_score DESC

        """), {"mid": mid}).fetchall()

        return rows

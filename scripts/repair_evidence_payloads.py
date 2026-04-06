#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.data.evidence import normalize_evidence_ids
from config import Config


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _normalize_payload_evidence(value: Any) -> tuple[str, bool]:
    if not isinstance(value, str):
        return _json_dump({}), True
    try:
        payload = json.loads(value)
    except Exception:
        return _json_dump({}), True
    if not isinstance(payload, dict):
        return _json_dump({}), True

    changed = False
    if "evidence_ids" in payload:
        normalized = normalize_evidence_ids(payload.get("evidence_ids"))
        if payload.get("evidence_ids") != normalized:
            payload["evidence_ids"] = normalized
            changed = True

    normalized_text = _json_dump(payload)
    return normalized_text, changed or normalized_text != value


def _repair_ops_cases(engine: Any) -> dict[str, Any]:
    updated = 0
    touched_case_ids: list[str] = []

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT case_id, evidence_ids_json, source_payload_json
                FROM ops_cases
                """
            )
        ).mappings().all()

        for row in rows:
            case_id = str(row.get("case_id") or "").strip()
            current_evidence = row.get("evidence_ids_json")
            normalized_evidence = _json_dump(normalize_evidence_ids(current_evidence))
            normalized_payload, payload_changed = _normalize_payload_evidence(row.get("source_payload_json"))
            evidence_changed = normalized_evidence != current_evidence

            if not case_id or not evidence_changed and not payload_changed:
                continue

            conn.execute(
                text(
                    """
                    UPDATE ops_cases
                    SET evidence_ids_json = :evidence_ids_json,
                        source_payload_json = :source_payload_json,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE case_id = :case_id
                    """
                ),
                {
                    "case_id": case_id,
                    "evidence_ids_json": normalized_evidence,
                    "source_payload_json": normalized_payload,
                },
            )
            updated += 1
            touched_case_ids.append(case_id)

    return {"updated_rows": updated, "case_ids": touched_case_ids}


def _repair_proactive_cards(engine: Any) -> dict[str, Any]:
    updated = 0
    touched_keys: list[str] = []

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT dedupe_key, evidence_ids, payload_json
                FROM proactive_cards
                """
            )
        ).mappings().all()

        for row in rows:
            dedupe_key = str(row.get("dedupe_key") or "").strip()
            current_evidence = row.get("evidence_ids")
            normalized_evidence = _json_dump(normalize_evidence_ids(current_evidence))
            normalized_payload, payload_changed = _normalize_payload_evidence(row.get("payload_json"))
            evidence_changed = normalized_evidence != current_evidence

            if not dedupe_key or not evidence_changed and not payload_changed:
                continue

            conn.execute(
                text(
                    """
                    UPDATE proactive_cards
                    SET evidence_ids = :evidence_ids,
                        payload_json = :payload_json,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE dedupe_key = :dedupe_key
                    """
                ),
                {
                    "dedupe_key": dedupe_key,
                    "evidence_ids": normalized_evidence,
                    "payload_json": normalized_payload,
                },
            )
            updated += 1
            touched_keys.append(dedupe_key)

    return {"updated_rows": updated, "dedupe_keys": touched_keys}


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize corrupted evidence payloads in proactive and ops tables.")
    parser.add_argument("--database-url", default=Config.DATABASE_URL, help="SQLAlchemy database URL")
    args = parser.parse_args()

    engine = create_engine(args.database_url)
    ops_result = _repair_ops_cases(engine)
    proactive_result = _repair_proactive_cards(engine)

    print(
        _json_dump(
            {
                "database_url": args.database_url,
                "ops_cases": ops_result,
                "proactive_cards": proactive_result,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

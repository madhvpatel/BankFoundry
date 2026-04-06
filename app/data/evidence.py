from __future__ import annotations

import ast
import json
from typing import Any


MAX_EVIDENCE_IDS = 80
MAX_EVIDENCE_ID_LENGTH = 256
MAX_EVIDENCE_CONTAINER_TEXT_LENGTH = 8192
MAX_EVIDENCE_PARSE_DEPTH = 8

_SKIP_CONTAINER = object()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _maybe_parse_container(text_value: str) -> Any:
    candidate = text_value.strip()
    if not candidate:
        return None
    if len(candidate) > MAX_EVIDENCE_CONTAINER_TEXT_LENGTH:
        if candidate[0] in "[{(" or (candidate[0] in "\"'" and candidate[-1] == candidate[0]):
            return _SKIP_CONTAINER
        return None

    looks_encoded = (
        (candidate[0] == "[" and candidate[-1] == "]")
        or (candidate[0] == "{" and candidate[-1] == "}")
        or (candidate[0] == "(" and candidate[-1] == ")")
        or (candidate[0] in "\"'" and candidate[-1] == candidate[0])
    )
    if not looks_encoded:
        return None

    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(candidate)
        except Exception:
            continue
        if parsed == candidate:
            return None
        return parsed
    return None


def normalize_evidence_ids(
    value: Any,
    *,
    limit: int = MAX_EVIDENCE_IDS,
    max_item_length: int = MAX_EVIDENCE_ID_LENGTH,
    max_depth: int = MAX_EVIDENCE_PARSE_DEPTH,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def add_item(raw_value: Any) -> None:
        text_value = _clean_text(raw_value)
        if text_value is None:
            return
        if len(text_value) > max_item_length:
            text_value = text_value[:max_item_length]
        if text_value in seen:
            return
        seen.add(text_value)
        out.append(text_value)

    def visit(node: Any, depth: int) -> None:
        if depth > max_depth or len(out) >= limit or node is None:
            return
        if isinstance(node, dict):
            for key in ("evidence_ids", "evidence_id", "id", "ref"):
                if key in node:
                    visit(node.get(key), depth + 1)
                    if len(out) >= limit:
                        return
            return
        if isinstance(node, (list, tuple, set)):
            for item in node:
                visit(item, depth + 1)
                if len(out) >= limit:
                    return
            return

        text_value = _clean_text(node)
        if text_value is None:
            return
        parsed = _maybe_parse_container(text_value)
        if parsed is _SKIP_CONTAINER:
            return
        if parsed is not None:
            visit(parsed, depth + 1)
            return
        add_item(text_value)

    visit(value, 0)
    return out


def merge_evidence_ids(*values: Any, limit: int = MAX_EVIDENCE_IDS) -> list[str]:
    return normalize_evidence_ids(list(values), limit=limit)

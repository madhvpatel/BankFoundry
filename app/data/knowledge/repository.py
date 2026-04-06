from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from app.project_paths import repo_path, repo_root


def _repo_root() -> Path:
    return repo_root()


def _kb_root() -> Path:
    return repo_path("bank_kb")


def _tokenize(value: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9]+", str(value or "").lower()) if len(token) > 2]


def _split_markdown(text: str) -> list[tuple[str, str]]:
    content = str(text or "").strip()
    if not content:
        return []

    chunks: list[tuple[str, str]] = []
    for part in re.split(r"\n(?=#+\s+)", content):
        block = part.strip()
        if not block:
            continue
        heading = block.splitlines()[0].strip()
        match = re.match(r"^#+\s+(.+)$", heading)
        chunks.append((match.group(1).strip() if match else "", block))
    return chunks or [("", content)]


def retrieve_payments_knowledge(*, query: str, top_k: int = 3) -> dict[str, Any]:
    search_query = str(query or "").strip()
    if not search_query:
        return {"query": "", "results": [], "evidence": [], "error": "query is required"}

    tokens = _tokenize(search_query)
    if not tokens:
        return {"query": search_query, "results": [], "evidence": [], "error": "query does not contain searchable terms"}

    kb_root = _kb_root()
    if not kb_root.exists():
        return {"query": search_query, "results": [], "evidence": [], "error": "bank_kb directory not found"}

    scored: list[dict[str, Any]] = []
    for path in sorted(kb_root.rglob("*.md")):
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            continue

        rel_path = str(path.relative_to(_repo_root()))
        for index, (title, chunk_text) in enumerate(_split_markdown(content), start=1):
            haystack = f"{title}\n{chunk_text}".lower()
            score = sum(haystack.count(token) for token in tokens)
            if search_query.lower() in haystack:
                score += len(tokens) + 3
            if score <= 0:
                continue
            chunk_id = f"{rel_path}::chunk{index}"
            scored.append(
                {
                    "score": float(score),
                    "chunk_id": chunk_id,
                    "source_path": rel_path,
                    "title": title or path.stem.replace("_", " ").title(),
                    "text": chunk_text[:1200],
                    "evidence_id": f"kb:{chunk_id}",
                }
            )

    scored.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("source_path") or ""), str(item.get("chunk_id") or "")))
    limit = max(1, min(int(top_k or 3), 10))
    results = scored[:limit]
    return {
        "query": search_query,
        "results": results,
        "evidence": [str(item.get("evidence_id") or "") for item in results if str(item.get("evidence_id") or "").strip()],
        "error": None,
    }

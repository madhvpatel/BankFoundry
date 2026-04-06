from __future__ import annotations

import hashlib
import os
import pickle
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

from app.project_paths import repo_path, repo_root


@dataclass
class KBChunk:
    chunk_id: str
    source_path: str  # repo-relative
    title: str
    text: str


@dataclass
class KBIndex:
    version: str
    root_dir: str
    file_hash: str
    vectorizer: TfidfVectorizer
    matrix: Any  # sparse
    chunks: list[KBChunk]


def _repo_root() -> Path:
    return repo_root()


def _kb_root() -> Path:
    return repo_path("bank_kb")


def _index_path() -> Path:
    return repo_path(".kb_index.pkl")


def _iter_md_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    files = [p for p in root.rglob("*.md") if p.is_file()]
    # Exclude README-ish files from chunking? Keep them; they can still be useful.
    return sorted(files)


def _hash_files(paths: list[Path]) -> str:
    h = hashlib.sha256()
    for p in paths:
        try:
            h.update(str(p.relative_to(_repo_root())).encode("utf-8"))
            h.update(b"\0")
            h.update(p.read_bytes())
            h.update(b"\0")
        except Exception:
            continue
    return h.hexdigest()


def _split_markdown(text: str) -> list[tuple[str, str]]:
    """Return list of (title, chunk_text).

    Simple chunker: split by headings; fallback to paragraph blocks.
    """
    text = (text or "").strip()
    if not text:
        return []

    # Split by headings
    parts = re.split(r"\n(?=#+\s+)", text)
    chunks: list[tuple[str, str]] = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        m = re.match(r"^(#+)\s+(.+)$", part.splitlines()[0].strip())
        title = m.group(2).strip() if m else ""
        chunks.append((title, part))

    # If we didn't get meaningful splits, do paragraph-based chunking.
    if len(chunks) <= 1:
        paras = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
        out: list[tuple[str, str]] = []
        buf: list[str] = []
        for para in paras:
            buf.append(para)
            if sum(len(x) for x in buf) > 900:
                out.append(("", "\n\n".join(buf)))
                buf = []
        if buf:
            out.append(("", "\n\n".join(buf)))
        return out

    return chunks


def reindex_kb() -> dict[str, Any]:
    """Build a TF-IDF index over bank_kb/*.md.

    Returns metadata and writes `.kb_index.pkl` at repo root.
    """
    root = _kb_root()
    files = _iter_md_files(root)
    file_hash = _hash_files(files)

    chunks: list[KBChunk] = []
    for p in files:
        rel = str(p.relative_to(_repo_root()))
        try:
            md = p.read_text(encoding="utf-8")
        except Exception:
            continue

        for i, (title, chunk_text) in enumerate(_split_markdown(md), start=1):
            chunk_id = f"{rel}::chunk{i}"
            chunks.append(
                KBChunk(
                    chunk_id=chunk_id,
                    source_path=rel,
                    title=title,
                    text=chunk_text.strip(),
                )
            )

    texts = [c.text for c in chunks]
    vectorizer = TfidfVectorizer(
        strip_accents="unicode",
        lowercase=True,
        stop_words="english",
        max_features=50_000,
        ngram_range=(1, 2),
    )

    if texts:
        matrix = vectorizer.fit_transform(texts)
    else:
        matrix = vectorizer.fit_transform(["empty"])  # keep valid object

    idx = KBIndex(
        version="tfidf-v1",
        root_dir=str(root),
        file_hash=file_hash,
        vectorizer=vectorizer,
        matrix=matrix,
        chunks=chunks,
    )

    with _index_path().open("wb") as f:
        pickle.dump(idx, f)

    return {
        "ok": True,
        "version": idx.version,
        "root": str(root),
        "files": len(files),
        "chunks": len(chunks),
        "index_path": str(_index_path()),
        "file_hash": file_hash,
        "evidence": [f"kb_index:{file_hash}"] ,
    }


def _load_index() -> KBIndex | None:
    p = _index_path()
    if not p.exists():
        return None
    try:
        with p.open("rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def search_kb(*, query: str, top_k: int = 5) -> dict[str, Any]:
    """Search bank KB for relevant snippets.

    Output contains chunks + evidence IDs of the form `kb:<path>::chunkN`.
    """
    query = (query or "").strip()
    if not query:
        return {"query": query, "results": [], "evidence": []}

    idx = _load_index()
    if not idx:
        # Best-effort auto-index.
        reindex_kb()
        idx = _load_index()

    if not idx or not idx.chunks:
        return {"query": query, "results": [], "evidence": []}

    qv = idx.vectorizer.transform([query])
    scores = (idx.matrix @ qv.T).toarray().reshape(-1)

    order = np.argsort(-scores)[: max(1, int(top_k))]

    results: list[dict[str, Any]] = []
    evidence: list[str] = []
    for j in order:
        s = float(scores[j])
        if s <= 0:
            continue
        c = idx.chunks[int(j)]
        evidence_id = f"kb:{c.chunk_id}"
        evidence.append(evidence_id)
        results.append(
            {
                "score": round(s, 4),
                "chunk_id": c.chunk_id,
                "source_path": c.source_path,
                "title": c.title,
                "text": c.text[:1200],
                "evidence_id": evidence_id,
            }
        )

    return {"query": query, "results": results, "evidence": evidence}

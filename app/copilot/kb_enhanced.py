"""
kb_enhanced.py — Enhanced RAG retrieval layer.

Improvements over kb.py:
  1. Intent-aware query expansion  — expands the raw query using the detected intent
     before computing TF-IDF similarity. Free: no extra model call.
  2. Hybrid retrieval              — combines TF-IDF (semantic) with BM25 (exact term)
     scores via score fusion. Better recall for response codes, settlement IDs, etc.
  3. Chunk-level usage tracking    — logs which chunks contributed to an answer.
     Feeds back into corpus quality analysis over time.

Usage (drop-in replacement for search_kb):
    from app.copilot.kb_enhanced import search_kb_enhanced
    results = search_kb_enhanced(query="settlement shortfall", intent="exact_shortfall", top_k=5)
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Any

import numpy as np

from app.copilot.kb import KBIndex, _load_index, reindex_kb

logger = logging.getLogger("kb_enhanced")


# ---------------------------------------------------------------------------
# Query expansion vocabulary (intent → extra search terms)
# ---------------------------------------------------------------------------

INTENT_EXPANSION: dict[str, str] = {
    "exact_shortfall":         "shortfall net payout deduction gross settlement difference",
    "recent_settlements":      "settlement payout cashflow status HELD PENDING",
    "settlement_total":        "total payout settlement amount gross net",
    "recent_transactions":     "transactions failed success UPI card wallet response code",
    "recent_chargebacks":      "chargeback dispute reason code deadline response",
    "recent_refunds":          "refund status amount created",
    "why_payments_failing":    "payment failure terminal response code decline retry routing",
    "success_rate_drop":       "success rate approval rate decline driver response code",
    "what_changed":            "KPI delta change variance period comparison",
    "top_growth_opportunities": "growth revenue opportunity acceptance rate upsell",
    "business_overview":       "KPIs performance revenue transactions settlements overview",
    "operational_risks":       "risk operational failure terminal hold chargeback",
    "terminal_expansion":      "terminal POS device health battery network failure",
}


def _expand_query(query: str, intent: str | None) -> str:
    """Append intent-specific expansion terms to the query."""
    expansion = INTENT_EXPANSION.get(str(intent or "").strip().lower(), "")
    if expansion:
        return f"{query} {expansion}"
    return query


# ---------------------------------------------------------------------------
# BM25 (simplified) scoring
# ---------------------------------------------------------------------------

def _bm25_scores(
    query_tokens: list[str],
    chunks_text: list[str],
    *,
    k1: float = 1.5,
    b: float = 0.75,
) -> np.ndarray:
    """
    Simplified BM25 scoring.
    Good for exact-term matching of response codes, entity IDs, and domain terms.
    """
    if not chunks_text or not query_tokens:
        return np.zeros(len(chunks_text))

    # Document lengths
    doc_lens = np.array([len(t.split()) for t in chunks_text], dtype=float)
    avg_dl = doc_lens.mean() if doc_lens.size else 1.0

    n_docs = len(chunks_text)
    scores = np.zeros(n_docs)

    for token in query_tokens:
        token_lower = token.lower()
        # df = number of docs containing token
        tf_vec = np.array([
            len(re.findall(re.escape(token_lower), t.lower()))
            for t in chunks_text
        ], dtype=float)
        df = np.count_nonzero(tf_vec)
        if df == 0:
            continue
        idf = np.log((n_docs - df + 0.5) / (df + 0.5) + 1)
        tf_norm = tf_vec * (k1 + 1) / (tf_vec + k1 * (1 - b + b * doc_lens / avg_dl))
        scores += idf * tf_norm

    return scores


# ---------------------------------------------------------------------------
# Score fusion
# ---------------------------------------------------------------------------

def _min_max_normalize(arr: np.ndarray) -> np.ndarray:
    """Normalize array to [0, 1]. Returns zeros if all values are equal."""
    lo, hi = arr.min(), arr.max()
    if hi - lo < 1e-9:
        return np.zeros_like(arr, dtype=float)
    return (arr - lo) / (hi - lo)


def _fuse_scores(
    tfidf_scores: np.ndarray,
    bm25_scores: np.ndarray,
    *,
    tfidf_weight: float = 0.55,
    bm25_weight: float = 0.45,
) -> np.ndarray:
    """Reciprocal Rank Fusion (simplified) over the two score arrays."""
    tfidf_norm = _min_max_normalize(tfidf_scores)
    bm25_norm  = _min_max_normalize(bm25_scores)
    return tfidf_weight * tfidf_norm + bm25_weight * bm25_norm


# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------

_usage_log: list[dict[str, Any]] = []


def record_chunk_usage(*, chunk_id: str, intent: str, validated: bool) -> None:
    """
    Record that a KB chunk was included in a response.

    Parameters
    ----------
    chunk_id   : KBChunk.chunk_id
    intent     : the detected intent for this turn
    validated  : whether the final answer was validated as "clean"
    """
    _usage_log.append({
        "chunk_id": chunk_id,
        "intent": intent,
        "validated": validated,
        "recorded_at": dt.datetime.utcnow().isoformat(timespec="seconds"),
    })
    if len(_usage_log) > 5000:
        _usage_log.pop(0)


def get_usage_stats() -> dict[str, Any]:
    """Return summary statistics over recorded chunk usage."""
    if not _usage_log:
        return {"total": 0, "chunks": {}}
    chunk_counts: dict[str, dict[str, int]] = {}
    for entry in _usage_log:
        cid = entry["chunk_id"]
        if cid not in chunk_counts:
            chunk_counts[cid] = {"total": 0, "validated": 0}
        chunk_counts[cid]["total"] += 1
        if entry.get("validated"):
            chunk_counts[cid]["validated"] += 1
    return {
        "total": len(_usage_log),
        "chunks": chunk_counts,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def search_kb_enhanced(
    *,
    query: str,
    intent: str | None = None,
    top_k: int = 5,
    tfidf_weight: float = 0.55,
    bm25_weight: float = 0.45,
    min_score: float = 0.05,
) -> dict[str, Any]:
    """
    Enhanced KB search with query expansion and hybrid TF-IDF + BM25 scoring.

    Parameters
    ----------
    query         : raw search query (e.g. the merchant's question or the intent query)
    intent        : detected intent string (used for query expansion)
    top_k         : number of results to return
    tfidf_weight  : weight for TF-IDF semantic scores in fusion
    bm25_weight   : weight for BM25 exact-match scores in fusion
    min_score     : minimum fused score to include in results

    Returns the same dict shape as search_kb() for drop-in compatibility.
    """
    raw_query = (query or "").strip()
    if not raw_query:
        return {"query": raw_query, "intent": intent, "results": [], "evidence": []}

    idx: KBIndex | None = _load_index()
    if not idx:
        reindex_kb()
        idx = _load_index()

    if not idx or not idx.chunks:
        return {"query": raw_query, "intent": intent, "results": [], "evidence": []}

    # Step 1: Intent-aware query expansion
    expanded_query = _expand_query(raw_query, intent)
    logger.debug("KB expanded query: %r → %r", raw_query, expanded_query)

    # Step 2: TF-IDF scores
    try:
        qv = idx.vectorizer.transform([expanded_query])
        tfidf_raw = (idx.matrix @ qv.T).toarray().reshape(-1)
    except Exception as exc:
        logger.warning("KB TF-IDF failed: %s", exc)
        tfidf_raw = np.zeros(len(idx.chunks))

    # Step 3: BM25 scores on raw query tokens (no stop-word removal for exact matching)
    query_tokens = [t for t in re.split(r"\W+", raw_query) if len(t) >= 2]
    chunks_text = [c.text for c in idx.chunks]
    bm25_raw = _bm25_scores(query_tokens, chunks_text)

    # Step 4: Fuse scores
    fused = _fuse_scores(tfidf_raw, bm25_raw, tfidf_weight=tfidf_weight, bm25_weight=bm25_weight)

    # Step 5: Top-K selection
    order = np.argsort(-fused)[: max(1, int(top_k))]

    results: list[dict[str, Any]] = []
    evidence: list[str] = []

    for j in order:
        score_val = float(fused[int(j)])
        if score_val < min_score:
            continue
        c = idx.chunks[int(j)]
        evidence_id = f"kb:{c.chunk_id}"
        evidence.append(evidence_id)
        results.append({
            "score":        round(score_val, 4),
            "tfidf_score":  round(float(tfidf_raw[int(j)]), 4),
            "bm25_score":   round(float(bm25_raw[int(j)]), 4),
            "chunk_id":     c.chunk_id,
            "source_path":  c.source_path,
            "title":        c.title,
            "text":         c.text[:1200],
            "evidence_id":  evidence_id,
        })

    return {
        "query":          raw_query,
        "expanded_query": expanded_query,
        "intent":         intent,
        "results":        results,
        "evidence":       evidence,
    }

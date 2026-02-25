"""
scorer.py — Hybrid relevance scoring: keyword + semantic (embeddings) + recency.
"""
import asyncio
import hashlib
import logging
from datetime import datetime
from typing import Optional

import numpy as np
from openai import AsyncOpenAI
from rapidfuzz import fuzz

import storage

logger = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {"semantic": 0.4, "keyword": 0.4, "recency": 0.2}
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_MAX_AGE_HOURS = 48.0


# ---------------------------------------------------------------------------
# Keyword scoring (sync)
# ---------------------------------------------------------------------------

def keyword_score(text: str, title: str, topics: list[str]) -> float:
    """Direct port of score() from agent.py."""
    base = 0.0
    hay = (title or "") + "\n" + (text or "")
    hay_lower = hay.lower()
    for t in topics:
        base += hay_lower.count(t.lower())
        if title:
            base += fuzz.partial_ratio(t.lower(), title.lower()) / 100.0
    return base


def normalize_keyword_score(raw: float, max_observed: float) -> float:
    if max_observed <= 0:
        return 0.0
    return min(raw / max_observed, 1.0)


def recency_score(
    published_dt: Optional[datetime],
    now_dt: datetime,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
) -> float:
    """Linear decay from 1.0 (just published) to 0.0 (max_age_hours old). 0.5 if unknown."""
    if published_dt is None:
        return 0.5
    age_hours = (now_dt - published_dt).total_seconds() / 3600.0
    if age_hours < 0:
        return 1.0
    return max(0.0, 1.0 - age_hours / max_age_hours)


# ---------------------------------------------------------------------------
# Embedding helpers (async)
# ---------------------------------------------------------------------------

def _cache_key(model: str, text: str) -> str:
    payload = model + ":" + text[:500]
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def get_embedding(
    client: AsyncOpenAI,
    text: str,
    db,
    model: str = DEFAULT_EMBEDDING_MODEL,
) -> list[float]:
    key = _cache_key(model, text)
    cached = await storage.get_cached_embedding(db, key)
    if cached is not None:
        logger.debug("Embedding cache hit for key %s", key[:16])
        return cached

    resp = await client.embeddings.create(input=[text], model=model)
    vector = resp.data[0].embedding
    await storage.set_cached_embedding(db, key, model, vector)
    return vector


async def get_embeddings_batch(
    client: AsyncOpenAI,
    texts: list[str],
    db,
    model: str = DEFAULT_EMBEDDING_MODEL,
) -> list[list[float]]:
    """Batch embed: check cache per item, one API call for all misses."""
    keys = [_cache_key(model, t) for t in texts]
    results: list[Optional[list[float]]] = [None] * len(texts)
    miss_indices: list[int] = []

    for i, key in enumerate(keys):
        cached = await storage.get_cached_embedding(db, key)
        if cached is not None:
            results[i] = cached
        else:
            miss_indices.append(i)

    if miss_indices:
        miss_texts = [texts[i] for i in miss_indices]
        resp = await client.embeddings.create(input=miss_texts, model=model)
        for j, idx in enumerate(miss_indices):
            vector = resp.data[j].embedding
            results[idx] = vector
            await storage.set_cached_embedding(db, keys[idx], model, vector)

    return results  # type: ignore[return-value]


async def build_interest_profile(
    client: AsyncOpenAI,
    topics: list[str],
    db,
    model: str = DEFAULT_EMBEDDING_MODEL,
) -> list[float]:
    """Embed all topics and return element-wise average vector."""
    vectors = await get_embeddings_batch(client, topics, db, model)
    arr = np.array(vectors, dtype=np.float32)
    mean_vec = arr.mean(axis=0)
    norm = np.linalg.norm(mean_vec)
    if norm > 0:
        mean_vec = mean_vec / norm
    return mean_vec.tolist()


# ---------------------------------------------------------------------------
# Similarity + combined score
# ---------------------------------------------------------------------------

def cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    a = np.array(vec_a, dtype=np.float32)
    b = np.array(vec_b, dtype=np.float32)
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def combined_score(
    semantic_sim: float,
    kw_normalized: float,
    recency: float,
    weights: dict,
) -> float:
    w = {**DEFAULT_WEIGHTS, **weights}
    return w["semantic"] * semantic_sim + w["keyword"] * kw_normalized + w["recency"] * recency


# ---------------------------------------------------------------------------
# Batch scoring
# ---------------------------------------------------------------------------

async def score_articles_batch(
    articles: list[dict],
    client: AsyncOpenAI,
    interest_profile: list[float],
    db,
    topics: list[str],
    now_dt: datetime,
    weights: dict,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
) -> list[dict]:
    """Score all articles; returns list sorted by combined_score descending."""
    if not articles:
        return []

    # 1. Keyword scores for all
    kw_scores = [
        keyword_score(a.get("text", a.get("description", "")), a.get("title", ""), topics)
        for a in articles
    ]
    max_kw = max(kw_scores) if kw_scores else 1.0

    # 2. Batch embed all article texts
    texts = [
        (a.get("text") or a.get("description") or a.get("title") or "")[:2000]
        for a in articles
    ]
    vectors = await get_embeddings_batch(client, texts, db, embedding_model)

    scored: list[dict] = []
    for i, article in enumerate(articles):
        sem_sim = cosine_similarity(interest_profile, vectors[i])
        kw_norm = normalize_keyword_score(kw_scores[i], max_kw)
        rec = recency_score(article.get("published"), now_dt, max_age_hours)
        score = combined_score(sem_sim, kw_norm, rec, weights)
        scored.append({**article, "score": score})

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored

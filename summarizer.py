"""
summarizer.py — LLM + extractive summarization (async).
"""
import asyncio
import logging
import re

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


def extractive_summarize(text: str, num_sentences: int = 6) -> str:
    """Naive word-frequency sentence scoring; returns bullet markdown."""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    sentences = [s.strip() for s in sentences if len(s.split()) > 8][:30]
    freq: dict[str, int] = {}
    for s in sentences:
        for w in re.findall(r"[A-Za-z]{3,}", s.lower()):
            freq[w] = freq.get(w, 0) + 1
    scored = []
    for s in sentences:
        s_score = sum(freq.get(w, 0) for w in re.findall(r"[A-Za-z]{3,}", s.lower()))
        scored.append((s_score, s))
    scored.sort(reverse=True, key=lambda x: x[0])
    lines = ["- " + s for _, s in scored[:num_sentences]]
    return "\n".join(lines)


async def openai_summarize(
    client: AsyncOpenAI,
    text: str,
    title: str,
    model: str,
    max_words: int,
    language: str,
) -> "str | None":
    """Call OpenAI Responses API to summarize. Returns None on error."""
    prompt = (
        f"Summarize this article in {max_words} words max in {language}. "
        f"Use bullets with crisp facts. Keep links/tickers if present.\n\n"
        f"TITLE: {title}\n\nTEXT:\n{text[:6000]}"
    )
    try:
        resp = await client.responses.create(
            model=model,
            input=prompt,
        )
        return resp.output_text.strip()
    except Exception as exc:
        logger.warning("OpenAI summarize failed for '%s': %s", title[:60], exc)
        return None


async def summarize(
    client: AsyncOpenAI,
    text: str,
    title: str,
    cfg: dict,
) -> str:
    """Try OpenAI if enabled + key present, fall back to extractive."""
    import os

    if cfg.get("enabled", True) and cfg.get("provider") == "openai" and os.environ.get("OPENAI_API_KEY"):
        result = await openai_summarize(
            client,
            text,
            title,
            model=cfg.get("model", "gpt-4o-mini"),
            max_words=cfg.get("max_words", 140),
            language=cfg.get("language", "en"),
        )
        if result:
            return result

    return extractive_summarize(text)


async def summarize_batch(
    client: AsyncOpenAI,
    articles: list[dict],
    cfg: dict,
) -> list[dict]:
    """Summarize all articles concurrently. Adds 'summary' key to each dict."""
    async def _one(article: dict) -> dict:
        text = article.get("text", "")
        title = article.get("title", "")
        summary = await summarize(client, text, title, cfg)
        return {**article, "summary": summary}

    return await asyncio.gather(*[_one(a) for a in articles])

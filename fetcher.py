"""
fetcher.py — Async RSS + article content fetching.
"""
import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import feedparser
import httpx
import trafilatura
from bs4 import BeautifulSoup

import storage

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "DailyBriefAgent/1.0 (+personal research; contact: you@example.com)"
}


@dataclass
class ArticleMetadata:
    url: str
    title: str
    description: str
    published: Optional[datetime] = None
    source_url: str = ""


async def polite_get_async(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    timeout: float = 20.0,
) -> httpx.Response:
    """Acquire semaphore, sleep briefly, GET with exponential-backoff retry."""
    async with semaphore:
        await asyncio.sleep(0.3)
        delays = [1, 2, 4]
        last_exc: Exception = RuntimeError("no attempts")
        for attempt, delay in enumerate([0] + delays):
            if attempt > 0:
                await asyncio.sleep(delay)
            try:
                resp = await client.get(url, headers=HEADERS, timeout=timeout, follow_redirects=True)
                resp.raise_for_status()
                return resp
            except Exception as exc:
                last_exc = exc
                logger.debug("polite_get attempt %d failed for %s: %s", attempt + 1, url, exc)
        raise last_exc


async def fetch_rss_async(
    source_url: str,
    semaphore: asyncio.Semaphore,
    local_tz,
) -> list[ArticleMetadata]:
    """Parse an RSS feed in a thread executor (feedparser is sync-only)."""
    loop = asyncio.get_event_loop()
    try:
        d = await loop.run_in_executor(None, feedparser.parse, source_url)
    except Exception as exc:
        logger.warning("RSS parse failed for %s: %s", source_url, exc)
        return []

    articles: list[ArticleMetadata] = []
    for e in d.entries:
        link = getattr(e, "link", None)
        if not link:
            continue
        title = getattr(e, "title", "(no title)")
        description = (
            getattr(e, "summary", "")
            or getattr(e, "description", "")
            or ""
        )
        published: Optional[datetime] = None
        for key in ("published_parsed", "updated_parsed"):
            val = getattr(e, key, None)
            if val:
                ts = int(time.mktime(val))
                published = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(local_tz)
                break
        articles.append(
            ArticleMetadata(
                url=link,
                title=title,
                description=description,
                published=published,
                source_url=source_url,
            )
        )
    return articles


async def fetch_all_rss(
    sources: list[str],
    semaphore: asyncio.Semaphore,
    local_tz,
    db,
) -> list[ArticleMetadata]:
    """Fetch all RSS sources concurrently, skipping disabled sources."""
    async def _fetch_one(src: str) -> list[ArticleMetadata]:
        if await storage.is_source_disabled(db, src):
            logger.info("Skipping disabled source: %s", src)
            return []
        try:
            articles = await fetch_rss_async(src, semaphore, local_tz)
            await storage.record_source_success(db, src)
            logger.info("Fetched %d articles from %s", len(articles), src)
            return articles
        except Exception as exc:
            logger.warning("Source failed: %s -> %s", src, exc)
            await storage.record_source_failure(db, src)
            return []

    results = await asyncio.gather(*[_fetch_one(s) for s in sources])
    all_articles: list[ArticleMetadata] = []
    for batch in results:
        all_articles.extend(batch)
    return all_articles


async def extract_main_text_async(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
) -> Optional[str]:
    """Extract article body text. trafilatura primary, BS4 fallback."""
    loop = asyncio.get_event_loop()

    # Primary: trafilatura (sync, run in executor)
    try:
        downloaded = await loop.run_in_executor(None, trafilatura.fetch_url, url)
        if downloaded:
            text = await loop.run_in_executor(
                None,
                lambda: trafilatura.extract(
                    downloaded, include_comments=False, include_tables=False
                ),
            )
            if text and len(text.split()) > 40:
                return text
    except Exception as exc:
        logger.debug("trafilatura failed for %s: %s", url, exc)

    # Fallback: httpx + BeautifulSoup
    try:
        resp = await polite_get_async(client, url, semaphore)
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "noscript", "header", "footer", "aside", "nav"]):
            tag.extract()
        text = " ".join(soup.stripped_strings)
        return text if len(text.split()) > 40 else None
    except Exception as exc:
        logger.debug("BS4 fallback failed for %s: %s", url, exc)
        return None


async def fetch_full_content_batch(
    candidates: list[dict],
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Fetch full text for each candidate article. Drops failures."""
    async def _fetch_one(item: dict) -> Optional[dict]:
        text = await extract_main_text_async(client, item["url"], semaphore)
        if not text:
            logger.debug("No content extracted for %s", item["url"])
            return None
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return {**item, "text": text, "content_hash": content_hash}

    results = await asyncio.gather(*[_fetch_one(c) for c in candidates])
    return [r for r in results if r is not None]

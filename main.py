"""
main.py — Async orchestration entry point for the daily-web-brief agent.
"""
import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

import httpx
import yaml
from dateutil import tz as dateutil_tz
from openai import AsyncOpenAI

import delivery
import fetcher
import report
import scorer
import storage
import summarizer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(HERE, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

CORRELATION_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "correlation_id": CORRELATION_ID,
        }
        if record.exc_info:
            obj["exc"] = self.formatException(record.exc_info)
        return json.dumps(obj)


def setup_logging(log_level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(log_level)

    # JSON file handler
    log_path = os.path.join(LOGS_DIR, f"{CORRELATION_ID}.jsonl")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(JSONFormatter())
    root.addHandler(fh)

    # Human-readable stream handler (terminal / GitHub Actions)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("[%(levelname)s] %(name)s — %(message)s"))
    root.addHandler(sh)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(os.path.join(HERE, "config.yaml"), encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run() -> None:
    setup_logging()
    logger.info("Starting run. correlation_id=%s", CORRELATION_ID)

    cfg = load_config()
    local_tz = dateutil_tz.gettz(cfg.get("timezone", "Asia/Kolkata"))

    db_path = os.path.join(HERE, cfg["storage"]["db_path"])
    reports_dir = os.path.join(HERE, cfg["storage"]["reports_dir"])

    ranking_cfg = cfg.get("ranking", {})
    score_weights = ranking_cfg.get("score_weights", scorer.DEFAULT_WEIGHTS)
    embedding_model = ranking_cfg.get("embedding_model", scorer.DEFAULT_EMBEDDING_MODEL)
    max_age_hours = ranking_cfg.get("max_age_hours", scorer.DEFAULT_MAX_AGE_HOURS)
    min_score = ranking_cfg.get("min_score", 1)
    max_fetch = cfg["limits"]["per_run_max_articles"]
    max_summary = cfg["limits"]["per_run_max_summary"]

    topics = [t.lower() for t in cfg.get("topics", [])]
    sources = cfg.get("sources", [])
    summarization_cfg = cfg.get("summarization", {})
    delivery_cfg = cfg.get("delivery", {})

    openai_api_key = os.environ.get("OPENAI_API_KEY")
    openai_client = AsyncOpenAI(api_key=openai_api_key) if openai_api_key else None

    semaphore = asyncio.Semaphore(5)

    db = await storage.get_db(db_path)
    await storage.init_db(db)

    async with httpx.AsyncClient() as http_client:

        # Stage 1: Fetch all RSS feeds
        logger.info("Stage 1: Fetching RSS feeds from %d sources", len(sources))
        all_articles = await fetcher.fetch_all_rss(sources, semaphore, local_tz, db)
        logger.info("Stage 1 complete: %d total articles", len(all_articles))

        # Stage 2: URL dedup
        logger.info("Stage 2: URL dedup")
        unseen: list[dict] = []
        for art in all_articles:
            if art.url and not await storage.is_url_seen(db, art.url):
                unseen.append({
                    "url": art.url,
                    "title": art.title,
                    "description": art.description,
                    "published": art.published,
                    "source_url": art.source_url,
                })
        logger.info("Stage 2: %d unseen after URL dedup (from %d)", len(unseen), len(all_articles))

        # Stage 3: Keyword pre-score and filter
        logger.info("Stage 3: Keyword scoring and filtering (min_score=%s)", min_score)
        now_dt = datetime.now(local_tz)
        kw_scored: list[dict] = []
        for art in unseen:
            kw = scorer.keyword_score(art.get("description", ""), art.get("title", ""), topics)
            if kw >= min_score:
                kw_scored.append({**art, "kw_score": kw})

        kw_scored.sort(
            key=lambda x: (x["kw_score"], x["published"] or datetime.min.replace(tzinfo=local_tz)),
            reverse=True,
        )
        top_candidates = kw_scored[:max_fetch]
        logger.info("Stage 3: %d candidates after keyword filter, taking top %d", len(kw_scored), len(top_candidates))

        # Stage 4: Fetch full content
        logger.info("Stage 4: Fetching full article content")
        with_content = await fetcher.fetch_full_content_batch(top_candidates, http_client, semaphore)
        logger.info("Stage 4: %d articles with content (of %d attempted)", len(with_content), len(top_candidates))

        # Stage 5: Content hash dedup + semantic scoring
        logger.info("Stage 5: Content hash dedup + semantic scoring")
        hash_deduped: list[dict] = []
        for art in with_content:
            if not await storage.is_hash_seen(db, art["content_hash"]):
                hash_deduped.append(art)
        logger.info("Stage 5: %d articles after content hash dedup", len(hash_deduped))

        if hash_deduped and openai_client:
            interest_profile = await scorer.build_interest_profile(
                openai_client, topics, db, embedding_model
            )
            scored_articles = await scorer.score_articles_batch(
                hash_deduped,
                openai_client,
                interest_profile,
                db,
                topics,
                now_dt,
                score_weights,
                max_age_hours,
                embedding_model,
            )
        else:
            if not openai_client:
                logger.warning("No OPENAI_API_KEY — skipping semantic scoring, using keyword scores")
            # Fall back to keyword-only sort
            scored_articles = sorted(
                hash_deduped,
                key=lambda x: (x.get("kw_score", 0), x.get("published") or datetime.min.replace(tzinfo=local_tz)),
                reverse=True,
            )

        # Stage 6: Summarize top N
        logger.info("Stage 6: Summarizing top %d articles", max_summary)
        to_summarize = scored_articles[:max_summary]
        if openai_client:
            summarized = await summarizer.summarize_batch(openai_client, to_summarize, summarization_cfg)
        else:
            # Extractive only fallback
            summarized = []
            for art in to_summarize:
                sm = summarizer.extractive_summarize(art.get("text", ""))
                summarized.append({**art, "summary": sm})
        logger.info("Stage 6: %d articles summarized", len(summarized))

        # Stage 7: Build and save report
        logger.info("Stage 7: Building report")
        report_md = report.build_report(summarized, local_tz)
        report_path = report.save_report(report_md, reports_dir, local_tz)
        logger.info("Stage 7: Report saved to %s", report_path)

        # Stage 8: Deliver
        logger.info("Stage 8: Delivering report")
        today_str = datetime.now(local_tz).strftime("%Y-%m-%d")
        subject = delivery_cfg.get("email", {}).get("subject_prefix", "[Daily Brief]") + " " + today_str
        delivery_results = await delivery.deliver(subject, report_md, delivery_cfg)
        logger.info("Stage 8: Delivery results: %s", delivery_results)

        # Stage 9: Persist seen items
        logger.info("Stage 9: Persisting seen items")
        import time
        now_ts = int(time.time())
        for art in summarized:
            await storage.mark_seen(db, art["url"], art["title"], art["content_hash"], now_ts)
        await db.commit()
        logger.info("Stage 9: %d items marked seen", len(summarized))

    await db.close()
    logger.info(
        "Run complete. Articles fetched=%d, summarized=%d, email=%s, slack=%s",
        len(all_articles),
        len(summarized),
        delivery_results.get("email"),
        delivery_results.get("slack"),
    )


if __name__ == "__main__":
    asyncio.run(run())

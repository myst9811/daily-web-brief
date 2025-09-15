import os
import re
import time
import yaml
import json
import sqlite3
import hashlib
import feedparser
import requests
from urllib.parse import urlparse
from datetime import datetime, timezone
from dateutil import tz
from bs4 import BeautifulSoup
import trafilatura
from rapidfuzz import fuzz
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from email.message import EmailMessage
import smtplib

# Set up logging once
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))


def load_config():
    """Load configuration from YAML file, create sample if missing."""
    config_path = os.path.join(HERE, "config.yaml")
    if not os.path.exists(config_path):
        logger.error("config.yaml not found. Creating a sample config file...")
        create_sample_config()
        raise FileNotFoundError("config.yaml not found. A sample has been created.")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def create_sample_config():
    sample_config = {
        "timezone": "Asia/Kolkata",
        "topics": ["technology", "AI", "machine learning"],
        "sources": [
            "https://feeds.feedburner.com/venturebeat/SZYF",
            "https://techcrunch.com/feed/"
        ],
        "storage": {
            "db_path": "brief.db",
            "reports_dir": "reports"
        },
        "ranking": {"min_score": 1},
        "limits": {
            "per_run_max_articles": 10,
            "per_run_max_summary": 5
        },
        "summarization": {
            "enabled": True,
            "language": "en",
            "max_words": 140,
            "provider": "builtin"
        },
        "delivery": {
            "email": {
                "enabled": False,
                "to": "your@email.com",
                "subject_prefix": "Daily Brief"
            },
            "slack": {
                "enabled": False,
                "webhook_url_env": "SLACK_WEBHOOK_URL"
            }
        }
    }
    with open(os.path.join(HERE, "config.yaml"), "w", encoding="utf-8") as f:
        yaml.dump(sample_config, f, default_flow_style=False)
    logger.info("Sample config.yaml created. Please edit it with your settings.")


CFG = load_config()
TZ = tz.gettz(CFG.get("timezone", "Asia/Kolkata"))

DB_PATH = os.path.join(HERE, CFG["storage"]["db_path"])
REPORTS_DIR = os.path.join(HERE, CFG["storage"]["reports_dir"])
os.makedirs(REPORTS_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "DailyBriefAgent/1.0 (+personal research; contact: you@example.com)"
}

# Initialize session and retry strategy once
session = requests.Session()
session.headers.update(HEADERS)
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)


def init_db():
    """Initialize SQLite DB and return connection."""
    con = sqlite3.connect(DB_PATH)
    with con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS seen (
                url TEXT PRIMARY KEY,
                title TEXT,
                content_hash TEXT UNIQUE,
                first_seen_ts INTEGER
            )
            """
        )
    return con


def safe_get(url, timeout=10):
    """Fetch a URL safely with error handling."""
    try:
        time.sleep(0.5)  # keep polite pacing, could be reduced if rate limits allow
        logger.info(f"Fetching URL: {url}")
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def extract_main_text(url):
    """Extract main text content with fallback mechanisms."""
    parsed = urlparse(url)
    if parsed.path.endswith(('.pdf', '.doc', '.docx', '.zip', '.exe')):
        logger.info(f"Skipping unsupported file type: {url}")
        return None

    try:
        downloaded = trafilatura.fetch_url(url, config=trafilatura.settings.use_config())
        if downloaded:
            text = trafilatura.extract(
                downloaded,
                include_comments=False,
                include_tables=False,
                favor_precision=True
            )
            if text and len(text.split()) > 40:
                logger.info(f"Trafilatura extracted {len(text.split())} words from {url}")
                return text
    except Exception as e:
        logger.warning(f"Trafilatura extraction failed for {url}: {e}")

    # Fallback to BeautifulSoup extraction
    response = safe_get(url)
    if not response:
        return None
    try:
        soup = BeautifulSoup(response.text, "html.parser")

        # Remove unwanted tags
        for tag in soup(["script", "style", "noscript", "header", "footer", "aside", "nav", "ads"]):
            tag.decompose()

        main_content = soup.find('main') or soup.find('article') or soup.find('div', class_=re.compile(r'content|article|post'))
        paragraphs = [p.get_text(strip=True) for p in (main_content.find_all("p") if main_content else soup.find_all("p")) if p.get_text(strip=True)]
        text = "\n\n".join(paragraphs)

        if len(text.split()) > 40:
            logger.info(f"BeautifulSoup fallback extraction succeeded with {len(text.split())} words at {url}")
            return text
        else:
            logger.warning(f"Extracted text too short ({len(text.split())} words) from {url}")
            return None
    except Exception as e:
        logger.warning(f"BeautifulSoup fallback failed for {url}: {e}")
        return None


def fetch_rss(url):
    """Yield feed entries from RSS URL."""
    logger.info(f"Fetching RSS feed: {url}")
    d = feedparser.parse(url, request_headers=HEADERS)
    if d.bozo and hasattr(d, 'bozo_exception'):
        logger.warning(f"RSS parsing issue for {url}: {d.bozo_exception}")
    for e in d.entries:
        link = getattr(e, "link", None)
        if not link:
            continue

        published = None
        for key in ("published_parsed", "updated_parsed"):
            parsed_time = getattr(e, key, None)
            if parsed_time:
                try:
                    ts = int(time.mktime(parsed_time))
                    published = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(TZ)
                    break
                except (ValueError, OverflowError):
                    continue

        yield {
            "title": getattr(e, "title", "(no title)"),
            "url": link,
            "published": published
        }


TOPICS = [t.lower() for t in CFG.get("topics", [])]


def calculate_score(text, title):
    """Calculate relevance score based on topic counts & fuzzy matching."""
    if not TOPICS:
        return 1.0  # Default minimum score if no topics

    haystack = (title or "").lower() + "\n" + (text or "").lower()
    score = sum(haystack.count(topic) for topic in TOPICS)

    if title:
        score += sum(fuzz.partial_ratio(topic, title.lower()) / 100 for topic in TOPICS)
    return score


def simple_summarize(text, max_sentences=4):
    """Extract a simple summary based on word frequency of sentences."""
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if 8 < len(s.split()) < 300]
    if len(sentences) <= max_sentences:
        return "\n".join(f"- {s}" for s in sentences)

    freq = {}
    for s in sentences:
        for w in re.findall(r'\b[a-zA-Z]{3,}\b', s.lower()):
            freq[w] = freq.get(w, 0) + 1

    scored = [(sum(freq.get(w, 0) for w in re.findall(r'\b[a-zA-Z]{3,}\b', s.lower())), s) for s in sentences]
    top_sentences = [s for _, s in sorted(scored, reverse=True)[:max_sentences]]

    return "\n".join(f"- {s}" for s in top_sentences)


def send_email(subject, body_md):
    """Send an email if enabled and configured properly."""
    email_cfg = CFG["delivery"]["email"]
    if not email_cfg.get("enabled", False):
        return False

    try:
        host = os.environ.get("SMTP_HOST")
        port = int(os.environ.get("SMTP_PORT", "587"))
        user = os.environ.get("SMTP_USER")
        pwd = os.environ.get("SMTP_PASSWORD")
        from_addr = os.environ.get("SMTP_FROM", user)
        to_addr = email_cfg["to"]

        if not all([host, port, user, pwd, from_addr, to_addr]):
            logger.warning("Missing SMTP environment variables for email sending.")
            return False

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg.set_content(body_md)

        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(user, pwd)
            server.send_message(msg)

        logger.info("Email sent successfully.")
        return True
    except Exception as e:
        logger.error(f"Email sending failed: {e}")
        return False


def build_report(articles):
    """Build markdown formatted daily brief report."""
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    lines = [f"# Daily Brief — {today}", ""]
    if not articles:
        lines.append("No new articles found today.")
    else:
        for i, art in enumerate(articles, 1):
            lines.append(f"## {i}. {art['title']}")
            if art.get("published"):
                lines.append(f"**Published:** {art['published'].strftime('%Y-%m-%d %H:%M %Z')}")
            lines.append(f"**Source:** {art['url']}")
            lines.append(f"**Score:** {art['score']:.2f}")
            if art.get("summary"):
                lines.append("\n**Summary:**")
                lines.append(art["summary"])
            lines.extend(["", "---", ""])
    return "\n".join(lines)


def run():
    logger.info("Starting Daily Brief Agent")

    con = init_db()
    cur = con.cursor()

    candidates = []
    min_score = CFG["ranking"].get("min_score", 1)
    max_articles = CFG["limits"].get("per_run_max_articles", 10)
    max_summaries = CFG["limits"].get("per_run_max_summary", 5)
    summarize_enabled = CFG["summarization"].get("enabled", True)

    # Prepare a batch query to get already seen content hashes and URLs for efficient lookup
    cur.execute("SELECT url, content_hash FROM seen")
    seen_set = set(cur.fetchall())  # set of (url, content_hash) tuples

    for source_url in CFG["sources"]:
        try:
            for entry in fetch_rss(source_url):
                url = entry.get("url")
                if not url:
                    continue
                title = entry.get("title")

                text = extract_main_text(url)
                if not text:
                    logger.info(f"No text extracted for {url}, skipping.")
                    continue

                content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
                if (url, content_hash) in seen_set or (None, content_hash) in seen_set or (url, None) in seen_set:
                    logger.info(f"Already seen: {url}")
                    continue

                score = calculate_score(text, title)
                if score < min_score:
                    logger.info(f"Skipping '{title}' due to low score ({score:.2f})")
                    continue

                candidates.append({
                    "title": title,
                    "url": url,
                    "published": entry.get("published"),
                    "text": text,
                    "score": score,
                    "hash": content_hash
                })
                logger.info(f"Candidate added: '{title}' with score {score:.2f}")
        except Exception as e:
            logger.error(f"Error processing source {source_url}: {e}")

    # Sort candidates by score then date (descending)
    candidates.sort(key=lambda x: (x["score"], x["published"] or datetime.min.replace(tzinfo=TZ)), reverse=True)
    top_candidates = candidates[:max_articles]

    summarized_articles = []
    for article in top_candidates[:max_summaries]:
        if summarize_enabled:
            article["summary"] = simple_summarize(article["text"])
        summarized_articles.append(article)

    report_content = build_report(summarized_articles)
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    report_path = os.path.join(REPORTS_DIR, f"{today}.md")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    logger.info(f"Report saved to {report_path}")

    subject = f"{CFG['delivery']['email']['subject_prefix']} {today}"
    email_sent = send_email(subject, report_content)

    # Bulk insert articles into DB to reduce commit overhead
    now_ts = int(time.time())
    with con:
        con.executemany(
            "INSERT OR IGNORE INTO seen(url, title, content_hash, first_seen_ts) VALUES (?, ?, ?, ?)",
            [(a["url"], a["title"], a["hash"], now_ts) for a in summarized_articles]
        )

    con.close()

    logger.info("✅ Daily Brief completed successfully!")
    logger.info(f"📊 Processed {len(candidates)} candidates, selected {len(summarized_articles)} articles")
    logger.info(f"📧 Email sent: {email_sent}")
    logger.info(f"📄 Report path: {report_path}")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise

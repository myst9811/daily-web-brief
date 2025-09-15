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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))

def load_config():
    """Load configuration from YAML file"""
    try:
        with open(os.path.join(HERE, "config.yaml"), "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("config.yaml not found. Creating a sample config file...")
        create_sample_config()
        raise

def create_sample_config():
    """Create a sample config.yaml file"""
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
        "ranking": {
            "min_score": 1
        },
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

def init_db():
    """Initialize the SQLite database"""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS seen (
        url TEXT PRIMARY KEY,
        title TEXT,
        content_hash TEXT,
        first_seen_ts INTEGER
    )
    """)
    con.commit()
    return con

# Improved session with better timeout and retry settings
HEADERS = {
    "User-Agent": "DailyBriefAgent/1.0 (+personal research; contact: you@example.com)"
}

session = requests.Session()
session.headers.update(HEADERS)

# Add retry adapter
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

def safe_get(url, timeout=10):
    """Safely fetch URL with proper error handling"""
    try:
        time.sleep(0.5)  # Reduced delay
        logger.info(f"Fetching: {url}")
        response = session.get(url, timeout=timeout, stream=False)
        response.raise_for_status()
        return response
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching {url}")
        return None
    except requests.exceptions.ConnectionError:
        logger.warning(f"Connection error for {url}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.warning(f"HTTP error {e.response.status_code} for {url}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error fetching {url}: {e}")
        return None

def extract_main_text(url):
    """Extract main text content from URL"""
    # Skip certain file types that might cause issues
    parsed = urlparse(url)
    if parsed.path.endswith(('.pdf', '.doc', '.docx', '.zip', '.exe')):
        logger.info(f"Skipping file type: {url}")
        return None
    
    # Try trafilatura first (faster and more reliable)
    try:
        logger.info(f"Extracting text from: {url}")
        downloaded = trafilatura.fetch_url(url, config=trafilatura.settings.use_config())
        if downloaded:
            text = trafilatura.extract(
                downloaded, 
                include_comments=False, 
                include_tables=False,
                favor_precision=True
            )
            if text and len(text.split()) > 40:
                logger.info(f"Successfully extracted {len(text.split())} words")
                return text
    except Exception as e:
        logger.warning(f"Trafilatura extraction failed for {url}: {e}")

    # Fallback to BeautifulSoup
    try:
        response = safe_get(url)
        if not response:
            return None
            
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Remove unwanted elements
        for tag in soup(["script", "style", "noscript", "header", "footer", "aside", "nav", "ads"]):
            tag.decompose()
        
        # Try to find main content area first
        main_content = soup.find('main') or soup.find('article') or soup.find('div', class_=re.compile(r'content|article|post'))
        
        if main_content:
            paragraphs = [p.get_text(strip=True) for p in main_content.find_all("p") if p.get_text(strip=True)]
        else:
            paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
        
        text = "\n\n".join(paragraphs)
        
        if len(text.split()) > 40:
            logger.info(f"Fallback extraction successful: {len(text.split())} words")
            return text
        else:
            logger.warning(f"Extracted text too short: {len(text.split())} words")
            return None
            
    except Exception as e:
        logger.warning(f"BeautifulSoup fallback failed for {url}: {e}")
        return None

def fetch_rss(url):
    """Fetch RSS feed entries"""
    try:
        logger.info(f"Fetching RSS: {url}")
        d = feedparser.parse(url, request_headers=HEADERS)
        
        if d.bozo and hasattr(d, 'bozo_exception'):
            logger.warning(f"RSS parsing issue for {url}: {d.bozo_exception}")
        
        entries_processed = 0
        for e in d.entries:
            link = getattr(e, "link", None)
            title = getattr(e, "title", "(no title)")
            
            if not link:
                continue
                
            published = None
            for key in ("published_parsed", "updated_parsed"):
                if getattr(e, key, None):
                    try:
                        ts = int(time.mktime(getattr(e, key)))
                        published = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(TZ)
                        break
                    except (ValueError, OverflowError):
                        continue
            
            entries_processed += 1
            yield {"title": title, "url": link, "published": published}
            
        logger.info(f"Processed {entries_processed} entries from {url}")
        
    except Exception as e:
        logger.error(f"RSS fetch failed for {url}: {e}")
        return

TOPICS = [t.lower() for t in CFG.get("topics", [])]

def calculate_score(text, title):
    """Calculate relevance score based on topics"""
    if not TOPICS:
        return 1  # Default score if no topics configured
    
    score = 0
    haystack = (title or "") + "\n" + (text or "")
    haystack_lower = haystack.lower()
    
    for topic in TOPICS:
        topic_lower = topic.lower()
        # Count occurrences
        score += haystack_lower.count(topic_lower)
        # Add fuzzy match bonus for title
        if title:
            score += fuzz.partial_ratio(topic_lower, title.lower()) / 100.0
    
    return score

def simple_summarize(text, title=None, max_sentences=4):
    """Simple extractive summarization"""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    sentences = [s.strip() for s in sentences if len(s.split()) > 8 and len(s) < 300]
    
    if len(sentences) <= max_sentences:
        return "\n".join("- " + s for s in sentences)
    
    # Simple frequency-based scoring
    word_freq = {}
    for sentence in sentences:
        for word in re.findall(r'\b[a-zA-Z]{3,}\b', sentence.lower()):
            word_freq[word] = word_freq.get(word, 0) + 1
    
    # Score sentences by word frequency
    sentence_scores = []
    for sentence in sentences:
        score = sum(word_freq.get(word, 0) for word in re.findall(r'\b[a-zA-Z]{3,}\b', sentence.lower()))
        sentence_scores.append((score, sentence))
    
    # Get top sentences
    sentence_scores.sort(reverse=True, key=lambda x: x[0])
    top_sentences = [s for _, s in sentence_scores[:max_sentences]]
    
    return "\n".join("- " + s for s in top_sentences)

def send_email(subject, body_md):
    """Send email notification"""
    if not CFG["delivery"]["email"]["enabled"]:
        return False
    
    try:
        import smtplib
        from email.message import EmailMessage
        
        host = os.environ.get("SMTP_HOST")
        port = int(os.environ.get("SMTP_PORT", "587"))
        user = os.environ.get("SMTP_USER")
        pwd = os.environ.get("SMTP_PASSWORD")
        from_addr = os.environ.get("SMTP_FROM", user)
        to_addr = CFG["delivery"]["email"]["to"]
        
        if not all([host, port, user, pwd, from_addr, to_addr]):
            logger.warning("Missing SMTP environment variables")
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
        
        logger.info("Email sent successfully")
        return True
        
    except Exception as e:
        logger.error(f"Email sending failed: {e}")
        return False

def build_report(articles):
    """Build markdown report"""
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    lines = [f"# Daily Brief — {today}", ""]
    
    if not articles:
        lines.append("No new articles found today.")
        return "\n".join(lines)
    
    for i, article in enumerate(articles, 1):
        lines.append(f"## {i}. {article['title']}")
        
        if article.get("published"):
            lines.append(f"**Published:** {article['published'].strftime('%Y-%m-%d %H:%M %Z')}")
        
        lines.append(f"**Source:** {article['url']}")
        lines.append(f"**Score:** {article['score']:.2f}")
        
        if article.get("summary"):
            lines.append("")
            lines.append("**Summary:**")
            lines.append(article["summary"])
        
        lines.append("")
        lines.append("---")
        lines.append("")
    
    return "\n".join(lines)

def run():
    """Main execution function"""
    logger.info("Starting Daily Brief Agent")
    
    con = init_db()
    cur = con.cursor()
    candidates = []
    
    # Process each RSS source
    for source_url in CFG["sources"]:
        logger.info(f"Processing source: {source_url}")
        
        try:
            for entry in fetch_rss(source_url):
                if not entry["url"]:
                    continue
                
                title = entry["title"]
                url = entry["url"]
                
                # Extract main text
                text = extract_main_text(url)
                if not text:
                    logger.info(f"Skipping {url} - no text extracted")
                    continue
                
                # Check if we've seen this content before
                content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
                cur.execute("SELECT 1 FROM seen WHERE url = ? OR content_hash = ?", (url, content_hash))
                
                if cur.fetchone():
                    logger.info(f"Skipping {url} - already seen")
                    continue
                
                # Calculate relevance score
                score = calculate_score(text, title)
                min_score = CFG["ranking"].get("min_score", 1)
                
                if score >= min_score:
                    candidates.append({
                        "title": title,
                        "url": url,
                        "published": entry["published"],
                        "text": text,
                        "score": score,
                        "hash": content_hash
                    })
                    logger.info(f"Added candidate: {title} (score: {score:.2f})")
                else:
                    logger.info(f"Skipping {title} - score too low: {score:.2f}")
                    
        except Exception as e:
            logger.error(f"Error processing source {source_url}: {e}")
            continue
    
    # Sort and limit candidates
    candidates.sort(key=lambda x: (x["score"], x["published"] or datetime.min.replace(tzinfo=TZ)), reverse=True)
    max_articles = CFG["limits"]["per_run_max_articles"]
    top_candidates = candidates[:max_articles]
    
    logger.info(f"Selected {len(top_candidates)} articles from {len(candidates)} candidates")
    
    # Generate summaries for top articles
    max_summaries = CFG["limits"]["per_run_max_summary"]
    summarized_articles = []
    
    for article in top_candidates[:max_summaries]:
        if CFG["summarization"].get("enabled", True):
            summary = simple_summarize(article["text"], article["title"])
            article["summary"] = summary
        summarized_articles.append(article)
    
    # Build and save report
    report_content = build_report(summarized_articles)
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    report_path = os.path.join(REPORTS_DIR, f"{today}.md")
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)
    
    logger.info(f"Report saved to: {report_path}")
    
    # Send notifications
    subject = f"{CFG['delivery']['email']['subject_prefix']} {today}"
    email_sent = send_email(subject, report_content)
    
    # Mark articles as seen
    now_ts = int(time.time())
    for article in summarized_articles:
        cur.execute(
            "INSERT OR IGNORE INTO seen(url, title, content_hash, first_seen_ts) VALUES (?, ?, ?, ?)",
            (article["url"], article["title"], article["hash"], now_ts)
        )
    
    con.commit()
    con.close()
    
    logger.info(f"✅ Daily Brief completed successfully!")
    logger.info(f"📊 Processed {len(candidates)} candidates, selected {len(summarized_articles)} articles")
    logger.info(f"📧 Email sent: {email_sent}")
    logger.info(f"📄 Report: {report_path}")

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
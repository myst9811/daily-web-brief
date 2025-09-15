import os
import re
import time
import yaml
import hashlib
import feedparser
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import trafilatura
from email.message import EmailMessage
import smtplib
import logging

# Set up logging
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
    """Create a sample configuration file."""
    sample_config = {
        "sources": [
            "https://feeds.feedburner.com/venturebeat/SZYF",
            "https://techcrunch.com/feed/"
        ],
        "email": {
            "to": "your@email.com",
            "subject_prefix": "Daily Brief"
        },
        "limits": {
            "max_articles": 5
        }
    }
    with open(os.path.join(HERE, "config.yaml"), "w", encoding="utf-8") as f:
        yaml.dump(sample_config, f, default_flow_style=False)
    logger.info("Sample config.yaml created. Please edit it with your settings.")

CFG = load_config()

HEADERS = {
    "User-Agent": "DailyBriefAgent/1.0 (+personal research; contact: you@example.com)"
}

# Track seen articles to avoid duplicates
seen_articles = set()

def safe_get(url, timeout=10):
    """Fetch a URL safely with error handling."""
    try:
        time.sleep(0.5)  # Be polite to servers
        logger.info(f"Fetching URL: {url}")
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None

def extract_main_text(url):
    """Extract main text content from article URL."""
    try:
        # Try trafilatura first (best for article extraction)
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, favor_precision=True)
            if text and len(text.split()) > 40:
                logger.info(f"Extracted {len(text.split())} words from {url}")
                return text
    except Exception as e:
        logger.warning(f"Trafilatura extraction failed for {url}: {e}")

    # Fallback to BeautifulSoup
    response = safe_get(url)
    if not response:
        return None
        
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Remove unwanted elements
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()
        
        # Find main content
        main_content = soup.find('main') or soup.find('article') or soup.find('div', class_=re.compile(r'content|article|post'))
        paragraphs = [p.get_text(strip=True) for p in (main_content.find_all("p") if main_content else soup.find_all("p")) if p.get_text(strip=True)]
        text = "\n\n".join(paragraphs)
        
        if len(text.split()) > 40:
            logger.info(f"Extracted {len(text.split())} words from {url}")
            return text
    except Exception as e:
        logger.warning(f"BeautifulSoup extraction failed for {url}: {e}")
    
    return None

def fetch_rss(url):
    """Fetch and parse RSS feed."""
    logger.info(f"Fetching RSS feed: {url}")
    try:
        feed = feedparser.parse(url, request_headers=HEADERS)
        for entry in feed.entries:
            if hasattr(entry, 'link'):
                yield {
                    "title": getattr(entry, "title", "(no title)"),
                    "url": entry.link,
                    "published": getattr(entry, "published", "")
                }
    except Exception as e:
        logger.error(f"Error fetching RSS from {url}: {e}")

def simple_summarize(text, max_sentences=3):
    """Create a simple summary by selecting the most important sentences."""
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if 10 < len(s.split()) < 200]
    
    if len(sentences) <= max_sentences:
        return "\n".join(f"• {s}" for s in sentences)
    
    # Simple word frequency scoring
    word_freq = {}
    for sentence in sentences:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', sentence.lower())
        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1
    
    # Score sentences based on word frequency
    sentence_scores = []
    for sentence in sentences:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', sentence.lower())
        score = sum(word_freq.get(word, 0) for word in words)
        sentence_scores.append((score, sentence))
    
    # Get top sentences
    top_sentences = [s for _, s in sorted(sentence_scores, reverse=True)[:max_sentences]]
    return "\n".join(f"• {s}" for s in top_sentences)

def send_email(subject, body):
    """Send email with the daily brief."""
    try:
        # Get SMTP settings from environment variables
        smtp_host = os.environ.get("SMTP_HOST")
        smtp_port = int(os.environ.get("SMTP_PORT", "587"))
        smtp_user = os.environ.get("SMTP_USER")
        smtp_password = os.environ.get("SMTP_PASSWORD")
        from_email = os.environ.get("SMTP_FROM", smtp_user)
        to_email = CFG["email"]["to"]
        
        if not all([smtp_host, smtp_user, smtp_password, to_email]):
            logger.error("Missing email configuration. Please set SMTP_HOST, SMTP_USER, SMTP_PASSWORD environment variables and email.to in config.yaml")
            return False
        
        # Create email message
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = to_email
        msg.set_content(body)
        
        # Send email
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logger.info(f"Email sent successfully to {to_email}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False

def run():
    """Main function to fetch articles and send email summary."""
    logger.info("Starting Daily Brief")
    
    articles = []
    max_articles = CFG["limits"].get("max_articles", 5)
    
    # Fetch articles from all sources
    for source_url in CFG["sources"]:
        try:
            for entry in fetch_rss(source_url):
                url = entry["url"]
                
                # Skip if we've already processed this URL
                url_hash = hashlib.sha256(url.encode()).hexdigest()
                if url_hash in seen_articles:
                    continue
                seen_articles.add(url_hash)
                
                # Extract article content
                text = extract_main_text(url)
                if not text:
                    logger.info(f"No content extracted from {url}, skipping")
                    continue
                
                # Create summary
                summary = simple_summarize(text)
                
                articles.append({
                    "title": entry["title"],
                    "url": url,
                    "published": entry["published"],
                    "summary": summary
                })
                
                logger.info(f"Added article: {entry['title']}")
                
                # Stop when we have enough articles
                if len(articles) >= max_articles:
                    break
                    
        except Exception as e:
            logger.error(f"Error processing source {source_url}: {e}")
        
        if len(articles) >= max_articles:
            break
    
    if not articles:
        logger.info("No articles found")
        return
    
    # Build email content
    today = datetime.now().strftime("%Y-%m-%d")
    email_body = f"Daily Brief — {today}\n" + "="*50 + "\n\n"
    
    for i, article in enumerate(articles, 1):
        email_body += f"{i}. {article['title']}\n"
        email_body += f"   Source: {article['url']}\n"
        if article['published']:
            email_body += f"   Published: {article['published']}\n"
        email_body += f"\n   Summary:\n   {article['summary']}\n\n"
        email_body += "-" * 80 + "\n\n"
    
    # Send email
    subject = f"{CFG['email']['subject_prefix']} — {today}"
    success = send_email(subject, email_body)
    
    if success:
        logger.info("✅ Daily brief sent successfully!")
    else:
        logger.error("❌ Failed to send daily brief")
    
    logger.info(f"📊 Processed {len(articles)} articles")

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
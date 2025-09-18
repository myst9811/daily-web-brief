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
from urllib.parse import urlparse, parse_qs, unquote

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

def resolve_google_news_url(url, max_redirects=5):
    """
    Resolve Google News stub URLs to actual article URLs.
    Handles both redirect following and URL parameter extraction.
    """
    if not url or "news.google.com" not in url:
        return url
        
    logger.info(f"Resolving Google News URL: {url}")
    
    # Method 1: Try to extract URL from Google News parameters
    try:
        parsed = urlparse(url)
        if "articles" in parsed.path:
            # Some Google News URLs have the actual URL encoded in parameters
            query_params = parse_qs(parsed.query)
            if 'url' in query_params:
                actual_url = unquote(query_params['url'][0])
                logger.info(f"Extracted URL from parameters: {actual_url}")
                return actual_url
    except Exception as e:
        logger.debug(f"Failed to extract URL from parameters: {e}")
    
    # Method 2: Follow redirects manually with session
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        
        current_url = url
        for redirect_count in range(max_redirects):
            time.sleep(0.5)  # Be polite
            
            response = session.get(current_url, allow_redirects=False, timeout=10)
            
            # Check for redirect
            if response.status_code in [301, 302, 303, 307, 308]:
                next_url = response.headers.get('Location')
                if next_url:
                    logger.debug(f"Redirect {redirect_count + 1}: {current_url} -> {next_url}")
                    current_url = next_url
                    
                    # If we've left Google News, we found the actual article
                    if "news.google.com" not in next_url:
                        logger.info(f"Resolved to actual article: {next_url}")
                        return next_url
                else:
                    break
            else:
                # No more redirects
                if "news.google.com" not in current_url:
                    logger.info(f"Resolved to actual article: {current_url}")
                    return current_url
                break
                
    except Exception as e:
        logger.warning(f"Failed to resolve Google News URL {url}: {e}")
    
    # If all else fails, return the original URL
    logger.warning(f"Could not resolve Google News URL, returning original: {url}")
    return url

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
    # First, resolve any Google News URLs
    resolved_url = resolve_google_news_url(url)
    
    # Skip if still on Google News after resolution attempts
    if "news.google.com" in resolved_url:
        logger.warning(f"Still on Google News after resolution, skipping: {resolved_url}")
        return None
    
    try:
        # Try trafilatura first (best for article extraction)
        logger.info(f"Attempting trafilatura extraction from: {resolved_url}")
        downloaded = trafilatura.fetch_url(resolved_url)
        if downloaded:
            text = trafilatura.extract(
                downloaded, 
                favor_precision=True,
                include_comments=False,
                include_tables=False
            )
            if text and len(text.split()) > 40:
                logger.info(f"✅ Trafilatura extracted {len(text.split())} words from {resolved_url}")
                return text
            else:
                logger.debug("Trafilatura extracted text too short, trying fallback")
    except Exception as e:
        logger.warning(f"Trafilatura extraction failed for {resolved_url}: {e}")

    # Fallback to BeautifulSoup
    logger.info(f"Attempting BeautifulSoup extraction from: {resolved_url}")
    response = safe_get(resolved_url)
    if not response:
        return None

    # Double-check we're not still on Google News
    if "news.google.com" in response.url:
        logger.warning(f"Response URL still on Google News, skipping: {response.url}")
        return None
        
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Remove unwanted elements
        for tag in soup(["script", "style", "nav", "header", "footer", "aside", "advertisement"]):
            tag.decompose()
        
        # Try multiple strategies to find main content
        main_content = None
        
        # Strategy 1: Look for common article containers
        selectors = [
            'article',
            '[role="main"]',
            'main',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.content',
            '.story-body',
            '.article-body'
        ]
        
        for selector in selectors:
            main_content = soup.select_one(selector)
            if main_content:
                logger.debug(f"Found content using selector: {selector}")
                break
        
        # Strategy 2: Find div with content-related class names
        if not main_content:
            main_content = soup.find('div', class_=re.compile(r'content|article|post|story|body', re.I))
        
        # Strategy 3: Fall back to all paragraphs
        if main_content:
            paragraphs = main_content.find_all("p")
        else:
            paragraphs = soup.find_all("p")
        
        # Extract text from paragraphs
        text_parts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text and len(text.split()) > 5:  # Skip very short paragraphs
                text_parts.append(text)
        
        full_text = "\n\n".join(text_parts)
        
        if len(full_text.split()) > 40:
            logger.info(f"✅ BeautifulSoup extracted {len(full_text.split())} words from {resolved_url}")
            return full_text
        else:
            logger.warning(f"Extracted text too short ({len(full_text.split())} words) from {resolved_url}")
            
    except Exception as e:
        logger.warning(f"BeautifulSoup extraction failed for {resolved_url}: {e}")
    
    return None

def fetch_rss(url):
    """Fetch and parse RSS feed."""
    logger.info(f"Fetching RSS feed: {url}")
    try:
        feed = feedparser.parse(url, request_headers=HEADERS)
        
        if feed.bozo:
            logger.warning(f"RSS feed has parsing errors: {feed.bozo_exception}")
        
        logger.info(f"Found {len(feed.entries)} entries in feed")
        
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
            logger.info(f"Processing RSS source: {source_url}")
            entry_count = 0
            
            for entry in fetch_rss(source_url):
                entry_count += 1
                url = entry["url"]
                title = entry["title"]
                
                logger.info(f"Processing entry {entry_count}: {title}")
                
                # Skip if we've already processed this URL
                url_hash = hashlib.sha256(url.encode()).hexdigest()
                if url_hash in seen_articles:
                    logger.info(f"Already processed this article, skipping")
                    continue
                seen_articles.add(url_hash)
                
                # Extract article content
                text = extract_main_text(url)
                if not text:
                    logger.warning(f"❌ No content extracted from {url}, skipping")
                    continue
                
                # Create summary
                summary = simple_summarize(text)
                
                articles.append({
                    "title": title,
                    "url": url,
                    "published": entry["published"],
                    "summary": summary
                })
                
                logger.info(f"✅ Added article: {title}")
                
                # Stop when we have enough articles
                if len(articles) >= max_articles:
                    logger.info(f"Reached maximum articles ({max_articles})")
                    break
                    
        except Exception as e:
            logger.error(f"Error processing source {source_url}: {e}")
        
        if len(articles) >= max_articles:
            break
    
    if not articles:
        logger.warning("❌ No articles found")
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
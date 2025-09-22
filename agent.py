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
from newspaper import Article
import json

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
            "https://techcrunch.com/feed/",
            "https://rss.cnn.com/rss/edition.rss",
            "https://feeds.bbci.co.uk/news/rss.xml"
        ],
        "email": {
            "to": "your@email.com",
            "subject_prefix": "Daily Brief"
        },
        "limits": {
            "max_articles": 5,
            "max_retries": 3,
            "timeout": 15
        },
        "content_extraction": {
            "min_words": 50,
            "max_summary_sentences": 3,
            "fallback_to_description": True
        }
    }
    with open(os.path.join(HERE, "config.yaml"), "w", encoding="utf-8") as f:
        yaml.dump(sample_config, f, default_flow_style=False)
    logger.info("Sample config.yaml created. Please edit it with your settings.")

CFG = load_config()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Track seen articles to avoid duplicates
seen_articles_file = os.path.join(HERE, "seen_articles.json")

def load_seen_articles():
    """Load previously seen articles from file."""
    if os.path.exists(seen_articles_file):
        try:
            with open(seen_articles_file, 'r') as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def save_seen_articles(seen_articles):
    """Save seen articles to file."""
    try:
        with open(seen_articles_file, 'w') as f:
            json.dump(list(seen_articles), f)
    except Exception as e:
        logger.warning(f"Failed to save seen articles: {e}")

seen_articles = load_seen_articles()

def resolve_google_news_url(url, max_redirects=5):
    """
    Resolve Google News stub URLs to actual article URLs.
    Enhanced with better error handling and multiple strategies.
    """
    if not url or "news.google.com" not in url:
        return url
        
    logger.info(f"Resolving Google News URL: {url}")
    
    # Method 1: Try to extract URL from Google News parameters
    try:
        parsed = urlparse(url)
        if "articles" in parsed.path:
            query_params = parse_qs(parsed.query)
            if 'url' in query_params:
                actual_url = unquote(query_params['url'][0])
                logger.info(f"Extracted URL from parameters: {actual_url}")
                return actual_url
            # Also try 'u' parameter which is sometimes used
            if 'u' in query_params:
                actual_url = unquote(query_params['u'][0])
                logger.info(f"Extracted URL from 'u' parameter: {actual_url}")
                return actual_url
    except Exception as e:
        logger.debug(f"Failed to extract URL from parameters: {e}")
    
    # Method 2: Follow redirects manually with session
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        
        current_url = url
        for redirect_count in range(max_redirects):
            time.sleep(1)  # Be more polite to Google
            
            response = session.get(current_url, allow_redirects=False, timeout=15)
            
            if response.status_code in [301, 302, 303, 307, 308]:
                next_url = response.headers.get('Location')
                if next_url:
                    logger.debug(f"Redirect {redirect_count + 1}: {current_url} -> {next_url}")
                    current_url = next_url
                    
                    if "news.google.com" not in next_url:
                        logger.info(f"Resolved to actual article: {next_url}")
                        return next_url
                else:
                    break
            elif response.status_code == 200:
                # Try to parse the page for the actual link
                try:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # Look for canonical URL
                    canonical = soup.find('link', rel='canonical')
                    if canonical and canonical.get('href'):
                        canonical_url = canonical['href']
                        if "news.google.com" not in canonical_url:
                            logger.info(f"Found canonical URL: {canonical_url}")
                            return canonical_url
                    
                    # Look for article links
                    article_links = soup.find_all('a', href=True)
                    for link in article_links:
                        href = link['href']
                        if href.startswith('http') and 'news.google.com' not in href:
                            # Basic validation that this looks like an article URL
                            if any(domain in href for domain in ['reuters.com', 'bbc.com', 'cnn.com', 'techcrunch.com', 'venturebeat.com']):
                                logger.info(f"Found article link in page: {href}")
                                return href
                except Exception as e:
                    logger.debug(f"Failed to parse Google News page: {e}")
                break
            else:
                break
                
    except Exception as e:
        logger.warning(f"Failed to resolve Google News URL {url}: {e}")
    
    logger.warning(f"Could not resolve Google News URL, returning original: {url}")
    return url

def safe_get(url, timeout=15):
    """Fetch a URL safely with error handling and retries."""
    max_retries = CFG["limits"].get("max_retries", 3)
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(2 ** attempt)  # Exponential backoff
            
            logger.debug(f"Fetching URL (attempt {attempt + 1}/{max_retries}): {url}")
            response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt == max_retries - 1:
                logger.error(f"All attempts failed for {url}")
                return None
    return None

def extract_main_text(url, title=None, description=None):
    """
    Extract main text content from article URL using multiple methods.
    Enhanced with fallback strategies.
    """
    resolved_url = resolve_google_news_url(url)
    
    if "news.google.com" in resolved_url:
        logger.warning(f"Still on Google News after resolution, using description fallback")
        if CFG["content_extraction"].get("fallback_to_description", True) and description:
            return description
        return None
    
    # Method 1: Try newspaper3k (often works better than trafilatura)
    try:
        logger.info(f"Attempting newspaper3k extraction from: {resolved_url}")
        article = Article(resolved_url)
        article.set_config(headers=HEADERS)
        article.download()
        article.parse()
        
        if article.text and len(article.text.split()) > CFG["content_extraction"].get("min_words", 50):
            logger.info(f"✅ Newspaper3k extracted {len(article.text.split())} words from {resolved_url}")
            return article.text
        else:
            logger.debug("Newspaper3k extracted text too short")
    except Exception as e:
        logger.debug(f"Newspaper3k extraction failed for {resolved_url}: {e}")
    
    # Method 2: Try trafilatura
    try:
        logger.info(f"Attempting trafilatura extraction from: {resolved_url}")
        downloaded = trafilatura.fetch_url(resolved_url, headers=HEADERS)
        if downloaded:
            text = trafilatura.extract(
                downloaded, 
                favor_precision=True,
                include_comments=False,
                include_tables=False,
                config=trafilatura.settings.use_config()
            )
            if text and len(text.split()) > CFG["content_extraction"].get("min_words", 50):
                logger.info(f"✅ Trafilatura extracted {len(text.split())} words from {resolved_url}")
                return text
    except Exception as e:
        logger.debug(f"Trafilatura extraction failed for {resolved_url}: {e}")

    # Method 3: BeautifulSoup fallback
    logger.info(f"Attempting BeautifulSoup extraction from: {resolved_url}")
    response = safe_get(resolved_url)
    if not response:
        # Final fallback to description if available
        if CFG["content_extraction"].get("fallback_to_description", True) and description:
            logger.info("Using RSS description as fallback content")
            return description
        return None

    if "news.google.com" in response.url:
        logger.warning(f"Response URL still on Google News: {response.url}")
        if CFG["content_extraction"].get("fallback_to_description", True) and description:
            return description
        return None
        
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Remove unwanted elements
        for tag in soup(["script", "style", "nav", "header", "footer", "aside", "advertisement", "ads"]):
            tag.decompose()
        
        # Try multiple strategies to find main content
        main_content = None
        
        # Strategy 1: JSON-LD structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'articleBody' in data:
                    text = data['articleBody']
                    if len(text.split()) > CFG["content_extraction"].get("min_words", 50):
                        logger.info(f"✅ JSON-LD extracted {len(text.split())} words from {resolved_url}")
                        return text
            except:
                continue
        
        # Strategy 2: Common article containers
        selectors = [
            'article',
            '[role="main"]',
            'main',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.content',
            '.story-body',
            '.article-body',
            '.post-body',
            '[data-module="ArticleBody"]',
            '.field-name-body'
        ]
        
        for selector in selectors:
            main_content = soup.select_one(selector)
            if main_content:
                logger.debug(f"Found content using selector: {selector}")
                break
        
        # Strategy 3: Find div with content-related class names
        if not main_content:
            main_content = soup.find('div', class_=re.compile(r'content|article|post|story|body|text', re.I))
        
        # Extract text from paragraphs
        if main_content:
            paragraphs = main_content.find_all("p")
        else:
            paragraphs = soup.find_all("p")
        
        text_parts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text and len(text.split()) > 5:  # Skip very short paragraphs
                text_parts.append(text)
        
        full_text = "\n\n".join(text_parts)
        
        if len(full_text.split()) > CFG["content_extraction"].get("min_words", 50):
            logger.info(f"✅ BeautifulSoup extracted {len(full_text.split())} words from {resolved_url}")
            return full_text
        else:
            logger.warning(f"Extracted text too short ({len(full_text.split())} words) from {resolved_url}")
            
    except Exception as e:
        logger.warning(f"BeautifulSoup extraction failed for {resolved_url}: {e}")
    
    # Final fallback to description
    if CFG["content_extraction"].get("fallback_to_description", True) and description:
        logger.info("Using RSS description as final fallback")
        return description
    
    return None

def fetch_rss(url):
    """Fetch and parse RSS feed with better error handling."""
    logger.info(f"Fetching RSS feed: {url}")
    try:
        # Use requests with headers first, then parse
        response = safe_get(url)
        if response:
            feed = feedparser.parse(response.content)
        else:
            # Fallback to feedparser's built-in fetching
            feed = feedparser.parse(url, request_headers=HEADERS)
        
        if feed.bozo:
            logger.warning(f"RSS feed has parsing errors: {feed.bozo_exception}")
        
        logger.info(f"Found {len(feed.entries)} entries in feed")
        
        for entry in feed.entries:
            if hasattr(entry, 'link'):
                # Get description/summary for fallback
                description = ""
                if hasattr(entry, 'description'):
                    # Clean HTML from description
                    desc_soup = BeautifulSoup(entry.description, 'html.parser')
                    description = desc_soup.get_text(strip=True)
                elif hasattr(entry, 'summary'):
                    # Clean HTML from summary
                    summary_soup = BeautifulSoup(entry.summary, 'html.parser')
                    description = summary_soup.get_text(strip=True)
                
                yield {
                    "title": getattr(entry, "title", "(no title)"),
                    "url": entry.link,
                    "published": getattr(entry, "published", ""),
                    "description": description
                }
    except Exception as e:
        logger.error(f"Error fetching RSS from {url}: {e}")

def simple_summarize(text, max_sentences=None):
    """Create a simple summary by selecting the most important sentences."""
    if max_sentences is None:
        max_sentences = CFG["content_extraction"].get("max_summary_sentences", 3)
    
    # Clean and split into sentences
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if 10 < len(s.split()) < 200]
    
    if len(sentences) <= max_sentences:
        return "\n".join(f"• {s}" for s in sentences)
    
    # Simple word frequency scoring
    word_freq = {}
    all_words = []
    
    for sentence in sentences:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', sentence.lower())
        all_words.extend(words)
        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1
    
    # Filter out too common words
    avg_freq = sum(word_freq.values()) / len(word_freq) if word_freq else 1
    important_words = {word: freq for word, freq in word_freq.items() if freq > 1 and freq < avg_freq * 3}
    
    # Score sentences based on important word frequency
    sentence_scores = []
    for sentence in sentences:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', sentence.lower())
        score = sum(important_words.get(word, 0) for word in words)
        # Boost score for sentences that appear early (likely more important)
        position_boost = len(sentences) - sentences.index(sentence)
        final_score = score + (position_boost * 0.1)
        sentence_scores.append((final_score, sentence))
    
    # Get top sentences, but preserve original order
    top_sentences = [s for _, s in sorted(sentence_scores, reverse=True)[:max_sentences]]
    # Sort by original appearance in text
    top_sentences.sort(key=lambda x: sentences.index(x) if x in sentences else 999)
    
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
    processed_count = 0
    
    # Fetch articles from all sources
    for source_url in CFG["sources"]:
        try:
            logger.info(f"Processing RSS source: {source_url}")
            entry_count = 0
            
            for entry in fetch_rss(source_url):
                entry_count += 1
                processed_count += 1
                url = entry["url"]
                title = entry["title"]
                description = entry["description"]
                
                logger.info(f"Processing entry {entry_count}: {title}")
                
                # Skip if we've already processed this URL
                url_hash = hashlib.sha256(url.encode()).hexdigest()
                if url_hash in seen_articles:
                    logger.info(f"Already processed this article, skipping")
                    continue
                seen_articles.add(url_hash)
                
                # Extract article content
                text = extract_main_text(url, title, description)
                if not text:
                    logger.warning(f"❌ No content extracted from {url}, skipping")
                    continue
                
                # Create summary
                try:
                    summary = simple_summarize(text)
                except Exception as e:
                    logger.warning(f"Failed to summarize, using first part of text: {e}")
                    # Fallback: use first few sentences
                    sentences = text.split('.')[:3]
                    summary = '. '.join(sentences) + '.'
                
                articles.append({
                    "title": title,
                    "url": url,
                    "published": entry["published"],
                    "summary": summary,
                    "word_count": len(text.split())
                })
                
                logger.info(f"✅ Added article: {title} ({len(text.split())} words)")
                
                # Stop when we have enough articles
                if len(articles) >= max_articles:
                    logger.info(f"Reached maximum articles ({max_articles})")
                    break
                    
        except Exception as e:
            logger.error(f"Error processing source {source_url}: {e}")
        
        if len(articles) >= max_articles:
            break
    
    # Save seen articles
    save_seen_articles(seen_articles)
    
    logger.info(f"📊 Processed {processed_count} entries, successfully extracted {len(articles)} articles")
    
    if not articles:
        logger.warning("❌ No articles found")
        # Send a notification email if configured
        if CFG.get("email", {}).get("notify_on_empty", False):
            subject = f"{CFG['email']['subject_prefix']} — No Articles Found — {datetime.now().strftime('%Y-%m-%d')}"
            body = f"No articles were found during the daily brief run on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\nProcessed {processed_count} entries from {len(CFG['sources'])} sources."
            send_email(subject, body)
        return
    
    # Build email content
    today = datetime.now().strftime("%Y-%m-%d")
    email_body = f"Daily Brief — {today}\n" + "="*50 + "\n\n"
    email_body += f"Found {len(articles)} articles from {len(CFG['sources'])} sources\n\n"
    
    for i, article in enumerate(articles, 1):
        email_body += f"{i}. {article['title']}\n"
        email_body += f"   Source: {article['url']}\n"
        if article['published']:
            email_body += f"   Published: {article['published']}\n"
        email_body += f"   Words: {article['word_count']}\n"
        email_body += f"\n   Summary:\n   {article['summary']}\n\n"
        email_body += "-" * 80 + "\n\n"
    
    # Add footer
    email_body += f"\nGenerated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    email_body += f"Processed {processed_count} total entries"
    
    # Send email
    subject = f"{CFG['email']['subject_prefix']} — {today}"
    success = send_email(subject, email_body)
    
    if success:
        logger.info("✅ Daily brief sent successfully!")
    else:
        logger.error("❌ Failed to send daily brief")
    
    logger.info(f"📊 Successfully processed {len(articles)} articles from {processed_count} total entries")

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
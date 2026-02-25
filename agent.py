import os
import re
import time
import yaml
import json
import math
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

# ---- Config loading ----
HERE = os.path.dirname(os.path.abspath(__file__))

def load_config():
    with open(os.path.join(HERE, "config.yaml"), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CFG = load_config()
TZ = tz.gettz(CFG.get("timezone", "Asia/Kolkata"))

# ---- Storage (SQLite) ----
DB_PATH = os.path.join(HERE, CFG["storage"]["db_path"])
REPORTS_DIR = os.path.join(HERE, CFG["storage"]["reports_dir"])
os.makedirs(REPORTS_DIR, exist_ok=True)

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS seen (
        url TEXT PRIMARY KEY,
        title TEXT,
        content_hash TEXT,
        first_seen_ts INTEGER
    )""")
    con.commit()
    return con

# ---- Fetch helpers ----
HEADERS = {
    "User-Agent": "DailyBriefAgent/1.0 (+personal research; contact: you@example.com)"
}

def polite_get(url, timeout=20):
    time.sleep(0.3)  # be polite
    return requests.get(url, headers=HEADERS, timeout=timeout)

def extract_main_text(url):
    """
    Use trafilatura for robust extraction. Fallback to BeautifulSoup text if needed.
    """
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
            if text and len(text.split()) > 40:
                return text
    except Exception:
        pass
    try:
        r = polite_get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        [s.extract() for s in soup(["script", "style", "noscript", "header", "footer", "aside", "nav"])]
        text = " ".join(soup.stripped_strings)
        return text if len(text.split()) > 40 else None
    except Exception:
        return None

def fetch_rss(url):
    d = feedparser.parse(url)
    for e in d.entries:
        link = getattr(e, "link", None)
        title = getattr(e, "title", "(no title)")
        description = getattr(e, "summary", "") or getattr(e, "description", "") or ""
        published = None
        for key in ("published_parsed", "updated_parsed"):
            if getattr(e, key, None):
                ts = int(time.mktime(getattr(e, key)))
                published = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(TZ)
                break
        yield {"title": title, "url": link, "published": published, "description": description}

# ---- Relevance scoring ----
TOPICS = [t.lower() for t in CFG.get("topics", [])]

def score(text, title):
    base = 0
    hay = (title or "") + "\n" + (text or "")
    hay_lower = hay.lower()
    for t in TOPICS:
        # simple keyword presence + fuzzy on title
        base += hay_lower.count(t.lower())
        if title:
            base += fuzz.partial_ratio(t.lower(), title.lower()) / 100.0
    return base

# ---- Summarization ----
def summarize(text, title=None):
    conf = CFG["summarization"]
    lang = conf.get("language", "en")
    max_words = conf.get("max_words", 140)

    if not conf.get("enabled", True):
        return None

    provider = conf.get("provider", "builtin")
    if provider == "openai" and os.environ.get("OPENAI_API_KEY"):
        try:
            from openai import OpenAI
            client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            prompt = f"Summarize this article in {max_words} words max in {lang}. Use bullets with crisp facts. Keep links/tickers if present.\n\nTITLE: {title}\n\nTEXT:\n{text[:6000]}"
            resp = client.responses.create(
                model=conf.get("model", "gpt-4o-mini"),
                input=prompt,
            )
            out = resp.output_text
            return out.strip()
        except Exception as e:
            # fallback
            pass

    # builtin extractive: take top sentences by naive frequency
    sentences = re.split(r'(?<=[.!?])\s+', text)
    sentences = [s.strip() for s in sentences if len(s.split()) > 8][:30]
    freq = {}
    for s in sentences:
        for w in re.findall(r"[A-Za-z]{3,}", s.lower()):
            freq[w] = freq.get(w, 0) + 1
    scored = []
    for s in sentences:
        s_score = sum(freq.get(w, 0) for w in re.findall(r"[A-Za-z]{3,}", s.lower()))
        scored.append((s_score, s))
    scored.sort(reverse=True, key=lambda x: x[0])
    out = []
    for _, s in scored[:6]:
        out.append("- " + s)
    return "\n".join(out)

# ---- Delivery ----
def send_email(subject, body_md):
    if not CFG["delivery"]["email"]["enabled"]:
        return False
    import smtplib
    from email.message import EmailMessage

    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    pwd  = os.environ.get("SMTP_PASSWORD")
    from_addr = os.environ.get("SMTP_FROM", user)
    to_addr = CFG["delivery"]["email"]["to"]

    if not all([host, port, user, pwd, from_addr, to_addr]):
        print("[email] Missing SMTP env vars.")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(body_md)

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)
    return True

def send_slack(subject, body_md):
    cfg = CFG["delivery"]["slack"]
    if not cfg.get("enabled"):
        return False
    url = os.environ.get(cfg.get("webhook_url_env", "SLACK_WEBHOOK_URL"))
    if not url:
        print("[slack] Missing webhook URL env var.")
        return False
    payload = {"text": f"*{subject}*\n{body_md}"}
    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.status_code // 100 == 2
    except Exception:
        return False

# ---- Report builder ----
def build_report(items):
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    lines = [f"# Daily Brief — {today}", ""]
    for i, it in enumerate(items, 1):
        lines.append(f"## {i}. {it['title']}")
        if it.get("published"):
            lines.append(f"_Published:_ {it['published'].strftime('%Y-%m-%d %H:%M %Z')}")
        lines.append(f"_Source:_ {it['url']}")
        if it.get("summary"):
            lines.append("")
            lines.append(it["summary"])
        lines.append("")
    return "\n".join(lines)

# ---- Main run ----
def run():
    con = init_db()
    cur = con.cursor()
    candidates = []

    # 1) Collect RSS metadata and score without fetching full content
    rss_candidates = []
    for src in CFG["sources"]:
        if "rss" in src or src.endswith(".xml") or src.startswith("http"):
            try:
                for e in fetch_rss(src):
                    if not e["url"]:
                        continue
                    # Skip already-seen URLs
                    cur.execute("SELECT 1 FROM seen WHERE url = ?", (e["url"],))
                    if cur.fetchone():
                        continue
                    # Pre-score using title + RSS description (no HTTP fetch yet)
                    s = score(e["description"], e["title"])
                    if s >= CFG["ranking"].get("min_score", 1):
                        rss_candidates.append({**e, "score": s})
            except Exception as ex:
                print(f"[warn] source failed: {src} -> {ex}")

    # 2) Rank by pre-score and limit before doing any full-content fetches
    rss_candidates.sort(key=lambda x: (x["score"], x["published"] or datetime.min.replace(tzinfo=TZ)), reverse=True)
    max_fetch = min(CFG["limits"]["per_run_max_articles"], len(rss_candidates))
    print(f"[info] {len(rss_candidates)} RSS candidates, fetching content for top {max_fetch}")

    # 3) Fetch full content only for top candidates
    for e in rss_candidates[:max_fetch]:
        url = e["url"]
        title = e["title"]
        text = extract_main_text(url)
        if not text:
            continue
        h = hashlib.sha256(text.encode("utf-8")).hexdigest()
        # Deduplicate by content hash
        cur.execute("SELECT 1 FROM seen WHERE content_hash = ?", (h,))
        if cur.fetchone():
            continue
        candidates.append({
            "title": title,
            "url": url,
            "published": e["published"],
            "text": text,
            "score": e["score"],
            "hash": h
        })
        print(f"[fetch] ({len(candidates)}/{max_fetch}) {title[:60]}")

    # 4) Final rank
    candidates.sort(key=lambda x: (x["score"], x["published"] or datetime.min.replace(tzinfo=TZ)), reverse=True)
    top = candidates

    # 6) Summarize
    summarized = []
    for it in top[:CFG["limits"]["per_run_max_summary"]]:
        sm = summarize(it["text"], title=it["title"])
        it["summary"] = sm
        summarized.append(it)

    # 7) Save report
    report_md = build_report(summarized)
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    report_path = os.path.join(REPORTS_DIR, f"{today}.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_md)

    # 8) Send
    subject = CFG["delivery"]["email"]["subject_prefix"] + " " + today
    sent_email = send_email(subject, report_md)
    sent_slack = send_slack(subject, report_md)

    # 9) Persist seen items
    now_ts = int(time.time())
    for it in summarized:
        cur.execute("INSERT OR IGNORE INTO seen(url, title, content_hash, first_seen_ts) VALUES (?, ?, ?, ?)",
                    (it["url"], it["title"], it["hash"], now_ts))
    con.commit()
    con.close()

    print(f"[ok] Wrote {report_path}. Email sent={sent_email}, Slack sent={sent_slack}")

if __name__ == "__main__":
    run()

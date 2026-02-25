"""
report.py — Markdown report builder. Pure sync.
"""
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


def build_report(items: list[dict], local_tz) -> str:
    """Build a markdown daily brief from summarized article dicts."""
    today = datetime.now(local_tz).strftime("%Y-%m-%d")
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


def save_report(report_md: str, reports_dir: str, local_tz) -> str:
    """Write report to disk; returns the file path."""
    os.makedirs(reports_dir, exist_ok=True)
    today = datetime.now(local_tz).strftime("%Y-%m-%d")
    path = os.path.join(reports_dir, f"{today}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(report_md)
    logger.info("Report saved to %s", path)
    return path

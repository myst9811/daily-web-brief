# Daily Web Briefing Agent (Starter)

A small Python agent that collects articles from RSS & web pages, extracts content, deduplicates, ranks by your keywords, summarizes with an LLM (optional), and sends you a daily Markdown brief via email or Slack. Ships with simple, legal-first defaults (RSS preferred) and respects site policies.

## 1) Quickstart

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python agent.py
```

- Edit `config.yaml` to add your topics and sources.
- Output is saved in `reports/YYYY-MM-DD.md`.
- To email the brief: set `delivery.email.enabled: true` and export
  `SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM`.

## 2) Optional LLM Summaries

- Set `summarization.enabled: true`.
- Provider `openai` requires `OPENAI_API_KEY`. The agent uses the Responses API via the official SDK.
- Or set provider to `builtin` to use a simple, local, extractive summary.

## 3) Schedule It

**Cron (Linux/macOS):**
```
0 7 * * * /full/path/.venv/bin/python /full/path/agent.py >> /full/path/cron.log 2>&1
```
This runs at 07:00 daily (your config uses Asia/Kolkata).

**GitHub Actions:** see `.github/workflows/run.yml`.

## 4) Legal & Ethical Use

- Prefer RSS feeds and official APIs. **Always** respect terms of service and `robots.txt`.
- Avoid overwhelming any site (the agent has polite delays).
- Use scraped content only for personal research unless you have permission.

## 5) Customize

- Add site-specific scrapers in `agent.py` if RSS is not available.
- Tune ranking/limits in `config.yaml`.
- Extend `send_slack()` or add Telegram/Notion.

## 6) Troubleshooting

- No summaries? Ensure `OPENAI_API_KEY` is set or switch to `builtin`.
- Gmail SMTP requires an App Password if 2FA is on.

Happy briefings!

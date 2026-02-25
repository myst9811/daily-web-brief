"""
delivery.py — Async email + Slack delivery.
"""
import asyncio
import logging
import os

import httpx

logger = logging.getLogger(__name__)


async def send_email(subject: str, body_md: str, cfg: dict) -> bool:
    """Send email via SMTP. smtplib is sync — runs in executor."""
    if not cfg.get("enabled", False):
        return False

    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    pwd = os.environ.get("SMTP_PASSWORD")
    from_addr = os.environ.get("SMTP_FROM", user)
    to_addr = cfg.get("to")

    missing = [k for k, v in {"SMTP_HOST": host, "SMTP_USER": user, "SMTP_PASSWORD": pwd}.items() if not v]
    if missing or not to_addr:
        logger.warning("send_email: missing env vars %s or config 'to'", missing)
        return False

    def _send() -> bool:
        import smtplib
        from email.message import EmailMessage

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

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, _send)
        logger.info("Email sent to %s", to_addr)
        return result
    except Exception as exc:
        logger.error("send_email failed: %s", exc)
        return False


async def send_slack(subject: str, body_md: str, cfg: dict) -> bool:
    """Post to Slack webhook via httpx."""
    if not cfg.get("enabled", False):
        return False

    webhook_env = cfg.get("webhook_url_env", "SLACK_WEBHOOK_URL")
    url = os.environ.get(webhook_env)
    if not url:
        logger.warning("send_slack: env var %s not set", webhook_env)
        return False

    payload = {"text": f"*{subject}*\n{body_md}"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=15.0)
            success = resp.status_code // 100 == 2
            if success:
                logger.info("Slack message sent")
            else:
                logger.warning("Slack returned status %d", resp.status_code)
            return success
    except Exception as exc:
        logger.error("send_slack failed: %s", exc)
        return False


async def deliver(
    subject: str,
    body_md: str,
    delivery_cfg: dict,
) -> dict[str, bool]:
    """Concurrent email + Slack delivery."""
    email_result, slack_result = await asyncio.gather(
        send_email(subject, body_md, delivery_cfg.get("email", {})),
        send_slack(subject, body_md, delivery_cfg.get("slack", {})),
    )
    return {"email": email_result, "slack": slack_result}

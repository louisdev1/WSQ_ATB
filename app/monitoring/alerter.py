"""
alerter.py – Telegram alert sender with cooldown.

Only fires for genuine unresolved problems.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from app.config import config

log = logging.getLogger(__name__)

# In-memory cooldown tracker: alert_type → last sent UTC timestamp
_last_sent: dict[str, datetime] = {}


async def send_alert(alert_type: str, message: str, db=None) -> bool:
    """
    Send a Telegram alert if cooldown has elapsed.
    db is optional – used to persist alert history.
    """
    if not config.alert_bot_token or not config.alert_chat_id:
        log.warning("Alert bot not configured – cannot send: %s", message)
        return False

    now = datetime.now(timezone.utc)
    last = _last_sent.get(alert_type)
    if last:
        elapsed = (now - last).total_seconds()
        if elapsed < config.alert_cooldown_seconds:
            log.debug("Alert %s suppressed (cooldown %.0fs remaining)",
                      alert_type, config.alert_cooldown_seconds - elapsed)
            return False

    text = f"🤖 *TradingBot Alert*\n`{alert_type}`\n\n{message}"
    url = f"https://api.telegram.org/bot{config.alert_bot_token}/sendMessage"
    payload = {
        "chat_id": config.alert_chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    _last_sent[alert_type] = now
                    log.info("Alert sent: %s", alert_type)
                    if db:
                        await db.save_alert(alert_type, message)
                    return True
                else:
                    body = await resp.text()
                    log.error("Alert send failed (HTTP %s): %s", resp.status, body)
    except Exception as exc:
        log.error("Alert send exception: %s", exc)

    return False

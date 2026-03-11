"""
main.py – Bot entry point.

Run with:
    python -m app.main

Architecture:
  - Telegram listener  → parse signals → TradeManager
  - Bybit WebSocket    → real-time fills → TradeManager (TP ratchet, SL enforcement)
  - Watchdog           → health checks, REST fallback fill sync (every 30s)
"""

import asyncio
import logging
import sys
import traceback

from app.config import config
from app.logger import setup_logging
from app.storage.database import Database
from app.exchange.bybit_client import BybitClient
from app.exchange.ws_stream import BybitStream
from app.parsing.parser import parse_message
from app.parsing.models import MessageType
from app.domain.trade_manager import TradeManager
from app.intake.telegram_listener import TelegramListener
from app.monitoring.watchdog import watchdog_loop, report_bybit_ok, report_bybit_fail
from app.monitoring.alerter import send_alert

log = logging.getLogger(__name__)

_TELEGRAM_RECONNECT_DELAY = 15


async def _on_telegram_message(db: Database, trade_manager: TradeManager,
                                raw_text: str, msg_id: int):
    if await db.is_duplicate_message(msg_id):
        log.debug("Duplicate message %d – skipped", msg_id)
        return

    parsed = parse_message(raw_text, msg_id)
    log.info("MSG[%d] type=%s symbol=%s",
             msg_id, parsed.message_type.value,
             getattr(parsed, "symbol", "-") or "-")

    await db.save_raw_message(msg_id, raw_text, parsed.message_type.value)

    if parsed.message_type in (MessageType.IGNORE, MessageType.COMMENTARY):
        return

    try:
        await trade_manager.handle(parsed)
    except Exception as exc:
        log.error("TradeManager.handle error: %s\n%s", exc, traceback.format_exc())


async def _telegram_loop(db: Database, trade_manager: TradeManager):
    """Keeps the Telegram listener alive with auto-reconnect."""
    while True:
        async def message_handler(raw_text: str, msg_id: int):
            await _on_telegram_message(db, trade_manager, raw_text, msg_id)

        listener = TelegramListener(on_message=message_handler)
        try:
            await listener.start()
            log.warning("Telegram listener stopped. Reconnecting in %ss…", _TELEGRAM_RECONNECT_DELAY)
        except Exception as exc:
            log.error("Telegram listener crashed: %s", exc)
        await asyncio.sleep(_TELEGRAM_RECONNECT_DELAY)


async def main():
    # ── setup ─────────────────────────────────────────────────────────────────
    config.ensure_dirs()
    setup_logging(config.log_file)
    log.info("=" * 60)
    log.info("Trading Bot starting (dry_run=%s, testnet=%s)",
             config.dry_run, config.bybit_testnet)

    # ── storage ───────────────────────────────────────────────────────────────
    db = Database(config.db_path)
    await db.connect()

    # ── exchange (REST) ───────────────────────────────────────────────────────
    bybit = BybitClient(
        api_key=config.bybit_api_key,
        api_secret=config.bybit_api_secret,
        testnet=config.bybit_testnet,
    )
    bybit._dry_run = config.dry_run
    bybit.set_health_callbacks(on_ok=report_bybit_ok, on_fail=report_bybit_fail)

    # ── domain ────────────────────────────────────────────────────────────────
    trade_manager = TradeManager(db=db, bybit=bybit)

    # ── exchange (WebSocket) ──────────────────────────────────────────────────
    ws = BybitStream(
        api_key=config.bybit_api_key,
        api_secret=config.bybit_api_secret,
        testnet=config.bybit_testnet,
        on_execution=trade_manager.on_ws_execution,
        on_order=trade_manager.on_ws_order,
        dry_run=config.dry_run,
    )

    # ── run all tasks concurrently ────────────────────────────────────────────
    try:
        await asyncio.gather(
            _telegram_loop(db, trade_manager),
            ws.start(),
            watchdog_loop(db, bybit, trade_manager),
        )
    except KeyboardInterrupt:
        log.info("Shutting down…")
    except Exception as exc:
        tb = traceback.format_exc()
        log.critical("Fatal crash: %s\n%s", exc, tb)
        await send_alert("fatal_crash", f"Bot crashed:\n```{tb[-800:]}```", db)
        sys.exit(1)
    finally:
        ws.stop()
        await db.close()
        log.info("Bot stopped")


if __name__ == "__main__":
    asyncio.run(main())

"""
watchdog.py – Monitors bot health and fires Telegram alerts for real problems.

Tracks:
- Telegram connectivity
- Bybit API connectivity
- Unprotected positions (no SL)
- Log tail scanning for tracebacks

Never alerts for: successful self-recoveries, ignored signals, commentary.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.config import config
from app.monitoring.alerter import send_alert

log = logging.getLogger(__name__)


class IssueTracker:
    """Tracks when an issue first appeared."""
    def __init__(self):
        self._issues: dict[str, datetime] = {}

    def mark(self, key: str):
        if key not in self._issues:
            self._issues[key] = datetime.now(timezone.utc)
            log.debug("Issue first seen: %s", key)

    def clear(self, key: str):
        if key in self._issues:
            del self._issues[key]
            log.debug("Issue resolved: %s", key)

    def age_seconds(self, key: str) -> float:
        if key not in self._issues:
            return 0.0
        return (datetime.now(timezone.utc) - self._issues[key]).total_seconds()


_tracker = IssueTracker()

# ── connectivity flags (set by intake/exchange layers) ────────────────────────

_telegram_ok: bool = True
_bybit_ok: bool = True


def report_telegram_ok():
    global _telegram_ok
    _telegram_ok = True
    _tracker.clear("telegram_down")


def report_telegram_fail():
    global _telegram_ok
    _telegram_ok = False
    _tracker.mark("telegram_down")


def report_bybit_ok():
    global _bybit_ok
    _bybit_ok = True
    _tracker.clear("bybit_down")


def report_bybit_fail():
    global _bybit_ok
    _bybit_ok = False
    _tracker.mark("bybit_down")


# ── log tail watcher ──────────────────────────────────────────────────────────

_TRACEBACK_RE = re.compile(r"Traceback \(most recent call last\)|CRITICAL|FATAL", re.IGNORECASE)
_last_log_pos: int = 0


async def _check_log_for_tracebacks(db) -> None:
    global _last_log_pos
    log_path: Path = config.log_file
    if not log_path.exists():
        return
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(_last_log_pos)
            new_content = f.read()
            _last_log_pos = f.tell()
        if _TRACEBACK_RE.search(new_content):
            snippet = new_content[-800:].strip()
            await send_alert("fatal_error", f"Log contains critical error:\n```\n{snippet}\n```", db)
    except Exception as exc:
        log.error("Log watcher error: %s", exc)


# ── entry order timeout (48h) ─────────────────────────────────────────────────

_ENTRY_TIMEOUT_HOURS = 48
_ENTRY_TIMEOUT_SECS  = _ENTRY_TIMEOUT_HOURS * 3600


async def _check_entry_timeouts(db, bybit) -> None:
    """
    Cancel open entry orders for any trade where:
      - entries_cancelled is still 0 (entries are still live)
      - the signal is older than 48h
      - no position has filled yet (filled_size == 0)

    If a position is already partially or fully filled we leave entries alone —
    the ladder is still working and TP1 will cancel them when it fires.
    """
    try:
        trades = await db.get_active_trades()
        now = datetime.now(timezone.utc)
        for trade in trades:
            if trade.get("entries_cancelled"):
                continue
            if (trade.get("filled_size") or 0) > 0:
                continue  # position already filling — let TP1 handle it

            created_raw = trade.get("created_at") or trade.get("updated_at")
            if not created_raw:
                continue
            created = datetime.fromisoformat(str(created_raw)).replace(tzinfo=timezone.utc)
            age_secs = (now - created).total_seconds()

            if age_secs >= _ENTRY_TIMEOUT_SECS:
                sym = trade["symbol"]
                age_h = age_secs / 3600
                log.warning(
                    "Entry timeout: %s signal is %.1fh old with no fill — "
                    "cancelling entry orders", sym, age_h,
                )
                bybit.cancel_entry_orders(sym)
                await db.update_trade(trade["id"], entries_cancelled=1)
                await send_alert(
                    f"entry_timeout_{sym}",
                    f"⏱ Entry timeout for {sym}: signal was {age_h:.1f}h old "
                    f"with no position filled. Entry orders cancelled.",
                    db,
                )
    except Exception as exc:
        log.error("Entry timeout check error: %s", exc)


# ── unprotected position check ────────────────────────────────────────────────

async def _check_unprotected_positions(db, bybit) -> None:
    if not bybit:
        return
    try:
        trades = await db.get_active_trades()
        for trade in trades:
            sym = trade["symbol"]
            pos = bybit.fetch_position(sym)
            if not pos:
                continue
            sl = pos.get("stopLoss")
            if not sl or float(sl) == 0:
                _tracker.mark(f"no_sl_{sym}")
                age = _tracker.age_seconds(f"no_sl_{sym}")
                if age > config.alert_sl_seconds:
                    await send_alert(
                        f"no_sl_{sym}",
                        f"⚠️ Position {sym} appears to have NO stop-loss! Age: {age:.0f}s",
                        db,
                    )
            else:
                _tracker.clear(f"no_sl_{sym}")
    except Exception as exc:
        log.error("Unprotected position check error: %s", exc)


# ── main watchdog loop ────────────────────────────────────────────────────────

async def watchdog_loop(db, bybit, trade_manager):
    """Runs forever in the background. Check interval: 30s."""
    log.info("Watchdog started")
    while True:
        try:
            # Telegram connectivity check
            if not _telegram_ok:
                age = _tracker.age_seconds("telegram_down")
                if age > config.alert_telegram_seconds:
                    await send_alert(
                        "telegram_down",
                        f"Telegram listener has been down for {age:.0f}s and has not recovered.",
                        db,
                    )

            # Bybit connectivity check
            if not _bybit_ok:
                age = _tracker.age_seconds("bybit_down")
                if age > config.alert_bybit_seconds:
                    await send_alert(
                        "bybit_down",
                        f"Bybit API has been unreachable for {age:.0f}s.",
                        db,
                    )

            # Log tail scan
            await _check_log_for_tracebacks(db)

            # Unprotected positions
            await _check_unprotected_positions(db, bybit)

            # Entry order timeout (48h)
            await _check_entry_timeouts(db, bybit)

            # Sync fills
            if trade_manager:
                await trade_manager.sync_fills()

        except Exception as exc:
            log.error("Watchdog loop error: %s", exc)

        await asyncio.sleep(30)

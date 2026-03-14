"""
trade_manager.py – Central domain layer.

All trade lifecycle decisions live here.
Exchange layer is called for execution only.

Entry ladder (dynamic by range %):
  range_pct = (entry_high - entry_low) / midpoint
  < 0.7%        →  80 / 15 / 5
  0.7% – 1.5%   →  70 / 20 / 10
  1.5% – 3%     →  65 / 25 / 10
  > 3%          →  50 / 30 / 20

  Distribution:
    Entry_high → largest fraction  (soonest to fill)
    Entry_mid  → middle fraction
    Entry_low  → smallest fraction (best price, least likely)

TP orders:    placed on first fill, qty split equally across remaining targets
SL:           placed immediately on signal arrival
TP ratchet:   TP1 fills → cancel entries, move SL to avg_entry
              TP2 fills → move SL to TP1 price
              TP3 fills → move SL to TP2 price  (etc.)
Break-even:   moves SL to avg entry and cancels remaining entry orders
"""

import logging
import math
from typing import Optional, List, Tuple

from app.config import config
from app.exchange.bybit_client import BybitClient
from app.storage.database import Database
from app.parsing.models import (
    ParsedMessage, MessageType, Direction,
    NewSignal, CloseAll, CloseSymbol, CancelRemainingEntries,
    MoveSLBreakEven, MoveSLPrice, UpdateTargets, AddEntries,
    MarketEntry, PartialClose, CancelSignal,
)

log = logging.getLogger(__name__)


# ── dynamic entry ladder ──────────────────────────────────────────────────────

def _calc_ladder(entry_low: float, entry_high: float, direction: str = "long") -> List[Tuple]:
    """
    Returns a list of (price, fraction) tuples for entry orders.

    LONG:  entry_high 65% / midpoint 25% / entry_low 10%
           Price falls into the range — highest price fills first, most size there.

    SHORT: entry_low 65% / midpoint 25% / entry_high 10%
           Price rises into the range — lowest price fills first, most size there.

    Collapses to single order at full qty for single-price signals.
    """
    if entry_low <= 0 or entry_high <= entry_low:
        price = entry_high if entry_high > 0 else entry_low
        return [(price, 1.0)]

    midpoint = (entry_low + entry_high) / 2
    if direction.lower() in ("short", "sell"):
        return [
            (entry_low,  0.65),
            (midpoint,   0.25),
            (entry_high, 0.10),
        ]
    return [
        (entry_high, 0.65),
        (midpoint,   0.25),
        (entry_low,  0.10),
    ]


def _opposite_side(direction: str) -> str:
    return "Sell" if direction.lower() in ("long", "buy") else "Buy"


def _entry_side(direction: str) -> str:
    return "Buy" if direction.lower() in ("long", "buy") else "Sell"


def _calc_qty(balance: float, risk_fraction: float, entry_price: float,
              stop_loss: float, leverage: int) -> float:
    """Return raw quantity. Caller must round via BybitClient._round_qty()
    to respect per-symbol qtyStep — do NOT floor here."""
    if entry_price <= 0 or stop_loss <= 0 or entry_price == stop_loss:
        return 0.0
    risk_amount = balance * risk_fraction
    distance    = abs(entry_price - stop_loss)
    qty = risk_amount / distance
    max_qty_by_balance = (balance * leverage) / entry_price
    return min(qty, max_qty_by_balance)


def _floor_frac(total: float, frac: float, step: float) -> float:
    """Apply a fraction to a total and floor to the given step size."""
    if step <= 0:
        step = 0.001
    raw = total * frac
    factor = 1.0 / step
    return math.floor(raw * factor) / factor


# ── TP distribution table ─────────────────────────────────────────────────────
# Keyed by total number of TP targets in the signal.
# Values are percentage weights (sum to 100). Front-loaded: earlier TPs get more.
# For n > 15 falls back to equal split.

_TP_DIST: dict = {
    1:  [100],
    2:  [70, 30],
    3:  [50, 30, 20],
    4:  [40, 30, 20, 10],
    5:  [35, 25, 20, 12, 8],
    6:  [30, 25, 18, 12, 9, 6],
    7:  [28, 22, 16, 12, 9, 7, 6],
    8:  [25, 20, 15, 12, 10, 8, 6, 4],
    9:  [23, 18, 14, 12, 10, 8, 6, 5, 4],
    10: [20, 17, 14, 12, 10, 8, 6, 5, 4, 4],
    11: [19, 16, 13, 11, 10, 8, 7, 6, 5, 3, 2],
    12: [18, 15, 12, 11, 10, 8, 7, 6, 5, 4, 2, 2],
    13: [17, 14, 12, 11,  9, 8, 7, 6, 5, 4, 3, 2, 2],
    14: [16, 14, 11, 10,  9, 8, 7, 6, 5, 4, 3, 3, 2, 2],
    15: [15, 13, 11, 10,  9, 8, 7, 6, 5, 4, 3, 3, 2, 2, 2],
}


def _tp_fractions(n: int) -> list:
    """Return a list of fractions (0.0–1.0) for n TP targets."""
    if n in _TP_DIST:
        pcts = _TP_DIST[n]
    else:
        # Equal split for anything beyond 15
        pcts = [100 / n] * n
    total = sum(pcts)
    return [p / total for p in pcts]


class TradeManager:
    def __init__(self, db: Database, bybit: BybitClient):
        self._db    = db
        self._bybit = bybit

    async def handle(self, msg: ParsedMessage):
        t = msg.message_type
        if t == MessageType.NEW_SIGNAL:
            await self._handle_new_signal(msg)
        elif t == MessageType.CLOSE_ALL:
            await self._handle_close_all(msg)
        elif t == MessageType.CLOSE_SYMBOL:
            await self._handle_close_symbol(msg)
        elif t == MessageType.CANCEL_REMAINING_ENTRIES:
            await self._handle_cancel_entries(msg)
        elif t == MessageType.MOVE_SL_BREAK_EVEN:
            await self._handle_move_sl_be(msg)
        elif t == MessageType.MOVE_SL_PRICE:
            await self._handle_move_sl_price(msg)
        elif t == MessageType.UPDATE_TARGETS:
            await self._handle_update_targets(msg)
        elif t == MessageType.MARKET_ENTRY:
            await self._handle_market_entry(msg)
        elif t == MessageType.PARTIAL_CLOSE:
            await self._handle_partial_close(msg)
        elif t == MessageType.CANCEL_SIGNAL:
            await self._handle_cancel_signal(msg)
        elif t == MessageType.ADD_ENTRIES:
            await self._handle_add_entries(msg)
        else:
            log.debug("Ignored message type %s", t)

    # ── startup position sync ─────────────────────────────────────────────────

    async def startup_position_sync(self):
        """
        On startup:
          1. Fetch all open Bybit positions and seed any missing from DB.
          2. Scan all active DB trades that have NO live Bybit position and
             close them immediately (avoids waiting for the first watchdog tick).

        Seeded records have:
          - filled_size / avg_entry_price populated from the live position
          - entries_cancelled = 1  (open entry orders cancelled immediately)
          - stop_loss read from position if Bybit has it set, else 0
          - targets = []  so sync_fills skips TP placement (user must re-add via Telegram)
        """
        if config.dry_run:
            return

        # ── Step 1: seed live positions not yet in DB ─────────────────────────
        positions = self._bybit.fetch_all_positions() or []
        live_symbols = set()
        for pos in positions:
            symbol = pos.get("symbol", "")
            if not symbol:
                continue
            live_symbols.add(symbol)
            existing = await self._db.get_trade_by_symbol(symbol)
            if existing:
                log.debug("Startup sync: %s already in DB – skipping seed", symbol)
                continue
            side      = pos.get("side", "Buy")
            direction = "long" if side == "Buy" else "short"
            size      = float(pos.get("size", 0))
            avg_price = float(pos.get("avgPrice", 0))
            sl_price  = float(pos.get("stopLoss") or 0)
            leverage  = int(float(pos.get("leverage", config.default_leverage)))
            log.warning(
                "Startup sync: found live position %s %s size=%.4f avg=%.6f sl=%.6f not in DB – "
                "seeding record. Add targets via Telegram to re-enable TP management.",
                symbol, direction, size, avg_price, sl_price,
            )
            trade_id = await self._db.upsert_trade({
                "signal_telegram_id": 0,
                "symbol":    symbol,
                "direction": direction,
                "leverage":  leverage,
                "entry_low":  avg_price,
                "entry_high": avg_price,
                "stop_loss":  sl_price,
                "targets":    [],
                "state":      "active",
            })
            await self._db.update_trade(
                trade_id,
                filled_size=size,
                avg_entry_price=avg_price,
                entries_cancelled=1,
            )
            # Do NOT cancel orders — this may be a manually placed trade.
            # Leave all orders and SL completely untouched.
            # Send "new targets for SYMBOL" via Telegram to enable TP management.
            log.warning(
                "Startup sync: seeded %s into DB — orders/SL left untouched. "
                "Send \'new targets for %s\' to enable TP management.",
                symbol, symbol,
            )

        if not positions:
            log.info("Startup sync: no open positions on Bybit")

        # ── Step 2: close DB trades that have no live Bybit position ──────────
        db_trades = await self._db.get_active_trades()
        newly_seeded = live_symbols  # symbols seeded in Step 1 already handled
        for trade in db_trades:
            symbol = trade["symbol"]
            if symbol in live_symbols:
                # ── Step 3: purge stale entry orders for bot-originated filled trades ──
                # Only run for trades that existed in DB BEFORE this startup
                # (signal_telegram_id != 0). Freshly seeded records (from Step 1,
                # signal_telegram_id == 0) may be manual trades — leave them alone.
                is_manual_seed = trade.get("signal_telegram_id", 0) == 0
                filled = trade.get("filled_size") or 0.0
                if filled > 0 and not is_manual_seed:
                    open_orders = self._bybit.fetch_open_orders(symbol)
                    entry_orders = [
                        o for o in open_orders
                        if str(o.get("reduceOnly", "false")).lower() != "true"
                        and o.get("orderType") == "Limit"
                    ]
                    if entry_orders:
                        for o in entry_orders:
                            log.info(
                                "Startup sync: cancelling stale entry order %s @ %.6f qty=%.4f for %s",
                                o.get("orderId", "?"), float(o.get("price", 0)),
                                float(o.get("qty", 0)), symbol,
                            )
                            # Cancel per-ID — preserves TP orders and position SL
                            self._bybit.cancel_order(symbol, o.get("orderId", ""))
                        await self._db.update_trade(trade["id"], entries_cancelled=1)
                        log.info(
                            "Startup sync: cancelled %d stale entry order(s) for %s (SL/TPs preserved)",
                            len(entry_orders), symbol,
                        )
                    else:
                        log.debug("Startup sync: %s — no stale entry orders", symbol)
                continue  # real live position — keep it

            # No position on Bybit. Cancel any open orders then close.
            self._bybit.cancel_orders_for_symbol(symbol)
            await self._db.update_trade_state(trade["id"], "closed")
            log.info(
                "Startup sync: %s has no live Bybit position – cancelled orders and marked closed",
                symbol,
            )

    async def _handle_new_signal(self, sig: NewSignal):
        if not sig.symbol or not sig.direction:
            log.warning("NewSignal missing symbol or direction – skipped")
            return

        if await self._db.get_trade_by_symbol(sig.symbol):
            log.info("Active trade already exists for %s – skipping", sig.symbol)
            return

        leverage = min(sig.leverage_max, config.max_leverage)
        self._bybit.set_leverage(sig.symbol, leverage)

        balance = self._bybit.fetch_wallet_balance()
        if balance <= 0 and not config.dry_run:
            log.error("Cannot determine balance – skipping %s", sig.symbol)
            return

        entry_ref = sig.entry_high if sig.entry_high > 0 else sig.entry_low
        qty = _calc_qty(balance, config.risk_per_trade, entry_ref, sig.stop_loss, leverage)

        if qty <= 0:
            log.warning("Calculated qty=0 for %s – skipping", sig.symbol)
            return

        trade_id = await self._db.upsert_trade({
            "signal_telegram_id": sig.telegram_message_id,
            "symbol":    sig.symbol,
            "direction": sig.direction.value,
            "leverage":  leverage,
            "entry_low":  sig.entry_low,
            "entry_high": sig.entry_high,
            "stop_loss":  sig.stop_loss,
            "targets":    sig.targets,
            "state":      "pending",
        })

        side   = _entry_side(sig.direction.value)
        ladder = _calc_ladder(sig.entry_low, sig.entry_high, sig.direction.value)
        qty_step   = self._bybit.get_qty_step(sig.symbol)
        tick_size  = self._bybit.get_tick_size(sig.symbol)
        total_qty  = self._bybit._round_qty(sig.symbol, qty)

        orders_placed = 0
        for price, fraction in ladder:
            price     = self._bybit._round_price(sig.symbol, price)
            order_qty = _floor_frac(total_qty, fraction, qty_step)
            if order_qty <= 0 or price <= 0:
                continue
            # Attach SL directly to each entry order so it activates the instant
            # that order fills — no separate move_stop_loss needed and no risk of
            # ErrCode 10001 "can not set tp/sl for zero position".
            order_id = self._bybit.place_limit_order(
                sig.symbol, side, order_qty, price,
                order_type_label="entry",
                stop_loss=sig.stop_loss if sig.stop_loss > 0 else None,
            )
            if order_id:
                orders_placed += 1
                await self._db.save_order(
                    trade_id, order_id, sig.symbol, "entry", side, price, order_qty
                )

        if orders_placed == 0:
            # All entry orders rejected — clean up DB record so signal can be retried.
            await self._db.update_trade_state(trade_id, "cancelled")
            log.error(
                "Trade FAILED: %s — all %d entry order(s) rejected by Bybit. "
                "DB record marked cancelled. Check price/qty precision.",
                sig.symbol, len(ladder),
            )
            return

        # SL is now embedded in each entry order — no standalone move_stop_loss needed.
        # on_ws_execution will call move_stop_loss on first fill to lock in the
        # position-level SL (belt-and-suspenders for WS fill detection).

        await self._db.update_trade_state(trade_id, "active")
        log.info(
            "Trade opened: %s %s | qty=%.4f | ladder=%s | sl=%.5f | orders=%d/%d",
            sig.symbol, sig.direction.value, qty,
            " / ".join(f"{p:.5f}({f*100:.0f}%)" for p, f in ladder),
            sig.stop_loss, orders_placed, len(ladder),
        )

    # ── TP order management ───────────────────────────────────────────────────

    async def _refresh_tp_orders(self, trade: dict, filled_qty: float):
        """
        Cancel open TP orders and replace with fresh ones based on actual
        filled position size. Skips TP levels already hit.
        """
        trade_id       = trade["id"]
        symbol         = trade["symbol"]
        direction      = trade["direction"]
        targets        = trade.get("targets", [])
        highest_tp_hit = trade.get("highest_tp_hit", 0) or 0

        if not targets or filled_qty <= 0:
            return

        open_orders = await self._db.get_open_orders_for_trade(trade_id)
        for order in open_orders:
            if order["order_type"].startswith("tp"):
                self._bybit.cancel_order(symbol, order["bybit_order_id"])
                await self._db.mark_order_status(order["bybit_order_id"], "cancelled")

        close_side        = _opposite_side(direction)
        remaining_targets = targets[highest_tp_hit:]

        if not remaining_targets:
            log.info("All TP levels already hit for %s", symbol)
            return

        # Weighted TP distribution based on total number of targets in signal
        fractions = _tp_fractions(len(targets))
        # Slice to the remaining (not-yet-hit) levels
        remaining_fractions = fractions[highest_tp_hit:]
        # Renormalise so remaining fractions sum to 1.0
        frac_sum = sum(remaining_fractions)
        if frac_sum <= 0:
            return
        remaining_fractions = [f / frac_sum for f in remaining_fractions]

        for i, (tp_price, frac) in enumerate(zip(remaining_targets, remaining_fractions)):
            tp_num    = highest_tp_hit + i + 1
            tp_price  = self._bybit._round_price(symbol, tp_price)
            qty_step  = self._bybit.get_qty_step(symbol)
            order_qty = _floor_frac(filled_qty, frac, qty_step)
            if tp_price <= 0 or order_qty <= 0:
                continue
            order_id = self._bybit.place_take_profit_order(
                symbol, close_side, order_qty, tp_price
            )
            if order_id:
                await self._db.save_order(
                    trade_id, order_id, symbol, f"tp{tp_num}",
                    close_side, tp_price, order_qty
                )

        log.info(
            "TP orders refreshed for %s | filled=%.4f | remaining=%d | dist=%s",
            symbol, filled_qty, len(remaining_targets),
            "/".join(f"{f*100:.0f}%" for f in remaining_fractions),
        )

    # ── TP ratchet ────────────────────────────────────────────────────────────

    async def on_tp_filled(self, symbol: str, tp_num: int):
        """
        Called when a TP order fills (via WebSocket or sync_fills).

        TP1: cancel remaining entries, move SL to avg_entry (break-even)
        TP2: move SL to TP1 price
        TP3: move SL to TP2 price  (etc.)

        Logs a clear message for every ratchet action and confirms
        with the actual Bybit position after the SL move.
        """
        trade = await self._db.get_trade_by_symbol(symbol)
        if not trade:
            log.info("on_tp_filled: no active trade for %s", symbol)
            return

        targets   = trade.get("targets", [])
        avg_entry = trade.get("avg_entry_price", 0) or 0.0

        await self._db.update_trade(trade["id"], highest_tp_hit=tp_num)

        log.info(
            "✓ TP%d FILLED  %s  (target: %.6f)",
            tp_num, symbol,
            targets[tp_num - 1] if len(targets) >= tp_num else 0.0,
        )

        if tp_num == 1:
            log.info(
                "  Ratchet: TP1 hit → cancelling remaining entry orders for %s", symbol
            )
            await self._handle_cancel_entries(
                CancelRemainingEntries(
                    raw_text="", message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                    symbol=symbol,
                )
            )
            if avg_entry > 0:
                log.info(
                    "  Ratchet: moving SL to break-even (avg entry %.6f) for %s",
                    avg_entry, symbol,
                )
                ok = self._bybit.move_stop_loss(symbol, avg_entry)
                if ok:
                    await self._db.update_trade(trade["id"], stop_loss=avg_entry)
                    # Verify against live position
                    pos = self._bybit.fetch_position(symbol)
                    live_sl = float(pos.get("stopLoss") or 0) if pos else 0.0
                    if live_sl and abs(live_sl - avg_entry) < avg_entry * 0.001:
                        log.info(
                            "  ✓ Bybit confirmed SL = %.6f for %s", live_sl, symbol
                        )
                    else:
                        log.warning(
                            "  ⚠ SL mismatch after move: expected %.6f, Bybit shows %.6f for %s",
                            avg_entry, live_sl, symbol,
                        )
                else:
                    log.error(
                        "  ✗ Failed to move SL to break-even for %s — check manually!", symbol
                    )
            else:
                log.warning(
                    "  ⚠ avg_entry unknown for %s — cannot set break-even SL", symbol
                )
        else:
            prev_tp_price = targets[tp_num - 2] if len(targets) >= tp_num - 1 else None
            if prev_tp_price and prev_tp_price > 0:
                log.info(
                    "  Ratchet: TP%d hit → moving SL to TP%d price (%.6f) for %s",
                    tp_num, tp_num - 1, prev_tp_price, symbol,
                )
                ok = self._bybit.move_stop_loss(symbol, prev_tp_price)
                if ok:
                    await self._db.update_trade(trade["id"], stop_loss=prev_tp_price)
                    # Verify against live position
                    pos = self._bybit.fetch_position(symbol)
                    live_sl = float(pos.get("stopLoss") or 0) if pos else 0.0
                    if live_sl and abs(live_sl - prev_tp_price) < prev_tp_price * 0.001:
                        log.info(
                            "  ✓ Bybit confirmed SL = %.6f for %s", live_sl, symbol
                        )
                    else:
                        log.warning(
                            "  ⚠ SL mismatch after move: expected %.6f, Bybit shows %.6f for %s",
                            prev_tp_price, live_sl, symbol,
                        )
                else:
                    log.error(
                        "  ✗ Failed to move SL to %.6f for %s — check manually!",
                        prev_tp_price, symbol,
                    )
            else:
                log.warning(
                    "  ⚠ Cannot determine previous TP price for TP%d ratchet on %s "
                    "(targets list has %d entries)",
                    tp_num, symbol, len(targets),
                )

    # ── WebSocket execution handler ───────────────────────────────────────────

    async def on_ws_execution(self, msg: dict):
        """Called by BybitStream on every execution (fill) event."""
        data = msg.get("data", [])
        if not data:
            return

        for exec_item in data:
            symbol    = exec_item.get("symbol", "")
            order_id  = exec_item.get("orderId", "")
            exec_type = exec_item.get("execType", "")
            exec_qty  = float(exec_item.get("execQty", 0))
            avg_price = float(exec_item.get("execPrice", 0))

            if exec_type != "Trade" or exec_qty <= 0:
                continue

            order = await self._db.get_order_by_bybit_id(order_id)
            if not order:
                continue

            trade = await self._db.get_trade_by_id(order["trade_id"])
            if not trade:
                continue

            order_type = order.get("order_type", "")
            log.info(
                "WS execution: %s %s qty=%.4f price=%.5f",
                symbol, order_type, exec_qty, avg_price,
            )

            await self._db.mark_order_status(order_id, "filled")

            if order_type == "entry":
                pos       = self._bybit.fetch_position(symbol)
                filled    = float(pos.get("size", 0))    if pos else exec_qty
                pos_avg   = float(pos.get("avgPrice", 0)) if pos else avg_price
                prev_filled = trade.get("filled_size", 0) or 0.0

                await self._db.update_trade(
                    trade["id"], filled_size=filled, avg_entry_price=pos_avg,
                )

                if prev_filled == 0.0:
                    sl_price = trade.get("stop_loss", 0)
                    if sl_price and sl_price > 0:
                        ok = self._bybit.move_stop_loss(symbol, sl_price)
                        log.info("SL enforced on first fill %s → %.5f (ok=%s)", symbol, sl_price, ok)

                fresh_trade = await self._db.get_trade_by_symbol(symbol)
                if fresh_trade:
                    await self._refresh_tp_orders(fresh_trade, filled)

            elif order_type.startswith("tp"):
                try:
                    tp_num = int(order_type[2:])
                except ValueError:
                    tp_num = 1

                await self.on_tp_filled(symbol, tp_num)

                pos       = self._bybit.fetch_position(symbol)
                remaining = float(pos.get("size", 0)) if pos else 0.0
                if remaining <= 0:
                    await self._db.update_trade_state(trade["id"], "closed")
                    log.info("All TPs filled for %s – trade closed", symbol)
                else:
                    fresh_trade = await self._db.get_trade_by_symbol(symbol)
                    if fresh_trade:
                        await self._refresh_tp_orders(fresh_trade, remaining)

    # ── WebSocket order status handler ────────────────────────────────────────

    async def on_ws_order(self, msg: dict):
        """Detects SL hit via order status change.

        We do NOT rely on finding the SL order in the local orders table because
        move_stop_loss() uses set_trading_stop (a position-level SL, not a regular
        order) so it is never saved to the DB. Instead we match by symbol directly
        from the active trades table.
        """
        data = msg.get("data", [])
        for item in data:
            order_status    = item.get("orderStatus", "")
            stop_order_type = item.get("stopOrderType", "")
            symbol          = item.get("symbol", "")

            if order_status == "Filled" and stop_order_type == "StopLoss":
                if not symbol:
                    log.warning("on_ws_order: SL filled but no symbol in payload – %s", item)
                    continue
                trade = await self._db.get_trade_by_symbol(symbol)
                if trade:
                    await self._db.update_trade_state(trade["id"], "sl_hit")
                    log.warning("SL hit for %s – trade marked sl_hit", symbol)
                else:
                    log.info("on_ws_order: SL fired for %s but no active trade found in DB", symbol)

    # ── fill-size sync (watchdog fallback) ────────────────────────────────────

    async def sync_fills(self):
        """
        REST polling fallback. Runs every 30–60s from the watchdog.
        Catches anything the WebSocket may have missed.
        """
        trades = await self._db.get_active_trades()
        for trade in trades:
            symbol      = trade["symbol"]
            prev_filled = trade.get("filled_size", 0) or 0.0

            pos       = self._bybit.fetch_position(symbol)
            filled    = float(pos.get("size", 0))    if pos else 0.0
            avg_price = float(pos.get("avgPrice", 0)) if pos else 0.0

            if filled <= 0 and prev_filled > 0:
                await self._db.update_trade_state(trade["id"], "closed")
                log.info("Sync: trade %s closed externally", symbol)
                continue

            if filled <= 0 and prev_filled == 0.0:
                # Symbol may be delisted or DB was seeded with a bad record.
                # If entries_cancelled is set (startup-seeded) and still no
                # position after one cycle, mark closed to stop polling.
                if trade.get("entries_cancelled"):
                    log.info(
                        "Sync: %s has no position and no prior fill – "
                        "likely delisted or manually closed before bot started. Marking closed.",
                        symbol,
                    )
                    await self._db.update_trade_state(trade["id"], "closed")
                continue

            if abs(filled - prev_filled) > 0.0001:
                log.info(
                    "Sync fallback fill %s: %.4f → %.4f (WS may have missed this)",
                    symbol, prev_filled, filled,
                )
                await self._db.update_trade(
                    trade["id"], filled_size=filled, avg_entry_price=avg_price,
                )
                if prev_filled == 0.0:
                    sl_price = trade.get("stop_loss", 0)
                    if sl_price and sl_price > 0:
                        self._bybit.move_stop_loss(symbol, sl_price)
                # Only place/refresh TP orders if the trade actually has targets.
                # Startup-seeded trades have targets=[] and must be managed manually
                # via a Telegram update_targets message.
                fresh_trade = await self._db.get_trade_by_symbol(symbol)
                if fresh_trade and fresh_trade.get("targets"):
                    await self._refresh_tp_orders(fresh_trade, filled)
                elif fresh_trade and not fresh_trade.get("targets"):
                    log.debug(
                        "Sync: skipping TP refresh for %s – no targets in DB "
                        "(trade seeded from startup sync; send 'new targets for %s' to enable)",
                        symbol, symbol,
                    )

    # ── close all ─────────────────────────────────────────────────────────────

    async def _handle_close_all(self, _msg: CloseAll):
        log.warning("CLOSE ALL triggered from Telegram")
        trades = await self._db.get_active_trades()
        for trade in trades:
            sym = trade["symbol"]
            self._bybit.cancel_orders_for_symbol(sym)
            pos = self._bybit.fetch_position(sym)
            if pos:
                size       = float(pos.get("size", 0))
                close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
                self._bybit.close_position(sym, size, close_side)
            await self._db.update_trade_state(trade["id"], "closed")
        log.info("Close-all complete: %d trades closed", len(trades))

    # ── close symbol ──────────────────────────────────────────────────────────

    async def _handle_close_symbol(self, msg: CloseSymbol):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – close_symbol ignored", msg.symbol)
            return
        self._bybit.cancel_orders_for_symbol(msg.symbol)
        pos = self._bybit.fetch_position(msg.symbol)
        if pos:
            size       = float(pos.get("size", 0))
            close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
            self._bybit.close_position(msg.symbol, size, close_side)
        await self._db.update_trade_state(trade["id"], "closed")
        log.info("Closed trade for %s", msg.symbol)

    # ── cancel remaining entries ──────────────────────────────────────────────

    async def _handle_cancel_entries(self, msg: CancelRemainingEntries):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – cancel_entries ignored", msg.symbol)
            return
        # Cancel DB-tracked entry orders
        open_orders = await self._db.get_open_orders_for_trade(trade["id"])
        for order in open_orders:
            if order["order_type"] == "entry":
                ok = self._bybit.cancel_order(msg.symbol, order["bybit_order_id"])
                if ok:
                    await self._db.mark_order_status(order["bybit_order_id"], "cancelled")
        # Also cancel any non-DB entry orders (e.g. manually placed) via targeted cancel
        # This does NOT touch TP orders or the position-level SL
        self._bybit.cancel_entry_orders(msg.symbol)
        await self._db.update_trade(trade["id"], entries_cancelled=1)
        log.info("Cancelled remaining entry orders for %s", msg.symbol)

    # ── move SL to break-even ─────────────────────────────────────────────────

    async def _handle_move_sl_be(self, msg: MoveSLBreakEven):
        symbol = msg.symbol or None
        trades = (
            [await self._db.get_trade_by_symbol(symbol)]
            if symbol
            else await self._db.get_active_trades()
        )
        for trade in trades:
            if not trade:
                continue
            pos = self._bybit.fetch_position(trade["symbol"])
            if not pos:
                log.info("No position for %s – cannot move SL to BE", trade["symbol"])
                continue
            avg_entry = float(pos.get("avgPrice", 0) or trade.get("avg_entry_price", 0))
            if avg_entry <= 0:
                avg_entry = (trade["entry_low"] + trade["entry_high"]) / 2

            ok = self._bybit.move_stop_loss(trade["symbol"], avg_entry)
            if ok:
                await self._db.update_trade(
                    trade["id"], break_even_activated=1, stop_loss=avg_entry
                )
                await self._handle_cancel_entries(
                    CancelRemainingEntries(
                        raw_text="", message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                        symbol=trade["symbol"],
                    )
                )
                await self._db.update_trade_state(trade["id"], "break_even")
                log.info("SL moved to break-even %.5f for %s", avg_entry, trade["symbol"])

    # ── move SL to price ──────────────────────────────────────────────────────

    async def _handle_move_sl_price(self, msg: MoveSLPrice):
        symbol = msg.symbol or None
        trades = (
            [await self._db.get_trade_by_symbol(symbol)]
            if symbol
            else await self._db.get_active_trades()
        )
        for trade in trades:
            if not trade:
                continue
            ok = self._bybit.move_stop_loss(trade["symbol"], msg.price)
            if ok:
                await self._db.update_trade(trade["id"], stop_loss=msg.price)
                log.info("SL updated to %.5f for %s", msg.price, trade["symbol"])

    # ── update targets ────────────────────────────────────────────────────────

    async def _handle_update_targets(self, msg: UpdateTargets):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – update_targets ignored", msg.symbol)
            return
        if not msg.targets:
            log.info("No targets parsed for %s – ignored", msg.symbol)
            return

        # Cancel existing TP orders — fetch from Bybit so we catch manually
        # placed orders not in the DB. Use per-ID cancel to preserve the SL.
        live_orders = self._bybit.fetch_open_orders(msg.symbol)
        tp_orders = [
            o for o in live_orders
            if str(o.get("reduceOnly", "false")).lower() == "true"
            or o.get("orderType") == "Market"  # market TPs
        ]
        for o in tp_orders:
            self._bybit.cancel_order(msg.symbol, o.get("orderId", ""))

        # Also mark DB-tracked TP orders as cancelled for consistency.
        open_orders = await self._db.get_open_orders_for_trade(trade["id"])
        for order in open_orders:
            if order["order_type"].startswith("tp"):
                await self._db.mark_order_status(order["bybit_order_id"], "cancelled")

        pos       = self._bybit.fetch_position(msg.symbol)
        total_qty = float(pos.get("size", 0)) if pos else trade.get("filled_size", 0)
        if total_qty <= 0:
            log.warning("Cannot place TP orders – unknown qty for %s", msg.symbol)
            return

        await self._db.update_trade(trade["id"], targets=msg.targets, highest_tp_hit=0)
        updated_trade = await self._db.get_trade_by_symbol(msg.symbol)
        await self._refresh_tp_orders(updated_trade, total_qty)
        log.info("Targets updated for %s: %s", msg.symbol, msg.targets)

    # ── add entries ───────────────────────────────────────────────────────────

    async def _handle_add_entries(self, msg: AddEntries):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – add_entries ignored", msg.symbol)
            return
        balance   = self._bybit.fetch_wallet_balance()
        leverage  = trade.get("leverage", config.default_leverage)
        entry_mid = (msg.entry_low + msg.entry_high) / 2
        qty = _calc_qty(balance, config.risk_per_trade / 2, entry_mid,
                        trade["stop_loss"], leverage)
        if qty <= 0:
            return
        side     = _entry_side(trade["direction"])
        prices   = [msg.entry_low, msg.entry_high] if msg.entry_low != msg.entry_high else [msg.entry_low]
        qty_step = self._bybit.get_qty_step(msg.symbol)
        half_qty = _floor_frac(qty, 1.0 / len(prices), qty_step)
        for price in prices:
            price    = self._bybit._round_price(msg.symbol, price)
            order_id = self._bybit.place_limit_order(msg.symbol, side, half_qty, price,
                                                     order_type_label="add_entry")
            if order_id:
                await self._db.save_order(trade["id"], order_id, msg.symbol,
                                          "entry", side, price, half_qty)
        log.info("Added entries for %s at %.5f-%.5f", msg.symbol, msg.entry_low, msg.entry_high)

    # ── market entry ──────────────────────────────────────────────────────────

    async def _handle_market_entry(self, msg: MarketEntry):
        trade = await self._db.get_trade_by_symbol(msg.symbol) if msg.symbol else None
        if not trade:
            log.info("market_entry: no active trade for %s – ignored", msg.symbol)
            return
        balance   = self._bybit.fetch_wallet_balance()
        leverage  = trade.get("leverage", config.default_leverage)
        pos       = self._bybit.fetch_position(msg.symbol)
        current_size = float(pos.get("size", 0)) if pos else 0.0
        entry_mid = (trade["entry_low"] + trade["entry_high"]) / 2
        qty = _calc_qty(balance, config.risk_per_trade, entry_mid, trade["stop_loss"], leverage)
        remaining_qty = max(0, qty - current_size)
        if remaining_qty <= 0:
            log.info("market_entry: position already full for %s", msg.symbol)
            return
        direction = msg.direction.value if msg.direction else trade["direction"]
        side = _entry_side(direction)
        await self._handle_cancel_entries(
            CancelRemainingEntries(
                raw_text="", message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                symbol=msg.symbol,
            )
        )
        order_id = self._bybit.place_market_order(msg.symbol, side, remaining_qty)
        if order_id:
            await self._db.save_order(trade["id"], order_id, msg.symbol,
                                      "entry", side, 0, remaining_qty)
        log.info("Market entry executed for %s", msg.symbol)

    # ── partial close ─────────────────────────────────────────────────────────

    async def _handle_partial_close(self, msg: PartialClose):
        symbol = msg.symbol if msg.symbol else None
        if symbol:
            trade = await self._db.get_trade_by_symbol(symbol)
            if trade:
                await self._partial_close_trade(trade, msg.percent)
            else:
                log.info("partial_close: no active trade for %s", symbol)
        else:
            # No symbol in the message — use the single most recently active trade.
            # If multiple trades are open, log a warning and skip rather than
            # nuking all of them accidentally.
            active = await self._db.get_active_trades()
            if len(active) == 1:
                log.info(
                    "partial_close: no symbol in message, applying to only active trade %s",
                    active[0]["symbol"],
                )
                await self._partial_close_trade(active[0], msg.percent)
            elif len(active) > 1:
                symbols = ", ".join(t["symbol"] for t in active)
                log.warning(
                    "partial_close: no symbol and %d trades open (%s) – "
                    "cannot determine which to close, skipping. "
                    "Re-send the message with a specific symbol.",
                    len(active), symbols,
                )
            else:
                log.info("partial_close: no symbol and no active trades – ignored")

    async def _partial_close_trade(self, trade: dict, percent: float):
        symbol = trade["symbol"]
        pos    = self._bybit.fetch_position(symbol)
        if not pos:
            log.info("partial_close: no position for %s", symbol)
            return
        total_size = float(pos.get("size", 0))
        qty_step   = self._bybit.get_qty_step(symbol)
        close_qty  = _floor_frac(total_size, percent / 100, qty_step)
        if close_qty <= 0:
            return
        close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
        order_id   = self._bybit.place_market_order(symbol, close_side, close_qty, reduce_only=True)
        if order_id:
            await self._db.save_order(trade["id"], order_id, symbol,
                                      "close", close_side, 0, close_qty)
        log.info("Partial close %.0f%% for %s qty=%.4f", percent, symbol, close_qty)

    # ── cancel signal ─────────────────────────────────────────────────────────

    async def _handle_cancel_signal(self, msg: CancelSignal):
        trade = await self._db.get_trade_by_symbol(msg.symbol) if msg.symbol else None
        if not trade:
            log.info("cancel_signal: no active trade for %s", msg.symbol)
            return
        pos = self._bybit.fetch_position(msg.symbol)
        if pos is not None and float(pos.get("size", 0)) > 0:
            log.info("cancel_signal: %s already has a live position – not cancelling", msg.symbol)
            return
        self._bybit.cancel_orders_for_symbol(msg.symbol)
        await self._db.update_trade_state(trade["id"], "cancelled")
        log.info("Signal cancelled for %s", msg.symbol)

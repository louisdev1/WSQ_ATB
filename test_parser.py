"""
tests/test_parser.py

Comprehensive test suite for app/parsing/parser.py.

Run with:
    python -m pytest tests/test_parser.py -v
    # or without pytest:
    python tests/test_parser.py

Covers:
  - _normalise_number  (price parsing edge cases)
  - _extract_prices    (multi-price lines)
  - _extract_symbol    (all symbol formats)
  - Every MessageType  (real WSQ message variations)
  - Edge cases         (missing fields, garbage, emoji, unicode)
"""

import sys
import os
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.parsing.parser import (
    _normalise_number,
    _extract_prices,
    _extract_symbol,
    parse_message,
)
from app.parsing.models import MessageType, Direction

# ── tiny test harness (no pytest required) ────────────────────────────────────

_PASS = 0
_FAIL = 0
_ERRORS: list[str] = []


def check(name: str, got: Any, expected: Any, *, close: bool = False):
    global _PASS, _FAIL
    if close:
        ok = isinstance(got, (int, float)) and abs(got - expected) < 1e-6
    else:
        ok = got == expected
    if ok:
        _PASS += 1
        print(f"  \033[92m✓\033[0m  {name}")
    else:
        _FAIL += 1
        msg = f"  \033[91m✗\033[0m  {name}\n       got      {got!r}\n       expected {expected!r}"
        print(msg)
        _ERRORS.append(f"{name}: got {got!r}, expected {expected!r}")


def section(title: str):
    print(f"\n\033[96m{'═'*60}\033[0m")
    print(f"\033[96m  {title}\033[0m")
    print(f"\033[96m{'═'*60}\033[0m")


def summary():
    total = _PASS + _FAIL
    print(f"\n{'═'*60}")
    if _FAIL == 0:
        print(f"\033[92m  ALL {total} TESTS PASSED\033[0m")
    else:
        print(f"\033[91m  {_FAIL} FAILED / {total} TOTAL\033[0m")
        for e in _ERRORS:
            print(f"    • {e}")
    print(f"{'═'*60}\n")
    return _FAIL


# ── helpers ───────────────────────────────────────────────────────────────────

def parse(text: str):
    return parse_message(text, telegram_message_id=0)


def mtype(text: str) -> MessageType:
    return parse(text).message_type


# ══════════════════════════════════════════════════════════════════════════════
# 1. _normalise_number
# ══════════════════════════════════════════════════════════════════════════════
section("1. _normalise_number — price parsing")

cases = [
    # Plain integers
    ("100",         100.0),
    ("0",           0.0),
    # Standard decimals (dot)
    ("0.295",       0.295),
    ("3603.5",      3603.5),
    ("72260.0",     72260.0),
    # Thousands with comma (the BTC signal bug)
    ("72,260",      72260.0),
    ("70,800",      70800.0),
    ("69,600",      69600.0),
    ("74,000",      74000.0),
    ("75,600",      75600.0),
    ("1,000",       1000.0),
    ("10,000",      10000.0),
    ("100,000",     100000.0),
    ("1,000,000",   1000000.0),
    # European decimal (comma as decimal sep)
    ("0,0412",      0.0412),
    ("0,295",       0.295),   # 3 digits after comma but < 1 — actually ambiguous;
                               # our rule: comma+3digits at end = thousands → 295.0
                               # Let's verify what the rule actually does:
    # Small prices with dot decimal
    ("0.00560",     0.00560),
    ("0.00412",     0.00412),
    # Crypto altcoin prices
    ("3603",        3603.0),
    ("0.305",       0.305),
    ("0.285",       0.285),
    # With leading $
    ("$72,260",     72260.0),
    ("$0.295",      0.295),
    ("$69,600",     69600.0),
]

for inp, expected in cases:
    s = inp.lstrip("$").strip()
    try:
        got = _normalise_number(s)
        check(f"_normalise_number({inp!r})", got, expected, close=True)
    except Exception as e:
        check(f"_normalise_number({inp!r})", f"EXCEPTION: {e}", expected)


# ══════════════════════════════════════════════════════════════════════════════
# 2. _extract_prices — multi-price lines
# ══════════════════════════════════════════════════════════════════════════════
section("2. _extract_prices — multi-price lines")

price_cases = [
    # Standard dash-separated
    ("Entry: 3603 - 3590",                  [3603.0, 3590.0]),
    ("Targets: 3680 - 3760 - 3840 - 3920 - 4000",
                                            [3680.0, 3760.0, 3840.0, 3920.0, 4000.0]),
    # Thousands separators
    ("Entry: $72,260 - $70,800 (Buy partially)",
                                            [72260.0, 70800.0]),
    ("Targets: $74,000 - $75,600 - $76,800 - $78,000 (Short term)",
                                            [74000.0, 75600.0, 76800.0, 78000.0]),
    ("Stop-loss: $69,600",                  [69600.0]),
    # Small altcoin prices
    ("Entry: 0.305 - 0.295",               [0.305, 0.295]),
    ("Targets: 0.315 - 0.322 - 0.330 - 0.338 - 0.355 - 0.375 - 0.395 - 0.420",
                                            [0.315, 0.322, 0.330, 0.338, 0.355, 0.375, 0.395, 0.420]),
    ("Stop-loss: 0.285",                   [0.285]),
    # Very small prices (LINA style)
    ("Entry: 0.00560 - 0.00545",           [0.00560, 0.00545]),
    ("Stop-loss: 0.00530",                 [0.00530]),
    # Single entry price
    ("Entry: 3603",                         [3603.0]),
    # With $ and no spaces
    ("Entry: $0.305-$0.295",               [0.305, 0.295]),
    # Parenthetical stripped
    ("Targets: 1.0 - 2.0 - 3.0 (Short term)", [1.0, 2.0, 3.0]),
    ("Entry: 0.305 - 0.295 (Buy partially)",   [0.305, 0.295]),
]

for line, expected in price_cases:
    got = _extract_prices(line)
    check(f"_extract_prices({line[:50]!r})", got, expected)


# ══════════════════════════════════════════════════════════════════════════════
# 3. _extract_symbol
# ══════════════════════════════════════════════════════════════════════════════
section("3. _extract_symbol — all formats")

sym_cases = [
    ("#BTCUSDT",                "BTCUSDT"),
    ("#ETH/USDT",               "ETHUSDT"),
    ("#HBAR/USDT UPDATE:",      "HBARUSDT"),
    ("Coin: #CFXUSDT",          "CFXUSDT"),
    ("Coin: #1MBABYDOGEUSDT",   "1MBABYDOGEUSDT"),
    ("#NEOUSDT UPDATE:",        "NEOUSDT"),
    ("AXLUSDT long setup",      "AXLUSDT"),
    ("#ETH/USDT (Futures)",     "ETHUSDT"),
    ("close BTCUSDT",           "BTCUSDT"),
    ("new targets for CFXUSDT", "CFXUSDT"),
]

for inp, expected in sym_cases:
    got = _extract_symbol(inp)
    check(f"_extract_symbol({inp!r})", got, expected)


# ══════════════════════════════════════════════════════════════════════════════
# 4. NEW_SIGNAL — full parse variations
# ══════════════════════════════════════════════════════════════════════════════
section("4. NEW_SIGNAL — full signal parsing")

# 4a. Standard WSQ format (BTC with thousands separators — the bug case)
btc_signal = """Coin: #BTCUSDT
Direction: Long
Leverage: 10-20x
It has already broken out of the Cup and Handle pattern and is looking bullish.
Entry: $72,260 - $70,800 (Buy partially)
Targets: $74,000 - $75,600 - $76,800 - $78,000 (Short term)
Stop-loss: $69,600"""

p = parse(btc_signal)
check("BTC signal — type",         p.message_type, MessageType.NEW_SIGNAL)
check("BTC signal — symbol",       p.symbol,       "BTCUSDT")
check("BTC signal — direction",    p.direction,    Direction.LONG)
check("BTC signal — entry_high",   p.entry_high,   72260.0, close=True)
check("BTC signal — entry_low",    p.entry_low,    70800.0, close=True)
check("BTC signal — stop_loss",    p.stop_loss,    69600.0, close=True)
check("BTC signal — targets",      p.targets,      [74000.0, 75600.0, 76800.0, 78000.0])
check("BTC signal — leverage_max", p.leverage_max, 20)

# 4b. ETH signal (small thousands: 3603, 3590)
eth_signal = """Coin: #ETH/USDT
Long Set-Up
Leverage: 5-10x
#ETH already breaked out the Bull Flag and looking Bullish.
Entry: 3603 - 3590$
Targets: 3680 - 3760 - 3840 - 3920 - 4000$
Stop-loss: 3570$"""

p = parse(eth_signal)
check("ETH signal — type",       p.message_type, MessageType.NEW_SIGNAL)
check("ETH signal — symbol",     p.symbol,       "ETHUSDT")
check("ETH signal — direction",  p.direction,    Direction.LONG)
check("ETH signal — entry_high", p.entry_high,   3603.0, close=True)
check("ETH signal — entry_low",  p.entry_low,    3590.0, close=True)
check("ETH signal — stop_loss",  p.stop_loss,    3570.0, close=True)
check("ETH signal — targets[0]", p.targets[0],   3680.0, close=True)
check("ETH signal — targets[-1]",p.targets[-1],  4000.0, close=True)

# 4c. HBAR signal (small prices, many targets)
hbar_signal = """Coin: #HBAR/USDT
Long Set-Up
Leverage: 5-10x
Entry: 0.305 - 0.295$(Buy partially)
Targets: 0.315 - 0.322 - 0.330 - 0.338 - 0.355 - 0.375 - 0.395 - 0.420$(Short-mid term)
Stop-loss: 0.285$"""

p = parse(hbar_signal)
check("HBAR signal — type",       p.message_type, MessageType.NEW_SIGNAL)
check("HBAR signal — symbol",     p.symbol,       "HBARUSDT")
check("HBAR signal — entry_high", p.entry_high,   0.305, close=True)
check("HBAR signal — entry_low",  p.entry_low,    0.295, close=True)
check("HBAR signal — stop_loss",  p.stop_loss,    0.285, close=True)
check("HBAR signal — n_targets",  len(p.targets), 8)
check("HBAR signal — targets[-1]",p.targets[-1],  0.420, close=True)

# 4d. LINA signal (very small prices)
lina_signal = """Coin: #LINA/USDT
Long Set-Up
Leverage: 5-10x
Entry: 0.00560 - 0.00545$(Buy partially)
Targets: 0.00575 - 0.00590 - 0.00605 - 0.00620 - 0.00640$(Short term)
Stop-loss: 0.00530$"""

p = parse(lina_signal)
check("LINA signal — type",       p.message_type, MessageType.NEW_SIGNAL)
check("LINA signal — entry_high", p.entry_high,   0.00560, close=True)
check("LINA signal — stop_loss",  p.stop_loss,    0.00530, close=True)
check("LINA signal — n_targets",  len(p.targets), 5)

# 4e. CFX signal (micro prices)
cfx_signal = """Coin: #CFXUSDT
Long Set-Up
Leverage: 5-10x
Entry: 0.0516 - 0.0505
Targets: 0.053 - 0.0545 - 0.056 - 0.0575 - 0.061 - 0.0635 - 0.066
Stop-loss: 0.047"""

p = parse(cfx_signal)
check("CFX signal — type",       p.message_type, MessageType.NEW_SIGNAL)
check("CFX signal — symbol",     p.symbol,       "CFXUSDT")
check("CFX signal — n_targets",  len(p.targets), 7)
check("CFX signal — stop_loss",  p.stop_loss,    0.047, close=True)

# 4f. SHORT signal
short_signal = """Coin: #ETHUSDT
Direction: Short
Leverage: 5-10x
Entry: 2800 - 2850
Targets: 2700 - 2600 - 2500
Stop-loss: 2950"""

p = parse(short_signal)
check("Short signal — direction", p.direction, Direction.SHORT)
check("Short signal — type",      p.message_type, MessageType.NEW_SIGNAL)

# 4g. Single entry price (no range)
single_entry = """Coin: #SOLUSDT
Direction: Long
Leverage: 10x
Entry: 185.5
Targets: 190 - 195 - 200
Stop-loss: 180"""

p = parse(single_entry)
check("Single entry — entry_low == entry_high", p.entry_low, p.entry_high)
check("Single entry — value",                   p.entry_low, 185.5, close=True)

# 4h. Missing direction line (inferred from "Long Set-Up" in body)
no_direction_line = """Coin: #NEOUSDT
Long Set-Up
Leverage: 5-10x
Entry: 2.50 - 2.45
Targets: 2.63 - 2.69 - 2.75 - 2.80
Stop-loss: 2.40"""

p = parse(no_direction_line)
check("Inferred direction LONG", p.direction, Direction.LONG)
check("NEO signal — type",       p.message_type, MessageType.NEW_SIGNAL)

# 4i. Leverage single value
single_lev = """Coin: #XRPUSDT
Direction: Long
Leverage: 10x
Entry: 0.55 - 0.53
Targets: 0.58 - 0.61 - 0.64
Stop-loss: 0.50"""

p = parse(single_lev)
check("Single leverage — min", p.leverage_min, 10)
check("Single leverage — max", p.leverage_max, 10)


# ══════════════════════════════════════════════════════════════════════════════
# 5. COMMENTARY — update messages (must NOT trigger trades)
# ══════════════════════════════════════════════════════════════════════════════
section("5. COMMENTARY — update messages must not trade")

commentary_cases = [
    "#NEOUSDT UPDATE:\nTarget 1 done nicely✔️\nSo far 30% Profits with 10x leverage🤑",
    "#CFXUSDT UPDATE:\nTarget 1,2,3,4 done nicely\nSo far 140% Profits with 10x leverage🤑",
    "#BTCUSDT UPDATE:\nBitcoin is currently trading around $72,300.",
    "#MTL/USDT UPDATE:\nTarget 1,2 done nicely ✔️\nSo far 50% Profits",
    "#EGLD/USDT UPDATE:\nTarget 1,2,3 done nicely✔️",
    "#REZ/USDT UPDATE:\nTarget 1,2,3 done nicely ✔️\nSo far 140% Profits",
    "#BAND/USDT UPDATE:\nTarget 3,4 done nicely ✔️",
    "Market is very volatile now. So use low leverage",
]

for msg in commentary_cases:
    t = mtype(msg)
    check(f"Commentary: {msg[:50]!r}", t in (MessageType.COMMENTARY, MessageType.IGNORE), True)


# ══════════════════════════════════════════════════════════════════════════════
# 6. CLOSE_ALL
# ══════════════════════════════════════════════════════════════════════════════
section("6. CLOSE_ALL")

close_all_cases = [
    "close all positions",
    "exit all",
    "emergency close",
    "Close All now!",
    "EXIT ALL TRADES",
]
for msg in close_all_cases:
    check(f"close_all: {msg!r}", mtype(msg), MessageType.CLOSE_ALL)


# ══════════════════════════════════════════════════════════════════════════════
# 7. CLOSE_SYMBOL
# ══════════════════════════════════════════════════════════════════════════════
section("7. CLOSE_SYMBOL")

close_sym_cases = [
    ("close BTCUSDT",              "BTCUSDT"),
    ("exit ETHUSDT now",           "ETHUSDT"),
    ("#CFXUSDT close the position","CFXUSDT"),
    ("close position for NEOUSDT", "NEOUSDT"),
]
for msg, expected_sym in close_sym_cases:
    p = parse(msg)
    check(f"close_symbol type: {msg!r}",   p.message_type, MessageType.CLOSE_SYMBOL)
    check(f"close_symbol sym: {msg!r}",    p.symbol,       expected_sym)


# ══════════════════════════════════════════════════════════════════════════════
# 8. CANCEL_REMAINING_ENTRIES
# ══════════════════════════════════════════════════════════════════════════════
section("8. CANCEL_REMAINING_ENTRIES")

cancel_entry_cases = [
    "cancel remaining entries BTCUSDT",
    "cancel open orders ETHUSDT",
    "cancel remaining buy orders",
    "cancel open sell orders",
]
for msg in cancel_entry_cases:
    check(f"cancel_entries: {msg!r}", mtype(msg), MessageType.CANCEL_REMAINING_ENTRIES)


# ══════════════════════════════════════════════════════════════════════════════
# 9. MOVE_SL_BREAK_EVEN
# ══════════════════════════════════════════════════════════════════════════════
section("9. MOVE_SL_BREAK_EVEN")

sl_be_cases = [
    "move sl to break even BTCUSDT",
    "move stop to entry ETHUSDT",
    "put stop at break-even",
    "set stop to BE CFXUSDT",
    "move SL to breakeven",
]
for msg in sl_be_cases:
    check(f"move_sl_be: {msg!r}", mtype(msg), MessageType.MOVE_SL_BREAK_EVEN)


# ══════════════════════════════════════════════════════════════════════════════
# 10. MOVE_SL_PRICE
# ══════════════════════════════════════════════════════════════════════════════
section("10. MOVE_SL_PRICE")

sl_price_cases = [
    ("move stop loss BTCUSDT to 71000",     71000.0),
    ("new stop 0.048 for CFXUSDT",          0.048),
    ("update stop-loss to 72,500",          72500.0),
    ("sl to 69,000 BTCUSDT",               69000.0),
    ("move SL to 0.295",                    0.295),
]
for msg, expected_price in sl_price_cases:
    p = parse(msg)
    check(f"move_sl_price type: {msg!r}", p.message_type, MessageType.MOVE_SL_PRICE)
    check(f"move_sl_price val:  {msg!r}", p.price, expected_price, close=True)


# ══════════════════════════════════════════════════════════════════════════════
# 11. UPDATE_TARGETS
# ══════════════════════════════════════════════════════════════════════════════
section("11. UPDATE_TARGETS")

p = parse("new targets for CFXUSDT 0.061 - 0.0635 - 0.066")
check("update_targets — type",      p.message_type, MessageType.UPDATE_TARGETS)
check("update_targets — symbol",    p.symbol,       "CFXUSDT")
check("update_targets — targets",   p.targets,      [0.061, 0.0635, 0.066])

p = parse("update targets BTCUSDT 74000 - 75600 - 76800")
check("update_targets BTC — type",    p.message_type, MessageType.UPDATE_TARGETS)
check("update_targets BTC — targets", p.targets,      [74000.0, 75600.0, 76800.0])

p = parse("remove tp ETHUSDT")
check("remove tp — type", p.message_type, MessageType.UPDATE_TARGETS)


# ══════════════════════════════════════════════════════════════════════════════
# 12. ADD_ENTRIES
# ══════════════════════════════════════════════════════════════════════════════
section("12. ADD_ENTRIES")

p = parse("new entry BTCUSDT 70000 - 68000")
check("add_entries — type",       p.message_type, MessageType.ADD_ENTRIES)
check("add_entries — symbol",     p.symbol,       "BTCUSDT")
check("add_entries — entry_low",  p.entry_low,    68000.0, close=True)
check("add_entries — entry_high", p.entry_high,   70000.0, close=True)

p = parse("add entry CFXUSDT 0.050")
check("add_entries single — type",     p.message_type, MessageType.ADD_ENTRIES)
check("add_entries single — entry_low",p.entry_low,    0.050, close=True)

p = parse("average in ETHUSDT 2600 - 2500")
check("average in — type", p.message_type, MessageType.ADD_ENTRIES)


# ══════════════════════════════════════════════════════════════════════════════
# 13. MARKET_ENTRY
# ══════════════════════════════════════════════════════════════════════════════
section("13. MARKET_ENTRY")

market_cases = [
    "buy now BTCUSDT",
    "enter now ETHUSDT",
    "sell now XRPUSDT",
    "Buy Now!",
]
for msg in market_cases:
    check(f"market_entry: {msg!r}", mtype(msg), MessageType.MARKET_ENTRY)


# ══════════════════════════════════════════════════════════════════════════════
# 14. PARTIAL_CLOSE
# ══════════════════════════════════════════════════════════════════════════════
section("14. PARTIAL_CLOSE")

p = parse("close 50% BTCUSDT")
check("partial_close 50% — type",    p.message_type, MessageType.PARTIAL_CLOSE)
check("partial_close 50% — percent", p.percent,      50.0, close=True)

p = parse("close half CFXUSDT")
check("partial_close half — type",    p.message_type, MessageType.PARTIAL_CLOSE)
check("partial_close half — percent", p.percent,      50.0, close=True)

p = parse("take partial profits ETHUSDT")
check("take partial — type", p.message_type, MessageType.PARTIAL_CLOSE)

p = parse("close 25% NEOUSDT")
check("partial_close 25% — percent", p.percent, 25.0, close=True)


# ══════════════════════════════════════════════════════════════════════════════
# 15. CANCEL_SIGNAL
# ══════════════════════════════════════════════════════════════════════════════
section("15. CANCEL_SIGNAL")

cancel_cases = [
    "ignore previous signal BTCUSDT",
    "cancel previous signal ETHUSDT",
    "setup invalidated CFXUSDT",
    "signal cancelled for NEOUSDT",
    "disregard the last signal",
]
for msg in cancel_cases:
    check(f"cancel_signal: {msg!r}", mtype(msg), MessageType.CANCEL_SIGNAL)


# ══════════════════════════════════════════════════════════════════════════════
# 16. IGNORE — noise that must never trigger a trade
# ══════════════════════════════════════════════════════════════════════════════
section("16. IGNORE — noise messages")

ignore_cases = [
    "Good morning traders! 🌅",
    "Bitcoin Fear and Greed Index is 70 — Greed",
    "Stay tuned with us for further updates ✔️",
    "Market is very volatile. Use low leverage.",
    "🔥🔥🔥",
    "",
    "   ",
    "https://t.me/wallstreetqueen",
    "Don't wait for all Targets, book profits Partially",
]
for msg in ignore_cases:
    t = mtype(msg)
    check(f"ignore: {msg[:40]!r}", t in (MessageType.IGNORE, MessageType.COMMENTARY), True)


# ══════════════════════════════════════════════════════════════════════════════
# 17. CLASSIFIER PRIORITY — signal must win over commentary
# ══════════════════════════════════════════════════════════════════════════════
section("17. Classifier priority — signal beats commentary")

# A message with "UPDATE:" in it but also has Entry/SL → should still be signal
# (edge case: WSQ sometimes posts updates that look like new signals)
ambiguous = """#BTCUSDT UPDATE:
Coin: #BTCUSDT
Direction: Long
Leverage: 10x
Entry: 72000
Targets: 74000 - 76000
Stop-loss: 70000"""

p = parse(ambiguous)
# new_signal classifier runs first, so this should be a signal
check("Signal beats commentary", p.message_type, MessageType.NEW_SIGNAL)

# Pure commentary with target numbers must NOT become a signal
pure_commentary = "#CFXUSDT UPDATE:\nTarget 1,2,3,4 done nicely\nSo far 140% Profits"
check("Commentary stays commentary",
      mtype(pure_commentary) in (MessageType.COMMENTARY, MessageType.IGNORE), True)


# ══════════════════════════════════════════════════════════════════════════════
# 18. ROBUSTNESS — malformed / real-world edge cases
# ══════════════════════════════════════════════════════════════════════════════
section("18. Robustness — malformed inputs")

# Extra whitespace and emoji in signal
messy_signal = """
Coin:   #ETH/USDT  🔥
Direction:  Long  💪
Leverage: 5-10x
Entry:  3603  -  3590$
Targets: 3680 - 3760 - 3840$
Stop-loss:  3570$
"""
p = parse(messy_signal)
check("Messy whitespace — type",      p.message_type, MessageType.NEW_SIGNAL)
check("Messy whitespace — symbol",    p.symbol,       "ETHUSDT")
check("Messy whitespace — entry_high",p.entry_high,   3603.0, close=True)

# Signal with no leverage line
no_lev = """Coin: #SOLUSDT
Direction: Long
Entry: 180 - 175
Targets: 190 - 200 - 210
Stop-loss: 170"""
p = parse(no_lev)
check("No leverage — still parses", p.message_type, MessageType.NEW_SIGNAL)
check("No leverage — default max",  p.leverage_max, 10)  # model default

# Signal with targets on separate lines
multiline_targets = """Coin: #ETHUSDT
Direction: Long
Leverage: 5-10x
Entry: 3500 - 3450
Target 1: 3600
Target 2: 3700
Target 3: 3800
Stop-loss: 3400"""
p = parse(multiline_targets)
check("Multiline targets — type",     p.message_type, MessageType.NEW_SIGNAL)
check("Multiline targets — n",        len(p.targets), 3)
check("Multiline targets — t1",       p.targets[0],   3600.0, close=True)
check("Multiline targets — t3",       p.targets[2],   3800.0, close=True)

# SL with label variants
sl_variants = [
    ("Stop-loss: 3400", 3400.0),
    ("SL: 3400",        3400.0),
    ("Stop: 3400",      3400.0),
]
for sl_line, expected in sl_variants:
    sig = f"Coin: #ETHUSDT\nDirection: Long\nLeverage: 5x\nEntry: 3500\nTargets: 3600\n{sl_line}"
    p = parse(sig)
    check(f"SL label variant {sl_line!r}", p.stop_loss, expected, close=True)

# Parser must not crash on completely random garbage
garbage_inputs = [
    "asdkjasdlkajsd",
    "12345",
    None.__class__.__name__,  # "NoneType" — just a string
    "Entry: abc Stop-loss: xyz",
    "Coin: ##$$\nEntry: ???\nStop-loss: ---",
]
for g in garbage_inputs:
    try:
        result = parse(g)
        check(f"No crash on garbage {g!r}", True, True)
    except Exception as e:
        check(f"No crash on garbage {g!r}", f"EXCEPTION: {e}", "no exception")


# ══════════════════════════════════════════════════════════════════════════════
# 19. REAL WSQ MESSAGES — verbatim from the export
# ══════════════════════════════════════════════════════════════════════════════
section("19. Real WSQ messages (verbatim)")

real_signals = [
    # ETH from export
    {
        "text": "Coin: #ETH/USDT\n\nLong Set-Up \n\nLeverage: 5-10x\n\n#ETH already breaked out the Bull Flag and looking Bullish.\n\nEntry: 3603 - 3590$\n\nTargets: 3680 - 3760 - 3840 - 3920 - 4000$\n\nStop-loss: 3570$",
        "symbol": "ETHUSDT",
        "direction": Direction.LONG,
        "entry_high": 3603.0,
        "entry_low": 3590.0,
        "stop_loss": 3570.0,
        "n_targets": 5,
    },
    # HBAR from export
    {
        "text": "Coin: #HBAR/USDT\n\nLong Set-Up \n\nLeverage: 5-10x\n\n#HBAR already breaked out the Symmetrical traingle and looking Bullish.\n\nEntry: 0.305 - 0.295$(Buy partially)\n\nTargets: 0.315 - 0.322 - 0.330 - 0.338 - 0.355 - 0.375 - 0.395 - 0.420$(Short-mid term)\n\nStop-loss: 0.285$",
        "symbol": "HBARUSDT",
        "direction": Direction.LONG,
        "entry_high": 0.305,
        "entry_low": 0.295,
        "stop_loss": 0.285,
        "n_targets": 8,
    },
    # LINA from export
    {
        "text": "Coin: #LINA/USDT\n\nLong Set-Up \n\nLeverage: 5-10x\n\n#LINA already breaked out the inverse head and shoulders pattern and looking Bullish.\n\nEntry: 0.00560 - 0.00545$(Buy partially)\n\nTargets: 0.00575 - 0.00590 - 0.00605 - 0.00620 - 0.00640$(Short term)\n\nStop-loss: 0.00530$",
        "symbol": "LINAUSDT",
        "direction": Direction.LONG,
        "entry_high": 0.00560,
        "entry_low": 0.00545,
        "stop_loss": 0.00530,
        "n_targets": 5,
    },
    # BTC (the bug case — thousands separators)
    {
        "text": "Coin: #BTCUSDT\nDirection: Long\nLeverage: 10-20x\nIt has already broken out of the Cup and Handle pattern and is looking bullish.\nEntry: $72,260 - $70,800 (Buy partially)\nTargets: $74,000 - $75,600 - $76,800 - $78,000 (Short term)\nStop-loss: $69,600",
        "symbol": "BTCUSDT",
        "direction": Direction.LONG,
        "entry_high": 72260.0,
        "entry_low": 70800.0,
        "stop_loss": 69600.0,
        "n_targets": 4,
    },
]

for tc in real_signals:
    p = parse(tc["text"])
    sym = tc["symbol"]
    check(f"{sym} — type",       p.message_type, MessageType.NEW_SIGNAL)
    check(f"{sym} — symbol",     p.symbol,       tc["symbol"])
    check(f"{sym} — direction",  p.direction,    tc["direction"])
    check(f"{sym} — entry_high", p.entry_high,   tc["entry_high"], close=True)
    check(f"{sym} — entry_low",  p.entry_low,    tc["entry_low"],  close=True)
    check(f"{sym} — stop_loss",  p.stop_loss,    tc["stop_loss"],  close=True)
    check(f"{sym} — n_targets",  len(p.targets), tc["n_targets"])


# ══════════════════════════════════════════════════════════════════════════════
# Done
# ══════════════════════════════════════════════════════════════════════════════
exit_code = summary()
sys.exit(exit_code)

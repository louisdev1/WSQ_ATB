import sys
sys.path.insert(0, '.')

from app.parsing.parser import parse_message
from app.parsing.models import MessageType

tests = [
    # ── NEW SIGNALS ──────────────────────────────────────────────────────────
    (
        "Coin: #AXLUSDT\nDirection: Short\nLeverage: 5-10x\nEntry: $0.05250 - $0.05300 (Enter partially)\nTargets: $0.05050 - $0.04920 - $0.04800 - $0.04700 - $0.04500 - $0.04200\nStop-loss: $0.05400",
        MessageType.NEW_SIGNAL, "AXLUSDT"
    ),
    (
        "Coin: #SAGAUSDT\nDirection: Long\nLeverage: 5-10x\n#SAGA has already broken out\nEntry: 0.03360 - 0.03200$ (Buy partially)\nTargets: 0.03450 - 0.03550 - 0.03650 - 0.03750 - 0.03950 - 0.04200$\nStop-loss: 0.03100$",
        MessageType.NEW_SIGNAL, "SAGAUSDT"
    ),
    (
        "Coin: #ZECUSDT\nDirection: Short\nLeverage: 5-10x\nEntry: 196.60 - 202.50$ (Enter partially)\nTargets: 190 - 184 - 178 - 172 - 162 - 150$\nStop-loss: 210$",
        MessageType.NEW_SIGNAL, "ZECUSDT"
    ),
    (
        "Coin: #INJUSDT\nDirection: Short\nLeverage: 5-10x\nEntry: 2.865 - 2.950$\nTargets: 2.800 - 2.740 - 2.680 - 2.620 - 2.480 - 2.300$\nStop-loss: 3.050$",
        MessageType.NEW_SIGNAL, "INJUSDT"
    ),
    (
        "Coin: #BNBUSDT\nDirection: Long\nLeverage: 5-10x\nEntry: 619.5 - 616$ (Buy partially)\nTargets: 630 - 642.5 - 655.3 - 668$\nStop-loss: 612$",
        MessageType.NEW_SIGNAL, "BNBUSDT"
    ),
    (
        "Coin: #1000LUNCUSDT\nDirection: Short\nLeverage: 5-10x\nEntry: 0.03310 - 0.03360 (Enter partially)\nTargets: 0.03240 - 0.03180 - 0.03120 - 0.03040 - 0.02920 - 0.02800\nStop-loss: 0.03420",
        MessageType.NEW_SIGNAL, "1000LUNCUSDT"
    ),
    (
        "Coin: #DOTUSDT\nDirection: Short\nLeverage: 5-10x\nEntry: $1.670 - $1.770(Enter partially)\nTargets: $1.640 - $1.600 - $1.570 - $1.530 - $1.460 - $1.400\nStop-loss: $1.820",
        MessageType.NEW_SIGNAL, "DOTUSDT"
    ),
    (
        "Coin: #KNCUSDT\nDirection: Long\nLeverage: 5-10x\nEntry: $0.1515 - $0.1490 (Buy partially)\nTargets: $0.1540 - $0.1570 - $0.1610 - $0.1650 - $0.1700 - $0.1750\nStop-loss: $0.1470",
        MessageType.NEW_SIGNAL, "KNCUSDT"
    ),

    # ── COMMENTARY / UPDATES (should NOT trade) ───────────────────────────────
    (
        "#SAGAUSDT UPDATE:\nTarget 1,2,3 done nicely ✔️\nSo far 100% Profits with 10x leverage🤑",
        MessageType.COMMENTARY, None
    ),
    (
        "#LAYERUSDT UPDATE:\nTarget 2 done nicely✔️\nSo far 30% Profits with 10x leverage🤑",
        MessageType.COMMENTARY, None
    ),
    (
        "#BTCUSDT UPDATE:\n#Bitcoin is now trading around $69,800. Keep an eye on it.",
        MessageType.COMMENTARY, None
    ),
    (
        "#SAFE/USDT Stop Target Hit ⛔\nLoss: 20.8443% 📉",
        MessageType.COMMENTARY, None
    ),
    (
        "Market is very volatile now. So use low leverage and low amount of your capital as per your Risk management.",
        MessageType.IGNORE, None
    ),
    (
        "Risky trade, only for risk-takers.",
        MessageType.IGNORE, None
    ),
    (
        "JUST IN: Bitmine added 40,613 $ETH over the past week.",
        MessageType.IGNORE, None
    ),

    # ── CLOSE ALL ─────────────────────────────────────────────────────────────
    (
        "close all trades",
        MessageType.CLOSE_ALL, None
    ),
    (
        "close all positions",
        MessageType.CLOSE_ALL, None
    ),
    (
        "emergency close all",
        MessageType.CLOSE_ALL, None
    ),

    # ── CLOSE SYMBOL ──────────────────────────────────────────────────────────
    (
        "close AXLUSDT",
        MessageType.CLOSE_SYMBOL, "AXLUSDT"
    ),
    (
        "exit BTCUSDT now",
        MessageType.CLOSE_SYMBOL, "BTCUSDT"
    ),
    (
        "close SAGAUSDT",
        MessageType.CLOSE_SYMBOL, "SAGAUSDT"
    ),

    # ── CANCEL REMAINING ENTRIES ──────────────────────────────────────────────
    (
        "cancel remaining entries for AXLUSDT",
        MessageType.CANCEL_REMAINING_ENTRIES, "AXLUSDT"
    ),
    (
        "cancel open buy orders for BTCUSDT",
        MessageType.CANCEL_REMAINING_ENTRIES, "BTCUSDT"
    ),

    # ── MOVE SL BREAK-EVEN ────────────────────────────────────────────────────
    (
        "move SL to entry",
        MessageType.MOVE_SL_BREAK_EVEN, None
    ),
    (
        "move SL to breakeven",
        MessageType.MOVE_SL_BREAK_EVEN, None
    ),
    (
        "put stop at BE",
        MessageType.MOVE_SL_BREAK_EVEN, None
    ),
    (
        "move stop loss to break even",
        MessageType.MOVE_SL_BREAK_EVEN, None
    ),

    # ── MOVE SL PRICE ─────────────────────────────────────────────────────────
    (
        "move stop loss to 0.03358",
        MessageType.MOVE_SL_PRICE, None
    ),
    (
        "new stop loss: 0.0310",
        MessageType.MOVE_SL_PRICE, None
    ),
    (
        "update stop for BTCUSDT to 65000",
        MessageType.MOVE_SL_PRICE, "BTCUSDT"
    ),
    (
        "move SL to 0.05100",
        MessageType.MOVE_SL_PRICE, None
    ),

    # ── CANCEL SIGNAL ─────────────────────────────────────────────────────────
    (
        "ignore previous signal",
        MessageType.CANCEL_SIGNAL, None
    ),
    (
        "cancel previous setup",
        MessageType.CANCEL_SIGNAL, None
    ),
    (
        "AXLUSDT setup invalidated",
        MessageType.CANCEL_SIGNAL, "AXLUSDT"
    ),

    # ── MARKET ENTRY ─────────────────────────────────────────────────────────
    (
        "buy now",
        MessageType.MARKET_ENTRY, None
    ),
    (
        "sell now",
        MessageType.MARKET_ENTRY, None
    ),
    (
        "enter now",
        MessageType.MARKET_ENTRY, None
    ),

    # ── PARTIAL CLOSE ────────────────────────────────────────────────────────
    (
        "close 50%",
        MessageType.PARTIAL_CLOSE, None
    ),
    (
        "close half",
        MessageType.PARTIAL_CLOSE, None
    ),
    (
        "take partial profits now",
        MessageType.PARTIAL_CLOSE, None
    ),
    (
        "close 25% of position",
        MessageType.PARTIAL_CLOSE, None
    ),
]

# ── run ───────────────────────────────────────────────────────────────────────

passed = 0
failed = 0

for text, expected_type, expected_symbol in tests:
    result = parse_message(text, 0)
    type_ok = result.message_type == expected_type
    symbol_ok = (expected_symbol is None) or (getattr(result, "symbol", None) == expected_symbol)
    ok = type_ok and symbol_ok

    status = "✓" if ok else "✗"
    if ok:
        passed += 1
    else:
        failed += 1

    # Always print the result
    label = text.strip().splitlines()[0][:60]
    print(f"{status} [{expected_type.value:30}] {label}")

    # On failure, show details
    if not ok:
        print(f"     expected type:   {expected_type.value}")
        print(f"     got type:        {result.message_type.value}")
        if expected_symbol:
            print(f"     expected symbol: {expected_symbol}")
            print(f"     got symbol:      {getattr(result, 'symbol', 'N/A')}")

    # Show parsed details for new signals
    if result.message_type == MessageType.NEW_SIGNAL:
        print(f"     symbol={result.symbol} dir={result.direction} "
              f"entry={result.entry_low}-{result.entry_high} "
              f"sl={result.stop_loss} targets={result.targets}")

print()
print(f"{'='*50}")
print(f"  {passed} passed / {failed} failed out of {len(tests)} tests")
print(f"{'='*50}")
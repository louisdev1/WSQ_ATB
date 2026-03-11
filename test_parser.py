import sys
sys.path.insert(0, '.')

from app.parsing.parser import parse_message
from app.parsing.models import MessageType, Direction

# ─────────────────────────────────────────────────────────────────────────────
# Each test is: (raw_text, expected_type, expected_symbol_or_None, extra_checks_dict)
# extra_checks keys: direction, entry_low, entry_high, sl, targets, price, percent
# ─────────────────────────────────────────────────────────────────────────────

tests = [

    # ══════════════════════════════════════════════════════════════════════════
    # NEW SIGNAL – variations
    # ══════════════════════════════════════════════════════════════════════════

    # Standard short with dollar signs
    (
        "Coin: #AXLUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: $0.05250 - $0.05300 (Enter partially)\n"
        "Targets: $0.05050 - $0.04920 - $0.04800 - $0.04700 - $0.04500 - $0.04200\n"
        "Stop-loss: $0.05400",
        MessageType.NEW_SIGNAL, "AXLUSDT",
        {"direction": Direction.SHORT, "entry_low": 0.0525, "entry_high": 0.053,
         "sl": 0.054, "targets": [0.0505, 0.0492, 0.048, 0.047, 0.045, 0.042]}
    ),

    # Standard long, prices without $, "Buy partially"
    (
        "Coin: #SAGAUSDT\nDirection: Long\nLeverage: 5-10x\n"
        "#SAGA has already broken out of the symmetrical triangle and is looking bullish.\n"
        "Entry: 0.03360 - 0.03200$ (Buy partially)\n"
        "Targets: 0.03450 - 0.03550 - 0.03650 - 0.03750 - 0.03950 - 0.04200$\n"
        "Stop-loss: 0.03100$",
        MessageType.NEW_SIGNAL, "SAGAUSDT",
        {"direction": Direction.LONG, "entry_low": 0.032, "entry_high": 0.0336, "sl": 0.031}
    ),

    # Large price numbers (ZEC)
    (
        "Coin: #ZECUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: 196.60 - 202.50$ (Enter partially)\n"
        "Targets: 190 - 184 - 178 - 172 - 162 - 150$\n"
        "Stop-loss: 210$",
        MessageType.NEW_SIGNAL, "ZECUSDT",
        {"direction": Direction.SHORT, "entry_low": 196.6, "entry_high": 202.5, "sl": 210.0,
         "targets": [190.0, 184.0, 178.0, 172.0, 162.0, 150.0]}
    ),

    # Mid-range prices (INJ), no dollar on entry
    (
        "Coin: #INJUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: 2.865 - 2.950$\n"
        "Targets: 2.800 - 2.740 - 2.680 - 2.620 - 2.480 - 2.300$\n"
        "Stop-loss: 3.050$",
        MessageType.NEW_SIGNAL, "INJUSDT",
        {"direction": Direction.SHORT, "entry_low": 2.865, "entry_high": 2.95, "sl": 3.05}
    ),

    # Large BNB prices
    (
        "Coin: #BNBUSDT\nDirection: Long\nLeverage: 5-10x\n"
        "Entry: 619.5 - 616$ (Buy partially)\n"
        "Targets: 630 - 642.5 - 655.3 - 668$\n"
        "Stop-loss: 612$",
        MessageType.NEW_SIGNAL, "BNBUSDT",
        {"direction": Direction.LONG, "entry_low": 616.0, "entry_high": 619.5, "sl": 612.0,
         "targets": [630.0, 642.5, 655.3, 668.0]}
    ),

    # 1000x prefixed symbol
    (
        "Coin: #1000LUNCUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: 0.03310 - 0.03360 (Enter partially)\n"
        "Targets: 0.03240 - 0.03180 - 0.03120 - 0.03040 - 0.02920 - 0.02800\n"
        "Stop-loss: 0.03420",
        MessageType.NEW_SIGNAL, "1000LUNCUSDT",
        {"direction": Direction.SHORT, "sl": 0.0342}
    ),

    # No space before $ on entry range
    (
        "Coin: #DOTUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: $1.670 - $1.770(Enter partially)\n"
        "Targets: $1.640 - $1.600 - $1.570 - $1.530 - $1.460 - $1.400\n"
        "Stop-loss: $1.820",
        MessageType.NEW_SIGNAL, "DOTUSDT",
        {"direction": Direction.SHORT, "entry_low": 1.67, "entry_high": 1.77, "sl": 1.82}
    ),

    # Small prices (KNC)
    (
        "Coin: #KNCUSDT\nDirection: Long\nLeverage: 5-10x\n"
        "Entry: $0.1515 - $0.1490 (Buy partially)\n"
        "Targets: $0.1540 - $0.1570 - $0.1610 - $0.1650 - $0.1700 - $0.1750\n"
        "Stop-loss: $0.1470",
        MessageType.NEW_SIGNAL, "KNCUSDT",
        {"direction": Direction.LONG, "entry_low": 0.149, "entry_high": 0.1515, "sl": 0.147}
    ),

    # With extra commentary line between fields
    (
        "Coin: #SNXUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "#SNX has already broken down the Inverse Cup and Handle pattern and is looking bearish.\n"
        "Entry: $0.335 - $0.342(Enter partially)\n"
        "Targets: $0.328 - $0.320 - $0.312 - $0.306 - $0.290 - $0.270\n"
        "Stop-loss: $0.350",
        MessageType.NEW_SIGNAL, "SNXUSDT",
        {"direction": Direction.SHORT, "entry_low": 0.335, "entry_high": 0.342, "sl": 0.35}
    ),

    # Long with AXS, prices without dollar sign anywhere
    (
        "Coin: #AXSUSDT\nDirection: Long\nLeverage: 5-10x\n"
        "#AXS already breaked out the Falling wedge pattern and looking Bullish.\n"
        "Entry: 1.375 - 1.340$(Buy partially)\n"
        "Targets: 1.410 - 1.440 - 1.470 - 1.510 - 1.600 - 1.680 - 1.750$\n"
        "Stop-loss: 1.300$",
        MessageType.NEW_SIGNAL, "AXSUSDT",
        {"direction": Direction.LONG, "entry_low": 1.34, "entry_high": 1.375, "sl": 1.3}
    ),

    # QNT with mixed $ placement
    (
        "Coin: #QNTUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: $64.10 - $65(Enter partially)\n"
        "Targets: $62.5 - $61 - $59.5 - $58 - $55\n"
        "Stop-loss: $66",
        MessageType.NEW_SIGNAL, "QNTUSDT",
        {"direction": Direction.SHORT, "entry_low": 64.1, "entry_high": 65.0, "sl": 66.0}
    ),

    # LAYER short with extra volatility commentary attached
    (
        "Market is very volatile now. So use low leverage.\n\n"
        "Coin: #LAYERUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: 0.08230 - 0.08400$ (Enter partially)\n"
        "Targets: 0.08000 - 0.07800 - 0.07600 - 0.07400 - 0.07000 - 0.06600$\n"
        "Stop-loss: 0.08600$",
        MessageType.NEW_SIGNAL, "LAYERUSDT",
        {"direction": Direction.SHORT, "sl": 0.086}
    ),

    # EPIC long
    (
        "Coin: #EPICUSDT\nDirection: Long\nLeverage: 5-10x\n"
        "Entry: 0.3065 - 0.3000$ (Buy partially)\n"
        "Targets: 0.3140 - 0.3200 - 0.3260 - 0.3340 - 0.3400$\n"
        "Stop-loss: 0.2950$",
        MessageType.NEW_SIGNAL, "EPICUSDT",
        {"direction": Direction.LONG, "entry_low": 0.3, "entry_high": 0.3065, "sl": 0.295}
    ),

    # ARK long
    (
        "Coin: #ARKUSDT\nDirection: Long\nLeverage: 5-10x\n"
        "Entry: 0.1925 - 0.1880$ (Buy partially)\n"
        "Targets: 0.1980 - 0.2020 - 0.2060 - 0.2120 - 0.2200$\n"
        "Stop-loss: 0.1840$",
        MessageType.NEW_SIGNAL, "ARKUSDT",
        {"direction": Direction.LONG, "entry_low": 0.188, "entry_high": 0.1925, "sl": 0.184}
    ),

    # DASH – direction says Short but entry says "Buy partially" (signal typo in real data)
    (
        "Coin: #DASHUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: 34.40 - 35$ (Buy partially)\n"
        "Targets: 33.60 - 32.80 - 32 - 31.40 - 30$\n"
        "Stop loss: 35.50$",
        MessageType.NEW_SIGNAL, "DASHUSDT",
        {"direction": Direction.SHORT, "entry_low": 34.4, "entry_high": 35.0, "sl": 35.5}
    ),

    # SAFE (short) – very small prices
    (
        "Coin: #SAFEUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: $0.0947 - $0.0960 (Enter partially)\n"
        "Targets: $0.0925 - $0.0905 - $0.0885 - $0.0865 - $0.0840\n"
        "Stop-loss: $0.0980",
        MessageType.NEW_SIGNAL, "SAFEUSDT",
        {"direction": Direction.SHORT, "entry_low": 0.0947, "entry_high": 0.096, "sl": 0.098}
    ),

    # ENJ short
    (
        "Coin: #ENJUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: $0.02030 - $0.02100 (Enter partially)\n"
        "Targets: $0.02000 - $0.01960 - $0.01920 - $0.01880 - $0.01780 - $0.01650\n"
        "Stop-loss: $0.02150",
        MessageType.NEW_SIGNAL, "ENJUSDT",
        {"direction": Direction.SHORT, "sl": 0.0215}
    ),

    # ══════════════════════════════════════════════════════════════════════════
    # COMMENTARY / UPDATES – must NOT trigger any trade action
    # ══════════════════════════════════════════════════════════════════════════

    ("Wallstreet Queen Official VIP, [11.03.2026 11:02]\n#SAGAUSDT UPDATE:\nTarget 1,2,3 done nicely ✔️\nSo far 100% Profits with 10x leverage🤑\nEnjoy the profits Guys🍾🍾",
     MessageType.COMMENTARY, None, {}),

    ("#LAYERUSDT UPDATE:\nTarget 2 done nicely✔️\nSo far 30% Profits with 10x leverage🤑",
     MessageType.COMMENTARY, None, {}),

    ("#BTCUSDT UPDATE:\n#Bitcoin is now trading around $69,800. In the weekly timeframe, Bitcoin is testing an important resistance zone.",
     MessageType.COMMENTARY, None, {}),

    ("#ETHUSDT UPDATE:\nEthereum is pumping exactly from support. If the pump continues, it may go towards the upper resistance zone.",
     MessageType.COMMENTARY, None, {}),

    ("#INJUSDT UPDATE:\nTarget 1 done nicely✔️\nSo far 30% Profits with 10x leverage🤑",
     MessageType.COMMENTARY, None, {}),

    ("#SNXUSDT UPDATE:\nTarget 1,2,3,4 done nicely✔️\n80% profit booked with 10x Leverage🤑",
     MessageType.COMMENTARY, None, {}),

    ("#MORPHOUSDT UPDATE:\nAll Targets done nicely✔️\n410% Profits with 10x leverage🤑",
     MessageType.COMMENTARY, None, {}),

    ("#ENJUSDT UPDATE:\nTarget 5 done nicely✔️\nSo far 140% Profits with 10x leverage🤑",
     MessageType.COMMENTARY, None, {}),

    ("#FLOWUSDT UPDATE:\nTarget 4 done nicely✔️\nSo far 140% Profits with 10x leverage🤑",
     MessageType.COMMENTARY, None, {}),

    ("#QNTUSDT UPDATE:\nTarget 1 done nicely✔️\nSo far 30% Profits with 10x leverage🤑",
     MessageType.COMMENTARY, None, {}),

    ("#LPTUSDT UPDATE:\nTarget 2,3 done nicely✔️\nSo far 90% Profits with 10x leverage🤑",
     MessageType.COMMENTARY, None, {}),

    ("#BTCUSDT UPDATE:\nBitcoin is currently trading around $65,700. It is moving inside a symmetrical triangle on the daily timeframe.",
     MessageType.COMMENTARY, None, {}),

    # Stop hit messages with /USDT format
    ("#SAFE/USDT Stop Target Hit ⛔\nLoss: 20.8443% 📉",
     MessageType.COMMENTARY, None, {}),

    ("#1000LUNC/USDT Stop Target Hit ⛔\nLoss: 19.1154% 📉",
     MessageType.COMMENTARY, None, {}),

    # ══════════════════════════════════════════════════════════════════════════
    # IGNORE – general noise, market commentary, news
    # ══════════════════════════════════════════════════════════════════════════

    ("Market is very volatile now. So use low leverage and low amount of your capital as per your Risk management . Don't wait for all Targets, book profits Partially",
     MessageType.IGNORE, None, {}),

    ("Risky trade, only for risk-takers.",
     MessageType.IGNORE, None, {}),

    ("JUST IN: Bitmine added 40,613 $ETH over the past week.\nThe firm now holds ~4.326M ETH, with ~2.9M ETH staked.",
     MessageType.IGNORE, None, {}),

    ("#Pin our channel to the top and #Unmute our channel✅",
     MessageType.IGNORE, None, {}),

    ("Those who followed us and opened a long position have booked good profits. 90% profits booked with 10x Lev. Enjoy the profits, guys",
     MessageType.IGNORE, None, {}),

    ("For more updates like this, stay tuned with us.",
     MessageType.IGNORE, None, {}),

    # ══════════════════════════════════════════════════════════════════════════
    # CLOSE ALL – multiple phrasings
    # ══════════════════════════════════════════════════════════════════════════

    ("close all trades",         MessageType.CLOSE_ALL, None, {}),
    ("close all positions",      MessageType.CLOSE_ALL, None, {}),
    ("emergency close all",      MessageType.CLOSE_ALL, None, {}),
    ("exit all now",             MessageType.CLOSE_ALL, None, {}),
    ("CLOSE ALL",                MessageType.CLOSE_ALL, None, {}),

    # ══════════════════════════════════════════════════════════════════════════
    # CLOSE SYMBOL – multiple phrasings
    # ══════════════════════════════════════════════════════════════════════════

    ("close AXLUSDT",            MessageType.CLOSE_SYMBOL, "AXLUSDT", {}),
    ("exit BTCUSDT now",         MessageType.CLOSE_SYMBOL, "BTCUSDT", {}),
    ("close SAGAUSDT",           MessageType.CLOSE_SYMBOL, "SAGAUSDT", {}),
    ("close INJUSDT position",   MessageType.CLOSE_SYMBOL, "INJUSDT", {}),
    ("exit BNBUSDT",             MessageType.CLOSE_SYMBOL, "BNBUSDT", {}),
    ("close position for DOTUSDT", MessageType.CLOSE_SYMBOL, "DOTUSDT", {}),

    # ══════════════════════════════════════════════════════════════════════════
    # CANCEL REMAINING ENTRIES
    # ══════════════════════════════════════════════════════════════════════════

    ("cancel remaining entries for AXLUSDT",   MessageType.CANCEL_REMAINING_ENTRIES, "AXLUSDT", {}),
    ("cancel open buy orders for BTCUSDT",     MessageType.CANCEL_REMAINING_ENTRIES, "BTCUSDT", {}),
    ("cancel remaining orders for SAGAUSDT",   MessageType.CANCEL_REMAINING_ENTRIES, "SAGAUSDT", {}),
    ("cancel open sell orders for INJUSDT",    MessageType.CANCEL_REMAINING_ENTRIES, "INJUSDT", {}),

    # ══════════════════════════════════════════════════════════════════════════
    # MOVE SL TO BREAK-EVEN
    # ══════════════════════════════════════════════════════════════════════════

    ("move SL to entry",                MessageType.MOVE_SL_BREAK_EVEN, None, {}),
    ("move SL to breakeven",            MessageType.MOVE_SL_BREAK_EVEN, None, {}),
    ("move SL to break even",           MessageType.MOVE_SL_BREAK_EVEN, None, {}),
    ("put stop at BE",                  MessageType.MOVE_SL_BREAK_EVEN, None, {}),
    ("move stop loss to break even",    MessageType.MOVE_SL_BREAK_EVEN, None, {}),
    ("set stop to entry price",         MessageType.MOVE_SL_BREAK_EVEN, None, {}),
    ("move stop to breakeven",          MessageType.MOVE_SL_BREAK_EVEN, None, {}),

    # ══════════════════════════════════════════════════════════════════════════
    # MOVE SL TO PRICE
    # ══════════════════════════════════════════════════════════════════════════

    ("move stop loss to 0.03358",                   MessageType.MOVE_SL_PRICE, None, {"price": 0.03358}),
    ("new stop loss: 0.0310",                       MessageType.MOVE_SL_PRICE, None, {"price": 0.031}),
    ("update stop for BTCUSDT to 65000",            MessageType.MOVE_SL_PRICE, "BTCUSDT", {"price": 65000.0}),
    ("move SL to 0.05100",                          MessageType.MOVE_SL_PRICE, None, {"price": 0.051}),
    ("move SL to 202",                              MessageType.MOVE_SL_PRICE, None, {"price": 202.0}),
    ("new stop loss: 3.10",                         MessageType.MOVE_SL_PRICE, None, {"price": 3.1}),
    ("stop loss updated to 640",                    MessageType.MOVE_SL_PRICE, None, {"price": 640.0}),
    ("move stop to 1.250",                          MessageType.MOVE_SL_PRICE, None, {"price": 1.25}),

    # ══════════════════════════════════════════════════════════════════════════
    # CANCEL SIGNAL
    # ══════════════════════════════════════════════════════════════════════════

    ("ignore previous signal",                  MessageType.CANCEL_SIGNAL, None, {}),
    ("cancel previous setup",                   MessageType.CANCEL_SIGNAL, None, {}),
    ("AXLUSDT setup invalidated",               MessageType.CANCEL_SIGNAL, "AXLUSDT", {}),
    ("disregard the last signal",               MessageType.CANCEL_SIGNAL, None, {}),
    ("signal cancelled for SAGAUSDT",           MessageType.CANCEL_SIGNAL, "SAGAUSDT", {}),
    ("DOTUSDT setup invalidated, ignore it",    MessageType.CANCEL_SIGNAL, "DOTUSDT", {}),

    # ══════════════════════════════════════════════════════════════════════════
    # MARKET ENTRY
    # ══════════════════════════════════════════════════════════════════════════

    ("buy now",     MessageType.MARKET_ENTRY, None, {}),
    ("sell now",    MessageType.MARKET_ENTRY, None, {}),
    ("enter now",   MessageType.MARKET_ENTRY, None, {}),
    ("BUY NOW",     MessageType.MARKET_ENTRY, None, {}),
    ("SELL NOW",    MessageType.MARKET_ENTRY, None, {}),

    # ══════════════════════════════════════════════════════════════════════════
    # PARTIAL CLOSE
    # ══════════════════════════════════════════════════════════════════════════

    ("close 50%",                       MessageType.PARTIAL_CLOSE, None, {"percent": 50.0}),
    ("close half",                      MessageType.PARTIAL_CLOSE, None, {"percent": 50.0}),
    ("take partial profits now",        MessageType.PARTIAL_CLOSE, None, {}),
    ("close 25% of position",           MessageType.PARTIAL_CLOSE, None, {"percent": 25.0}),
    ("close 75%",                       MessageType.PARTIAL_CLOSE, None, {"percent": 75.0}),
    ("partial close now",               MessageType.PARTIAL_CLOSE, None, {}),
    ("take partial profits on ENJUSDT", MessageType.PARTIAL_CLOSE, None, {}),

    # ══════════════════════════════════════════════════════════════════════════
    # NEW SIGNAL – additional edge cases
    # ══════════════════════════════════════════════════════════════════════════

    # TP/SL abbreviations instead of full words
    (
        "Coin: #SOLUSDT\nDirection: Long\nLeverage: 5x\n"
        "Entry: 130 - 133\nTP: 138 - 142 - 148\nSL: 127",
        MessageType.NEW_SIGNAL, "SOLUSDT",
        {"direction": Direction.LONG, "entry_low": 130.0, "entry_high": 133.0,
         "sl": 127.0, "targets": [138.0, 142.0, 148.0]}
    ),

    # Stop: label (no dash, no 'loss')
    (
        "Coin: #AVAXUSDT\nDirection: Short\nLeverage: 10x\n"
        "Entry: 25 - 26\nTargets: 24 - 23 - 22\nStop: 27",
        MessageType.NEW_SIGNAL, "AVAXUSDT",
        {"direction": Direction.SHORT, "entry_low": 25.0, "entry_high": 26.0,
         "sl": 27.0, "targets": [24.0, 23.0, 22.0]}
    ),

    # Targets on individual numbered lines
    (
        "Coin: #ETHUSDT\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: 1820 - 1850\nTarget 1: 1780\nTarget 2: 1750\nTarget 3: 1700\nStop-loss: 1880",
        MessageType.NEW_SIGNAL, "ETHUSDT",
        {"direction": Direction.SHORT, "entry_low": 1820.0, "entry_high": 1850.0,
         "sl": 1880.0, "targets": [1780.0, 1750.0, 1700.0]}
    ),

    # Emoji prefix + very small price (PEPE-style)
    (
        "⚠️ High risk trade\nCoin: #PEPEUSDT\nDirection: Long\nLeverage: 5x\n"
        "Entry: 0.00001050 - 0.00001000\nTargets: 0.00001150 - 0.00001250\nStop-loss: 0.00000950",
        MessageType.NEW_SIGNAL, "PEPEUSDT",
        {"direction": Direction.LONG, "entry_low": 0.000010, "entry_high": 0.00001050,
         "sl": 0.0000095}
    ),

    # (Futures) label on coin line
    (
        "Coin: #XRPUSDT (Futures)\nDirection: Short\nLeverage: 5-10x\n"
        "Entry: 2.10 - 2.15\nTargets: 2.05 - 2.00 - 1.95\nStop-loss: 2.20",
        MessageType.NEW_SIGNAL, "XRPUSDT",
        {"direction": Direction.SHORT, "sl": 2.20}
    ),

    # No leverage line at all — should still parse
    (
        "Coin: #LINKUSDT\nDirection: Long\n"
        "Entry: 12.50 - 12.00\nTargets: 13.00 - 13.50 - 14.00\nStop-loss: 11.50",
        MessageType.NEW_SIGNAL, "LINKUSDT",
        {"direction": Direction.LONG, "entry_low": 12.0, "entry_high": 12.5, "sl": 11.5}
    ),

    # NEO signal (the one we actually traded)
    (
        "Coin: #NEOUSDT\nDirection: Long\nLeverage: 5-10x\n"
        "#NEO has already broken out of the Cup and Handle pattern and is looking bullish.\n"
        "Entry: 2.583 - 2.550$ (Buy partially)\n"
        "Targets: 2.630 - 2.690 - 2.750 - 2.800$ (Short term)\n"
        "Stop-loss: 2.520$",
        MessageType.NEW_SIGNAL, "NEOUSDT",
        {"direction": Direction.LONG, "entry_low": 2.55, "entry_high": 2.583,
         "sl": 2.52, "targets": [2.63, 2.69, 2.75, 2.80]}
    ),

    # ══════════════════════════════════════════════════════════════════════════
    # CLOSE SYMBOL – hashtag + inline phrasing
    # ══════════════════════════════════════════════════════════════════════════

    ("#AXLUSDT close the position",        MessageType.CLOSE_SYMBOL, "AXLUSDT", {}),
    ("#BTCUSDT exit now",                  MessageType.CLOSE_SYMBOL, "BTCUSDT", {}),
    ("all targets done, close ETHUSDT",    MessageType.CLOSE_SYMBOL, "ETHUSDT", {}),

    # ══════════════════════════════════════════════════════════════════════════
    # MOVE SL TO PRICE – with symbol inline
    # ══════════════════════════════════════════════════════════════════════════

    ("Move SL for BTCUSDT to 82000",       MessageType.MOVE_SL_PRICE, "BTCUSDT", {"price": 82000.0}),
    ("Move SL for NEOUSDT to 2.50",        MessageType.MOVE_SL_PRICE, "NEOUSDT", {"price": 2.50}),

    # ══════════════════════════════════════════════════════════════════════════
    # PARTIAL CLOSE – secure profits phrasing
    # ══════════════════════════════════════════════════════════════════════════

    ("secure profits on ETHUSDT",          MessageType.PARTIAL_CLOSE, None, {}),
    ("Secure profits",                     MessageType.PARTIAL_CLOSE, None, {}),

]

# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

passed = 0
failed = 0

for entry in tests:
    text, expected_type, expected_symbol, checks = entry

    result = parse_message(text, 0)

    type_ok   = result.message_type == expected_type
    symbol_ok = (expected_symbol is None) or (getattr(result, "symbol", None) == expected_symbol)

    # Extra field checks
    field_errors = []
    for key, expected_val in checks.items():
        if key == "direction":
            actual = getattr(result, "direction", None)
            if actual != expected_val:
                field_errors.append(f"direction: expected {expected_val} got {actual}")
        elif key == "entry_low":
            actual = getattr(result, "entry_low", None)
            if actual is None or abs(actual - expected_val) > 0.00001:
                field_errors.append(f"entry_low: expected {expected_val} got {actual}")
        elif key == "entry_high":
            actual = getattr(result, "entry_high", None)
            if actual is None or abs(actual - expected_val) > 0.00001:
                field_errors.append(f"entry_high: expected {expected_val} got {actual}")
        elif key == "sl":
            actual = getattr(result, "stop_loss", None)
            if actual is None or abs(actual - expected_val) > 0.00001:
                field_errors.append(f"stop_loss: expected {expected_val} got {actual}")
        elif key == "targets":
            actual = getattr(result, "targets", [])
            if actual != expected_val:
                field_errors.append(f"targets: expected {expected_val} got {actual}")
        elif key == "price":
            actual = getattr(result, "price", None)
            if actual is None or abs(actual - expected_val) > 0.00001:
                field_errors.append(f"price: expected {expected_val} got {actual}")
        elif key == "percent":
            actual = getattr(result, "percent", None)
            if actual is None or abs(actual - expected_val) > 0.00001:
                field_errors.append(f"percent: expected {expected_val} got {actual}")

    ok = type_ok and symbol_ok and not field_errors

    status = "✓" if ok else "✗"
    label  = text.strip().splitlines()[0][:65]

    if ok:
        passed += 1
        print(f"{status} [{expected_type.value:30}] {label}")
    else:
        failed += 1
        print(f"{status} [{expected_type.value:30}] {label}")
        if not type_ok:
            print(f"     TYPE:   expected={expected_type.value}  got={result.message_type.value}")
        if not symbol_ok:
            print(f"     SYMBOL: expected={expected_symbol}  got={getattr(result, 'symbol', 'N/A')}")
        for err in field_errors:
            print(f"     FIELD:  {err}")

    # Always show full parsed values for new signals
    if result.message_type == MessageType.NEW_SIGNAL:
        print(f"        → symbol={result.symbol} dir={result.direction.value if result.direction else '?'} "
              f"entry={result.entry_low}-{result.entry_high} sl={result.stop_loss} "
              f"targets={result.targets} leverage={result.leverage_min}-{result.leverage_max}x")

print()
print("=" * 60)
print(f"  {passed} passed  /  {failed} failed  /  {len(tests)} total")
print("=" * 60)


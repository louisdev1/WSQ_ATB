import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

# ============================================================
# CONFIG
# ============================================================
JSON_PATH = r"C:\Users\louis\OneDrive\Louis\WSQ_ATB\Trade_Predictor_Setup\result.json"
OUTPUT_DIR = "output"

USE_SYMBOL_FALLBACK = True
MAX_UPDATE_MESSAGES_AHEAD = 120

KNOWN_QUOTES = ["USDT", "BTC", "BUSD", "ETH", "USDC", "USD", "FDUSD"]

# ============================================================
# DATA CLASSES
# ============================================================
@dataclass
class Signal:
    message_id: int
    date: str
    symbol: str
    pair: str
    side: str
    entry_values: List[float]
    entry_mid: float
    stop_loss: float
    targets: List[float]
    raw_text: str


@dataclass
class TradeResult:
    message_id: int
    highest_target_hit: int
    was_profitable: bool
    was_full_tp: bool
    was_partial_tp: bool
    was_loss: bool
    had_unresolved_profit_signal: bool
    linked_update_ids: List[int]


# ============================================================
# GENERAL HELPERS
# ============================================================
def ensure_output_dir(path: str) -> Path:
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def normalize_text(text_field: Any) -> str:
    if isinstance(text_field, str):
        return text_field

    if isinstance(text_field, list):
        parts: List[str] = []
        for item in text_field:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))
        return "".join(parts)

    return ""


def clean_symbol(value: str) -> str:
    return (
        str(value).upper()
        .replace("#", "")
        .replace(" ", "")
        .replace("\n", "")
        .strip()
    )


def parse_float_list(text: str) -> List[float]:
    matches = re.findall(r"\d+(?:\.\d+)?", text)
    return [float(x) for x in matches]


def deduplicate_preserve_order(values: List[float]) -> List[float]:
    seen = set()
    result = []
    for value in values:
        if value not in seen:
            result.append(value)
            seen.add(value)
    return result


# ============================================================
# SIGNAL PARSING
# ============================================================
PAIR_RE = re.compile(
    r"#?([A-Z0-9]{2,})\s*/\s*(USDT|BTC|BUSD|ETH|USDC|USD|FDUSD)\b",
    re.IGNORECASE
)

COMPACT_PAIR_RE = re.compile(
    r"#?([A-Z0-9]{2,}?)(USDT|BTC|BUSD|ETH|USDC|USD|FDUSD)\b",
    re.IGNORECASE
)

ENTRY_LINE_RE = re.compile(
    r"(entry|buy|buy zone|entry zone|short entry|cmp)\s*:\s*([^\n]+)",
    re.IGNORECASE
)

STOP_LINE_RE = re.compile(
    r"(stoploss|stop loss|stop-loss|sl)\s*:\s*([^\n]+)",
    re.IGNORECASE
)

TARGET_BLOCK_RE = re.compile(
    r"(targets?|take profit|take-profit|tp)\s*:\s*(.*?)(?=(stoploss|stop loss|stop-loss|sl|entry|buy|buy zone|entry zone|cmp|$))",
    re.IGNORECASE | re.DOTALL
)

DIRECTION_RE = re.compile(
    r"direction\s*:\s*(long|short)",
    re.IGNORECASE
)


def detect_side(text: str) -> str:
    match = DIRECTION_RE.search(text)
    if match:
        return match.group(1).lower()

    lower = text.lower()

    if "long set-up" in lower or "long set up" in lower:
        return "long"

    if "short set-up" in lower or "short set up" in lower:
        return "short"

    return "long"


def extract_symbol_and_pair(text: str) -> Optional[Tuple[str, str]]:
    match = PAIR_RE.search(text)
    if match:
        symbol = clean_symbol(match.group(1))
        pair = clean_symbol(match.group(2))
        return symbol, pair

    for match in COMPACT_PAIR_RE.finditer(text):
        symbol = clean_symbol(match.group(1))
        pair = clean_symbol(match.group(2))

        if len(symbol) >= 2 and pair in KNOWN_QUOTES:
            return symbol, pair

    return None


def extract_entry_values(text: str) -> Optional[List[float]]:
    match = ENTRY_LINE_RE.search(text)
    if not match:
        return None

    values = parse_float_list(match.group(2))
    if not values:
        return None

    return values


def extract_stop_loss(text: str) -> Optional[float]:
    match = STOP_LINE_RE.search(text)
    if not match:
        return None

    values = parse_float_list(match.group(2))
    if not values:
        return None

    return values[0]


def extract_targets(text: str) -> List[float]:
    targets: List[float] = []

    for match in TARGET_BLOCK_RE.finditer(text):
        block = match.group(2)
        block_numbers = parse_float_list(block)
        targets.extend(block_numbers)

    tp_line_matches = re.findall(
        r"(?:tp\s*\d+|take profit\s*\d*)\s*:\s*([^\n]+)",
        text,
        flags=re.IGNORECASE
    )
    for block in tp_line_matches:
        targets.extend(parse_float_list(block))

    return deduplicate_preserve_order(targets)


def looks_like_signal(text: str) -> bool:
    lower = text.lower()

    entry_words = [
        "entry:", "buy zone", "entry zone", "buy:", "short entry", "cmp:"
    ]
    target_words = [
        "targets:", "target:", "take profit", "tp1", "tp 1", "tp:"
    ]
    stop_words = [
        "stop loss:", "stoploss:", "stop-loss:", "sl:"
    ]

    has_entry = any(word in lower for word in entry_words)
    has_target = any(word in lower for word in target_words)
    has_stop = any(word in lower for word in stop_words)
    has_pair = extract_symbol_and_pair(text) is not None

    return has_entry and has_target and has_stop and has_pair


def parse_signal_message(msg: Dict[str, Any]) -> Optional[Signal]:
    if msg.get("type") != "message":
        return None

    text = normalize_text(msg.get("text", ""))
    if not text or not looks_like_signal(text):
        return None

    raw_date = msg.get("date", "")
    clean_date = pd.to_datetime(raw_date, errors="coerce")
    if pd.isna(clean_date):
        return None
    clean_date = clean_date.strftime("%Y-%m-%dT%H:%M:%S")

    symbol_pair = extract_symbol_and_pair(text)
    entry_values = extract_entry_values(text)
    stop_loss = extract_stop_loss(text)
    targets = extract_targets(text)

    if symbol_pair is None or entry_values is None or stop_loss is None or not targets:
        return None

    symbol, pair = symbol_pair
    entry_mid = sum(entry_values) / len(entry_values)

    return Signal(
        message_id=int(msg["id"]),
        date=clean_date,
        symbol=symbol,
        pair=pair,
        side=detect_side(text),
        entry_values=entry_values,
        entry_mid=entry_mid,
        stop_loss=float(stop_loss),
        targets=targets,
        raw_text=text,
    )


# ============================================================
# UPDATE / OUTCOME PARSING
# ============================================================
GENERIC_RISK_WARNING_RE = re.compile(
    r"market is very volatile now|use low leverage|low amount of your capital|risk management|book profits partially",
    re.IGNORECASE,
)

ANALYSIS_POST_RE = re.compile(
    r"\banalysis\b|bullish scenario|bearish scenario|breakout targets|wait for confirmation of a breakout|consolidating within",
    re.IGNORECASE,
)

ALL_TARGETS_PATTERNS = [
    re.compile(r"\ball targets done\b", re.IGNORECASE),
    re.compile(r"\ball targets done nicely\b", re.IGNORECASE),
    re.compile(r"\ball short-mid-long terms targets done\b", re.IGNORECASE),
    re.compile(r"\ball short mid long terms targets done\b", re.IGNORECASE),
]

STOP_LOSS_ONLY_PATTERNS = [
    re.compile(r"\bsl hit\b", re.IGNORECASE),
    re.compile(r"\bstop loss hit\b", re.IGNORECASE),
    re.compile(r"\bstoploss hit\b", re.IGNORECASE),
    re.compile(r"\bstop-loss hit\b", re.IGNORECASE),
]

STOP_LOSS_AFTER_TARGET_RE = re.compile(
    r"(?:stop\s*loss|stoploss|stop-loss|sl)\s*hit\s*after\s*target\s*(\d+)\s*hit",
    re.IGNORECASE,
)

SINGLE_TARGET_RE = re.compile(
    r"target\s*(\d+)\s*(?:also\s*)?(?:done|hit|complete|completed)",
    re.IGNORECASE
)

MULTI_TARGET_RE = re.compile(
    r"target\s*([\d,\s]+)\s*(?:also\s*)?(?:done|hit|complete|completed)",
    re.IGNORECASE
)

TP_SINGLE_RE = re.compile(
    r"\btp\s*(\d+)\s*(?:also\s*)?(?:done|hit|complete|completed)\b|\btp(\d+)\s*(?:also\s*)?(?:done|hit|complete|completed)\b",
    re.IGNORECASE,
)

PROFIT_BOOKED_RE = re.compile(
    r"\bprofit booked\b|\bprofits booked\b|\bbooked profit\b|\bbooked profits\b|\benjoy the profits\b",
    re.IGNORECASE,
)

PROFIT_PERCENT_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*%\s*profits?",
    re.IGNORECASE,
)

UPDATE_WORD_RE = re.compile(
    r"update|target|all targets|sl hit|stop loss hit|stoploss hit|stop-loss hit|profits|profit booked|heading towards|tp\d|tp\s*\d",
    re.IGNORECASE,
)


def is_generic_risk_warning(text: str) -> bool:
    return bool(GENERIC_RISK_WARNING_RE.search(text))


def is_analysis_post(text: str) -> bool:
    return bool(ANALYSIS_POST_RE.search(text))


def is_noise_update(text: str) -> bool:
    return is_generic_risk_warning(text) or is_analysis_post(text)


def is_stop_loss_only_message(text: str) -> bool:
    if STOP_LOSS_AFTER_TARGET_RE.search(text):
        return False
    return any(pattern.search(text) for pattern in STOP_LOSS_ONLY_PATTERNS)


def is_all_targets_message(text: str) -> bool:
    return any(pattern.search(text) for pattern in ALL_TARGETS_PATTERNS)


def extract_hit_targets_from_update(text: str, max_targets: int) -> List[int]:
    hits: List[int] = []

    for match in STOP_LOSS_AFTER_TARGET_RE.findall(text):
        value = int(match)
        if 1 <= value <= max_targets:
            hits.append(value)

    for raw_group in MULTI_TARGET_RE.findall(text):
        numbers = re.findall(r"\d+", raw_group)
        for n in numbers:
            value = int(n)
            if 1 <= value <= max_targets:
                hits.append(value)

    for n in SINGLE_TARGET_RE.findall(text):
        value = int(n)
        if 1 <= value <= max_targets:
            hits.append(value)

    for m1, m2 in TP_SINGLE_RE.findall(text):
        raw = m1 or m2
        if raw:
            value = int(raw)
            if 1 <= value <= max_targets:
                hits.append(value)

    return sorted(set(hits))


def is_profit_only_update(text: str) -> bool:
    if is_noise_update(text):
        return False

    has_profit_language = bool(PROFIT_BOOKED_RE.search(text) or PROFIT_PERCENT_RE.search(text))
    has_explicit_target = bool(
        STOP_LOSS_AFTER_TARGET_RE.search(text)
        or SINGLE_TARGET_RE.search(text)
        or MULTI_TARGET_RE.search(text)
        or TP_SINGLE_RE.search(text)
        or is_all_targets_message(text)
    )

    return has_profit_language and not has_explicit_target


def find_signal_index(messages: List[Dict[str, Any]], signal_message_id: int) -> int:
    for idx, msg in enumerate(messages):
        if int(msg.get("id", -1)) == signal_message_id:
            return idx
    return -1


def is_reasonable_fallback_update(signal: Signal, text: str) -> bool:
    upper = text.upper()

    symbol_ok = signal.symbol in upper
    slash_pair_ok = f"{signal.symbol}/{signal.pair}" in upper
    compact_pair_ok = f"{signal.symbol}{signal.pair}" in upper
    hashtag_compact_ok = f"#{signal.symbol}{signal.pair}" in upper
    hashtag_slash_ok = f"#{signal.symbol}/{signal.pair}" in upper

    update_ok = bool(UPDATE_WORD_RE.search(text))

    return (
        symbol_ok or
        slash_pair_ok or
        compact_pair_ok or
        hashtag_compact_ok or
        hashtag_slash_ok
    ) and update_ok


def collect_updates_for_signal(signal: Signal, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signal_index = find_signal_index(messages, signal.message_id)
    if signal_index == -1:
        return []

    updates: List[Dict[str, Any]] = []
    allowed_reply_ids: Set[int] = {signal.message_id}
    used_ids: Set[int] = set()

    search_slice = messages[signal_index + 1: signal_index + 1 + MAX_UPDATE_MESSAGES_AHEAD]

    changed = True
    while changed:
        changed = False
        for msg in search_slice:
            if msg.get("type") != "message":
                continue

            msg_id = int(msg.get("id", 0))
            if msg_id in used_ids:
                continue

            reply_to = msg.get("reply_to_message_id")
            if reply_to in allowed_reply_ids:
                updates.append(msg)
                used_ids.add(msg_id)
                allowed_reply_ids.add(msg_id)
                changed = True

    if USE_SYMBOL_FALLBACK:
        for msg in search_slice:
            if msg.get("type") != "message":
                continue

            msg_id = int(msg.get("id", 0))
            if msg_id in used_ids:
                continue

            text = normalize_text(msg.get("text", ""))
            if is_reasonable_fallback_update(signal, text):
                updates.append(msg)
                used_ids.add(msg_id)

    updates.sort(key=lambda x: int(x.get("id", 0)))
    return updates


def compute_trade_result(signal: Signal, updates: List[Dict[str, Any]]) -> TradeResult:
    highest_target_hit = 0
    linked_update_ids: List[int] = []
    had_unresolved_profit_signal = False
    saw_stop_loss_only = False

    if not updates:
        return TradeResult(
            message_id=signal.message_id,
            highest_target_hit=0,
            was_profitable=False,
            was_full_tp=False,
            was_partial_tp=False,
            was_loss=True,
            had_unresolved_profit_signal=False,
            linked_update_ids=[],
        )

    for update in updates:
        linked_update_ids.append(int(update["id"]))
        text = normalize_text(update.get("text", ""))

        if is_noise_update(text):
            continue

        if is_all_targets_message(text):
            highest_target_hit = len(signal.targets)
            continue

        hits = extract_hit_targets_from_update(text, max_targets=len(signal.targets))
        if hits:
            highest_target_hit = max(highest_target_hit, max(hits))
            continue

        if is_profit_only_update(text):
            had_unresolved_profit_signal = True
            continue

        if is_stop_loss_only_message(text):
            saw_stop_loss_only = True
            continue

    was_profitable = highest_target_hit >= 1
    was_full_tp = highest_target_hit == len(signal.targets) and len(signal.targets) > 0
    was_partial_tp = (highest_target_hit >= 1) and (highest_target_hit < len(signal.targets))

    # Conservative rule:
    # if there is only a profit message but no explicit TP, keep as loss=False? No.
    # We keep it as unresolved so TP analysis stays honest.
    if highest_target_hit == 0:
        was_loss = True
    else:
        was_loss = False

    # stop loss after TP is already handled because TP is extracted before SL-only
    _ = saw_stop_loss_only  # kept for clarity / future use

    return TradeResult(
        message_id=signal.message_id,
        highest_target_hit=highest_target_hit,
        was_profitable=was_profitable,
        was_full_tp=was_full_tp,
        was_partial_tp=was_partial_tp,
        was_loss=was_loss,
        had_unresolved_profit_signal=had_unresolved_profit_signal,
        linked_update_ids=linked_update_ids,
    )


# ============================================================
# FEATURE ENGINEERING
# ============================================================
def compute_r_multiple_features(signal: Signal) -> Dict[str, Any]:
    entry = signal.entry_mid

    if signal.side == "long":
        stop_distance = abs(entry - signal.stop_loss)
        target_rs = [
            abs(target - entry) / stop_distance if stop_distance > 0 else None
            for target in signal.targets
        ]
    else:
        stop_distance = abs(signal.stop_loss - entry)
        target_rs = [
            abs(entry - target) / stop_distance if stop_distance > 0 else None
            for target in signal.targets
        ]

    entry_min = min(signal.entry_values)
    entry_max = max(signal.entry_values)
    entry_range_pct = ((entry_max - entry_min) / entry) if entry else None
    stop_loss_pct = (stop_distance / entry) if entry else None

    row: Dict[str, Any] = {
        "entry_range_pct": entry_range_pct,
        "stop_loss_pct": stop_loss_pct,
        "number_of_targets": len(signal.targets),
    }

    for i, r_value in enumerate(target_rs[:14], start=1):
        row[f"tp{i}_R"] = r_value

    return row


# ============================================================
# DATAFRAME BUILDERS
# ============================================================
def load_messages(json_path: str) -> List[Dict[str, Any]]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("messages", [])


def build_signal_dataframe(messages: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    candidate_signals = 0
    parsed_signals = 0

    for msg in messages:
        text = normalize_text(msg.get("text", ""))

        if looks_like_signal(text):
            candidate_signals += 1

        signal = parse_signal_message(msg)
        if signal is None:
            continue

        parsed_signals += 1

        row = {
            "message_id": signal.message_id,
            "date": signal.date,
            "symbol": signal.symbol,
            "pair": signal.pair,
            "side": signal.side,
            "entry_values": signal.entry_values,
            "entry_mid": signal.entry_mid,
            "stop_loss": signal.stop_loss,
            "targets": signal.targets,
            "raw_text": signal.raw_text,
        }
        row.update(compute_r_multiple_features(signal))
        rows.append(row)

    print(f"Candidate signals found: {candidate_signals}")
    print(f"Parsed signals: {parsed_signals}")
    print(f"Missed signals: {candidate_signals - parsed_signals}")

    return pd.DataFrame(rows, columns=[
        "message_id",
        "date",
        "symbol",
        "pair",
        "side",
        "entry_values",
        "entry_mid",
        "stop_loss",
        "targets",
        "raw_text",
        "entry_range_pct",
        "stop_loss_pct",
        "number_of_targets",
        "tp1_R",
        "tp2_R",
        "tp3_R",
        "tp4_R",
        "tp5_R",
        "tp6_R",
        "tp7_R",
        "tp8_R",
        "tp9_R",
        "tp10_R",
        "tp11_R",
        "tp12_R",
        "tp13_R",
        "tp14_R",
    ])


def build_results_dataframe(messages: List[Dict[str, Any]], signal_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for _, row in signal_df.iterrows():
        signal = Signal(
            message_id=int(row["message_id"]),
            date=str(row["date"]),
            symbol=str(row["symbol"]),
            pair=str(row["pair"]),
            side=str(row["side"]),
            entry_values=list(row["entry_values"]),
            entry_mid=float(row["entry_mid"]),
            stop_loss=float(row["stop_loss"]),
            targets=list(row["targets"]),
            raw_text=str(row["raw_text"]),
        )

        updates = collect_updates_for_signal(signal, messages)
        result = compute_trade_result(signal, updates)

        rows.append({
            "message_id": result.message_id,
            "highest_target_hit": result.highest_target_hit,
            "was_profitable": result.was_profitable,
            "was_full_tp": result.was_full_tp,
            "was_partial_tp": result.was_partial_tp,
            "was_loss": result.was_loss,
            "had_unresolved_profit_signal": result.had_unresolved_profit_signal,
            "linked_update_ids": result.linked_update_ids,
        })

    return pd.DataFrame(rows, columns=[
        "message_id",
        "highest_target_hit",
        "was_profitable",
        "was_full_tp",
        "was_partial_tp",
        "was_loss",
        "had_unresolved_profit_signal",
        "linked_update_ids",
    ])


def build_summary_dataframe(trades_df: pd.DataFrame) -> pd.DataFrame:
    total = len(trades_df)

    profitable_trades = int(trades_df["was_profitable"].sum()) if total else 0
    loss_trades = int(trades_df["was_loss"].sum()) if total else 0
    full_target_trades = int(trades_df["was_full_tp"].sum()) if total else 0
    partial_target_trades = int(trades_df["was_partial_tp"].sum()) if total else 0
    unresolved_profit_signals = int(trades_df["had_unresolved_profit_signal"].sum()) if total else 0

    return pd.DataFrame([{
        "total_trades": total,
        "profitable_trades": profitable_trades,
        "loss_trades": loss_trades,
        "profitable_percentage": (profitable_trades / total * 100) if total else 0.0,
        "loss_percentage": (loss_trades / total * 100) if total else 0.0,
        "full_target_trades": full_target_trades,
        "partial_target_trades": partial_target_trades,
        "percentage_full_targets": (full_target_trades / total * 100) if total else 0.0,
        "percentage_partial_targets": (partial_target_trades / total * 100) if total else 0.0,
        "unresolved_profit_signals": unresolved_profit_signals,
        "percentage_unresolved_profit_signals": (unresolved_profit_signals / total * 100) if total else 0.0,
        "average_targets_per_trade": float(trades_df["number_of_targets"].mean()) if total else 0.0,
        "average_targets_hit": float(trades_df["highest_target_hit"].mean()) if total else 0.0,
        "median_targets_hit": float(trades_df["highest_target_hit"].median()) if total else 0.0,
    }])


def print_human_summary(summary_df: pd.DataFrame) -> None:
    if summary_df.empty:
        print("No summary available.")
        return

    row = summary_df.iloc[0].to_dict()
    print("Pipeline finished.")
    print()
    print("SUMMARY")
    print("-" * 40)
    for key, value in row.items():
        print(f"{key}: {value}")


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    output_dir = ensure_output_dir(OUTPUT_DIR)
    messages = load_messages(JSON_PATH)

    signal_df = build_signal_dataframe(messages)
    results_df = build_results_dataframe(messages, signal_df)

    print("Parsed signals:", len(signal_df))
    print("Parsed results:", len(results_df))
    print("Signal columns:", list(signal_df.columns))
    print("Result columns:", list(results_df.columns))

    trades_df = signal_df.merge(results_df, on="message_id", how="left")
    summary_df = build_summary_dataframe(trades_df)

    signal_df.to_csv(output_dir / "signals_clean.csv", index=False)
    results_df.to_csv(output_dir / "trade_results.csv", index=False)
    trades_df.to_csv(output_dir / "trades_dataset.csv", index=False)
    summary_df.to_csv(output_dir / "summary.csv", index=False)

    review_df = trades_df[trades_df["had_unresolved_profit_signal"] == True].copy()
    review_df.to_csv(output_dir / "manual_review_profit_signals.csv", index=False)

    print_human_summary(summary_df)
    print()
    print(f"Manual review rows saved: {len(review_df)}")


if __name__ == "__main__":
    main()
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# ============================================================
# CONFIG
# ============================================================
JSON_PATH = r"C:\Users\louis\OneDrive\Louis\WSQ_ATB\Trade_Predictor_Setup\result.json"
TRADES_PATH = r"output/trades_dataset.csv"
OUTPUT_DIR = Path("output")

MAX_MESSAGES_AHEAD = 120

# ============================================================
# HELPERS
# ============================================================
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


def load_messages(json_path: str) -> List[Dict[str, Any]]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("messages", [])


def find_message_index(messages: List[Dict[str, Any]], message_id: int) -> int:
    for idx, msg in enumerate(messages):
        if int(msg.get("id", -1)) == int(message_id):
            return idx
    return -1


def clean_symbol(value: str) -> str:
    return (
        str(value)
        .upper()
        .replace("#", "")
        .replace(" ", "")
        .replace("\n", "")
        .strip()
    )


def is_related_message(
    signal_message_id: int,
    signal_symbol: str,
    signal_pair: str,
    msg: Dict[str, Any],
    text: str,
) -> bool:
    upper = text.upper()
    compact_pair = f"{signal_symbol}{signal_pair}"
    slash_pair = f"{signal_symbol}/{signal_pair}"

    reply_to = msg.get("reply_to_message_id")
    reply_match = reply_to == signal_message_id

    symbol_match = signal_symbol in upper
    compact_pair_match = compact_pair in upper or f"#{compact_pair}" in upper
    slash_pair_match = slash_pair in upper or f"#{slash_pair}" in upper

    return reply_match or symbol_match or compact_pair_match or slash_pair_match


# ============================================================
# PATTERNS
# ============================================================
GENERIC_RISK_WARNING_RE = re.compile(
    r"market is very volatile now|use low leverage|low amount of your capital|risk management|book profits partially",
    re.IGNORECASE,
)

ANALYSIS_POST_RE = re.compile(
    r"\banalysis\b|bullish scenario|bearish scenario|breakout targets|wait for confirmation of a breakout|consolidating within",
    re.IGNORECASE,
)

ALL_TARGETS_DONE_RE = re.compile(
    r"\ball targets done\b|\ball targets done nicely\b|\ball short-mid-long terms targets done\b|\ball short mid long terms targets done\b",
    re.IGNORECASE,
)

STOP_LOSS_AFTER_TARGET_RE = re.compile(
    r"(?:stop\s*loss|stoploss|stop-loss|sl)\s*hit\s*after\s*target\s*(\d+)\s*hit",
    re.IGNORECASE,
)

EXPLICIT_MULTI_TARGET_RE = re.compile(
    r"target\s*([\d,\s]+)\s*(?:also\s*)?(?:done|hit|complete|completed)",
    re.IGNORECASE,
)

EXPLICIT_SINGLE_TARGET_RE = re.compile(
    r"target\s*(\d+)\s*(?:also\s*)?(?:done|hit|complete|completed)",
    re.IGNORECASE,
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
    r"(\d+(?:\.\d+)?)\s*%\s*profits?|\bprofits?\s*with\s*\d+x\b|\bwith\s*\d+x\s*leverage\b",
    re.IGNORECASE,
)

STOP_LOSS_ONLY_RE = re.compile(
    r"\bsl hit\b|\bstop loss hit\b|\bstoploss hit\b|\bstop-loss hit\b",
    re.IGNORECASE,
)

ENTRY_OR_NEW_SIGNAL_RE = re.compile(
    r"\bentry\s*:|\btargets?\s*:|\bstop\s*loss\s*:|\bstoploss\s*:|\bleverage\s*:",
    re.IGNORECASE,
)


# ============================================================
# CLASSIFICATION
# ============================================================
def extract_target_hits(text: str) -> List[int]:
    hits: List[int] = []

    for match in STOP_LOSS_AFTER_TARGET_RE.findall(text):
        try:
            hits.append(int(match))
        except ValueError:
            pass

    for raw_group in EXPLICIT_MULTI_TARGET_RE.findall(text):
        nums = re.findall(r"\d+", raw_group)
        for n in nums:
            try:
                hits.append(int(n))
            except ValueError:
                pass

    for match in EXPLICIT_SINGLE_TARGET_RE.findall(text):
        try:
            hits.append(int(match))
        except ValueError:
            pass

    for m1, m2 in TP_SINGLE_RE.findall(text):
        raw = m1 or m2
        if raw:
            try:
                hits.append(int(raw))
            except ValueError:
                pass

    return sorted(set(hits))


def classify_update(text: str) -> Dict[str, Any]:
    text = text.strip()

    categories: List[str] = []
    target_hits = extract_target_hits(text)
    profit_percent_values = re.findall(r"(\d+(?:\.\d+)?)\s*%\s*profits?", text, flags=re.IGNORECASE)

    if GENERIC_RISK_WARNING_RE.search(text):
        categories.append("generic_risk_warning")

    if ANALYSIS_POST_RE.search(text):
        categories.append("analysis_post")

    if ALL_TARGETS_DONE_RE.search(text):
        categories.append("all_targets_done")

    if STOP_LOSS_AFTER_TARGET_RE.search(text):
        categories.append("stop_loss_after_target_hit")

    if target_hits:
        categories.append("explicit_target_hit")

    if PROFIT_BOOKED_RE.search(text):
        categories.append("profit_booked")

    if PROFIT_PERCENT_RE.search(text):
        categories.append("profit_percentage_only")

    if STOP_LOSS_ONLY_RE.search(text) and "stop_loss_after_target_hit" not in categories:
        categories.append("stop_loss_only")

    if ENTRY_OR_NEW_SIGNAL_RE.search(text):
        categories.append("possible_new_signal_or_setup")

    if not categories:
        categories.append("unclassified_related")

    severity = "low"

    if "all_targets_done" in categories:
        severity = "high"
    elif "stop_loss_after_target_hit" in categories:
        severity = "high"
    elif "explicit_target_hit" in categories:
        severity = "high"
    elif "profit_booked" in categories:
        severity = "medium"
    elif "profit_percentage_only" in categories:
        severity = "medium"
    elif "stop_loss_only" in categories:
        severity = "medium"
    elif "analysis_post" in categories or "generic_risk_warning" in categories:
        severity = "low"

    likely_real_outcome = any(
        c in categories
        for c in [
            "all_targets_done",
            "stop_loss_after_target_hit",
            "explicit_target_hit",
            "profit_booked",
            "profit_percentage_only",
            "stop_loss_only",
        ]
    )

    likely_false_positive = any(
        c in categories
        for c in [
            "generic_risk_warning",
            "analysis_post",
            "possible_new_signal_or_setup",
        ]
    ) and not likely_real_outcome

    return {
        "categories": categories,
        "target_hits_found": target_hits,
        "profit_percent_values": profit_percent_values,
        "severity": severity,
        "likely_real_outcome": likely_real_outcome,
        "likely_false_positive": likely_false_positive,
    }


# ============================================================
# MAIN AUDIT
# ============================================================
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    messages = load_messages(JSON_PATH)
    trades_df = pd.read_csv(TRADES_PATH)

    required_cols = {"message_id", "symbol", "pair", "was_loss", "raw_text"}
    missing_cols = required_cols - set(trades_df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in trades dataset: {sorted(missing_cols)}")

    losses_df = trades_df[trades_df["was_loss"] == True].copy()

    print(f"Total losses: {len(losses_df)}")
    print()

    detailed_rows: List[Dict[str, Any]] = []
    trade_summary_rows: List[Dict[str, Any]] = []

    for _, trade in losses_df.iterrows():
        signal_message_id = int(trade["message_id"])
        symbol = clean_symbol(trade["symbol"])
        pair = clean_symbol(trade["pair"])
        signal_text = str(trade["raw_text"])

        signal_index = find_message_index(messages, signal_message_id)
        if signal_index == -1:
            trade_summary_rows.append({
                "message_id": signal_message_id,
                "symbol": symbol,
                "pair": pair,
                "signal_found_in_json": False,
                "related_updates_found": 0,
                "real_outcome_like_updates": 0,
                "false_positive_like_updates": 0,
                "highest_target_found_in_audit": 0,
                "contains_all_targets_done": False,
                "contains_stop_loss_after_target": False,
                "contains_profit_booked": False,
                "contains_profit_percentage_only": False,
                "contains_stop_loss_only": False,
                "contains_analysis_post": False,
                "contains_generic_warning": False,
            })
            continue

        future_slice = messages[signal_index + 1: signal_index + 1 + MAX_MESSAGES_AHEAD]

        related_count = 0
        real_outcome_like_updates = 0
        false_positive_like_updates = 0
        highest_target_found_in_audit = 0

        contains_all_targets_done = False
        contains_stop_loss_after_target = False
        contains_profit_booked = False
        contains_profit_percentage_only = False
        contains_stop_loss_only = False
        contains_analysis_post = False
        contains_generic_warning = False

        for msg in future_slice:
            if msg.get("type") != "message":
                continue

            update_id = int(msg.get("id", 0))
            text = normalize_text(msg.get("text", ""))

            if not is_related_message(signal_message_id, symbol, pair, msg, text):
                continue

            related_count += 1

            classified = classify_update(text)

            if classified["likely_real_outcome"]:
                real_outcome_like_updates += 1

            if classified["likely_false_positive"]:
                false_positive_like_updates += 1

            if classified["target_hits_found"]:
                highest_target_found_in_audit = max(
                    highest_target_found_in_audit,
                    max(classified["target_hits_found"])
                )

            if "all_targets_done" in classified["categories"]:
                contains_all_targets_done = True
            if "stop_loss_after_target_hit" in classified["categories"]:
                contains_stop_loss_after_target = True
            if "profit_booked" in classified["categories"]:
                contains_profit_booked = True
            if "profit_percentage_only" in classified["categories"]:
                contains_profit_percentage_only = True
            if "stop_loss_only" in classified["categories"]:
                contains_stop_loss_only = True
            if "analysis_post" in classified["categories"]:
                contains_analysis_post = True
            if "generic_risk_warning" in classified["categories"]:
                contains_generic_warning = True

            detailed_rows.append({
                "signal_message_id": signal_message_id,
                "symbol": symbol,
                "pair": pair,
                "update_id": update_id,
                "reply_to_message_id": msg.get("reply_to_message_id"),
                "categories": " | ".join(classified["categories"]),
                "severity": classified["severity"],
                "likely_real_outcome": classified["likely_real_outcome"],
                "likely_false_positive": classified["likely_false_positive"],
                "target_hits_found": classified["target_hits_found"],
                "profit_percent_values": classified["profit_percent_values"],
                "update_text": text[:1000],
                "signal_text": signal_text[:800],
            })

        trade_summary_rows.append({
            "message_id": signal_message_id,
            "symbol": symbol,
            "pair": pair,
            "signal_found_in_json": True,
            "related_updates_found": related_count,
            "real_outcome_like_updates": real_outcome_like_updates,
            "false_positive_like_updates": false_positive_like_updates,
            "highest_target_found_in_audit": highest_target_found_in_audit,
            "contains_all_targets_done": contains_all_targets_done,
            "contains_stop_loss_after_target": contains_stop_loss_after_target,
            "contains_profit_booked": contains_profit_booked,
            "contains_profit_percentage_only": contains_profit_percentage_only,
            "contains_stop_loss_only": contains_stop_loss_only,
            "contains_analysis_post": contains_analysis_post,
            "contains_generic_warning": contains_generic_warning,
        })

    detailed_df = pd.DataFrame(detailed_rows)
    trade_summary_df = pd.DataFrame(trade_summary_rows)

    detailed_path = OUTPUT_DIR / "loss_structure_audit_detailed.csv"
    summary_path = OUTPUT_DIR / "loss_structure_audit_summary.csv"
    counts_path = OUTPUT_DIR / "loss_structure_category_counts.csv"

    detailed_df.to_csv(detailed_path, index=False)
    trade_summary_df.to_csv(summary_path, index=False)

    category_counts: Dict[str, int] = {}
    if not detailed_df.empty:
        for value in detailed_df["categories"].fillna(""):
            parts = [x.strip() for x in str(value).split("|") if x.strip()]
            for part in parts:
                category_counts[part] = category_counts.get(part, 0) + 1

    counts_df = pd.DataFrame(
        [{"category": k, "count": v} for k, v in sorted(category_counts.items(), key=lambda x: (-x[1], x[0]))]
    )
    counts_df.to_csv(counts_path, index=False)

    print(f"Saved detailed audit to: {detailed_path}")
    print(f"Saved trade summary to: {summary_path}")
    print(f"Saved category counts to: {counts_path}")
    print()

    if not trade_summary_df.empty:
        real_outcome_trades = int((trade_summary_df["real_outcome_like_updates"] > 0).sum())
        false_positive_only_trades = int(
            (
                (trade_summary_df["real_outcome_like_updates"] == 0)
                & (trade_summary_df["false_positive_like_updates"] > 0)
            ).sum()
        )
        clean_losses = int(
            (
                (trade_summary_df["real_outcome_like_updates"] == 0)
                & (trade_summary_df["false_positive_like_updates"] == 0)
            ).sum()
        )

        sl_after_target_count = int(trade_summary_df["contains_stop_loss_after_target"].sum())
        all_targets_done_count = int(trade_summary_df["contains_all_targets_done"].sum())
        profit_booked_count = int(trade_summary_df["contains_profit_booked"].sum())
        profit_pct_only_count = int(trade_summary_df["contains_profit_percentage_only"].sum())
        stop_loss_only_count = int(trade_summary_df["contains_stop_loss_only"].sum())
        analysis_count = int(trade_summary_df["contains_analysis_post"].sum())
        generic_warning_count = int(trade_summary_df["contains_generic_warning"].sum())

        print("SUMMARY")
        print("-" * 60)
        print(f"Loss trades analysed: {len(trade_summary_df)}")
        print(f"Losses with real outcome-like updates: {real_outcome_trades}")
        print(f"Losses with only false-positive-like updates: {false_positive_only_trades}")
        print(f"Losses with no related suspicious structure: {clean_losses}")
        print()
        print(f"Contains stop_loss_after_target_hit: {sl_after_target_count}")
        print(f"Contains all_targets_done: {all_targets_done_count}")
        print(f"Contains profit_booked: {profit_booked_count}")
        print(f"Contains profit_percentage_only: {profit_pct_only_count}")
        print(f"Contains stop_loss_only: {stop_loss_only_count}")
        print(f"Contains analysis_post: {analysis_count}")
        print(f"Contains generic_risk_warning: {generic_warning_count}")

        examples = trade_summary_df[
            (trade_summary_df["real_outcome_like_updates"] > 0)
        ].head(10)

        if not examples.empty:
            print()
            print("EXAMPLES OF IMPORTANT LOSS STRUCTURES")
            print("-" * 60)
            for _, ex in examples.iterrows():
                print(
                    f"message_id={ex['message_id']} | "
                    f"{ex['symbol']}/{ex['pair']} | "
                    f"highest_target_found_in_audit={ex['highest_target_found_in_audit']} | "
                    f"stop_loss_after_target={ex['contains_stop_loss_after_target']} | "
                    f"all_targets_done={ex['contains_all_targets_done']} | "
                    f"profit_booked={ex['contains_profit_booked']}"
                )


if __name__ == "__main__":
    main()
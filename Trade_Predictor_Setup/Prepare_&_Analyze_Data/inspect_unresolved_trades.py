import json
import pandas as pd
from pathlib import Path

JSON_PATH = r"C:\Users\louis\OneDrive\Louis\WSQ_ATB\Trade_Predictor_Setup\result.json"

DATASET_PATH = "output/trades_dataset.csv"

def normalize_text(text_field):
    if isinstance(text_field, str):
        return text_field
    if isinstance(text_field, list):
        parts = []
        for item in text_field:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(str(item.get("text", "")))
        return "".join(parts)
    return ""

def load_messages():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["messages"]

def main():

    df = pd.read_csv(DATASET_PATH)

    review_df = df[
        (df["was_loss"] == True) &
        (df["had_unresolved_profit_signal"] == False)
    ].copy()

    print("Unresolved trades:", len(review_df))
    print()

    messages = load_messages()

    message_map = {m["id"]: m for m in messages}

    for _, row in review_df.iterrows():

        print("="*80)

        signal_id = int(row["message_id"])

        print("Signal ID:", signal_id)
        print("Symbol:", row["symbol"], "/", row["pair"])
        print("Side:", row["side"])

        print("\nENTRY:", row["entry_values"])
        print("TARGETS:", row["targets"])
        print("STOP LOSS:", row["stop_loss"])

        print("\nSIGNAL TEXT:")
        print("-"*40)
        print(row["raw_text"])

        update_ids = eval(row["linked_update_ids"])

        print("\nUPDATES:")
        print("-"*40)

        for uid in update_ids:

            msg = message_map.get(uid)

            if not msg:
                continue

            text = normalize_text(msg.get("text", ""))

            print(f"\nUpdate ID: {uid}")
            print(text)

        print("\nPARSER RESULT:")
        print("-"*40)
        print("highest_target_hit:", row["highest_target_hit"])
        print("was_profitable:", row["was_profitable"])
        print("was_loss:", row["was_loss"])

        print()

if __name__ == "__main__":
    main()
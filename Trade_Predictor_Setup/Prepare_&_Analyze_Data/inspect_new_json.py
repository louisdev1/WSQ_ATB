import json
from typing import Any, List


JSON_PATH = r"C:\Users\louis\OneDrive\Louis\WSQ_ATB\Trade_Predictor_Setup\result.json"


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


def main():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = data.get("messages", [])

    keywords = [
        "entry", "buy zone", "entry zone", "buy",
        "target", "tp1", "tp 1", "take profit",
        "stoploss", "stop loss", "stop-loss", "sl",
        "usdt"
    ]

    found = 0

    for msg in messages:
        if msg.get("type") != "message":
            continue

        text = normalize_text(msg.get("text", ""))
        lower = text.lower()

        if any(keyword in lower for keyword in keywords):
            print("=" * 100)
            print("ID:", msg.get("id"))
            print("DATE:", msg.get("date"))
            print(text[:2000])
            print()
            found += 1

        if found >= 40:
            break

    print(f"Printed {found} candidate messages.")


if __name__ == "__main__":
    main()
import pandas as pd
from pathlib import Path

# ============================
# CONFIG
# ============================
INPUT_PATH = Path("output/trades_dataset.csv")
OUTPUT_PATH = Path("output/tp_probabilities.csv")

# ============================
# LOAD DATASET
# ============================
if not INPUT_PATH.exists():
    raise FileNotFoundError(f"File not found: {INPUT_PATH}")

df = pd.read_csv(INPUT_PATH)

required_columns = ["highest_target_hit", "number_of_targets"]
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise ValueError(f"Missing required columns: {missing_columns}")

if df.empty:
    raise ValueError("The dataset is empty.")

# Clean numeric columns safely
df["highest_target_hit"] = pd.to_numeric(df["highest_target_hit"], errors="coerce").fillna(0).astype(int)
df["number_of_targets"] = pd.to_numeric(df["number_of_targets"], errors="coerce").fillna(0).astype(int)

total_trades = len(df)

print("Total trades:", total_trades)
print()

# ============================
# DETERMINE MAX TARGET LEVEL
# ============================
max_tp = int(df["number_of_targets"].max())

print("Maximum TP levels detected:", max_tp)
print()

# ============================
# TP HIT PROBABILITY ANALYSIS
# ============================
results = []

for tp in range(1, max_tp + 1):
    # All trades that reached at least this TP
    hits = int((df["highest_target_hit"] >= tp).sum())

    # Overall probability across all trades
    overall_probability = hits / total_trades if total_trades > 0 else 0.0

    # Only trades where this TP level actually exists
    eligible_trades = int((df["number_of_targets"] >= tp).sum())
    eligible_probability = hits / eligible_trades if eligible_trades > 0 else 0.0

    # Conditional probability: chance to reach TPn if TP(n-1) was already hit
    if tp == 1:
        conditional_base = total_trades
        conditional_probability = overall_probability
    else:
        conditional_base = int((df["highest_target_hit"] >= (tp - 1)).sum())
        conditional_probability = hits / conditional_base if conditional_base > 0 else 0.0

    results.append({
        "TP": tp,
        "Trades_hitting_TP": hits,
        "Overall_Probability": overall_probability,
        "Eligible_Trades": eligible_trades,
        "Eligible_Probability": eligible_probability,
        "Conditional_Base": conditional_base,
        "Conditional_Probability": conditional_probability
    })

tp_df = pd.DataFrame(results)

print(tp_df)

# ============================
# SAVE
# ============================
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
tp_df.to_csv(OUTPUT_PATH, index=False)

print()
print(f"Saved to {OUTPUT_PATH}")
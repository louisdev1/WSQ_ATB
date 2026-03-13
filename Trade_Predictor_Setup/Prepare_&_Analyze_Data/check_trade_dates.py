import pandas as pd

df = pd.read_csv("output/trades_dataset.csv")

df["date"] = pd.to_datetime(df["date"], errors="coerce")

print("Rows:", len(df))
print("Min date:", df["date"].min())
print("Max date:", df["date"].max())
print()
print("Top 20 newest dates:")
print(df["date"].sort_values(ascending=False).head(20).to_string(index=False))
print()
print("Top 20 oldest dates:")
print(df["date"].sort_values(ascending=True).head(20).to_string(index=False))
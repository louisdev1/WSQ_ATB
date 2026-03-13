import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split

# ============================================================
# CONFIG
# ============================================================
TRADES_PATH = Path("output/trades_dataset.csv")
OUTPUT_DIR = Path("output")

CMC_API_KEY = "929008a363264f5e9e7fa6b514612457"

CMC_MAP_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
CMC_QUOTES_LATEST_URL = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"

REQUEST_SLEEP_SECONDS = 2.2  # safe under 30 req/min
REQUEST_TIMEOUT = 30

USE_LOCAL_CMC_MAP_CACHE = True
CMC_MAP_CACHE_PATH = OUTPUT_DIR / "cmc_symbol_map_cache.csv"

TRAIN_TEST_RANDOM_STATE = 42
TEST_SIZE = 0.25

# If True, skip model training when too few rows remain after enrichment
MIN_ROWS_FOR_MODEL = 20

# ============================================================
# SYMBOL NORMALIZATION
# ============================================================
KNOWN_QUOTES = [
    "USDT", "USDC", "BUSD", "FDUSD", "USD", "BTC", "ETH", "EUR", "TRY"
]

DERIVATIVE_PREFIX_RE = re.compile(r"^(1000+)([A-Z0-9]+)$", re.IGNORECASE)


def clean_symbol(symbol: str) -> str:
    if not symbol:
        return ""
    s = str(symbol).upper().strip()
    s = s.replace("#", "").replace(" ", "").replace("\n", "")
    return s


def strip_quote_suffix(symbol: str) -> str:
    s = clean_symbol(symbol)
    for quote in sorted(KNOWN_QUOTES, key=len, reverse=True):
        if s.endswith(quote) and len(s) > len(quote):
            return s[:-len(quote)]
    return s


def normalize_base_symbol(symbol: str) -> str:
    """
    Normalizes raw trade symbols to a base asset candidate.
    Examples:
        #GLMUSDT -> GLM
        1000PEPE -> PEPE
        AAVE     -> AAVE
    """
    s = clean_symbol(symbol)
    s = strip_quote_suffix(s)

    m = DERIVATIVE_PREFIX_RE.match(s)
    if m:
        s = m.group(2)

    return s


def generate_symbol_candidates(symbol: str) -> List[str]:
    """
    Try original first, then normalized fallback forms.
    Examples:
        1000WHY -> ['1000WHY', 'WHY']
        GLMUSDT -> ['GLMUSDT', 'GLM']
        #AAVE   -> ['AAVE']
    """
    original = clean_symbol(symbol)
    base = normalize_base_symbol(symbol)

    candidates: List[str] = []
    for item in [original, strip_quote_suffix(original), base]:
        if item and item not in candidates:
            candidates.append(item)

    return candidates


# ============================================================
# HTTP / CMC HELPERS
# ============================================================
def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def cmc_headers() -> Dict[str, str]:
    return {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": CMC_API_KEY,
    }


def safe_request_json(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(
            url,
            headers=cmc_headers(),
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        status_code = getattr(e.response, "status_code", "unknown")
        print(f"HTTP error {status_code} for {url} with params={params}: {e}")
        return None
    except requests.RequestException as e:
        print(f"Request failed for {url} with params={params}: {e}")
        return None


def load_local_cmc_map_cache() -> pd.DataFrame:
    if USE_LOCAL_CMC_MAP_CACHE and CMC_MAP_CACHE_PATH.exists():
        try:
            df = pd.read_csv(CMC_MAP_CACHE_PATH)
            required = {"id", "symbol", "name", "slug", "is_active"}
            if required.issubset(set(df.columns)):
                df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
                return df
        except Exception as e:
            print(f"Could not read local CMC map cache: {e}")

    return pd.DataFrame(columns=["id", "symbol", "name", "slug", "is_active"])


def save_local_cmc_map_cache(df: pd.DataFrame) -> None:
    if not USE_LOCAL_CMC_MAP_CACHE:
        return
    try:
        df.to_csv(CMC_MAP_CACHE_PATH, index=False)
    except Exception as e:
        print(f"Could not save local CMC map cache: {e}")


def fetch_symbol_map_from_cmc(symbol: str) -> pd.DataFrame:
    """
    Uses /map?symbol=... to retrieve exact CMC candidates for one symbol.
    Returns a DataFrame, possibly empty.
    """
    payload = safe_request_json(
        CMC_MAP_URL,
        {
            "symbol": symbol,
            "listing_status": "active,untracked,inactive",
        },
    )

    time.sleep(REQUEST_SLEEP_SECONDS)

    if not payload or "data" not in payload:
        return pd.DataFrame(columns=["id", "symbol", "name", "slug", "is_active"])

    rows = payload.get("data", [])
    if not rows:
        return pd.DataFrame(columns=["id", "symbol", "name", "slug", "is_active"])

    df = pd.DataFrame(rows)
    keep_cols = [c for c in ["id", "symbol", "name", "slug", "is_active"] if c in df.columns]
    if not keep_cols:
        return pd.DataFrame(columns=["id", "symbol", "name", "slug", "is_active"])

    df = df[keep_cols].copy()
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    if "is_active" not in df.columns:
        df["is_active"] = 1

    return df


def resolve_symbol_to_cmc_id(
    raw_symbol: str,
    global_cache_df: pd.DataFrame,
    resolved_cache: Dict[str, Optional[int]],
) -> Tuple[Optional[int], Optional[str], str]:
    """
    Returns:
        (cmc_id, resolved_symbol, resolution_note)
    """
    raw_symbol_clean = clean_symbol(raw_symbol)

    if raw_symbol_clean in resolved_cache:
        cmc_id = resolved_cache[raw_symbol_clean]
        if cmc_id is None:
            return None, None, "cached_unresolved"
        return cmc_id, None, "cached_resolved"

    candidates = generate_symbol_candidates(raw_symbol_clean)

    # First: check local/global cache exact symbol matches
    for candidate in candidates:
        matches = global_cache_df[global_cache_df["symbol"] == candidate].copy()
        if matches.empty:
            continue

        # Prefer active rows
        active = matches[matches["is_active"].fillna(0).astype(int) == 1]
        chosen = active.iloc[0] if not active.empty else matches.iloc[0]

        cmc_id = int(chosen["id"])
        resolved_cache[raw_symbol_clean] = cmc_id
        return cmc_id, candidate, f"cache_match:{candidate}"

    # Second: fetch each candidate from CMC map endpoint
    for candidate in candidates:
        fetched = fetch_symbol_map_from_cmc(candidate)
        if fetched.empty:
            continue

        # merge fetched rows into cache
        global_cache_df = pd.concat([global_cache_df, fetched], ignore_index=True).drop_duplicates(
            subset=["id"], keep="first"
        )
        save_local_cmc_map_cache(global_cache_df)

        exact = fetched[fetched["symbol"] == candidate].copy()
        if exact.empty:
            exact = fetched.copy()

        active = exact[exact["is_active"].fillna(0).astype(int) == 1]
        chosen = active.iloc[0] if not active.empty else exact.iloc[0]

        cmc_id = int(chosen["id"])
        resolved_cache[raw_symbol_clean] = cmc_id
        return cmc_id, candidate, f"api_match:{candidate}"

    resolved_cache[raw_symbol_clean] = None
    return None, None, f"unresolved:{','.join(candidates)}"


def fetch_quotes_latest(cmc_id: int) -> Dict[str, Optional[float]]:
    payload = safe_request_json(
        CMC_QUOTES_LATEST_URL,
        {
            "id": str(cmc_id),
            "convert": "USD",
        },
    )
    time.sleep(REQUEST_SLEEP_SECONDS)

    if not payload or "data" not in payload:
        return {
            "price_usd": None,
            "market_cap_usd": None,
            "volume_24h_usd": None,
        }

    data = payload["data"]
    key = str(cmc_id)

    if key not in data or not data[key]:
        return {
            "price_usd": None,
            "market_cap_usd": None,
            "volume_24h_usd": None,
        }

    asset_data = data[key]

    # Sometimes CMC may return a list, sometimes a dict-like object
    if isinstance(asset_data, list):
        if not asset_data:
            return {
                "price_usd": None,
                "market_cap_usd": None,
                "volume_24h_usd": None,
            }
        asset_data = asset_data[0]

    quote = asset_data.get("quote", {}).get("USD", {})

    return {
        "price_usd": quote.get("price"),
        "market_cap_usd": quote.get("market_cap"),
        "volume_24h_usd": quote.get("volume_24h"),
    }


# ============================================================
# FEATURE ENGINEERING
# ============================================================
def add_basic_trade_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["entry_mid"] = pd.to_numeric(out["entry_mid"], errors="coerce")
    out["stop_loss"] = pd.to_numeric(out["stop_loss"], errors="coerce")
    out["number_of_targets"] = pd.to_numeric(out["number_of_targets"], errors="coerce")
    out["highest_target_hit"] = pd.to_numeric(out["highest_target_hit"], errors="coerce").fillna(0)

    out["stop_loss_pct"] = pd.to_numeric(out.get("stop_loss_pct"), errors="coerce")
    out["entry_range_pct"] = pd.to_numeric(out.get("entry_range_pct"), errors="coerce")

    out["side_is_long"] = out["side"].astype(str).str.lower().eq("long").astype(int)
    out["side_is_short"] = out["side"].astype(str).str.lower().eq("short").astype(int)

    # Keep it consistent even if stop_loss_pct/entry_range_pct were missing
    missing_stop = out["stop_loss_pct"].isna()
    out.loc[missing_stop, "stop_loss_pct"] = (
        (out["entry_mid"] - out["stop_loss"]).abs() / out["entry_mid"]
    )

    # Simple RR estimates from tp columns if present
    tp_r_cols = [f"tp{i}_R" for i in range(1, 15) if f"tp{i}_R" in out.columns]
    for col in tp_r_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["avg_tp_R"] = out[tp_r_cols].mean(axis=1) if tp_r_cols else None
    out["max_tp_R"] = out[tp_r_cols].max(axis=1) if tp_r_cols else None
    out["min_tp_R"] = out[tp_r_cols].min(axis=1) if tp_r_cols else None

    # Ensure target variable exists and is clean
    out["was_profitable"] = (
        out["was_profitable"]
        .astype(str)
        .str.lower()
        .map({"true": 1, "false": 0, "1": 1, "0": 0})
    )

    if out["was_profitable"].isna().any():
        out["was_profitable"] = pd.to_numeric(out["was_profitable"], errors="coerce")

    return out


def enrich_with_cmc(df: pd.DataFrame) -> pd.DataFrame:
    enriched_rows: List[Dict[str, Any]] = []
    resolved_cache: Dict[str, Optional[int]] = {}
    quote_cache: Dict[int, Dict[str, Optional[float]]] = {}

    global_cache_df = load_local_cmc_map_cache()

    total = len(df)
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        raw_symbol = str(row["symbol"])
        cmc_id, resolved_symbol, resolution_note = resolve_symbol_to_cmc_id(
            raw_symbol=raw_symbol,
            global_cache_df=global_cache_df,
            resolved_cache=resolved_cache,
        )

        market_cap = None
        volume_24h = None
        price_usd = None

        if cmc_id is not None:
            if cmc_id not in quote_cache:
                quote_cache[cmc_id] = fetch_quotes_latest(cmc_id)

            quote = quote_cache[cmc_id]
            market_cap = quote.get("market_cap_usd")
            volume_24h = quote.get("volume_24h_usd")
            price_usd = quote.get("price_usd")

        enriched_row = row.to_dict()
        enriched_row["raw_symbol"] = raw_symbol
        enriched_row["normalized_symbol"] = normalize_base_symbol(raw_symbol)
        enriched_row["symbol_candidates"] = " | ".join(generate_symbol_candidates(raw_symbol))
        enriched_row["cmc_id"] = cmc_id
        enriched_row["cmc_resolved_symbol"] = resolved_symbol
        enriched_row["cmc_resolution_note"] = resolution_note
        enriched_row["market_cap_usd"] = market_cap
        enriched_row["volume_24h_usd"] = volume_24h
        enriched_row["price_usd"] = price_usd
        enriched_row["volume_market_cap_ratio"] = (
            (volume_24h / market_cap)
            if (volume_24h is not None and market_cap not in [None, 0])
            else None
        )

        enriched_rows.append(enriched_row)

        status_symbol = resolved_symbol if resolved_symbol else raw_symbol
        print(f"Processed {i}/{total}: {status_symbol}")

    return pd.DataFrame(enriched_rows)


def train_model(df: pd.DataFrame) -> Tuple[Optional[RandomForestClassifier], pd.DataFrame]:
    feature_columns = [
        "market_cap_usd",
        "volume_24h_usd",
        "price_usd",
        "volume_market_cap_ratio",
        "stop_loss_pct",
        "entry_range_pct",
        "number_of_targets",
        "avg_tp_R",
        "max_tp_R",
        "min_tp_R",
        "side_is_long",
        "side_is_short",
    ]

    required = feature_columns + ["was_profitable"]
    model_df = df[required].copy()

    # target must be known
    model_df = model_df[model_df["was_profitable"].notna()].copy()
    model_df["was_profitable"] = model_df["was_profitable"].astype(int)

    # Need enough rows and at least 2 classes
    if len(model_df) < MIN_ROWS_FOR_MODEL:
        print(f"Not enough rows for model training: {len(model_df)} rows")
        return None, model_df

    if model_df["was_profitable"].nunique() < 2:
        print("Target column has less than 2 classes. Cannot train classifier.")
        return None, model_df

    X = model_df[feature_columns]
    y = model_df["was_profitable"]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=TRAIN_TEST_RANDOM_STATE,
        stratify=y,
    )

    imputer = SimpleImputer(strategy="median")
    X_train_imp = imputer.fit_transform(X_train)
    X_test_imp = imputer.transform(X_test)

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=None,
        min_samples_split=4,
        min_samples_leaf=2,
        random_state=TRAIN_TEST_RANDOM_STATE,
        class_weight="balanced",
        n_jobs=-1,
    )

    model.fit(X_train_imp, y_train)
    preds = model.predict(X_test_imp)

    print()
    print("MODEL EVALUATION")
    print("-" * 60)
    print("Rows used:", len(model_df))
    print("Train rows:", len(X_train))
    print("Test rows:", len(X_test))
    print()
    print("Confusion matrix:")
    print(confusion_matrix(y_test, preds))
    print()
    print("Classification report:")
    print(classification_report(y_test, preds, digits=4))

    feature_importance_df = pd.DataFrame(
        {
            "feature": feature_columns,
            "importance": model.feature_importances_,
        }
    ).sort_values("importance", ascending=False)

    print()
    print("Feature importances:")
    print(feature_importance_df.to_string(index=False))

    # Save feature importances
    feature_importance_df.to_csv(OUTPUT_DIR / "rf_feature_importances.csv", index=False)

    # Save predictions on all model rows
    all_X_imp = imputer.transform(X)
    model_df = model_df.copy()
    model_df["predicted_profitable"] = model.predict(all_X_imp)
    if hasattr(model, "predict_proba"):
        model_df["predicted_profitable_probability"] = model.predict_proba(all_X_imp)[:, 1]
    else:
        model_df["predicted_profitable_probability"] = None

    model_df.to_csv(OUTPUT_DIR / "ml_model_dataset_scored.csv", index=False)
    return model, model_df


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    ensure_output_dir(OUTPUT_DIR)

    if not TRADES_PATH.exists():
        raise FileNotFoundError(f"Trades dataset not found: {TRADES_PATH}")

    df = pd.read_csv(TRADES_PATH)

    required_columns = {
        "symbol",
        "entry_mid",
        "stop_loss",
        "number_of_targets",
        "side",
        "was_profitable",
    }
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in trades dataset: {sorted(missing)}")

    df = add_basic_trade_features(df)

    print("Starting CMC enrichment...")
    print(f"Rows to enrich: {len(df)}")
    print()

    enriched_df = enrich_with_cmc(df)

    # Save raw enriched dataset
    enriched_df.to_csv(OUTPUT_DIR / "trades_dataset_enriched.csv", index=False)

    # Summary
    resolved_count = enriched_df["cmc_id"].notna().sum()
    unresolved_count = enriched_df["cmc_id"].isna().sum()

    print()
    print("ENRICHMENT SUMMARY")
    print("-" * 60)
    print(f"Total trades: {len(enriched_df)}")
    print(f"Resolved CMC IDs: {resolved_count}")
    print(f"Unresolved symbols: {unresolved_count}")

    unresolved_df = enriched_df[enriched_df["cmc_id"].isna()][
        ["raw_symbol", "normalized_symbol", "symbol_candidates", "cmc_resolution_note"]
    ].drop_duplicates()

    unresolved_df.to_csv(OUTPUT_DIR / "unresolved_symbols.csv", index=False)

    if not unresolved_df.empty:
        print()
        print("Unresolved symbols sample:")
        print(unresolved_df.head(20).to_string(index=False))

    print()
    print("Training RandomForest model...")
    train_model(enriched_df)

    print()
    print("Saved files:")
    print("-", OUTPUT_DIR / "trades_dataset_enriched.csv")
    print("-", OUTPUT_DIR / "unresolved_symbols.csv")
    print("-", OUTPUT_DIR / "rf_feature_importances.csv")
    print("-", OUTPUT_DIR / "ml_model_dataset_scored.csv")


if __name__ == "__main__":
    main()
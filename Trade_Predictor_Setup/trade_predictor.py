"""
WSQ_ATB — Trade Enrichment & ML Prediction Pipeline
=====================================================
Step 1: Enrich each trade with CoinGecko historical market data
Step 2: Build ML features from signal + market context
Step 3: Train a model to predict profitable vs unprofitable trades

Usage:
  # Step 1: Fetch CoinGecko data (free, no API key needed)
  python trade_predictor.py --enrich

  # Step 2: Train model
  python trade_predictor.py --train

  # Both in one go
  python trade_predictor.py --enrich --train

Input:  trade_analysis_report.json (from trade_analysis.py)
Output: enriched_trades.json       (trades + market data)
        model_results.json         (feature importances + metrics)
        trade_predictor_model.pkl  (trained model)
"""

import json
import os
import sys
import time
import pickle
import argparse
import logging
import math
from datetime import datetime, timedelta
from collections import defaultdict

import requests

# ── Config ──
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRADES_PATH = os.path.join(BASE_DIR, "trade_analysis_report.json")
ENRICHED_PATH = os.path.join(BASE_DIR, "enriched_trades.json")
CACHE_PATH = os.path.join(BASE_DIR, "coingecko_cache.json")
MODEL_PATH = os.path.join(BASE_DIR, "trade_predictor_model.pkl")
RESULTS_PATH = os.path.join(BASE_DIR, "model_results.json")

CG_BASE_URL = "https://api.coingecko.com/api/v3"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SYMBOL_REMAP = {
    "1000SHIB": "SHIB", "1000PEPE": "PEPE", "1000LUNC": "LUNC",
    "1000XEC": "XEC", "1000RATS": "RATS", "1000CAT": "CAT",
    "1000CHEEMS": "CHEEMS", "1000WHY": "WHY", "1000X": "X",
    "1MBABYDOGE": "BABYDOGE", "1000BONK": "BONK", "1000FLOKI": "FLOKI",
    "1000SATS": "SATS",
}


# ═══════════════════════════════════════════════════
# STEP 1: COINGECKO ENRICHMENT (free, no key needed)
# ═══════════════════════════════════════════════════

def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def cg_request(endpoint, params=None):
    url = f"{CG_BASE_URL}{endpoint}"
    time.sleep(8)  # Free tier: conservative ~7-8 calls/min
    resp = requests.get(url, params=params or {})
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 429:
        log.warning("Rate limited, waiting 90s...")
        time.sleep(90)
        return cg_request(endpoint, params)
    else:
        log.debug(f"CG HTTP {resp.status_code} for {endpoint}")
        return None

def get_coingecko_id_map():
    cache_file = os.path.join(BASE_DIR, "cg_id_map.json")
    if os.path.exists(cache_file):
        age = time.time() - os.path.getmtime(cache_file)
        if age < 86400 * 7:
            with open(cache_file, encoding="utf-8") as f:
                return json.load(f)

    log.info("Fetching CoinGecko coin list...")
    data = cg_request("/coins/list")
    if not data:
        log.error("Failed to fetch CoinGecko coin list")
        return {}

    sym_map = {}
    for coin in data:
        sym = coin["symbol"].upper()
        cg_id = coin["id"]
        if sym not in sym_map or len(cg_id) < len(sym_map[sym]):
            sym_map[sym] = cg_id

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(sym_map, f, indent=2)
    log.info(f"CoinGecko ID map: {len(sym_map)} symbols")
    return sym_map

def fetch_coin_history(cg_id, date_str, cache):
    dt = datetime.fromisoformat(date_str)
    cg_date = dt.strftime("%d-%m-%Y")
    cache_key = f"{cg_id}_{cg_date}"
    if cache_key in cache:
        return cache[cache_key]

    data = cg_request(f"/coins/{cg_id}/history", {"date": cg_date, "localization": "false"})
    if data and "market_data" in data:
        md = data["market_data"]
        result = {
            "price": md.get("current_price", {}).get("usd"),
            "market_cap": md.get("market_cap", {}).get("usd"),
            "total_volume": md.get("total_volume", {}).get("usd"),
        }
        cache[cache_key] = result
        return result

    cache[cache_key] = None
    return None

def enrich_trades():
    with open(TRADES_PATH, encoding="utf-8") as f:
        report = json.load(f)

    trades = report["trades"]
    cache = load_cache()
    id_map = get_coingecko_id_map()

    log.info(f"Trades to enrich: {len(trades)}")
    log.info(f"Cache entries: {len(cache)}")

    # Collect unique queries to minimize API calls
    unique_queries = {}
    btc_dates = set()
    for trade in trades:
        symbol = SYMBOL_REMAP.get(trade["symbol"], trade["symbol"])
        cg_id = id_map.get(symbol)
        date_str = trade.get("date", "")
        if cg_id and date_str:
            dt = datetime.fromisoformat(date_str)
            cg_date = dt.strftime("%d-%m-%Y")
            key = f"{cg_id}_{cg_date}"
            if key not in cache:
                unique_queries[key] = (cg_id, date_str)
            btc_key = f"bitcoin_{cg_date}"
            if btc_key not in cache:
                btc_dates.add(cg_date)  # dedup by date only, not datetime

    total_calls = len(unique_queries) + len(btc_dates)
    log.info(f"API calls needed: {len(unique_queries)} coins + {len(btc_dates)} BTC dates = {total_calls}")
    log.info(f"Estimated time: {total_calls * 8 / 60:.0f} minutes")

    # Fetch BTC context
    for i, cg_date in enumerate(sorted(btc_dates)):
        cache_key = f"bitcoin_{cg_date}"
        if cache_key not in cache:
            # Convert cg_date back to ISO for the function
            dt = datetime.strptime(cg_date, "%d-%m-%Y")
            fetch_coin_history("bitcoin", dt.isoformat(), cache)
        if (i + 1) % 25 == 0:
            log.info(f"BTC progress: {i+1}/{len(btc_dates)}")
            save_cache(cache)

    # Fetch coin data
    for i, (key, (cg_id, date_str)) in enumerate(unique_queries.items()):
        fetch_coin_history(cg_id, date_str, cache)
        if (i + 1) % 25 == 0:
            log.info(f"Coin progress: {i+1}/{len(unique_queries)}")
            save_cache(cache)

    save_cache(cache)

    # Attach data to trades
    enriched = 0
    failed = 0
    for trade in trades:
        symbol = SYMBOL_REMAP.get(trade["symbol"], trade["symbol"])
        cg_id = id_map.get(symbol)
        date_str = trade.get("date", "")
        if cg_id and date_str:
            dt = datetime.fromisoformat(date_str)
            cg_date = dt.strftime("%d-%m-%Y")
            trade["cg_data"] = cache.get(f"{cg_id}_{cg_date}")
            trade["btc_data"] = cache.get(f"bitcoin_{cg_date}")
            enriched += 1 if trade["cg_data"] else 0
            failed += 0 if trade["cg_data"] else 1
        else:
            trade["cg_data"] = None
            trade["btc_data"] = None
            failed += 1

    with open(ENRICHED_PATH, "w", encoding="utf-8") as f:
        json.dump({"trades": trades, "summary": report.get("summary")}, f, indent=2, default=str)

    log.info(f"Enrichment complete: {enriched} enriched, {failed} failed/skipped")
    log.info(f"Saved to: {ENRICHED_PATH}")
    return trades


# ═══════════════════════════════════════════════════
# STEP 2: FEATURE ENGINEERING
# ═══════════════════════════════════════════════════

def build_features(trades):
    rows = []
    for t in trades:
        if t["outcome"] in ("UNKNOWN",):
            continue

        is_profitable = 1 if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL") else 0

        dt = None
        if t["date"]:
            try:
                dt = datetime.fromisoformat(t["date"])
            except:
                pass

        features = {
            "profitable": is_profitable,
            "highest_target_hit": t["highest_target_hit"],
            "side_long": 1 if t["side"] == "LONG" else 0,
            "stop_loss_pct": t.get("stop_loss_pct"),
            "entry_range_pct": t.get("entry_range_pct"),
            "num_targets": t.get("num_targets", 0),
            "tp1_rr": t.get("rr_per_target", {}).get("tp1_R"),
            "tp2_rr": t.get("rr_per_target", {}).get("tp2_R"),
            "tp3_rr": t.get("rr_per_target", {}).get("tp3_R"),
            "hour": dt.hour if dt else None,
            "day_of_week": dt.weekday() if dt else None,
            "month": dt.month if dt else None,
            "year": dt.year if dt else None,
            "is_weekend": 1 if dt and dt.weekday() >= 5 else 0,
        }

        cg = t.get("cg_data")
        if cg and isinstance(cg, dict):
            mc = cg.get("market_cap")
            vol = cg.get("total_volume")
            features["market_cap"] = mc
            features["volume_24h"] = vol
            if mc:
                features["mcap_tier"] = 3 if mc >= 10e9 else 2 if mc >= 1e9 else 1 if mc >= 100e6 else 0
            else:
                features["mcap_tier"] = None
            features["vol_mcap_ratio"] = (vol / mc) if mc and vol and mc > 0 else None
            features["log_mcap"] = math.log10(mc) if mc and mc > 0 else None
            features["log_volume"] = math.log10(vol) if vol and vol > 0 else None
        else:
            features.update({"market_cap": None, "volume_24h": None, "mcap_tier": None,
                             "vol_mcap_ratio": None, "log_mcap": None, "log_volume": None})

        btc = t.get("btc_data")
        if btc and isinstance(btc, dict):
            btc_mc = btc.get("market_cap")
            features["btc_market_cap"] = btc_mc
            coin_mc = features.get("market_cap")
            features["coin_vs_btc_mcap"] = (coin_mc / btc_mc) if btc_mc and coin_mc and btc_mc > 0 else None
        else:
            features.update({"btc_market_cap": None, "coin_vs_btc_mcap": None})

        features["symbol"] = t["symbol"]
        features["date"] = t["date"]
        features["message_id"] = t["message_id"]
        rows.append(features)

    return rows


# ═══════════════════════════════════════════════════
# STEP 3: ML TRAINING
# ═══════════════════════════════════════════════════

def train_model(rows):
    try:
        import numpy as np
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
        from sklearn.model_selection import cross_val_score, StratifiedKFold
        from sklearn.metrics import classification_report, confusion_matrix
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        log.error("Install: pip install scikit-learn numpy")
        sys.exit(1)

    exclude = {"profitable", "highest_target_hit", "symbol", "date", "message_id"}
    feature_cols = [k for k in rows[0].keys() if k not in exclude
                    and isinstance(rows[0].get(k), (int, float, type(None)))]

    log.info(f"Feature columns ({len(feature_cols)}): {feature_cols}")

    X_raw = np.array([[r.get(c) for c in feature_cols] for r in rows], dtype=float)
    y = np.array([r["profitable"] for r in rows])

    log.info(f"Dataset: {X_raw.shape[0]} samples, {X_raw.shape[1]} features")
    log.info(f"Class distribution: {sum(y)} profitable ({sum(y)/len(y)*100:.1f}%), {len(y)-sum(y)} not ({(len(y)-sum(y))/len(y)*100:.1f}%)")

    imputer = SimpleImputer(strategy="median")
    X = imputer.fit_transform(X_raw)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    gb = GradientBoostingClassifier(n_estimators=200, max_depth=4, learning_rate=0.05,
                                     min_samples_leaf=10, subsample=0.8, random_state=42)
    rf = RandomForestClassifier(n_estimators=200, max_depth=6, min_samples_leaf=10, random_state=42)

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    gb_roc = cross_val_score(gb, X_scaled, y, cv=cv, scoring="roc_auc")
    gb_f1 = cross_val_score(gb, X_scaled, y, cv=cv, scoring="f1")
    gb_acc = cross_val_score(gb, X_scaled, y, cv=cv, scoring="accuracy")
    log.info(f"\nGradient Boosting: Acc={gb_acc.mean():.3f} F1={gb_f1.mean():.3f} AUC={gb_roc.mean():.3f}")

    rf_roc = cross_val_score(rf, X_scaled, y, cv=cv, scoring="roc_auc")
    rf_f1 = cross_val_score(rf, X_scaled, y, cv=cv, scoring="f1")
    rf_acc = cross_val_score(rf, X_scaled, y, cv=cv, scoring="accuracy")
    log.info(f"Random Forest:     Acc={rf_acc.mean():.3f} F1={rf_f1.mean():.3f} AUC={rf_roc.mean():.3f}")

    best_model = gb if gb_roc.mean() >= rf_roc.mean() else rf
    best_name = "GradientBoosting" if best_model is gb else "RandomForest"
    best_scores = (gb_acc, gb_f1, gb_roc) if best_model is gb else (rf_acc, rf_f1, rf_roc)
    log.info(f"Best model: {best_name}")

    best_model.fit(X_scaled, y)
    fi = sorted(zip(feature_cols, best_model.feature_importances_), key=lambda x: x[1], reverse=True)

    print("\n" + "=" * 60)
    print("  FEATURE IMPORTANCE RANKING")
    print("=" * 60)
    for feat, imp in fi:
        print(f"  {feat:<25}: {imp:.4f}  {'█' * int(imp * 200)}")

    y_pred = best_model.predict(X_scaled)
    print("\n" + "=" * 60)
    print("  CLASSIFICATION REPORT")
    print("=" * 60)
    print(classification_report(y, y_pred, target_names=["Loss/NoUpdate", "Profitable"]))
    cm = confusion_matrix(y, y_pred)
    print(f"  Confusion Matrix:  Predicted Loss | Predicted Profit")
    print(f"  Actual Loss:       {cm[0][0]:>5}          | {cm[0][1]:>5}")
    print(f"  Actual Profit:     {cm[1][0]:>5}          | {cm[1][1]:>5}")

    # ── Insights ──
    print("\n" + "=" * 60)
    print("  KEY INSIGHTS FOR BOT OPTIMIZATION")
    print("=" * 60)

    for label, key, buckets in [
        ("Market Cap Tier", "mcap_tier",
         {0: "Micro (<100M)", 1: "Small (100M-1B)", 2: "Mid (1B-10B)", 3: "Large (>10B)"}),
    ]:
        stats = defaultdict(lambda: {"t": 0, "w": 0})
        for r in rows:
            v = r.get(key)
            if v is not None:
                stats[v]["t"] += 1
                if r["profitable"]:
                    stats[v]["w"] += 1
        if stats:
            print(f"\n  {label}:")
            for k in sorted(stats):
                s = stats[k]
                print(f"    {buckets.get(k, k):<20}: {s['t']:>4} trades | {s['w']/s['t']*100:.1f}% WR")

    for label, key, edges, names in [
        ("Stop Loss Distance", "stop_loss_pct", [0, 3, 8, 15, 999],
         ["tight (<3%)", "normal (3-8%)", "wide (8-15%)", "very wide (>15%)"]),
        ("Vol/MCap Ratio", "vol_mcap_ratio", [0, 0.1, 0.5, 1.0, 999],
         ["low (<0.1)", "normal (0.1-0.5)", "high (0.5-1)", "very high (>1)"]),
    ]:
        bkts = [[] for _ in names]
        for r in rows:
            v = r.get(key)
            if v is not None:
                for i in range(len(edges) - 1):
                    if edges[i] <= v < edges[i + 1]:
                        bkts[i].append(r["profitable"])
                        break
        if any(bkts):
            print(f"\n  {label}:")
            for name, vals in zip(names, bkts):
                if vals:
                    print(f"    {name:<20}: {len(vals):>4} trades | {sum(vals)/len(vals)*100:.1f}% WR")

    # Save
    pkg = {"model": best_model, "imputer": imputer, "scaler": scaler,
           "feature_cols": feature_cols, "best_name": best_name}
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(pkg, f)

    results = {
        "best_model": best_name,
        "cv_accuracy": round(float(best_scores[0].mean()), 4),
        "cv_f1": round(float(best_scores[1].mean()), 4),
        "cv_roc_auc": round(float(best_scores[2].mean()), 4),
        "feature_importance": {f: round(float(i), 4) for f, i in fi},
        "feature_columns": feature_cols,
        "dataset_size": len(rows),
    }
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    log.info(f"Model saved to: {MODEL_PATH}")
    log.info(f"Results saved to: {RESULTS_PATH}")
    return results


# ═══════════════════════════════════════════════════
# STEP 4: PREDICTION (for live bot)
# ═══════════════════════════════════════════════════

def predict_trade(trade_features: dict) -> dict:
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"No model at {MODEL_PATH}. Run --train first.")
    with open(MODEL_PATH, "rb") as f:
        pkg = pickle.load(f)
    import numpy as np
    fc = pkg["feature_cols"]
    X = np.array([[trade_features.get(c) for c in fc]], dtype=float)
    X = pkg["imputer"].transform(X)
    X = pkg["scaler"].transform(X)
    prob = pkg["model"].predict_proba(X)[0]
    pred = pkg["model"].predict(X)[0]
    return {
        "prediction": "PROFITABLE" if pred == 1 else "SKIP",
        "probability": round(float(prob[1]), 4),
        "confidence": round(float(max(prob)), 4),
        "model": pkg["best_name"],
    }


# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="WSQ_ATB Trade Predictor")
    parser.add_argument("--enrich", action="store_true", help="Fetch CoinGecko market data")
    parser.add_argument("--train", action="store_true", help="Train ML model")
    args = parser.parse_args()

    if not args.enrich and not args.train:
        parser.print_help()
        print("\nQuick start:")
        print("  1. python trade_predictor.py --enrich")
        print("  2. python trade_predictor.py --train")
        return

    trades = None

    if args.enrich:
        trades = enrich_trades()

    if args.train:
        if trades is None:
            for path in [ENRICHED_PATH, TRADES_PATH]:
                if os.path.exists(path):
                    with open(path, encoding="utf-8") as f:
                        data = json.load(f)
                    trades = data["trades"]
                    log.info(f"Loaded {len(trades)} trades from {path}")
                    break
            else:
                log.error("No trade data found. Run trade_analysis.py first.")
                sys.exit(1)

        rows = build_features(trades)
        log.info(f"Built {len(rows)} feature rows")
        has_cg = sum(1 for r in rows if r.get("market_cap") is not None)
        log.info(f"Rows with market data: {has_cg}/{len(rows)}")
        train_model(rows)


if __name__ == "__main__":
    main()

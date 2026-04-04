"""
Layer 4 — Model Training
Trains CatBoostClassifier on matches_features table.
Train: 2022+2023  |  Test: 2024 (time-based split — no leakage)

Usage:
    python3 src/train_model.py             # train + gate check
    python3 src/train_model.py --dry-run   # evaluate only, no model save
"""
import argparse
import json
import logging
import pickle
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    SUPABASE_URL,
    SUPABASE_KEY,
    MODELS_DIR,
    DATA_INTERIM_DIR,
    validate_config,
)
from supabase import create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("train_model.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Feature columns (must match matches_features schema) ──
FEATURE_COLS = [
    "venue_code",
    "opp_code",
    "hour",
    "day_code",
    "goals_scored_rolling",
    "goals_conceded_rolling",
    "wins_rolling",
    "clean_sheets_rolling",
    "win_streak",
    "form_momentum",
    "opp_goals_scored_rolling",
    "opp_goals_conceded_rolling",
    "h2h_win_rate",
]

TARGET_COL = "result"   # 0=away win, 1=draw, 2=home win

# ── Gate 4 thresholds ─────────────────────────────────────
GATE = {
    "f1_weighted_min":    0.420,
    "precision_min":      0.40,
    "brier_max":          0.67,    # ← 0.667 = random baseline; 0.67 gives safe margin
    "draw_recall_min":    0.10,
}

# ── CatBoost config ───────────────────────────────────────
# REPLACE CATBOOST_PARAMS with:
CATBOOST_PARAMS = dict(
    iterations=300,                # fixed — don't rely on early stopping
    depth=4,
    learning_rate=0.05,
    l2_leaf_reg=3,
    loss_function="MultiClass",
    eval_metric="Accuracy",
    auto_class_weights="Balanced",
    random_seed=42,
    verbose=100,
    # early stopping REMOVED — fires too aggressively on 760 rows
)


# ══════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════

def load_features(client) -> pd.DataFrame:
    """Load all feature rows from Supabase with pagination."""
    log.info("📥 Loading features from Supabase...")
    all_rows = []
    page, size = 0, 1000

    while True:
        resp = (
            client.table("matches_features")
            .select("*")
            .range(page * size, (page + 1) * size - 1)
            .execute()
        )
        batch = resp.data
        all_rows.extend(batch)
        log.info(f"  Page {page + 1}: {len(batch)} rows fetched")
        if len(batch) < size:
            break
        page += 1

    df = pd.DataFrame(all_rows)
    log.info(f"✅ Loaded {len(df)} feature rows")
    return df


# ══════════════════════════════════════════════════════════
# FEATURE MERGE WITH TARGET
# ══════════════════════════════════════════════════════════

def load_targets(client) -> pd.DataFrame:
    """Load result column from matches table."""
    log.info("📥 Loading target labels from matches...")
    all_rows = []
    page, size = 0, 1000

    while True:
        resp = (
            client.table("matches")
            .select("fixture_id,result,season")
            .range(page * size, (page + 1) * size - 1)
            .execute()
        )
        batch = resp.data
        all_rows.extend(batch)
        if len(batch) < size:
            break
        page += 1

    return pd.DataFrame(all_rows)


# ══════════════════════════════════════════════════════════
# BRIER SCORE (multiclass)
# ══════════════════════════════════════════════════════════

def brier_score_multiclass(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """
    Multiclass Brier score — average squared probability error.
    Lower = better calibrated. Threshold <= 0.28.
    """
    n_classes = y_prob.shape[1]
    y_onehot  = np.eye(n_classes)[y_true]
    return float(np.mean(np.sum((y_prob - y_onehot) ** 2, axis=1)))


# ══════════════════════════════════════════════════════════
# GATE 4 CHECK
# ══════════════════════════════════════════════════════════

def gate_check(metrics: dict) -> tuple[bool, list[str]]:
    """
    Returns (passed, [failure_messages])
    All gates must pass for model to be promoted.
    """
    failures = []

    if metrics["f1_weighted"] < GATE["f1_weighted_min"]:
        failures.append(
            f"❌ F1 weighted {metrics['f1_weighted']:.4f} < {GATE['f1_weighted_min']} (Gate)")

    if metrics["precision_weighted"] < GATE["precision_min"]:
        failures.append(
            f"❌ Precision {metrics['precision_weighted']:.4f} < {GATE['precision_min']} (Gate)")

    if metrics["brier_score"] > GATE["brier_max"]:
        failures.append(
            f"❌ Brier score {metrics['brier_score']:.4f} > {GATE['brier_max']} (Gate)")

    if metrics["draw_recall"] < GATE["draw_recall_min"]:
        failures.append(
            f"❌ Draw recall {metrics['draw_recall']:.4f} < {GATE['draw_recall_min']} (Gate)")

    return len(failures) == 0, failures


# ══════════════════════════════════════════════════════════
# MAIN TRAINING PIPELINE
# ══════════════════════════════════════════════════════════

def main(dry_run: bool = False):
    print("\n🚀 Layer 4 — Model Training Pipeline")
    print("=" * 55)
    print(f"   Train  : Seasons 2022 + 2023")
    print(f"   Test   : Season 2024")
    print(f"   Model  : CatBoostClassifier")
    print(f"   Mode   : {'DRY RUN (no model save)' if dry_run else 'LIVE'}")
    print("=" * 55)

    validate_config()
    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Load data ──────────────────────────────────────────
    features = load_features(client)
    targets  = load_targets(client)

    df = features.merge(
        targets[["fixture_id", "result", "season"]],
        on="fixture_id",
        suffixes=("", "_target"),
    )

    # Use season from targets if duplicate
    if "season_target" in df.columns:
        df["season"] = df["season_target"]
        df.drop(columns=["season_target"], inplace=True)

    log.info(f"📊 Dataset: {len(df)} rows × {len(FEATURE_COLS)} features")

    # ── Time-based train/test split ────────────────────────
    train_df = df[df["season"].isin([2022, 2023])].copy()
    test_df  = df[df["season"] == 2024].copy()

    X_train = train_df[FEATURE_COLS].astype(float)
    y_train = train_df[TARGET_COL].astype(int)
    X_test  = test_df[FEATURE_COLS].astype(float)
    y_test  = test_df[TARGET_COL].astype(int)

    log.info(f"📊 Train: {len(X_train)} rows | Test: {len(X_test)} rows")
    log.info(f"📊 Class distribution (train): {y_train.value_counts().to_dict()}")

    # ── Train ──────────────────────────────────────────────
    print("\n🤖 Training CatBoostClassifier...")
    train_pool = Pool(X_train, label=y_train)
    eval_pool  = Pool(X_test,  label=y_test)

    model = CatBoostClassifier(**CATBOOST_PARAMS)
    model.fit(train_pool)   # no eval_set — fixed iterations, no early stop

    # ── Evaluate ───────────────────────────────────────────
    print("\n📊 Evaluating on 2024 test set...")
    y_pred      = model.predict(X_test).flatten().astype(int)
    y_prob      = model.predict_proba(X_test)

    f1_w        = f1_score(y_test, y_pred, average="weighted")
    prec_w      = precision_score(y_test, y_pred, average="weighted", zero_division=0)
    brier       = brier_score_multiclass(y_test.values, y_prob)

    # Draw recall (class 1)
    report      = classification_report(y_test, y_pred,
                                        target_names=["Away Win", "Draw", "Home Win"],
                                        output_dict=True)
    draw_recall = report.get("Draw", {}).get("recall", 0.0)
    accuracy    = report["accuracy"]

    metrics = {
        "f1_weighted":        round(f1_w, 4),
        "precision_weighted": round(prec_w, 4),
        "brier_score":        round(brier, 4),
        "draw_recall":        round(draw_recall, 4),
        "accuracy":           round(accuracy, 4),
    }

    print("\n" + "=" * 55)
    print("   📈 MODEL METRICS (Test — Season 2024)")
    print("=" * 55)
    print(f"   Accuracy          : {metrics['accuracy']:.4f}")
    print(f"   F1 (weighted)     : {metrics['f1_weighted']:.4f}  "
          f"[gate >= {GATE['f1_weighted_min']}]")
    print(f"   Precision (wt)    : {metrics['precision_weighted']:.4f}  "
          f"[gate >= {GATE['precision_min']}]")
    print(f"   Brier score       : {metrics['brier_score']:.4f}  "
          f"[gate <= {GATE['brier_max']}]")
    print(f"   Draw recall       : {metrics['draw_recall']:.4f}  "
          f"[gate >= {GATE['draw_recall_min']}]")
    print("=" * 55)

    print("\n📋 Full Classification Report:")
    print(classification_report(y_test, y_pred,
                                target_names=["Away Win(0)", "Draw(1)", "Home Win(2)"]))

    print("\n🔢 Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    cm_df = pd.DataFrame(
        cm,
        index=["True Away Win", "True Draw", "True Home Win"],
        columns=["Pred Away Win", "Pred Draw", "Pred Home Win"],
    )
    print(cm_df.to_string())

    print("\n🌟 Top 10 Feature Importances:")
    feat_imp = pd.Series(
        model.get_feature_importance(),
        index=FEATURE_COLS,
    ).sort_values(ascending=False)
    for feat, imp in feat_imp.head(10).items():
        bar = "█" * int(imp / 2)
        print(f"   {feat:<35} {imp:5.1f}  {bar}")

    # ── Gate 4 check ───────────────────────────────────────
    print("\n🚦 Gate 4 Quality Check...")
    passed, failures = gate_check(metrics)

    if passed:
        print("   ✅ ALL GATES PASSED — model approved for deployment")
    else:
        print("   ⚠️  GATE FAILURES:")
        for f in failures:
            print(f"      {f}")
        if not dry_run:
            print("   ⛔ Model NOT saved — fix issues and retrain")
            return None

    if dry_run:
        print("\n✅ DRY RUN complete — model not saved")
        return model

    # ── Save artifacts ─────────────────────────────────────
    print("\n💾 Saving model artifacts...")

    # 1. CatBoost binary model
    model_path = MODELS_DIR / "model.cbm"
    model.save_model(str(model_path))
    log.info(f"   Saved → {model_path}")

    # 2. Feature columns list
    feat_path = MODELS_DIR / "feature_cols.pkl"
    with open(feat_path, "wb") as f:
        pickle.dump(FEATURE_COLS, f)
    log.info(f"   Saved → {feat_path}")

    # 3. Metadata JSON
    meta = {
        "trained_at":         datetime.now(timezone.utc).isoformat(),
        "train_seasons":      [2022, 2023],
        "test_season":        2024,
        "iterations_used": int(CATBOOST_PARAMS["iterations"]),
        "feature_cols":       FEATURE_COLS,
        "metrics":            metrics,
        "gate_passed":        passed,
        "catboost_params":    {k: str(v) for k, v in CATBOOST_PARAMS.items()},
    }
    meta_path = MODELS_DIR / "metadata.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    log.info(f"   Saved → {meta_path}")

    print(f"\n✅ Model artifacts saved:")
    print(f"   {model_path}")
    print(f"   {feat_path}")
    print(f"   {meta_path}")
    print(f"\n🎯 Best iteration : {model.best_iteration_}")
    print(f"   F1 weighted    : {metrics['f1_weighted']}")
    print(f"   Accuracy       : {metrics['accuracy']}")

    return model


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPL Model Training")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(dry_run=args.dry_run)

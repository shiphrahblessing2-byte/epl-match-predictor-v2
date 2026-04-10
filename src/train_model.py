"""
Layer 4 — Multi-League Model Training (v2)
Trains CatBoostClassifier on 4 leagues × 4 seasons of ESPN data.

Walk-forward validation:
  Fold 1: Train 2021-22+2022-23  →  Validate 2023-24
  Fold 2: Train 2021-22 to 2023-24  →  Validate 2024-25
  Final:  Train ALL 4 seasons  →  Production model

Usage:
    python3 -m src.train_model             # train + gate check
    python3 -m src.train_model --dry-run   # evaluate only, no model save
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
from config import MODELS_DIR, REPORTS_DIR, validate_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("train_model.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Feature columns ───────────────────────────────────────
FEATURE_COLS = [
    "home_team_id",          # ✅ ADD — home team identity
    "opp_code",              # ✅ ADD — away team identity (= away_team_id)
    "league_key",
    "home_goals_scored_rolling",
    "home_goals_conceded_rolling",
    "home_wins_rolling",
    "home_clean_sheets_rolling",
    "home_win_streak",
    "home_form_momentum",
    "opp_goals_scored_rolling",
    "opp_goals_conceded_rolling",
    "opp_wins_rolling",
    "opp_clean_sheets_rolling",
    "opp_win_streak",
    "opp_form_momentum",
    "h2h_win_rate",
    "attack_vs_defence",
    "opp_attack_vs_defence",
    # ✅ REMOVED: form_diff, goal_diff_rolling, opp_goal_diff_rolling
]

CAT_FEATURES = ["league_key", "home_team_id", "opp_code"]   # CatBoost categorical columns
TARGET_COL   = "result"          # 0=away win, 1=draw, 2=home win
WEIGHT_COL   = "sample_weight"   # time-decay weights

# ── Season decay weights ──────────────────────────────────
SEASON_WEIGHTS = {
    2020: 0.40,
    2021: 0.50,
    2022: 0.65,
    2023: 0.82,
    2024: 1.00,
}

# ── Gate 4 thresholds ─────────────────────────────────────
GATE = {
    "f1_weighted_min":  0.420,
    "precision_min":    0.40,
    "brier_max":        0.67,
    "draw_recall_min":  0.10,
}

# ── CatBoost config ───────────────────────────────────────
CATBOOST_PARAMS = dict(
    iterations=800,
    depth=4,                   # ✅ reduce 5→4 (16 leaves max vs 32)
    learning_rate=0.05,
    l2_leaf_reg=10,            # ✅ increase 5→10 (much stronger penalty)
    min_data_in_leaf=30,       # ✅ increase 20→30
    bootstrap_type="Bernoulli", 
    subsample=0.8,             # ✅ ADD — use 80% of rows per tree
    colsample_bylevel=0.8,     # ✅ ADD — use 80% of features per split
    loss_function="MultiClass",
    eval_metric="Accuracy",
    auto_class_weights="Balanced",
    cat_features=CAT_FEATURES,
    random_seed=42,
    verbose=100,
    early_stopping_rounds=100,
)


# ══════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════

def load_features() -> pd.DataFrame:
    """Load multi-league feature CSV built by build_training_features.py"""
    path = Path("data/training/features_2122_2425.csv")
    if not path.exists():
        log.error(f"Features file not found: {path}")
        log.error("Run: python -m src.build_training_features")
        sys.exit(1)
    log.info(f"📥 Loading features from {path}...")
    df = pd.read_csv(path)
    df["match_date"]  = pd.to_datetime(df["match_date"])
    df["season_year"] = df["season"].astype(str).str[:4].astype(int)
    df["sample_weight"] = df["season_year"].map(SEASON_WEIGHTS).fillna(0.5)
    log.info(f"✅ Loaded {len(df)} feature rows across {df['league_key'].nunique()} leagues")
    return df


# ══════════════════════════════════════════════════════════
# BRIER SCORE (multiclass)
# ══════════════════════════════════════════════════════════

def brier_score_multiclass(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    n_classes = y_prob.shape[1]
    y_onehot  = np.eye(n_classes)[y_true]
    return float(np.mean(np.sum((y_prob - y_onehot) ** 2, axis=1)))


# ══════════════════════════════════════════════════════════
# GATE 4 CHECK
# ══════════════════════════════════════════════════════════

def gate_check(metrics: dict) -> tuple[bool, list[str]]:
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
# TRAIN HELPERS
# ══════════════════════════════════════════════════════════

def train_catboost(X_train, y_train, weights,
                   X_val=None, y_val=None) -> CatBoostClassifier:
    params = {**CATBOOST_PARAMS}
    model = CatBoostClassifier(**params)
    eval_set = (X_val, y_val) if X_val is not None else None
    model.fit(
        X_train, y_train,
        sample_weight=weights,
        eval_set=eval_set,
        use_best_model=(X_val is not None),
    )
    return model


def evaluate(model, X_test, y_test, label: str) -> dict:
    """Run full evaluation suite and print results."""
    y_pred = model.predict(X_test).flatten().astype(int)
    y_prob = model.predict_proba(X_test)

    f1_w   = f1_score(y_test, y_pred, average="weighted")
    prec_w = precision_score(y_test, y_pred, average="weighted", zero_division=0)
    brier  = brier_score_multiclass(y_test.values, y_prob)

    report     = classification_report(y_test, y_pred,
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

    print(f"\n{'='*55}")
    print(f"   📈 MODEL METRICS — {label}")
    print(f"{'='*55}")
    print(f"   Accuracy          : {metrics['accuracy']:.4f}")
    print(f"   F1 (weighted)     : {metrics['f1_weighted']:.4f}  [gate >= {GATE['f1_weighted_min']}]")
    print(f"   Precision (wt)    : {metrics['precision_weighted']:.4f}  [gate >= {GATE['precision_min']}]")
    print(f"   Brier score       : {metrics['brier_score']:.4f}  [gate <= {GATE['brier_max']}]")
    print(f"   Draw recall       : {metrics['draw_recall']:.4f}  [gate >= {GATE['draw_recall_min']}]")
    print(f"{'='*55}")

    print("\n📋 Full Classification Report:")
    print(classification_report(y_test, y_pred,
          target_names=["Away Win(0)", "Draw(1)", "Home Win(2)"]))

    print("\n🔢 Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    cm_df = pd.DataFrame(
        cm,
        index=["True Away", "True Draw", "True Home"],
        columns=["Pred Away", "Pred Draw", "Pred Home"],
    )
    print(cm_df.to_string())

    return metrics


# ══════════════════════════════════════════════════════════
# WALK-FORWARD VALIDATION
# ══════════════════════════════════════════════════════════

def walk_forward_validate(df: pd.DataFrame):
    folds = [
        (2020, 2022, 2023, "Fold 1 — Train 2020 to 2022-23 → Validate 2023-24"),
        (2020, 2023, 2024, "Fold 2 — Train 2020 to 2023-24 → Validate 2024-25"),
    ]
    fold_metrics = []
    for train_start, train_end, val_year, label in folds:
        mask_train = (df.season_year >= train_start) & (df.season_year <= train_end)
        mask_val   = df.season_year == val_year

        X_tr = df[mask_train][FEATURE_COLS]
        y_tr = df[mask_train][TARGET_COL].astype(int)
        w_tr = df[mask_train][WEIGHT_COL]
        X_v  = df[mask_val][FEATURE_COLS]
        y_v  = df[mask_val][TARGET_COL].astype(int)

        log.info(f"\n🔄 {label}")
        log.info(f"   Train: {len(X_tr)} rows | Val: {len(X_v)} rows")

        model   = train_catboost(X_tr, y_tr, w_tr, X_v, y_v)
        metrics = evaluate(model, X_v, y_v, label)
        fold_metrics.append(metrics)

        passed, failures = gate_check(metrics)
        if passed:
            print("   ✅ GATES PASSED")
        else:
            print("   ⚠️  Gate warnings (validation fold — training continues):")
            for f in failures:
                print(f"      {f}")

    return fold_metrics


# ══════════════════════════════════════════════════════════
# FINAL PRODUCTION MODEL
# ══════════════════════════════════════════════════════════

def train_final_model(df: pd.DataFrame, dry_run: bool, fold_metrics: list) -> CatBoostClassifier:
    X = df[FEATURE_COLS]
    y = df[TARGET_COL].astype(int)
    w = df[WEIGHT_COL]

    log.info("\n🚀 Training final production model on ALL 4 seasons...")
    model = train_catboost(X, y, w)

    # Use Fold 2 metrics as the gate check for production save
    metrics = fold_metrics[-1]
    passed, failures = gate_check(metrics)

    print("\n🚦 Gate 4 Quality Check (based on Fold 2 validation)...")
    if passed:
        print("   ✅ ALL GATES PASSED — model approved for deployment")
    else:
        print("   ⚠️  GATE FAILURES:")
        for f in failures:
            print(f"      {f}")
        if not dry_run:
            print("   ⛔ Model NOT saved — fix issues and retrain")
            return model

    if dry_run:
        print("\n✅ DRY RUN complete — model not saved")
        return model

    # ── Save artifacts ─────────────────────────────────────
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    print("\n💾 Saving model artifacts...")

    # 1. CatBoost binary
    model_path = MODELS_DIR / "catboost_multileague_v2.cbm"
    model.save_model(str(model_path))

    # 2. Feature columns list (used by prediction API)
    feat_path = MODELS_DIR / "feature_cols.pkl"
    with open(feat_path, "wb") as f:
        pickle.dump(FEATURE_COLS, f)

    # 3. Metadata JSON
    meta = {
        "trained_at":      datetime.now(timezone.utc).isoformat(),
        "train_seasons":   [2020, 2021, 2022, 2023, 2024],   # ✅ add 2020
        "leagues":         ["EPL", "LIGA", "UCL", "UEL"],
        "feature_cols":    FEATURE_COLS,
        "cat_features":    CAT_FEATURES,
        "metrics_fold1":   fold_metrics[0],
        "metrics_fold2":   fold_metrics[1],
        "gate_passed":     passed,
        "catboost_params": {k: str(v) for k, v in CATBOOST_PARAMS.items()},
    # ✅ ADD — what health endpoint reads
    }
    meta_path = MODELS_DIR / "metadata.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"   ✅ {model_path}")
    print(f"   ✅ {feat_path}")
    print(f"   ✅ {meta_path}")

     # 4. Evaluation summary for monitor.py  ← ADD HERE, still indented inside function
    summary = {
        "f1_weighted":        metrics["f1_weighted"],
        "precision_weighted": metrics["precision_weighted"],
        "brier_score":        metrics["brier_score"],
        "draw_recall":        metrics["draw_recall"],
        "accuracy":           metrics["accuracy"],
        "gate_passed":        passed,
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = REPORTS_DIR / "evaluation_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"   ✅ {summary_path}")

    # ── Feature importance ─────────────────────────────────
    print("\n🌟 Top Feature Importances:")
    feat_imp = pd.Series(
        model.get_feature_importance(),
        index=FEATURE_COLS,
    ).sort_values(ascending=False)
    for feat, imp in feat_imp.head(10).items():
        bar = "█" * int(imp / 2)
        print(f"   {feat:<40} {imp:5.1f}  {bar}")

    return model


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main(dry_run: bool = False):
    print("\n🚀 Layer 4 — Multi-League Model Training Pipeline (v2)")
    print("=" * 60)
    print(f"   Leagues  : EPL, La Liga, UCL, UEL")
    print(f"   Seasons  : 2021-22 → 2024-25 (4 seasons)")
    print(f"   Model    : CatBoostClassifier + league_key categorical")
    print(f"   Mode     : {'DRY RUN (no model save)' if dry_run else 'LIVE'}")
    print("=" * 60)

    validate_config()
    df = load_features()

    log.info(f"📊 Dataset: {len(df)} rows × {len(FEATURE_COLS)} features")
    log.info(f"📊 Class distribution:\n{df[TARGET_COL].value_counts().to_string()}")

    print("\n=== Walk-Forward Validation ===")
    fold_metrics = walk_forward_validate(df)

    print("\n=== Training Final Production Model ===")
    train_final_model(df, dry_run, fold_metrics)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPL Multi-League Model Training")
    parser.add_argument("--dry-run", action="store_true",
                        help="Evaluate only — do not save model artifacts")
    args = parser.parse_args()
    main(dry_run=args.dry_run)



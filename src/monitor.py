"""
Layer 6 — Monitor & Drift Detection

Usage:
    python src/monitor.py --gate    # gate check before deploy
    python src/monitor.py --drift   # weekly drift check vs Supabase
"""
import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

MODELS_DIR  = Path(__file__).resolve().parent.parent / "models"
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# ── Gate thresholds (must match train_model.py) ────────────
GATE = {
    "f1_weighted":        0.42,
    "brier_score_max":    0.67,
    "draw_recall":        0.10,
    "precision_weighted": 0.40,
}


# ══════════════════════════════════════════════════════════
# GATE CHECK
# ══════════════════════════════════════════════════════════

def run_gate_check():
    """
    Compare new model metrics vs production baseline.
    Exits 1 (blocks deploy) if any gate fails.
    Exits 0 if all gates pass.
    """
    print("\n🚦 Gate Check — New vs Production")
    print("=" * 55)

    # Load production baseline
    prod_meta_path = MODELS_DIR / "metadata.json"
    if not prod_meta_path.exists():
        log.error("❌ models/metadata.json not found — run train_model.py first")
        sys.exit(1)

    with open(prod_meta_path) as f:
        prod = json.load(f)

    prod_metrics = prod.get("metrics", {})

    # Load new model evaluation (written by train_model.py)
    eval_path = REPORTS_DIR / "evaluation_summary.json"
    if eval_path.exists():
        with open(eval_path) as f:
            new_metrics = json.load(f)
        log.info("📄 Loaded new metrics from reports/evaluation_summary.json")
    else:
        # Fallback — use metadata.json (same run)
        new_metrics = prod_metrics
        log.warning("⚠️  No evaluation_summary.json — using metadata.json as baseline")

    failures = []

    # Gate 1 — F1 must not regress vs production
    new_f1   = new_metrics.get("f1_weighted", 0)
    prod_f1  = prod_metrics.get("f1_weighted", 0)
    if new_f1 < prod_f1:
        failures.append(
            f"F1 regression: new={new_f1:.4f} < prod={prod_f1:.4f}"
        )
    print(f"   F1 weighted   : {new_f1:.4f} (prod={prod_f1:.4f}) "
          f"{'✅' if new_f1 >= prod_f1 else '❌'}")

    # Gate 2 — F1 absolute floor
    if new_f1 < GATE["f1_weighted"]:
        failures.append(
            f"F1 below floor: {new_f1:.4f} < {GATE['f1_weighted']}"
        )
    print(f"   F1 floor      : {new_f1:.4f} >= {GATE['f1_weighted']} "
          f"{'✅' if new_f1 >= GATE['f1_weighted'] else '❌'}")

    # Gate 3 — Brier score
    new_brier = new_metrics.get("brier_score", 1.0)
    if new_brier > GATE["brier_score_max"]:
        failures.append(
            f"Brier too high: {new_brier:.4f} > {GATE['brier_score_max']}"
        )
    print(f"   Brier score   : {new_brier:.4f} <= {GATE['brier_score_max']} "
          f"{'✅' if new_brier <= GATE['brier_score_max'] else '❌'}")

    # Gate 4 — Draw recall
    new_draw = new_metrics.get("draw_recall", 0)
    if new_draw < GATE["draw_recall"]:
        failures.append(
            f"Draw recall too low: {new_draw:.4f} < {GATE['draw_recall']}"
        )
    print(f"   Draw recall   : {new_draw:.4f} >= {GATE['draw_recall']} "
          f"{'✅' if new_draw >= GATE['draw_recall'] else '❌'}")

    print("=" * 55)

    if failures:
        print(f"\n❌ GATE FAILED — {len(failures)} issue(s):")
        for f in failures:
            print(f"   • {f}")
        print("\n🚫 Deploy blocked.\n")
        sys.exit(1)

    print("\n✅ ALL GATES PASSED — deploy approved\n")
    sys.exit(0)


# ══════════════════════════════════════════════════════════
# DRIFT CHECK
# ══════════════════════════════════════════════════════════

def run_drift_check():
    """
    Load last 10 graded predictions from Supabase.
    Alert if rolling precision drops > 5% below production baseline.
    """
    try:
        from config import SUPABASE_URL, SUPABASE_KEY
        from supabase import create_client
    except ImportError:
        log.error("config.py or supabase not available")
        sys.exit(1)

    print("\n📉 Drift Detection — Rolling Precision Check")
    print("=" * 55)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Load last 10 graded predictions
    resp = (
        client.table("predictions")
        .select("predicted,actual,correct")
        .not_.is_("actual", "null")     # only graded rows
        .order("predicted_at", desc=True)
        .limit(10)
        .execute()
    )
    rows = resp.data

    if not rows:
        print("   ⚠️  No graded predictions yet — drift check skipped")
        sys.exit(0)

    if len(rows) < 5:
        print(f"   ⚠️  Only {len(rows)} graded predictions — need ≥5 for drift check")
        sys.exit(0)

    # Compute rolling accuracy
    correct       = sum(1 for r in rows if r.get("correct"))
    rolling_acc   = correct / len(rows)

    # Load production baseline
    with open(MODELS_DIR / "metadata.json") as f:
        prod = json.load(f)
    baseline_acc = prod.get("metrics", {}).get("accuracy", 0.4342)

    drop_pct = (baseline_acc - rolling_acc) / baseline_acc * 100

    print(f"   Production baseline : {baseline_acc:.4f}")
    print(f"   Rolling accuracy    : {rolling_acc:.4f} ({len(rows)} matches)")
    print(f"   Drift               : {drop_pct:+.1f}%")
    print("=" * 55)

    # Save drift report
    report = {
        "baseline_accuracy":  baseline_acc,
        "rolling_accuracy":   rolling_acc,
        "sample_size":        len(rows),
        "drift_pct":          round(drop_pct, 2),
        "alert":              drop_pct > 5.0,
    }
    with open(REPORTS_DIR / "drift_report.json", "w") as f:
        json.dump(report, f, indent=2)

    if drop_pct > 5.0:
        print(f"\n🚨 DRIFT ALERT — accuracy dropped {drop_pct:.1f}% from baseline")
        print("   Recommendation: trigger manual retrain via workflow_dispatch")
        sys.exit(1)

    print(f"\n✅ No significant drift detected ({drop_pct:+.1f}%)\n")
    sys.exit(0)


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPL Model Monitor")
    parser.add_argument("--gate",  action="store_true",
                        help="Run gate check before deploy")
    parser.add_argument("--drift", action="store_true",
                        help="Run weekly drift detection")
    args = parser.parse_args()

    if args.gate:
        run_gate_check()
    elif args.drift:
        run_drift_check()
    else:
        parser.print_help()
        sys.exit(1)

"""
Layer 7 — Prediction Logger + Grader

Usage:
    python src/log_predictions.py --log      # log upcoming predictions
    python src/log_predictions.py --grade    # grade completed matches
    python src/log_predictions.py --accuracy # update accuracy_log table
"""
import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import SUPABASE_URL, SUPABASE_KEY
from predict import MODEL, FEATURE_COLS, predict_match
from supabase import create_client

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MODELS_DIR = Path(__file__).resolve().parent.parent / "models"

# Result mapping: API result int → W/D/L char
RESULT_MAP = {2: "W", 1: "D", 0: "L"}


def get_model_version() -> str:
    meta_path = MODELS_DIR / "metadata.json"
    if meta_path.exists():
        with open(meta_path) as f:
            return json.load(f).get("trained_at", "unknown")[:10]
    return "unknown"


# ══════════════════════════════════════════════════════════
# LOG — predict upcoming + store to Supabase
# ══════════════════════════════════════════════════════════

def log_upcoming_predictions(client):
    """Fetch upcoming fixtures, predict, store to predictions table."""
    print("\n📝 Logging upcoming predictions...")

    resp = (
        client.table("matches")
        .select("fixture_id,home_team_id,away_team_id,match_date")
        .eq("status_short", "NS")
        .order("match_date", desc=False)
        .limit(10)
        .execute()
    )
    fixtures = resp.data
    if not fixtures:
        log.warning("No upcoming fixtures found")
        return 0

    version   = get_model_version()
    logged    = 0

    for fix in fixtures:
        try:
            match_date = datetime.fromisoformat(fix["match_date"])
            pred = predict_match(
                fix["home_team_id"], fix["away_team_id"],
                client, match_date
            )
            probs = pred["probabilities"]

            # Map probabilities to W/D/L
            prob_W = probs["home_win"]
            prob_D = probs["draw"]
            prob_L = probs["away_win"]
            prediction = (
                "W" if prob_W == max(prob_W, prob_D, prob_L)
                else "D" if prob_D == max(prob_W, prob_D, prob_L)
                else "L"
            )

            row = {
                "fixture_id":   fix["fixture_id"],
                "predicted_at": datetime.now(timezone.utc).isoformat(),
                "prediction":   prediction,
                "prob_W":       prob_W,
                "prob_D":       prob_D,
                "prob_L":       prob_L,
                "confidence":   pred["confidence"],
                "model_version": version,
            }

            # Upsert — avoid duplicates on same fixture
            client.table("predictions").upsert(
                row, on_conflict="fixture_id"
            ).execute()
            logged += 1
            log.info(f"  ✅ fixture {fix['fixture_id']}: {prediction} "
                     f"(conf={pred['confidence']:.2f})")

        except Exception as e:
            log.error(f"  ❌ fixture {fix['fixture_id']}: {e}")

    print(f"\n✅ Logged {logged} predictions")
    return logged


# ══════════════════════════════════════════════════════════
# GRADE — fill actual_result + was_correct for finished matches
# ══════════════════════════════════════════════════════════

def grade_predictions(client):
    """
    For each ungraded prediction, check if match is now FT.
    Fill actual_result and was_correct.
    """
    print("\n🎯 Grading completed predictions...")

    # Fetch ungraded predictions
    resp = (
        client.table("predictions")
        .select("id,fixture_id,prediction")
        .is_("actual_result", "null")
        .execute()
    )
    ungraded = resp.data
    if not ungraded:
        log.info("No ungraded predictions found")
        return 0

    graded = 0
    for pred in ungraded:
        fid = pred["fixture_id"]

        # Get actual result from matches table
        match_resp = (
            client.table("matches")
            .select("result,status_short")
            .eq("fixture_id", fid)
            .single()
            .execute()
        )
        if not match_resp.data:
            continue

        match = match_resp.data
        if match["status_short"] != "FT":
            continue  # not finished yet

        actual_char = RESULT_MAP.get(match["result"])
        was_correct = (pred["prediction"] == actual_char)

        client.table("predictions").update({
            "actual_result": actual_char,
            "was_correct":   was_correct,
        }).eq("id", pred["id"]).execute()

        graded += 1
        status = "✅" if was_correct else "❌"
        log.info(f"  {status} fixture {fid}: "
                 f"predicted={pred['prediction']} actual={actual_char}")

    print(f"\n✅ Graded {graded} predictions")
    return graded


# ══════════════════════════════════════════════════════════
# ACCURACY — compute rolling metrics + write to accuracy_log
# ══════════════════════════════════════════════════════════

def update_accuracy_log(client):
    """Compute rolling 10-match accuracy and log to accuracy_log."""
    print("\n📊 Updating accuracy log...")

    resp = (
        client.table("predictions")
        .select("was_correct,prediction,actual_result")
        .not_.is_("actual_result", "null")
        .order("predicted_at", desc=True)
        .limit(10)
        .execute()
    )
    rows = resp.data
    if not rows or len(rows) < 3:
        log.warning(f"Only {len(rows)} graded predictions — skipping")
        return

    n_total   = len(rows)
    n_correct = sum(1 for r in rows if r.get("was_correct"))
    rolling_acc = round(n_correct / n_total, 4)

    # Simple F1 — weighted by class frequency
    classes = ["W", "D", "L"]
    f1_scores = []
    for cls in classes:
        tp = sum(1 for r in rows
                 if r["prediction"] == cls and r["actual_result"] == cls)
        fp = sum(1 for r in rows
                 if r["prediction"] == cls and r["actual_result"] != cls)
        fn = sum(1 for r in rows
                 if r["prediction"] != cls and r["actual_result"] == cls)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall    = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = (2 * precision * recall / (precision + recall)
              if (precision + recall) > 0 else 0)
        f1_scores.append(f1)
    rolling_f1 = round(sum(f1_scores) / len(f1_scores), 4)

    log_row = {
        "logged_at":      datetime.now(timezone.utc).isoformat(),
        "model_version":  get_model_version(),
        "rolling_10_acc": rolling_acc,
        "rolling_10_f1":  rolling_f1,
        "n_correct":      n_correct,
        "n_total":        n_total,
    }
    client.table("accuracy_log").insert(log_row).execute()

    print(f"   Rolling accuracy : {rolling_acc:.4f} ({n_correct}/{n_total})")
    print(f"   Rolling F1       : {rolling_f1:.4f}")
    print(f"✅ Accuracy log updated")


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPL Prediction Logger")
    parser.add_argument("--log",      action="store_true",
                        help="Log upcoming match predictions")
    parser.add_argument("--grade",    action="store_true",
                        help="Grade completed predictions")
    parser.add_argument("--accuracy", action="store_true",
                        help="Update rolling accuracy log")
    parser.add_argument("--all",      action="store_true",
                        help="Run all 3 steps in sequence")
    args = parser.parse_args()

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.all or args.log:
        log_upcoming_predictions(client)
    if args.all or args.grade:
        grade_predictions(client)
    if args.all or args.accuracy:
        update_accuracy_log(client)

    if not any([args.log, args.grade, args.accuracy, args.all]):
        parser.print_help()
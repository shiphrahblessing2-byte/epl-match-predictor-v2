"""
Builds data/training/features_2122_2425.csv
- Fetches match results (home_goals, away_goals) from Supabase matches table
- Joins with interim/matches_features.csv
- Renames columns to match train_model.py expectations
- Computes result: 0=away win, 1=draw, 2=home win
"""
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from supabase import create_client
from dotenv import load_dotenv          # ✅ ADD THIS

load_dotenv()                           # ✅ AND THIS — loads .env into os.environ

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

INPUT_PATH  = Path("data/interim/matches_features.csv")
OUTPUT_DIR  = Path("data/training")
OUTPUT_PATH = OUTPUT_DIR / "features_2122_2425.csv"

# Column rename map: feature_engineer output → train_model expectations
RENAME_MAP = {
    "goals_scored_rolling":   "home_goals_scored_rolling",
    "goals_conceded_rolling":  "home_goals_conceded_rolling",
    "wins_rolling":            "home_wins_rolling",
    "clean_sheets_rolling":    "home_clean_sheets_rolling",
    "win_streak":              "home_win_streak",
    "form_momentum":           "home_form_momentum",
    # opp columns already have right prefix — but check for missing ones
    "opp_wins_rolling":        "opp_wins_rolling",
    "opp_clean_sheets_rolling":"opp_clean_sheets_rolling",
    "opp_win_streak":          "opp_win_streak",
    "opp_form_momentum":       "opp_form_momentum",
}

def fetch_match_results() -> pd.DataFrame:
    """Fetch fixture_id, home_goals, away_goals, match_date from Supabase."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        log.error("SUPABASE_URL and SUPABASE_KEY env vars required")
        sys.exit(1)

    client = create_client(url, key)
    log.info("📡 Fetching match results from Supabase...")

    rows, offset, limit = [], 0, 1000
    while True:
        resp = (
            client.table("matches")
            .select("fixture_id, home_goals, away_goals, match_date, season")
            .order("match_date", desc=False)
            .range(offset, offset + limit - 1)
            .execute()
        )
        batch = resp.data
        if not batch:
            break
        rows.extend(batch)
        log.info(f"   Fetched {len(rows)} rows so far...")
        if len(batch) < limit:
            break
        offset += limit

    df = pd.DataFrame(rows)
    log.info(f"✅ Got {len(df)} match results from Supabase")
    return df


def main():
    # ── Load features ──────────────────────────────────────
    if not INPUT_PATH.exists():
        log.error(f"Input not found: {INPUT_PATH}")
        log.error("Run: python src/feature_engineer.py first")
        sys.exit(1)

    log.info(f"📥 Reading {INPUT_PATH}...")
    features = pd.read_csv(INPUT_PATH)
    log.info(f"   Features shape: {features.shape}")

    # ── Fetch results from Supabase ────────────────────────
    results = fetch_match_results()

    # Keep only completed matches (has scores)
    results = results.dropna(subset=["home_goals", "away_goals"])
    results["home_goals"] = results["home_goals"].astype(int)
    results["away_goals"] = results["away_goals"].astype(int)

    # ── Compute result column ──────────────────────────────
    results["result"] = results.apply(
        lambda r: 2 if r["home_goals"] > r["away_goals"]
                  else (1 if r["home_goals"] == r["away_goals"] else 0),
        axis=1,
    )
    log.info(f"   Result distribution:\n{results['result'].value_counts().to_string()}")

    # ── Join features + results on fixture_id ─────────────
    df = features.merge(
        results[["fixture_id", "home_goals", "away_goals", "result", "match_date"]],
        on="fixture_id",
        how="inner",
    )
    log.info(f"   After join: {len(df)} rows (dropped {len(features) - len(df)} unmatched)")

    # ── Rename columns to match train_model.py ────────────
    df = df.rename(columns=RENAME_MAP)

    # ── New engineered features ────────────────────────────
    df["match_date"] = pd.to_datetime(df["match_date"])
    df = df.sort_values("match_date")

    # 1. Attack vs defence matchup ratio
    df["attack_vs_defence"] = (
        df["home_goals_scored_rolling"] / (df["opp_goals_conceded_rolling"] + 0.1)
    )
    df["opp_attack_vs_defence"] = (
        df["opp_goals_scored_rolling"] / (df["home_goals_conceded_rolling"] + 0.1)
    )

    # 2. Form differential — home form minus opponent form
    df["form_diff"] = df["home_form_momentum"] - df["opp_form_momentum"]

    # 3. Goal difference rolling
    df["goal_diff_rolling"] = (
        df["home_goals_scored_rolling"] - df["home_goals_conceded_rolling"]
    )
    df["opp_goal_diff_rolling"] = (
        df["opp_goals_scored_rolling"] - df["opp_goals_conceded_rolling"]
    )

    # ── Add any missing opp rolling columns (fill with 0) ─
    opp_cols = ["opp_wins_rolling", "opp_clean_sheets_rolling",
                "opp_win_streak", "opp_form_momentum"]
    for col in opp_cols:
        if col not in df.columns:
            df[col] = 0.0
            log.warning(f"   ⚠️  Missing column filled with 0: {col}")

    # ── Add league_key ────────────────────────────────────
    #df["league_key"] = "EPL"
    df["league_key"] = df["league_key"].fillna("EPL")
    log.info(f"   League distribution:\n{df['league_key'].value_counts().to_string()}")

    # ── Add match_date and season ─────────────────────────
    df["match_date"] = pd.to_datetime(df["match_date"])
    if "season" not in df.columns:
        df["season"] = df["match_date"].apply(
            lambda d: f"{d.year}-{str(d.year+1)[2:]}" if d.month >= 8
                      else f"{d.year-1}-{str(d.year)[2:]}"
        )

    # ── Final column check ────────────────────────────────
    required = [
        "league_key", "home_goals_scored_rolling", "home_goals_conceded_rolling",
        "home_wins_rolling", "home_clean_sheets_rolling", "home_win_streak",
        "home_form_momentum", "opp_goals_scored_rolling", "opp_goals_conceded_rolling",
        "opp_wins_rolling", "opp_clean_sheets_rolling", "opp_win_streak",
        "opp_form_momentum", "h2h_win_rate", "result", "match_date", "season",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        log.error(f"❌ Still missing required columns: {missing}")
        sys.exit(1)

    # ── Save ──────────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    log.info(f"✅ Saved {len(df)} rows → {OUTPUT_PATH}")
    log.info(f"   Seasons : {sorted(df['season'].unique().tolist())}")
    log.info(f"   Columns : {list(df.columns)}")


if __name__ == "__main__":
    main()
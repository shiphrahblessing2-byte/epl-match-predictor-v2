"""
Builds data/training/features_2122_2425.csv
- Fetches match results from Supabase matches table
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
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

INPUT_PATH  = Path("data/interim/matches_features.csv")
OUTPUT_DIR  = Path("data/training")
OUTPUT_PATH = OUTPUT_DIR / "features_2122_2425.csv"

# ── CHANGE 1: Extended RENAME_MAP with new source columns ──────────────────
RENAME_MAP = {
    "goals_scored_rolling":    "home_goals_scored_rolling",
    "goals_conceded_rolling":  "home_goals_conceded_rolling",
    "wins_rolling":            "home_wins_rolling",
    "clean_sheets_rolling":    "home_clean_sheets_rolling",
    "win_streak":              "home_win_streak",
    "form_momentum":           "home_form_momentum",
    "opp_wins_rolling":        "opp_wins_rolling",
    "opp_clean_sheets_rolling":"opp_clean_sheets_rolling",
    "opp_win_streak":          "opp_win_streak",
    "opp_form_momentum":       "opp_form_momentum",
    # ✅ NEW — ESPN stats
    "home_shots":              "home_shots",
    "away_shots":              "away_shots",
    "home_shots_on_target":    "home_shots_on_target",
    "away_shots_on_target":    "away_shots_on_target",
    "home_corners":            "home_corners",
    "away_corners":            "away_corners",
    "home_possession":         "home_possession",
    "away_possession":         "away_possession",
    # ✅ NEW — Injuries
    "home_injuries":           "home_injuries",
    "away_injuries":           "away_injuries",
    "injury_diff":             "injury_diff",
    # ✅ NEW — Elo + venue form (already correctly named, but explicit is safer)
    "home_elo":                "home_elo",
    "away_elo":                "away_elo",
    "elo_diff":                "elo_diff",
    "home_form_at_home":       "home_form_at_home",
    "away_form_away":          "away_form_away",
}


def fetch_match_results() -> pd.DataFrame:
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
    if not INPUT_PATH.exists():
        log.error(f"Input not found: {INPUT_PATH}")
        log.error("Run: python src/feature_engineer.py first")
        sys.exit(1)

    log.info(f"📥 Reading {INPUT_PATH}...")
    features = pd.read_csv(INPUT_PATH)
    log.info(f"   Features shape: {features.shape}")

    features = features[features["league_key"].isin(["EPL", "LIGA", "UCL", "UEL"])]
    log.info(f"   Filtered to 4 leagues: {len(features)} rows")

    results = fetch_match_results()
    results = results.dropna(subset=["home_goals", "away_goals"])
    results["home_goals"] = results["home_goals"].astype(int)
    results["away_goals"] = results["away_goals"].astype(int)

    results["result"] = results.apply(
        lambda r: 2 if r["home_goals"] > r["away_goals"]
                  else (1 if r["home_goals"] == r["away_goals"] else 0),
        axis=1,
    )
    log.info(f"   Result distribution:\n{results['result'].value_counts().to_string()}")

    df = features.merge(
        results[["fixture_id", "home_goals", "away_goals", "result", "match_date"]],
        on="fixture_id",
        how="inner",
    )
    log.info(f"   After join: {len(df)} rows (dropped {len(features) - len(df)} unmatched)")

    df = df.rename(columns=RENAME_MAP)

    df["match_date"] = pd.to_datetime(df["match_date"])
    df = df.sort_values("match_date")

    # ── Existing engineered features (unchanged) ───────────────────────────
    df["attack_vs_defence"] = (
        df["home_goals_scored_rolling"] / (df["opp_goals_conceded_rolling"] + 0.1)
    )
    df["opp_attack_vs_defence"] = (
        df["opp_goals_scored_rolling"] / (df["home_goals_conceded_rolling"] + 0.1)
    )
    df["form_diff"] = df["home_form_momentum"] - df["opp_form_momentum"]
    df["goal_diff_rolling"] = (
        df["home_goals_scored_rolling"] - df["home_goals_conceded_rolling"]
    )
    df["opp_goal_diff_rolling"] = (
        df["opp_goals_scored_rolling"] - df["opp_goals_conceded_rolling"]
    )

    # ── CHANGE 2: New engineered features from new sources ─────────────────

    # Shots on target ratio — quality of chances, not just volume
    df["home_sot_ratio"] = df["home_shots_on_target"] / (df["home_shots"] + 0.1)
    df["away_sot_ratio"] = df["away_shots_on_target"] / (df["away_shots"] + 0.1)
    df["sot_diff"] = df["home_sot_ratio"] - df["away_sot_ratio"]

    # Possession differential
    df["possession_diff"] = df["home_possession"] - df["away_possession"]

    # Corners differential — proxy for territorial dominance
    df["corners_diff"] = df["home_corners"] - df["away_corners"]

    # Shots differential
    df["shots_diff"] = df["home_shots"] - df["away_shots"]
    df["shots_on_target_diff"] = df["home_shots_on_target"] - df["away_shots_on_target"]

    # xG differential — most predictive single feature
    df["xg_diff"] = df["home_xg"] - df["away_xg"]
    df["xg_rolling_diff"] = df["home_xg_rolling"] - df["opp_xg_rolling"]

    # Injury-adjusted form — penalise teams with more injuries
    df["home_form_adj"] = df["home_form_momentum"] * (1 - df["home_injuries"] * 0.05)
    df["away_form_adj"] = df["opp_form_momentum"] * (1 - df["away_injuries"] * 0.05)
    df["form_adj_diff"] = df["home_form_adj"] - df["away_form_adj"]

    # Elo + venue form combined
    df["venue_elo_boost"] = df["elo_diff"] + (df["home_form_at_home"] * 50)

    # ── CHANGE 3: Graceful fill for ALL new columns ─────────────────────────
    new_cols_defaults = {
        # Stats
        "home_shots": 0.0, "away_shots": 0.0,
        "home_shots_on_target": 0.0, "away_shots_on_target": 0.0,
        "home_corners": 0.0, "away_corners": 0.0,
        "home_possession": 50.0, "away_possession": 50.0,
        # Injuries
        "home_injuries": 0, "away_injuries": 0, "injury_diff": 0,
        # Elo
        "home_elo": 1500.0, "away_elo": 1500.0, "elo_diff": 0.0,
        # Venue form
        "home_form_at_home": 0.0, "away_form_away": 0.0,
        # xG (UCL/UEL will be 0 — that's correct)
        "home_xg": 0.0, "away_xg": 0.0,
        "home_xg_rolling": 0.0, "opp_xg_rolling": 0.0,
        "forecast_home_win": 0.0, "forecast_draw": 0.0, "forecast_away_win": 0.0,
        # Derived
        "home_sot_ratio": 0.0, "away_sot_ratio": 0.0, "sot_diff": 0.0,
        "possession_diff": 0.0, "corners_diff": 0.0,
        "shots_diff": 0.0, "shots_on_target_diff": 0.0,
        "xg_diff": 0.0, "xg_rolling_diff": 0.0,
        "home_form_adj": 0.0, "away_form_adj": 0.0, "form_adj_diff": 0.0,
        "venue_elo_boost": 0.0,
        # Existing opp cols
        "opp_wins_rolling": 0.0, "opp_clean_sheets_rolling": 0.0,
        "opp_win_streak": 0, "opp_form_momentum": 0.0,
    }
    for col, default in new_cols_defaults.items():
        if col not in df.columns:
            df[col] = default
            log.warning(f"   ⚠️  Missing column filled with default ({default}): {col}")
        else:
            df[col] = df[col].fillna(default)

    df["league_key"] = df["league_key"].fillna("EPL")
    log.info(f"   League distribution:\n{df['league_key'].value_counts().to_string()}")

    df["match_date"] = pd.to_datetime(df["match_date"])
    if "season" not in df.columns:
        df["season"] = df["match_date"].apply(
            lambda d: f"{d.year}-{str(d.year+1)[2:]}" if d.month >= 8
                      else f"{d.year-1}-{str(d.year)[2:]}"
        )

    # ── CHANGE 4: Updated required columns list ────────────────────────────
    required = [
        # Core rolling stats
        "league_key", "home_goals_scored_rolling", "home_goals_conceded_rolling",
        "home_wins_rolling", "home_clean_sheets_rolling", "home_win_streak",
        "home_form_momentum", "opp_goals_scored_rolling", "opp_goals_conceded_rolling",
        "opp_wins_rolling", "opp_clean_sheets_rolling", "opp_win_streak",
        "opp_form_momentum", "h2h_win_rate", "result", "match_date", "season",
        # xG (0 for UCL/UEL — acceptable)
        "home_xg", "away_xg",
        "forecast_home_win", "forecast_draw", "forecast_away_win",
        "home_xg_rolling", "opp_xg_rolling",
        # ✅ NEW required
        "home_shots", "away_shots",
        "home_shots_on_target", "away_shots_on_target",
        "home_corners", "away_corners",
        "home_possession", "away_possession",
        "home_injuries", "away_injuries", "injury_diff",
        "home_elo", "away_elo", "elo_diff",
        "home_form_at_home", "away_form_away",
        # ✅ NEW derived
        "sot_diff", "possession_diff", "corners_diff",
        "xg_diff", "xg_rolling_diff",
        "form_adj_diff", "venue_elo_boost",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        log.error(f"❌ Still missing required columns: {missing}")
        sys.exit(1)

    # ── CHANGE 5: Log new feature coverage stats ───────────────────────────
    log.info("📊 New feature coverage (non-zero rows):")
    coverage_cols = [
        "home_shots", "home_possession", "home_corners",
        "home_xg", "home_injuries", "home_elo"
    ]
    for col in coverage_cols:
        if col in df.columns:
            pct = (df[col] != 0).mean() * 100
            log.info(f"   {col}: {pct:.1f}% non-zero ({int(pct * len(df) / 100)}/{len(df)} rows)")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    log.info(f"✅ Saved {len(df)} rows → {OUTPUT_PATH}")
    log.info(f"   Seasons : {sorted(df['season'].unique().tolist())}")
    log.info(f"   Total columns : {len(df.columns)}")
    log.info(f"   Columns : {list(df.columns)}")


if __name__ == "__main__":
    main()
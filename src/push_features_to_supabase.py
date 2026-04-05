"""
Push multi-league features from CSV into Supabase matches_features table.

Usage:
    python -m src.push_features_to_supabase --leagues LIGA UCL UEL
"""
import argparse
import logging
import math
import sys
from pathlib import Path

import pandas as pd
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import SUPABASE_URL, SUPABASE_KEY, validate_config

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

COL_RENAME = {
    "home_goals_scored_rolling":   "goals_scored_rolling",
    "home_goals_conceded_rolling": "goals_conceded_rolling",
    "home_wins_rolling":           "wins_rolling",
    "home_clean_sheets_rolling":   "clean_sheets_rolling",
    "home_win_streak":             "win_streak",
    "home_form_momentum":          "form_momentum",
}

# These columns are SMALLINT in Supabase — send as int, not float
SMALLINT_COLS = {"win_streak", "form_momentum", "season"}


def get_table_columns(client) -> list[str]:
    resp = client.table("matches_features").select("*").limit(1).execute()
    if resp.data:
        return list(resp.data[0].keys())
    return [
        "fixture_id", "home_team_id", "away_team_id", "league_key",
        "season", "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling", "win_streak",
        "form_momentum", "opp_goals_scored_rolling",
        "opp_goals_conceded_rolling", "h2h_win_rate",
    ]


def clean_record(record: dict) -> dict:
    """Replace NaN/Inf with None and cast SMALLINT columns to int."""
    cleaned = {}
    for k, v in record.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            cleaned[k] = None
        elif k in SMALLINT_COLS and v is not None:
            cleaned[k] = int(round(v))  # cast 2.0 → 2, 0.0 → 0
        else:
            cleaned[k] = v
    return cleaned


def push_league(df: pd.DataFrame, league: str,
                client, table_cols: list[str],
                batch_size: int = 500) -> int:
    league_df = df[df["league_key"] == league].copy()

    if league_df.empty:
        log.warning(f"⚠️  No rows found for league: {league}")
        return 0

    league_df.rename(columns=COL_RENAME, inplace=True)

    cols_to_use = [c for c in table_cols if c in league_df.columns]
    missing = [c for c in table_cols if c not in league_df.columns]
    if missing:
        log.info(f"   ℹ️  Skipping missing cols: {missing}")

    league_df = league_df[cols_to_use]
    records = [clean_record(r) for r in league_df.to_dict(orient="records")]
    total = len(records)
    log.info(f"📤 Pushing {total} rows for {league}...")

    inserted = 0
    for i in range(0, total, batch_size):
        batch = records[i: i + batch_size]
        try:
            client.table("matches_features").upsert(
                batch, on_conflict="fixture_id"
            ).execute()
            inserted += len(batch)
            log.info(f"   ✅ {inserted}/{total} rows pushed")
        except Exception as e:
            log.error(f"   ❌ Batch {i}–{i+batch_size} failed: {e}")
            log.error(f"   Sample row: {batch[0]}")

    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", nargs="+", default=["LIGA", "UCL", "UEL"])
    parser.add_argument("--source", default="data/training/features_2122_2425.csv")
    args = parser.parse_args()

    validate_config()
    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    source = Path(args.source)
    if not source.exists():
        log.error(f"❌ Source file not found: {source}")
        sys.exit(1)

    log.info("🔍 Fetching matches_features table schema...")
    table_cols = get_table_columns(client)
    log.info(f"   Table columns: {table_cols}")

    log.info(f"📥 Loading features from {source}...")
    df = pd.read_csv(source)
    log.info(f"✅ Loaded {len(df)} rows")

    total_inserted = 0
    for league in args.leagues:
        n = push_league(df, league.upper(), client, table_cols)
        total_inserted += n

    print(f"\n✅ Done — {total_inserted} total rows pushed")
    print(f"   Leagues: {', '.join(args.leagues)}")


if __name__ == "__main__":
    main()
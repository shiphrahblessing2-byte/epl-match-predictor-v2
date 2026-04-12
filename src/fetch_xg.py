"""
Fetches xG data from Understat for EPL + La Liga.
Matches fixtures via match_date + home/away goals (robust join).
FIXED: Understat season offset (Supabase 2022 = Understat "2021")
Usage: python src/fetch_xg.py --season 2023
"""
import argparse
import logging
import os
import time
from pathlib import Path

import pandas as pd
from understatapi import UnderstatClient
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

UNDERSTAT_LEAGUES = {
    "EPL":  "EPL",
    "LIGA": "La_Liga",
}

def fetch_league_xg(league_name: str, season: int) -> pd.DataFrame:
    with UnderstatClient() as understat:
        matches = understat.league(league=league_name).get_match_data(
            season=str(season)
        )

    rows = []
    for m in matches:
        if not m.get("isResult"):
            continue

        forecast = m.get("forecast") or {}

        rows.append({
            "understat_id":      m["id"],
            "match_date":        pd.to_datetime(m["datetime"]).strftime("%Y-%m-%d"),
            "home_team":         m["h"]["title"],
            "away_team":         m["a"]["title"],
            "home_goals":        int(m["goals"]["h"]),
            "away_goals":        int(m["goals"]["a"]),
            "home_xg":           float(m["xG"]["h"]),
            "away_xg":           float(m["xG"]["a"]),
            "forecast_home_win": float(forecast.get("w", 0)),
            "forecast_draw":     float(forecast.get("d", 0)),
            "forecast_away_win": float(forecast.get("l", 0)),
        })

    return pd.DataFrame(rows)

def main(season: int):
    client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
    
    # 🔧 FIXED: Understat season = Supabase season - 1
    if season == 2021:
        understat_season = 2020
    elif season <= 2023:
        understat_season = season   # 2022→"2022", 2023→"2023"
    else:
        understat_season = season - 1
    if season == 2024:
        understat_season = 2024  # Ongoing 2024/25
    else:
        understat_season = season
    log.info(f"🔄 Supabase {season} → Understat '{understat_season}'")
    log.info(f"🔄 Supabase season {season} → Understat '{understat_season}'")
    
    # Load fixtures (unchanged)
    all_rows = []
    offset, limit = 0, 1000
    while True:
        resp = client.table("matches").select(
            "fixture_id, match_date, league_key, home_goals, away_goals"
        ).eq("season", season) \
         .in_("league_key", ["EPL", "LIGA"]) \
         .not_.is_("result", "null") \
         .range(offset, offset + limit - 1) \
         .execute()
        batch = resp.data
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    
    fixtures = pd.DataFrame(all_rows)
    fixtures["match_date"] = pd.to_datetime(fixtures["match_date"]).dt.strftime("%Y-%m-%d")
    fixtures["home_goals"] = fixtures["home_goals"].astype(int)
    fixtures["away_goals"] = fixtures["away_goals"].astype(int)
    log.info(f"Loaded {len(fixtures)} fixtures from Supabase for season {season}")

    all_xg = []
    for league_key, understat_name in UNDERSTAT_LEAGUES.items():
        league_fixtures = fixtures[fixtures["league_key"] == league_key].copy()
        if league_fixtures.empty:
            log.warning(f"  No fixtures for {league_key} season {season}")
            continue

        try:
            xg_df = fetch_league_xg(understat_name, understat_season)  # 🔧 FIXED!
            xg_df["league_key"] = league_key

            # Join on date + scoreline
            merged = league_fixtures.merge(
                xg_df, on=["match_date", "home_goals", "away_goals"], how="inner"
            )

            merged = merged.drop_duplicates(subset=["fixture_id"], keep="first")
            merged = merged.drop_duplicates(subset=["understat_id"], keep="first")
            log.info(f"  {league_key}: matched {len(merged)}/{len(league_fixtures)} fixtures")

            merged = merged.drop_duplicates(subset=["fixture_id"])
            all_xg.append(merged[[
                "fixture_id", "home_xg", "away_xg",
                "forecast_home_win", "forecast_draw", "forecast_away_win"
            ]])

        except Exception as e:
            log.error(f"  Failed {league_key}: {e}")

        time.sleep(2)

    if not all_xg:
        log.error("No xG data matched — check season/league availability")
        return

    xg_combined = pd.concat(all_xg, ignore_index=True)
    log.info(f"Total matched: {len(xg_combined)} fixtures")

    # Upsert (unchanged)
    rows = xg_combined.to_dict(orient="records")
    for i in range(0, len(rows), 100):
        client.table("match_xg").upsert(
            rows[i:i+100], on_conflict="fixture_id"
        ).execute()
        log.info(f"  Batch {i//100+1}: {len(rows[i:i+100])} rows → match_xg")

    log.info(f"✅ Done: {len(rows)} fixtures with xG saved for season {season} (Understat {understat_season})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    main(args.season)
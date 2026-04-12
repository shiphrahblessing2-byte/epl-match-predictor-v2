"""
Fetches shots, possession, corners per match from ESPN summary endpoint.
Stores in Supabase match_stats table.
Usage: python src/fetch_match_stats.py --season 2024
"""
import argparse
import logging
import os
import time
from pathlib import Path
import requests
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

LEAGUE_CODES = {
    "EPL":  "eng.1",
    "LIGA": "esp.1",
    "UCL":  "uefa.champions",
    "UEL":  "uefa.europa",
}

def fetch_stats_for_fixture(fixture_id: int, league_code: str) -> dict | None:
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league_code}/summary"
    try:
        resp = requests.get(url, params={"event": fixture_id}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        box = data.get("boxscore", {})
        teams = box.get("teams", [])
        if len(teams) < 2:
            return None

        def extract(team_data, stat_name):
            for stat in team_data.get("statistics", []):
                name = stat.get("name", "").lower()
                if stat_name in name:  # Fuzzy match
                    return float(stat.get("displayValue", "0").replace("%", "") or 0)
            return None

        # Updated calls:
        return {
            "fixture_id": fixture_id,
            "home_shots": extract(teams[0], "shot"),           # ✅ "totalShots"
            "away_shots": extract(teams[1], "shot"),
            "home_shots_on_target": extract(teams[0], "goal"), # ✅ "shotsOnGoal"
            "away_shots_on_target": extract(teams[1], "goal"),
            "home_possession": extract(teams[0], "possession"), # ✅ works
            "away_possession": extract(teams[1], "possession"),
            "home_corners": extract(teams[0], "corner"),       # ✅ "corners"
            "away_corners": extract(teams[1], "corner"),
        }
    except Exception as e:
        log.warning(f"  Failed fixture {fixture_id}: {e}")
        return None


def main(season: int):
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    client = create_client(url, key)

    # Load fixture IDs from Supabase for this season
    resp = client.table("matches").select(
        "fixture_id, league_key"
    ).eq("season", season).not_.is_("result", "null").execute()

    fixtures = pd.DataFrame(resp.data)
    log.info(f"Fetching stats for {len(fixtures)} fixtures (season {season})...")

    rows = []
    for i, row in fixtures.iterrows():
        league_code = LEAGUE_CODES.get(row["league_key"])
        if not league_code:
            continue
        stats = fetch_stats_for_fixture(row["fixture_id"], league_code)
        if stats:
            rows.append(stats)
        if i % 50 == 0:
            log.info(f"  {i}/{len(fixtures)} done...")
        time.sleep(0.5)  # be polite to ESPN

    if not rows:
        log.error("No stats fetched — check ESPN API response format")
        return

    # Upsert to Supabase
    for i in range(0, len(rows), 100):
        client.table("match_stats").upsert(
            rows[i:i+100], on_conflict="fixture_id"
        ).execute()
        log.info(f"  Batch {i//100 + 1}: {len(rows[i:i+100])} rows → match_stats")

    log.info(f"✅ Done: {len(rows)} fixtures with stats saved")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    main(args.season)
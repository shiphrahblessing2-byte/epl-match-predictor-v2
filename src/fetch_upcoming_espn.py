"""
Fetch upcoming EPL fixtures from ESPN's public API.
No API key required. No registration. Always free.
Endpoint: site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard
"""
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
import requests
from supabase import create_client

sys.path.insert(0, str(Path(__file__).parent))
from config import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard"

# ESPN team ID → your ML model's api-sports team ID
ESPN_TEAM_MAP = {
    # ✅ Confirmed correct mappings
    "331":  51,   # Brighton
    "349":  73,   # Bournemouth
    "359":  42,   # Arsenal          ← WAS 33 (Man Utd) — CRITICAL FIX
    "360":  33,   # Manchester United ← WAS missing
    "361":  34,   # Newcastle United
    "362":  66,   # Aston Villa
    "363":  49,   # Chelsea
    "364":  40,   # Liverpool
    "367":  47,   # Tottenham
    "368":  45,   # Everton
    "370":  67,   # Fulham
    "371":  48,   # West Ham
    "380":  71,   # Wolves
    "382":  50,   # Manchester City
    "384":  52,   # Crystal Palace
    "393":  65,   # Nottingham Forest ← WAS unmapped (393)
    "397":  55,   # Brentford
    # ⚠️  Newly promoted — no training data, use closest proxy
    "366":  41,   # Sunderland → proxy: Southampton (similar promoted profile)
    "379":  46,   # Burnley → proxy: Leicester (similar mid-table profile)
    "333":  57,   # Ipswich
    "375":  46,   # Leicester
}


def fetch_week_fixtures(date_str: str = None) -> list[dict]:
    """Fetch fixtures for a specific date (YYYYMMDD) or current week."""
    params = {}
    if date_str:
        params["dates"] = date_str
    resp = requests.get(ESPN_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("events", [])


def fetch_upcoming_fixtures(weeks_ahead: int = 2) -> list[dict]:
    """Fetch fixtures for the next N weeks."""
    all_events = []
    today = datetime.now(timezone.utc)

    for week in range(weeks_ahead):
        target = today + timedelta(weeks=week)
        date_str = target.strftime("%Y%m%d")
        log.info(f"  Fetching week of {date_str}...")
        try:
            events = fetch_week_fixtures(date_str)
            # Keep only scheduled (not started/finished)
            upcoming = [e for e in events if e.get("status", {}).get("type", {}).get("state") == "pre"]
            all_events.extend(upcoming)
            log.info(f"  Got {len(upcoming)} upcoming fixtures")
        except Exception as e:
            log.warning(f"  Failed for week {date_str}: {e}")

    return all_events


def parse_event(event: dict) -> dict | None:
    """Convert ESPN event → Supabase matches row."""
    try:
        comp  = event["competitions"][0]
        home  = next(t for t in comp["competitors"] if t["homeAway"] == "home")
        away  = next(t for t in comp["competitors"] if t["homeAway"] == "away")

        espn_home_id = home["team"]["id"]
        espn_away_id = away["team"]["id"]

        # Map ESPN IDs to your model's team IDs
        home_id = ESPN_TEAM_MAP.get(espn_home_id, int(espn_home_id))
        away_id = ESPN_TEAM_MAP.get(espn_away_id, int(espn_away_id))

        return {
            "fixture_id":    int(event["id"]),
            "match_date":    event["date"],
            "season":        "2025",
            "league_round":  event.get("week", {}).get("number", ""),
            "home_team_id":  home_id,
            "away_team_id":  away_id,
            "home_goals":    None,
            "away_goals":    None,
            "result":        None,
            "status_short":  "NS",
            # For display in logs
            "_home_name":    home["team"]["displayName"],
            "_away_name":    away["team"]["displayName"],
            "_espn_home_id": espn_home_id,
            "_espn_away_id": espn_away_id,
        }
    except (KeyError, StopIteration) as e:
        log.warning(f"  Skipping event {event.get('id')}: {e}")
        return None


def run(dry_run: bool = False):
    log.info("Fetching upcoming EPL fixtures from ESPN (no API key needed)...")
    events  = fetch_upcoming_fixtures(weeks_ahead=4)
    parsed  = [parse_event(e) for e in events]
    rows    = [r for r in parsed if r is not None]

    if not rows:
        log.warning("No upcoming fixtures found")
        return 0

    log.info(f"\nFound {len(rows)} upcoming fixtures:")
    for r in rows:
        log.info(
            f"  {r['fixture_id']}: "
            f"{r['_home_name']} (espn:{r['_espn_home_id']}→model:{r['home_team_id']}) vs "
            f"{r['_away_name']} (espn:{r['_espn_away_id']}→model:{r['away_team_id']}) "
            f"on {str(r['match_date'])[:10]}"
        )

    if dry_run:
        log.info("\nDRY RUN — skipping Supabase write")
        return len(rows)

    # Strip internal display fields before inserting
    safe_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]

    db = create_client(SUPABASE_URL, SUPABASE_KEY)
    db.table("matches").upsert(safe_rows, on_conflict="fixture_id").execute()
    log.info(f"\n✅ Upserted {len(safe_rows)} fixtures into Supabase")
    return len(safe_rows)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    count = run(dry_run=args.dry_run)
    print(f"\n{'DRY RUN: ' if args.dry_run else ''}✅ {count} fixtures ready")

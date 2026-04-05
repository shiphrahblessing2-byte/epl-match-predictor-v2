"""
Fetch fixtures from multiple leagues via ESPN public API.
Covers: Premier League, La Liga, Champions League, Europa League.
Fetches both past results (last 2 weeks) and upcoming (next 4 weeks).
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

# Updated LEAGUES — add weekly flag
LEAGUES = {
    "EPL":  {"code": "eng.1",          "name": "Premier League",   "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "weekly": True},
    "UCL":  {"code": "uefa.champions",  "name": "Champions League", "flag": "⭐",         "weekly": False},
    "UEL":  {"code": "uefa.europa",     "name": "Europa League",    "flag": "🟠",         "weekly": False},
    "LIGA": {"code": "esp.1",           "name": "La Liga",          "flag": "🇪🇸",         "weekly": True},
}

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

# EPL team ID mappings (ESPN → model)
ESPN_TEAM_MAP = {
    "331": 51,  "349": 73,  "359": 42,  "360": 33,  "361": 34,
    "362": 66,  "363": 49,  "364": 40,  "367": 47,  "368": 45,
    "370": 67,  "371": 48,  "380": 71,  "382": 50,  "384": 52,
    "393": 65,  "397": 55,  "366": 41,  "379": 46,  "333": 57,
    "375": 46,
}


def fetch_fixtures_for_date(league_code: str, date_str: str) -> list[dict]:
    """Fetch fixtures for a specific date from ESPN."""
    url = f"{ESPN_BASE}/{league_code}/scoreboard"
    try:
        resp = requests.get(url, params={"dates": date_str}, timeout=15)
        resp.raise_for_status()
        return resp.json().get("events", [])
    except Exception as e:
        log.warning(f"  Failed {league_code} on {date_str}: {e}")
        return []


def parse_event(event: dict, league_key: str) -> dict | None:
    """Convert ESPN event → Supabase row."""
    try:
        comp   = event["competitions"][0]
        home   = next(t for t in comp["competitors"] if t["homeAway"] == "home")
        away   = next(t for t in comp["competitors"] if t["homeAway"] == "away")
        status = event.get("status", {}).get("type", {}).get("state", "pre")

        espn_home_id = home["team"]["id"]
        espn_away_id = away["team"]["id"]

        # Map team IDs — EPL uses lookup, others use ESPN IDs directly
        home_id = ESPN_TEAM_MAP.get(espn_home_id, int(espn_home_id))
        away_id = ESPN_TEAM_MAP.get(espn_away_id, int(espn_away_id))

        # Parse scores for finished matches
        home_goals = None
        away_goals = None
        result     = None
        if status == "post":
            try:
                home_goals = int(home.get("score", 0))
                away_goals = int(away.get("score", 0))
                result = 2 if home_goals > away_goals else (1 if home_goals == away_goals else 0)
            except (ValueError, TypeError):
                pass

        return {
            "fixture_id":    int(event["id"]),
            "match_date":    event["date"],
            "season":        "2025",
            "league_key":    league_key,
            "league_name":   LEAGUES[league_key]["name"],
            "league_round":  str(event.get("week", {}).get("number", "")),
            "home_team_id":  home_id,
            "away_team_id":  away_id,
            "home_team":     home["team"]["displayName"],
            "away_team":     away["team"]["displayName"],
            "home_goals":    home_goals,
            "away_goals":    away_goals,
            "result":        result,
            "status_short":  "FT" if status == "post" else ("LIVE" if status == "in" else "NS"),
        }
    except (KeyError, StopIteration, ValueError) as e:
        log.warning(f"  Skipping event {event.get('id')}: {e}")
        return None


def run(dry_run: bool = False):
    today  = datetime.now(timezone.utc)
    all_rows = []

    for league_key, league_info in LEAGUES.items():
        code = league_info["code"]
        log.info(f"\n{'='*50}")
        log.info(f"Fetching {league_info['name']} ({code})...")

        if league_info["weekly"]:
            # Weekly leagues — check weekly intervals (-2 to +4 weeks)
            offsets = [timedelta(weeks=w) for w in range(-2, 4)]
        else:
            # Mid-week (UCL/UEL) — scan every day for past 14 + next 30 days
            offsets = [timedelta(days=d) for d in range(-14, 30)]

        seen_dates = set()
        for offset in offsets:
            target   = today + offset
            date_str = target.strftime("%Y%m%d")
            if date_str in seen_dates:
                continue
            seen_dates.add(date_str)

            events = fetch_fixtures_for_date(code, date_str)
            for event in events:
                row = parse_event(event, league_key)
                if row:
                    all_rows.append(row)

            if events:
                label = "past" if offset.days < 0 else ("current" if offset.days == 0 else "upcoming")
                log.info(f"  {date_str} ({label}): {len(events)} fixtures")

    # Deduplicate by fixture_id
    seen     = set()
    unique   = []
    for r in all_rows:
        if r["fixture_id"] not in seen:
            seen.add(r["fixture_id"])
            unique.append(r)

    log.info(f"\n✅ Total: {len(unique)} unique fixtures across all leagues")

    if dry_run:
        for r in unique:
            log.info(
                f"  [{r['league_key']}] {r['status_short']} "
                f"{r['home_team']} vs {r['away_team']} "
                f"on {str(r['match_date'])[:10]}"
                + (f" — {r['home_goals']}-{r['away_goals']}" if r['home_goals'] is not None else "")
            )
        log.info("\nDRY RUN — skipping Supabase write")
        return len(unique)

    # Upsert to Supabase in batches
    db = create_client(SUPABASE_URL, SUPABASE_KEY)
    for i in range(0, len(unique), 50):
        batch = unique[i:i+50]
        db.table("matches").upsert(batch, on_conflict="fixture_id").execute()
        log.info(f"  Upserted batch {i//50 + 1}: {len(batch)} rows")

    log.info(f"\n✅ Done — {len(unique)} fixtures in Supabase")
    return len(unique)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    count = run(dry_run=args.dry_run)
    print(f"\n{'DRY RUN: ' if args.dry_run else ''}✅ {count} fixtures ready")

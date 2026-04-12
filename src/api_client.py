"""
Layer 1 — External Sources (ESPN Free API)
Layer 2 — Data Ingestion

Fetches fixtures from ESPN for EPL, La Liga, UCL, UEL.
No API key required — ESPN is completely free.

Usage:
    python3 src/api_client.py               # fetch current season
    python3 src/api_client.py --season 2024 # fetch specific season
    python3 src/api_client.py --dry-run     # fetch + validate, no DB write
"""
import argparse
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests
from supabase import create_client

from config import (
    SUPABASE_URL,
    SUPABASE_KEY,
    EPL_SEASON,
    CACHE_DIR,
    DATA_RAW_DIR,
    validate_config,
)

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("api.log"),
    ],
)
log = logging.getLogger(__name__)

# ── ESPN League Config ─────────────────────────────────────
ESPN_LEAGUES = {
    "EPL":  {"slug": "eng.1",         "name": "Premier League"},
    "LIGA": {"slug": "esp.1",         "name": "La Liga"},
    "UCL":  {"slug": "uefa.champions","name": "Champions League"},
    "UEL":  {"slug": "uefa.europa",   "name": "Europa League"},
    "BUN":  {"slug": "ger.1",     "name": "Bundesliga",  "flag": "🇩🇪"},  # ← ADD
    "SA":   {"slug": "ita.1",     "name": "Serie A",     "flag": "🇮🇹"},  # ← ADD
}

# Season date ranges — ESPN uses year the season STARTS (2024 = 2024/25)
SEASON_DATES = {
    2020: ("20200901", "20210531"),
    2021: ("20210801", "20220531"),
    2022: ("20220801", "20230531"),
    2023: ("20230801", "20240531"),
    2024: ("20240801", "20250531"),
    2025: ("20250801", "20260430"),
}

REQUEST_DELAY = 0.5   # seconds between ESPN calls — be polite


# ══════════════════════════════════════════════════════════
# ESPN API CLIENT
# ══════════════════════════════════════════════════════════

class ESPNClient:
    """Free ESPN scoreboard API — no key required."""

    BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def fetch_scoreboard(self, slug: str, date_str: str) -> list[dict]:
        """Fetch all events for a league on a specific date (YYYYMMDD)."""
        url    = f"{self.BASE}/{slug}/scoreboard"
        params = {"dates": date_str, "limit": 100}

        try:
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return data.get("events", [])
        except Exception as e:
            log.warning(f"  ESPN fetch failed for {slug} on {date_str}: {e}")
            return []

    def fetch_season(self, slug: str, league_key: str,
                 season: int) -> list[dict]:
        """
        Fetch all completed fixtures using ESPN date range parameter.
        One call per month instead of one call per week.
        """
        dates = SEASON_DATES.get(season)
        if not dates:
            log.error(f"No date range defined for season {season}")
            return []

        start = datetime.strptime(dates[0], "%Y%m%d")
        end   = datetime.strptime(dates[1], "%Y%m%d")

        all_events = []

        # ✅ Scan month by month — ESPN supports date ranges YYYYMMDD-YYYYMMDD
        current = start
        while current <= end:
            # Build month range: first day → last day of month
            if current.month == 12:
                month_end = current.replace(day=31)
            else:
                month_end = (current.replace(month=current.month + 1, day=1)
                            - timedelta(days=1))
            month_end = min(month_end, end)

            date_range = f"{current.strftime('%Y%m%d')}-{month_end.strftime('%Y%m%d')}"

            url    = f"{self.BASE}/{slug}/scoreboard"
            params = {"dates": date_range, "limit": 1000}

            try:
                resp = self.session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                data   = resp.json()
                events = data.get("events", [])

                # Filter completed only
                completed = [
                    e for e in events
                    if e.get("competitions", [{}])[0]
                    .get("status", {})
                    .get("type", {})
                    .get("completed", False)
                ]

                if completed:
                    all_events.extend(completed)
                    log.info(f"    {date_range}: {len(completed)} completed matches")

            except Exception as e:
                log.warning(f"  ESPN fetch failed {slug} {date_range}: {e}")

            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)

            time.sleep(REQUEST_DELAY)

        log.info(f"  ✅ Total: {len(all_events)} events fetched")
        return all_events


# ══════════════════════════════════════════════════════════
# PARSE ESPN EVENT → Supabase row
# ══════════════════════════════════════════════════════════

def parse_espn_event(event: dict, league_key: str, season: int) -> dict | None:
    """Flatten ESPN event → matches row. Returns None if invalid."""
    try:
        comp        = event["competitions"][0]
        competitors = comp["competitors"]

        # Find home and away
        home = next((c for c in competitors if c["homeAway"] == "home"), None)
        away = next((c for c in competitors if c["homeAway"] == "away"), None)

        if not home or not away:
            return None

        home_goals = int(home.get("score", 0) or 0)
        away_goals = int(away.get("score", 0) or 0)
        result     = 2 if home_goals > away_goals else (
                     1 if home_goals == away_goals else 0)

        return {
            "fixture_id":   int(event["id"]),
            "match_date":   comp.get("date", event.get("date")),
            "season":       season,
            "league_key":   league_key,
            "league_round": event.get("week", {}).get("number") if isinstance(
                            event.get("week"), dict) else None,
            "espn_event_id": event["id"],
            "home_team_id": int(home["team"]["id"]),
            "away_team_id": int(away["team"]["id"]),
            "home_goals":   home_goals,
            "away_goals":   away_goals,
            "result":       result,
            "status_short": "FT",
        }
    except (KeyError, ValueError, TypeError) as e:
        log.warning(f"  Could not parse event {event.get('id')}: {e}")
        return None


def parse_all_events(events: list[dict], league_key: str,
                     season: int) -> pd.DataFrame:
    """Parse + deduplicate all ESPN events into a DataFrame."""
    rows = []
    for event in events:
        row = parse_espn_event(event, league_key, season)
        if row:
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset=["fixture_id"])
    if len(df) < before:
        log.info(f"  Removed {before - len(df)} duplicate fixture_ids")

    return df


# ══════════════════════════════════════════════════════════
# SUPABASE WRITER
# ══════════════════════════════════════════════════════════

def write_to_supabase(df: pd.DataFrame, table: str, client) -> int:
    """Upsert rows to Supabase in batches of 100."""
    if df.empty:
        return 0

    rows    = df.to_dict(orient="records")
    written = 0

    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        client.table(table).upsert(batch, on_conflict="fixture_id").execute()
        written += len(batch)
        log.info(f"  Batch {i//100 + 1}: {len(batch)} rows → {table}")

    log.info(f"✅ Written {written} rows → Supabase.{table}")
    return written


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main(season: int = EPL_SEASON, dry_run: bool = False):
    print("\n🚀 Layer 1 & 2 — ESPN Multi-League Data Ingestion")
    print("=" * 55)
    print(f"   Leagues : EPL, La Liga, UCL, UEL")
    print(f"   Season  : {season} ({season}/{str(season+1)[-2:]})")
    print(f"   Source  : ESPN Free API (no key required)")
    print(f"   Mode    : {'DRY RUN (no DB write)' if dry_run else 'LIVE'}")
    print("=" * 55)

    validate_config()
    espn     = ESPNClient()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    all_dfs  = []

    for league_key, league_info in ESPN_LEAGUES.items():
        slug        = league_info["slug"]
        league_name = league_info["name"]

        print(f"\n📥 Fetching {league_name} ({slug}) season {season}...")

        # Try cache first
        cache_path = CACHE_DIR / f"{league_key}_{season}_espn.json"
        if cache_path.exists():
            log.info(f"  📦 Cache hit → {cache_path}")
            with open(cache_path) as f:
                events = json.load(f)
        else:
            events = espn.fetch_season(slug, league_key, season)
            # Cache raw events
            with open(cache_path, "w") as f:
                json.dump(events, f)
            log.info(f"  💾 Cached {len(events)} events → {cache_path}")

        if not events:
            log.warning(f"  ⚠️  No events found for {league_name}")
            continue

        # Parse into DataFrame
        df = parse_all_events(events, league_key, season)

        if df.empty:
            log.warning(f"  ⚠️  No valid rows parsed for {league_name}")
            continue

        print(f"  ✅ {len(df)} valid fixtures parsed")

        # CSV backup
        csv_path = DATA_RAW_DIR / f"{league_key.lower()}_espn_{season}.csv"
        df.to_csv(csv_path, index=False)
        log.info(f"  💾 CSV → {csv_path}")

        all_dfs.append(df)

        # Write to Supabase
        if not dry_run:
            written = write_to_supabase(df, "matches", supabase)
            print(f"  🗄️  Written {written} rows → Supabase.matches")

    if not all_dfs:
        print("\n❌ No data fetched for any league.")
        return None

    combined = pd.concat(all_dfs, ignore_index=True)

    print(f"\n✅ Pipeline complete:")
    print(f"   Total rows : {len(combined)}")
    print(f"   Breakdown  :")
    print(combined["league_key"].value_counts().to_string())

    if dry_run:
        print("\n✅ DRY RUN complete — Supabase write skipped")
        print(combined[["fixture_id","league_key","match_date",
                        "home_team_id","away_team_id",
                        "home_goals","away_goals","result"]].head(5).to_string(index=False))

    return combined


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ESPN Multi-League Ingestion")
    parser.add_argument("--season",  type=int, default=EPL_SEASON)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(season=args.season, dry_run=args.dry_run)
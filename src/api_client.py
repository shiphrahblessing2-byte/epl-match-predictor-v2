"""
Layer 1 — External Sources
Layer 2 — Data Ingestion

Fetches EPL fixtures from API-Football, validates them,
caches locally, and writes to Supabase.

Usage:
    python3 src/api_client.py               # fetch current season
    python3 src/api_client.py --season 2024 # fetch specific season
    python3 src/api_client.py --dry-run     # fetch + validate, no DB write
"""
import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from supabase import create_client

from config import (
    API_FOOTBALL_KEY,
    API_FOOTBALL_BASE,
    SUPABASE_URL,
    SUPABASE_KEY,
    EPL_LEAGUE_ID,
    EPL_SEASON,
    CACHE_DIR,
    DATA_RAW_DIR,
    validate_config,
)

from validate_data import validate_and_parse

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

# ── Constants ──────────────────────────────────────────────
MAX_RETRIES   = 3
RETRY_DELAY   = 5       # seconds between retries
REQUEST_DELAY = 1.2     # seconds between API calls (rate-limit safety)
DAILY_LIMIT   = 100     # free tier limit


# ══════════════════════════════════════════════════════════
# LAYER 1 — API Client
# ══════════════════════════════════════════════════════════

class APIFootballClient:
    """Thin wrapper around API-Football v3 with retry + rate-limit tracking."""

    def __init__(self, api_key: str):
        self.api_key  = api_key
        self.base_url = API_FOOTBALL_BASE
        self.session  = requests.Session()
        self.session.headers.update({
            "x-apisports-key": self.api_key,
            "x-rapidapi-host": "v3.football.api-sports.io",
        })
        self.requests_remaining = DAILY_LIMIT
        self.requests_used      = 0

    def _get(self, endpoint: str, params: dict) -> dict:
        """
        Make a GET request with retry logic and rate-limit tracking.
        Raises RuntimeError if all retries fail.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                log.info(f"GET {endpoint} params={params} (attempt {attempt})")
                resp = self.session.get(url, params=params, timeout=15)

                # Track rate limit from response headers
                remaining = resp.headers.get("x-ratelimit-requests-remaining")
                if remaining is not None:
                    self.requests_remaining = int(remaining)
                    self.requests_used += 1
                    log.info(f"  Rate limit: {self.requests_remaining} requests remaining today")

                # Stop if quota nearly gone — preserve 2 requests as buffer
                if self.requests_remaining <= 2:
                    log.warning("⚠️  Only 2 API requests remaining today — stopping")
                    raise RuntimeError("Daily quota nearly exhausted — aborting")

                resp.raise_for_status()
                data = resp.json()

                # API-level error check (200 can still contain errors)
                if data.get("errors"):
                    log.error(f"  API errors: {data['errors']}")
                    raise ValueError(f"API returned errors: {data['errors']}")

                log.info(f"  ✅ {data.get('results', 0)} results")
                time.sleep(REQUEST_DELAY)
                return data

            except (requests.Timeout, requests.ConnectionError) as e:
                log.warning(f"  Network error: {e}. Retrying in {RETRY_DELAY}s...")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)
                else:
                    raise RuntimeError(f"All {MAX_RETRIES} retries failed: {e}") from e

            except requests.HTTPError as e:
                if resp.status_code == 429:
                    log.warning("  Rate limited (429). Waiting 60s...")
                    time.sleep(60)
                elif resp.status_code in (499, 500):
                    log.warning(f"  Server error ({resp.status_code}). Retrying in {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                else:
                    raise RuntimeError(f"HTTP {resp.status_code}: {e}") from e

        raise RuntimeError(f"All retries failed for {endpoint}")

    def check_status(self) -> dict:
        """Check API quota status — costs 1 request."""
        return self._get("status", {})

    def fetch_fixtures(self, league_id: int, season: int) -> list[dict]:
        """
        Fetch all finished fixtures for a league-season.
        Caches raw JSON response locally before returning.
        """
        log.info(f"📥 Fetching fixtures: league={league_id} season={season}")

        data = self._get("fixtures", {
            "league": league_id,
            "season": season,
            "status": "FT",    # finished matches only
        })

        fixtures = data.get("response", [])
        log.info(f"✅ Fetched {len(fixtures)} finished fixtures")

        # Cache raw response locally — avoids repeat API hit same day
        cache_path = CACHE_DIR / f"fixtures_{league_id}_{season}_{datetime.now().strftime('%Y%m%d')}.json"
        with open(cache_path, "w") as f:
            json.dump(data, f, indent=2)
        log.info(f"💾 Cached raw response → {cache_path}")

        return fixtures

    def fetch_upcoming(self, league_id: int, season: int, next_n: int = 10) -> list[dict]:
        """Fetch upcoming (not yet played) fixtures for next-matchweek predictions."""
        log.info(f"📥 Fetching upcoming fixtures: next={next_n}")
        data = self._get("fixtures", {
            "league": league_id,
            "season": season,
            "status": "NS",    # not started
            "next":   next_n,
        })
        return data.get("response", [])


# ══════════════════════════════════════════════════════════
# LAYER 2 — Data Validation (DQ-001 → DQ-010)
# ══════════════════════════════════════════════════════════

KNOWN_EPL_TEAM_IDS = {
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
    43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
    55, 66, 71, 85, 529, 715, 741, 762, 777,
    2284, 2285, 2288,
}

def validate_fixture(fix: dict, season: int) -> tuple[bool, list[str]]:
    """
    Run DQ-001 → DQ-010 on a single raw API-Football fixture dict.
    Returns (is_valid, [errors + warnings]).
    DQ-007 is a soft warning (kept). All others are hard drops.
    """
    errors = []
    f   = fix.get("fixture", {})
    t   = fix.get("teams",   {})
    g   = fix.get("goals",   {})
    lea = fix.get("league",  {})

    # DQ-001
    if not f.get("id"):
        errors.append("DQ-001: null fixture_id")

    # DQ-002
    try:
        datetime.fromisoformat(f.get("date", "").replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        errors.append("DQ-002: invalid match_date")

    # DQ-003
    home_g = g.get("home")
    away_g = g.get("away")
    if home_g is None or away_g is None:
        errors.append("DQ-003: missing goals")
    elif not (isinstance(home_g, int) and isinstance(away_g, int)
              and home_g >= 0 and away_g >= 0):
        errors.append("DQ-003: goals not non-negative integers")

    # DQ-004
    if home_g is not None and away_g is not None:
        result = 2 if home_g > away_g else (1 if home_g == away_g else 0)
        if result not in (0, 1, 2):
            errors.append("DQ-004: result not in {0,1,2}")

    # DQ-005
    status = f.get("status", {}).get("short", "")
    if status != "FT":
        errors.append(f"DQ-005: status={status} not FT")

    # DQ-007 (soft warn — not a hard drop)
    home_id = t.get("home", {}).get("id")
    away_id = t.get("away", {}).get("id")
    warnings = []
    if home_id and home_id not in KNOWN_EPL_TEAM_IDS:
        warnings.append(f"DQ-007: unknown home_team_id={home_id}")
    if away_id and away_id not in KNOWN_EPL_TEAM_IDS:
        warnings.append(f"DQ-007: unknown away_team_id={away_id}")

    # DQ-008
    try:
        match_dt = datetime.fromisoformat(f.get("date", "").replace("Z", "+00:00"))
        if match_dt > datetime.now(timezone.utc):
            errors.append("DQ-008: match_date is in the future")
    except (ValueError, AttributeError):
        pass  # already caught in DQ-002

    # DQ-009
    if lea.get("season") != season:
        errors.append(f"DQ-009: season mismatch {lea.get('season')} != {season}")

    return len(errors) == 0, errors + warnings


def parse_fixture(fix: dict) -> dict:
    """Flatten raw API-Football fixture → Supabase matches row."""
    f   = fix["fixture"]
    t   = fix["teams"]
    g   = fix["goals"]
    lea = fix["league"]

    home_goals = g.get("home", 0) or 0
    away_goals = g.get("away", 0) or 0
    result     = 2 if home_goals > away_goals else (1 if home_goals == away_goals else 0)

    return {
        "fixture_id":   f["id"],
        "match_date":   f["date"],
        "season":       lea["season"],
        "league_round": lea.get("round"),
        "home_team_id": t["home"]["id"],
        "away_team_id": t["away"]["id"],
        "home_goals":   home_goals,
        "away_goals":   away_goals,
        "result":       result,
        "status_short": f["status"]["short"],
    }


def validate_and_parse(fixtures: list[dict], season: int) -> tuple[pd.DataFrame, dict]:
    """Run all DQ checks and return (clean_df, stats)."""
    rows    = []
    dropped = 0
    warned  = 0

    for fix in fixtures:
        is_valid, messages = validate_fixture(fix, season)
        hard_errors = [m for m in messages if not m.startswith("DQ-007")]
        soft_warns  = [m for m in messages if m.startswith("DQ-007")]

        if soft_warns:
            warned += 1

        if not is_valid and hard_errors:
            dropped += 1
            continue

        rows.append(parse_fixture(fix))

    df = pd.DataFrame(rows)

    # DQ-006 — deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["fixture_id"])
    dupes = before - len(df)
    if dupes:
        log.warning(f"DQ-006: removed {dupes} duplicate fixture_ids")

    stats = {
        "total":   len(fixtures),
        "passed":  len(df),
        "dropped": dropped,
        "warned":  warned,
        "dupes":   dupes,
    }

    log.info(
        f"✅ Validation: {stats['passed']} passed | "
        f"{stats['dropped']} dropped | "
        f"{stats['warned']} warned"
    )
    return df, stats


# ══════════════════════════════════════════════════════════
# LAYER 2 — Supabase Writer
# ══════════════════════════════════════════════════════════

def write_to_supabase(df: pd.DataFrame, table: str, client) -> int:
    """Upsert rows to Supabase in batches of 100."""
    if df.empty:
        log.warning(f"No rows to write to {table}")
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
# LAYER 1 — Cache + CSV Fallback
# ══════════════════════════════════════════════════════════

def load_from_cache(league_id: int, season: int) -> list[dict] | None:
    """Return today's cached JSON if it exists, else None."""
    today = datetime.now().strftime("%Y%m%d")
    path  = CACHE_DIR / f"fixtures_{league_id}_{season}_{today}.json"
    if path.exists():
        log.info(f"📦 Cache hit → {path}")
        with open(path) as f:
            return json.load(f).get("response", [])
    return None


GITHUB_CSV_URLS = {
    # All pointing to openfootball — consistent schema
    2022: "https://raw.githubusercontent.com/openfootball/england/master/2022-23/1-epl.csv",
    2023: "https://raw.githubusercontent.com/openfootball/england/master/2023-24/1-epl.csv",
    2024: "https://raw.githubusercontent.com/openfootball/england/master/2024-25/1-epl.csv",
}

def load_from_csv_fallback(season: int) -> pd.DataFrame:
    # 1st fallback — local CSV
    local_path = DATA_RAW_DIR / f"epl_raw_39_{season}.csv"
    if local_path.exists():
        log.info(f"📂 Local CSV fallback → {local_path}")
        return pd.read_csv(local_path)

    # 2nd fallback — GitHub CSV
    url = GITHUB_CSV_URLS.get(season)
    if url:
        log.warning(f"🌐 GitHub CSV fallback → {url}")
        try:
            df = pd.read_csv(url)
            df.to_csv(local_path, index=False)   # cache it locally
            return df
        except Exception as e:
            log.error(f"❌ GitHub CSV fallback failed: {e}")

    raise FileNotFoundError(f"No CSV fallback found for season {season}")


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main(season: int = EPL_SEASON, dry_run: bool = False):
    print("\n🚀 Layer 1 & 2 — Data Ingestion Pipeline")
    print("=" * 55)
    print(f"   League : EPL (ID={EPL_LEAGUE_ID})")
    print(f"   Season : {season}")
    print(f"   Mode   : {'DRY RUN (no DB write)' if dry_run else 'LIVE'}")
    print("=" * 55)

    validate_config()

    # Step 1: Try cache first (saves API quota)
    cached = load_from_cache(EPL_LEAGUE_ID, season)
    if cached:
        fixtures = cached
        log.info(f"✅ Using cached data ({len(fixtures)} fixtures)")
    else:
        # Step 2: Fetch from API-Football
        try:
            api = APIFootballClient(API_FOOTBALL_KEY)
            status_data = api.check_status()
            req_info = status_data.get("response", {}).get("requests", {})
            log.info(
                f"📊 Quota: {req_info.get('current',0)}"
                f"/{req_info.get('limit_day',100)} used today"
            )
            fixtures = api.fetch_fixtures(EPL_LEAGUE_ID, season)

        except Exception as e:
            log.error(f"❌ API fetch failed: {e}")
            log.warning("⚠️  Falling back to CSV backup...")
            try:
                return load_from_csv_fallback(season)
            except FileNotFoundError as fe:
                log.error(f"❌ CSV fallback also failed: {fe}")
                print("\n❌ Both API and CSV fallback unavailable.")
                print("   Fix: change EPL_SEASON in .env to 2022, 2023, or 2024")
                return None

    # Step 3: Validate (DQ-001 → DQ-010)
    print("\n🔍 Running data quality checks...")
    df, stats = validate_and_parse(fixtures, season)

    # DQ-010: partial matchweek guard
    if "league_round" in df.columns:
        thin = df.groupby("league_round").size()
        thin = thin[thin < 8]
        if not thin.empty:
            log.warning(f"DQ-010: {len(thin)} rounds have <8 matches: {thin.index.tolist()}")

    print(f"\n  ✅ {stats['passed']} rows passed")
    print(f"  ⚠️  {stats['warned']} rows warned (kept)")
    print(f"  ❌ {stats['dropped']} rows dropped")

    # Step 4: CSV backup (always — even before Supabase)
    csv_path = DATA_RAW_DIR / f"epl_raw_{EPL_LEAGUE_ID}_{season}.csv"
    df.to_csv(csv_path, index=False)
    log.info(f"💾 CSV backup saved → {csv_path}")

    if dry_run:
        print("\n✅ DRY RUN complete — Supabase write skipped")
        print(df.head(3).to_string(index=False))
        return df

    # Step 5: Write to Supabase
    print("\n🗄️  Writing to Supabase...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    written  = write_to_supabase(df, "matches", supabase)

    print(f"\n✅ Pipeline complete:")
    print(f"   Fetched  : {stats['total']} fixtures")
    print(f"   Passed   : {stats['passed']} rows")
    print(f"   Written  : {written} rows → Supabase.matches")
    print(f"   CSV      : {csv_path}")

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPL Data Ingestion")
    parser.add_argument("--season",  type=int, default=EPL_SEASON)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(season=args.season, dry_run=args.dry_run)

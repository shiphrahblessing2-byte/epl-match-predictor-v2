"""
Layer 2 — Data Quality Validation (DQ-001 → DQ-010)
Called automatically by api_client.py via import.
"""
import logging
from datetime import datetime, timezone

import pandas as pd

log = logging.getLogger(__name__)

KNOWN_EPL_TEAM_IDS = {
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
    43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
    55, 66, 71, 85, 529, 715, 741, 762, 777,
    2284, 2285, 2288,
}


def validate_fixture(fix: dict, season: int) -> tuple[bool, list[str]]:
    errors   = []
    f   = fix.get("fixture", {})
    t   = fix.get("teams",   {})
    g   = fix.get("goals",   {})
    lea = fix.get("league",  {})

    if not f.get("id"):
        errors.append("DQ-001: null fixture_id")

    try:
        datetime.fromisoformat(f.get("date", "").replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        errors.append("DQ-002: invalid match_date")

    home_g = g.get("home")
    away_g = g.get("away")
    if home_g is None or away_g is None:
        errors.append("DQ-003: missing goals")
    elif not (isinstance(home_g, int) and isinstance(away_g, int)
              and home_g >= 0 and away_g >= 0):
        errors.append("DQ-003: goals not non-negative integers")

    status = f.get("status", {}).get("short", "")
    if status != "FT":
        errors.append(f"DQ-005: status={status} not FT")

    warnings = []
    home_id = t.get("home", {}).get("id")
    away_id = t.get("away", {}).get("id")
    if home_id and home_id not in KNOWN_EPL_TEAM_IDS:
        warnings.append(f"DQ-007: unknown home_team_id={home_id}")
    if away_id and away_id not in KNOWN_EPL_TEAM_IDS:
        warnings.append(f"DQ-007: unknown away_team_id={away_id}")

    try:
        match_dt = datetime.fromisoformat(f.get("date", "").replace("Z", "+00:00"))
        if match_dt > datetime.now(timezone.utc):
            errors.append("DQ-008: match_date is in the future")
    except (ValueError, AttributeError):
        pass

    if lea.get("season") != season:
        errors.append(f"DQ-009: season mismatch {lea.get('season')} != {season}")

    return len(errors) == 0, errors + warnings


def parse_fixture(fix: dict) -> dict:
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
    """Run all DQ checks — returns (clean_df, stats)."""
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

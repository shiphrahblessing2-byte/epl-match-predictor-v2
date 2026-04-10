"""
Layer 3 — Feature Engineering
Reads from Supabase matches table, computes features,
writes to Supabase matches_features table.

Usage:
    python3 src/feature_engineer.py            # all seasons
    python3 src/feature_engineer.py --dry-run  # compute only, no DB write
    python3 src/feature_engineer.py --season 2024
"""
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from supabase import create_client

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    SUPABASE_URL,
    SUPABASE_KEY,
    EPL_SEASON,
    DATA_INTERIM_DIR,
    validate_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("feature_engineer.log"),
    ],
)
log = logging.getLogger(__name__)

ROLLING_WINDOW = 5   # last 5 matches for rolling stats


# ══════════════════════════════════════════════════════════
# LOAD DATA FROM SUPABASE
# ══════════════════════════════════════════════════════════

def load_matches(client) -> pd.DataFrame:
    log.info("📥 Loading matches from Supabase...")
    all_rows = []
    page     = 0
    size     = 1000

    while True:
        resp = (
            client.table("matches")
            .select("*")
            .order("match_date", desc=False)
            .range(page * size, (page + 1) * size - 1)
            .execute()
        )
        batch = resp.data
        all_rows.extend(batch)
        log.info(f"  Page {page + 1}: {len(batch)} rows fetched")
        if len(batch) < size:
            break
        page += 1

    df = pd.DataFrame(all_rows)
    df["match_date"] = pd.to_datetime(df["match_date"], utc=True)
    log.info(f"✅ Loaded {len(df)} matches across seasons: {sorted(df['season'].unique())}")
    return df


# ══════════════════════════════════════════════════════════
# FEATURE FUNCTIONS
# ══════════════════════════════════════════════════════════

def rolling_averages(team_df: pd.DataFrame, window: int = ROLLING_WINDOW) -> pd.DataFrame:
    """
    Compute rolling stats for a single team's matches.
    closed='left' ensures we NEVER include the current match — only past matches.
    This is the most critical rule to prevent data leakage.
    """
    team_df = team_df.sort_values("match_date").copy()

    team_df["goals_scored_rolling"]   = (
        team_df["goals_scored"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    team_df["goals_conceded_rolling"] = (
        team_df["goals_conceded"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    team_df["wins_rolling"]           = (
        team_df["win"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    team_df["clean_sheets_rolling"]   = (
        team_df["clean_sheet"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    return team_df


def win_streak(team_df: pd.DataFrame) -> pd.DataFrame:
    """
    Count consecutive wins BEFORE each match (not including current).
    Resets to 0 on draw or loss.
    """
    team_df = team_df.sort_values("match_date").copy()
    streaks = []
    current = 0
    for win in team_df["win"]:
        streaks.append(current)      # record streak BEFORE this match
        current = current + 1 if win == 1 else 0
    team_df["win_streak"] = streaks
    return team_df


def momentum(team_df: pd.DataFrame, window: int = ROLLING_WINDOW) -> pd.DataFrame:
    """
    Weighted recent form — more recent matches count more.
    Weights: [1, 2, 3, 4, 5] so last match = weight 5.
    closed='left' — no data leakage.
    """
    team_df   = team_df.sort_values("match_date").copy()
    weights   = list(range(1, window + 1))   # [1, 2, 3, 4, 5]
    form_vals = []

    wins = team_df["win"].tolist()
    for i in range(len(wins)):
        past = wins[max(0, i - window):i]    # up to 5 results BEFORE current
        if not past:
            form_vals.append(0.0)
        else:
            w = weights[-len(past):]         # align weights to available history
            form_vals.append(
                sum(p * wt for p, wt in zip(past, w)) / sum(w)
            )

    team_df["form_momentum"] = form_vals
    return team_df


# ✅ FIX — add league_key param
def h2h_win_rate(df: pd.DataFrame, home_id: int, away_id: int,
                 before_date: pd.Timestamp, league_key: str = None) -> float:
    mask = (
        (
            ((df["home_team_id"] == home_id) & (df["away_team_id"] == away_id)) |
            ((df["home_team_id"] == away_id) & (df["away_team_id"] == home_id))
        )
        & (df["match_date"] < before_date)
    )
    if league_key:
        mask = mask & (df["league_key"] == league_key)   # ✅ same-league H2H only
    h2h = df[mask]
    if h2h.empty:
        return 0.5
    wins = 0
    for _, row in h2h.iterrows():
        if row["home_team_id"] == home_id and row["result"] == 2:
            wins += 1
        elif row["away_team_id"] == home_id and row["result"] == 0:
            wins += 1
    return round(wins / len(h2h), 4)


def encode_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    venue_code : 1=home, 0=away
    opp_code   : integer ID of the opponent
    hour       : kick-off hour (UTC)
    day_code   : 0=Mon … 6=Sun
    """
    df["venue_code"] = 1   # always building from home team's perspective
    df["opp_code"]   = df["away_team_id"].astype(int)
    df["hour"]       = df["match_date"].dt.hour.astype("Int8")
    df["day_code"]   = df["match_date"].dt.dayofweek.astype("Int8")
    return df


# ══════════════════════════════════════════════════════════
# BUILD TEAM-LEVEL VIEW
# ══════════════════════════════════════════════════════════

def build_team_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand each match into TWO rows — one for each team
    so rolling stats can be computed per-team chronologically.
    """
    home_rows = df.rename(columns={
        "home_team_id": "team_id",
        "away_team_id": "opp_id",
        "home_goals":   "goals_scored",
        "away_goals":   "goals_conceded",
    }).copy()
    home_rows["venue"]       = "home"
    home_rows["win"]         = (home_rows["result"] == 2).astype(int)
    home_rows["clean_sheet"] = (home_rows["goals_conceded"] == 0).astype(int)

    away_rows = df.rename(columns={
        "away_team_id": "team_id",
        "home_team_id": "opp_id",
        "away_goals":   "goals_scored",
        "home_goals":   "goals_conceded",
    }).copy()
    away_rows["venue"]       = "away"
    away_rows["win"]         = (away_rows["result"] == 0).astype(int)
    away_rows["clean_sheet"] = (away_rows["goals_conceded"] == 0).astype(int)

    combined = pd.concat([home_rows, away_rows], ignore_index=True)
    return combined.sort_values("match_date")


# ══════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full feature engineering pipeline.
    Returns one row per fixture_id with all features.
    """
    log.info("⚙️  Building team-level rows...")
    team_rows = build_team_rows(df)

    # ✅ FIX — groups by league_key + team_id (no cross-league contamination)
    all_team_leagues = team_rows[["league_key", "team_id"]].drop_duplicates()
    log.info(f"⚙️  Computing rolling stats for {len(all_team_leagues)} team-league combos...")
    processed = []
    for _, grp in all_team_leagues.iterrows():
        t = team_rows[
            (team_rows["team_id"] == grp["team_id"]) &
            (team_rows["league_key"] == grp["league_key"])
        ].copy()
        t = rolling_averages(t)
        t = win_streak(t)
        t = momentum(t)
        processed.append(t)

    enriched = pd.concat(processed, ignore_index=True).sort_values("match_date")

    # Separate home and away enriched rows
    home_feats = enriched[enriched["venue"] == "home"][[
        "fixture_id", "team_id", "opp_id", "match_date",
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "win_streak", "form_momentum",
    ]].copy()
    home_feats.columns = [
        "fixture_id", "home_team_id", "away_team_id", "match_date",
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "win_streak", "form_momentum",
    ]

    away_feats = enriched[enriched["venue"] == "away"][[
        "fixture_id", "team_id",
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "win_streak", "form_momentum",
    ]].copy()
    away_feats.columns = [
        "fixture_id", "away_team_id",
        "opp_goals_scored_rolling", "opp_goals_conceded_rolling",
        "opp_wins_rolling", "opp_clean_sheets_rolling",
        "opp_win_streak", "opp_form_momentum",
    ]

    # Merge home + away features into one row per fixture
    features = home_feats.merge(away_feats, on=["fixture_id", "away_team_id"])

    # Add categorical encodings
    features = encode_categories(features)

    # ✅ FIX — include league_key in merge
    features = features.merge(
        df[["fixture_id", "season", "league_key"]], on="fixture_id"
    )

    # Add H2H win rate
    log.info("⚙️  Computing H2H win rates...")
    features["h2h_win_rate"] = features.apply(
        lambda row: h2h_win_rate(
            df,
            row["home_team_id"],
            row["away_team_id"],
            row["match_date"],
            row["league_key"],   # ✅ same-league H2H only
        ),
        axis=1,
    )

    # Final column selection
    output_cols = [
        "fixture_id", "home_team_id", "away_team_id", "season",
        "league_key",
        "venue_code", "opp_code", "hour", "day_code",
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "win_streak", "form_momentum",
        "opp_goals_scored_rolling", "opp_goals_conceded_rolling",
        "opp_wins_rolling", "opp_clean_sheets_rolling",
        "opp_win_streak", "opp_form_momentum",
        "h2h_win_rate",
    ]

    # ✅ FIX — round numeric columns only (league_key is string)
    features = features[output_cols]
    numeric_cols = features.select_dtypes(include="number").columns
    features[numeric_cols] = features[numeric_cols].round(4)

    # Fill NaN rolling stats with league average defaults
    rolling_cols = [
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "opp_goals_scored_rolling", "opp_goals_conceded_rolling",
        "opp_wins_rolling", "opp_clean_sheets_rolling",
        "opp_win_streak", "opp_form_momentum",
    ]
    for col in rolling_cols:
        features[col] = features[col].fillna(features[col].mean())

    # Fill remaining NaN with 0
    features = features.fillna(0)

    log.info(f"✅ Features built: {len(features)} rows × {len(output_cols)} columns")
    return features


def write_features(df: pd.DataFrame, client) -> int:
    """Upsert feature rows to Supabase in batches of 100."""
    rows    = df.to_dict(orient="records")
    written = 0
    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        client.table("matches_features").upsert(
            batch, on_conflict="fixture_id"
        ).execute()
        written += len(batch)
        log.info(f"  Batch {i//100 + 1}: {len(batch)} rows → matches_features")
    log.info(f"✅ Written {written} rows → Supabase.matches_features")
    return written


def main(season: int | None = None, dry_run: bool = False):
    print("\n🚀 Layer 3 — Feature Engineering Pipeline")
    print("=" * 55)
    print(f"   Season : {'ALL' if season is None else season}")
    print(f"   Window : Last {ROLLING_WINDOW} matches (rolling)")
    print(f"   Mode   : {'DRY RUN (no DB write)' if dry_run else 'LIVE'}")
    print("=" * 55)

    validate_config()
    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Load all seasons (need full history for H2H + rolling across seasons)
    df = load_matches(client)

    # Build features on ALL data (rolling needs full history)
    features = build_features(df)

    # Filter output to requested season if specified
    if season:
        features_out = features[features["season"] == season]
        log.info(f"📊 Filtered to season {season}: {len(features_out)} rows")
    else:
        features_out = features

    # Save interim CSV
    csv_path = DATA_INTERIM_DIR / "matches_features.csv"
    features_out.to_csv(csv_path, index=False)
    log.info(f"💾 CSV saved → {csv_path}")

    print(f"\n📊 Sample output:")
    print(features_out.head(3).to_string(index=False))

    if dry_run:
        print("\n✅ DRY RUN complete — Supabase write skipped")
        return features_out

    print("\n🗄️  Writing to Supabase...")
    written = write_features(features_out, client)

    print(f"\n✅ Pipeline complete:")
    print(f"   Fixtures processed : {len(features_out)}")
    print(f"   Written            : {written} rows → Supabase.matches_features")
    print(f"   CSV                : {csv_path}")
    return features_out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPL Feature Engineering")
    parser.add_argument("--season",  type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(season=args.season, dry_run=args.dry_run)
